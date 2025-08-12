# Let's create a reusable Python module `lca_pipeline.py` that you can download and run.
# It reads your workbook, computes each U_i_Midpoint from the non-U_*_Midpoint sheets and the
# "Unit process & Utilities" coefficients, and (optionally) writes the results back into the workbook.
#
# Requirements: pandas, openpyxl
#
# Usage example (after download):
#   python -c "import lca_pipeline as lca; lca.run_lca('20250408-dsRNA in vitro synthesis-LCA calculation.xlsx')"
#
# Or from a notebook / Python REPL:
#   import lca_pipeline as lca
#   results = lca.run_lca('20250408-dsRNA in vitro synthesis-LCA calculation.xlsx', write_back=True)
#   # 'results' is a dict: {'U_1_Midpoint': df, ...}
#
import os
import shutil
from typing import Dict, List, Optional, Tuple

import pandas as pd

KEY_COLS = ["Time Frame", "SSPs", "RCPs", "Impact Categories"]

def _get_unit_sheet(xls: pd.ExcelFile) -> pd.DataFrame:
    """Return the 'Unit process & Utilities' sheet without headers."""
    df = xls.parse(sheet_name="Unit process & Utilities", header=None)
    print(f"Loaded 'Unit process & Utilities' with shape {df.shape}")
    return df

def _discover_u_columns(unit_df: pd.DataFrame) -> Dict[str, int]:
    """
    Return a mapping like {'U_1': 2, 'U_2': 3, ...} from the header row (row 0).
    """
    ucols = {}
    for j, val in unit_df.iloc[0].items():
        if isinstance(val, str) and val.startswith("U_"):
            ucols[val] = j
    return dict(sorted(ucols.items(), key=lambda kv: int(kv[0].split("_")[1])))

def _find_row_indices(unit_df: pd.DataFrame, prefix: str) -> Dict[str, int]:
    """
    Find rows whose first cell starts with the given prefix, e.g., 'M_', 'W_', 'E_', 'T_'.
    Returns a mapping like {'M_1': row_index, ...}
    """
    out = {}
    for i, val in unit_df.iloc[:, 0].items():
        if isinstance(val, str) and val.startswith(prefix):
            out[val] = i
    # Sort by numeric suffix if possible
    def suffix(v: str) -> Tuple:
        # For T_ and E_ can be like T_1, E_10, etc.
        try:
            return (int(v.split("_")[1]),)
        except Exception:
            return (v,)
    return dict(sorted(out.items(), key=lambda kv: suffix(kv[0])))

def _get_coeff_vector(unit_df: pd.DataFrame, u_col: int, index_map: Dict[str, int]) -> Dict[str, float]:
    coeffs = {}
    for k, row in index_map.items():
        val = unit_df.iat[row, u_col]
        coeffs[k] = 0.0 if pd.isna(val) else float(val)
    return coeffs

def _read_sheet(xls: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    df = xls.parse(sheet_name=sheet)
    print(f"Loaded sheet '{sheet}' with shape {df.shape}")
    return df

def _ensure_same_index(frames: List[pd.DataFrame]) -> pd.MultiIndex:
    """
    Ensure all frames contain the same KEY_COLS and return a multiindex built from them.
    Throws if keys mismatch.
    """
    keys = None
    for f in frames:
        if not all(k in f.columns for k in KEY_COLS):
            raise ValueError(f"Sheet missing key columns {KEY_COLS}: has {f.columns.tolist()}")
        idx = pd.MultiIndex.from_frame(f[KEY_COLS])
        if keys is None:
            keys = idx
        else:
            if len(idx) != len(keys) or not idx.equals(keys):
                # Try to align by sorting
                keys = None
                break
    if keys is None:
        # Build a union index from all frames then reindex each later
        all_idx = None
        for f in frames:
            idx = pd.MultiIndex.from_frame(f[KEY_COLS])
            all_idx = idx if all_idx is None else all_idx.union(idx)
        return all_idx
    return keys

def _to_indexed_series(df: pd.DataFrame, value_col: str) -> pd.Series:
    s = df.set_index(KEY_COLS)[value_col]
    # If duplicate keys exist, sum them
    if s.index.has_duplicates:
        s = s.groupby(level=list(range(s.index.nlevels))).sum()
    return s

def _sum_aligned(series_list: List[pd.Series]) -> pd.Series:
    """Return the elementwise sum of series, aligning on their indexes."""
    if not series_list:
        return pd.Series(dtype=float)
    if len(series_list) == 1:
        return series_list[0]
    return pd.concat(series_list, axis=1).fillna(0).sum(axis=1)

def _compute_materials(xls: pd.ExcelFile, m_coeffs: Dict[str, float]) -> pd.Series:
    # Each M_k sheet has 'Impacts per kg/m³'
    print(f"Computing materials with {len(m_coeffs)} coefficients")
    series_list = []
    for m_name, qty in m_coeffs.items():
        sheet = f"{m_name}_Midpoint"
        df = _read_sheet(xls, sheet)
        print(f"  material {m_name}: coeff {qty}, df shape {df.shape}")
        s = _to_indexed_series(df, "Impacts per kg/m³")
        series_list.append(s * qty)
    if not series_list:
        raise ValueError("No material sheets found.")
    # Align and sum
    out = _sum_aligned(series_list)
    print(f"Material impacts series length {len(out)}")
    return out

def _compute_waste(xls: pd.ExcelFile, w_coeffs: Dict[str, float]) -> pd.Series:
    print(f"Computing waste with {len(w_coeffs)} coefficients")
    series_list = []
    for w_name, qty in w_coeffs.items():
        sheet = f"{w_name}_Midpoint"
        df = _read_sheet(xls, sheet)
        print(f"  waste {w_name}: coeff {qty}, df shape {df.shape}")
        s = _to_indexed_series(df, "Impacts per kg/m³")
        series_list.append(s * qty)
    if not series_list:
        # If no waste items, return zero series built from a template
        # Use W_1_Midpoint as template if present, else zero later
        try:
            df = _read_sheet(xls, "W_1_Midpoint")
            template = _to_indexed_series(df, "Impacts per kg/m³")
            out = template * 0.0
            print("No waste coefficients; returning zeros with template")
            return out
        except Exception:
            raise ValueError("No waste sheets found and no template available.")
    out = _sum_aligned(series_list)
    print(f"Waste impacts series length {len(out)}")
    return out

def _compute_energy(xls: pd.ExcelFile, e_coeffs: Dict[str, float]) -> pd.Series:
    # Electricity, Heat, Steam each with 'Impacts per kWh'
    print("Computing energy use")
    parts = []
    for name in ("Electricity", "Heat", "Steam"):
        try:
            df = _read_sheet(xls, f"{name}_Midpoint")
            s = _to_indexed_series(df, "Impacts per kWh")
            qty = e_coeffs.get(name, 0.0)
            print(f"  energy {name}: coeff {qty}, df shape {df.shape}")
            parts.append(s * qty)
        except Exception as exc:
            print(f"  energy {name}: sheet missing ({exc})")
            # Sheet might be absent; treat as zero
            pass
    if not parts:
        # try to build a zero template from any of the energy sheets
        for name in ("Electricity", "Heat", "Steam"):
            try:
                df = _read_sheet(xls, f"{name}_Midpoint")
                tmpl = _to_indexed_series(df, "Impacts per kWh")
                print("No energy coefficients; returning zeros with template")
                return tmpl * 0.0
            except Exception:
                continue
        raise ValueError("No energy sheets found.")
    out = _sum_aligned(parts)
    print(f"Energy impacts series length {len(out)}")
    return out

def _compute_transport(xls: pd.ExcelFile, t_coeffs: Dict[str, float]) -> pd.Series:
    # Transportation_Midpoint has columns T_1, T_2, ... (if more added later)
    print(f"Computing transport with coeffs: {t_coeffs}")
    df = _read_sheet(xls, "Transportation_Midpoint")
    df = df.copy()
    # Keep only T_* columns present
    t_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("T_")]
    if not t_cols:
        print("No transport columns found; returning zeros")
        # Build zero template using any sheet previously seen: fallback to electricity or a materials sheet
        try:
            tmpl = _to_indexed_series(_read_sheet(xls, "Electricity_Midpoint"), "Impacts per kWh")
            return tmpl * 0.0
        except Exception:
            # Final fallback: first materials sheet
            m_sheet = next((s for s in xls.sheet_names if s.startswith("M_") and s.endswith("_Midpoint")), None)
            if m_sheet is None:
                raise ValueError("Cannot construct transport template; no suitable sheet found.")
            tmpl = _to_indexed_series(_read_sheet(xls, m_sheet), "Impacts per kg/m³")
            return tmpl * 0.0
    print(f"  transport columns used: {t_cols}")
    # Build rowwise dot product
    # Align index first
    base_idx = pd.MultiIndex.from_frame(df[KEY_COLS])
    vals = pd.DataFrame({c: _to_indexed_series(df, c).reindex(base_idx) for c in t_cols})
    # Quantity vector for these columns
    weights = pd.Series({c: float(t_coeffs.get(c, 0.0)) for c in t_cols})
    out = (vals * weights).sum(axis=1)
    out.index = base_idx
    print(f"Transport impacts series length {len(out)}")
    return out

def _compute_emissions(xls: pd.ExcelFile, e_coeffs: Dict[str, float]) -> pd.Series:
    # Emissions_Midpoint has columns E_1 .. E_n
    print(f"Computing emissions with {len(e_coeffs)} coefficients")
    df = _read_sheet(xls, "Emissions_Midpoint")
    df = df.copy()
    e_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("E_")]
    if not e_cols:
        # Build zero template
        try:
            tmpl = _to_indexed_series(_read_sheet(xls, "Electricity_Midpoint"), "Impacts per kWh")
            return tmpl * 0.0
        except Exception:
            m_sheet = next((s for s in xls.sheet_names if s.startswith("M_") and s.endswith("_Midpoint")), None)
            if m_sheet is None:
                raise ValueError("Cannot construct emissions template; no suitable sheet found.")
            tmpl = _to_indexed_series(_read_sheet(xls, m_sheet), "Impacts per kg/m³")
            return tmpl * 0.0
    print(f"  emission columns used: {e_cols}")
    base_idx = pd.MultiIndex.from_frame(df[KEY_COLS])
    vals = pd.DataFrame({c: _to_indexed_series(df, c).reindex(base_idx) for c in e_cols})
    # Quantity vector for these columns
    weights = pd.Series({c: float(e_coeffs.get(c, 0.0)) for c in e_cols})
    out = (vals * weights).sum(axis=1)
    out.index = base_idx
    print(f"Emission impacts series length {len(out)}")
    return out


def _assemble_ui_frame(material: pd.Series,
                       waste: pd.Series,
                       transport: pd.Series,
                       energy: pd.Series,
                       emission: pd.Series) -> pd.DataFrame:
    # Build a full index (union) and reindex all series to it
    all_idx = material.index.union(waste.index).union(transport.index).union(energy.index).union(emission.index)
    print(f"Assembling UI frame with {len(all_idx)} rows")
    def align(s: pd.Series) -> pd.Series:
        return s.reindex(all_idx, fill_value=0.0)
    cols = {
        "Material consumption": align(material),
        "Waste": align(waste),
        "Transportation": align(transport),
        "Energy use": align(energy),
        "Emission": align(emission),
    }
    df = pd.DataFrame(cols)
    df.insert(0, "Impact Categories", [ix[-1] for ix in df.index])  # quick add then will overwrite with full keys
    # Build a DataFrame with KEY_COLS from index
    keys_df = pd.DataFrame(list(all_idx), columns=KEY_COLS)
    # Ensure ordering matches idx
    df = pd.concat([keys_df.reset_index(drop=True), df.reset_index(drop=True)], axis=1)
    # Total
    df["Total Impacts per FU"] = df[["Material consumption","Waste","Transportation","Energy use","Emission"]].sum(axis=1)
    # Column order
    ordered = KEY_COLS + ["Total Impacts per FU", "Material consumption","Waste","Transportation","Energy use","Emission"]
    df = df[ordered]
    print(f"Assembled UI frame shape {df.shape}")
    return df

def compute_ui_midpoint(xls: pd.ExcelFile, ui_name: str) -> pd.DataFrame:
    """
    Compute the Ui_Midpoint table for one U (e.g., 'U_1').
    Returns a DataFrame with columns KEY_COLS + five subcategories + total.
    """
    unit_df = _get_unit_sheet(xls)
    u_cols = _discover_u_columns(unit_df)
    if ui_name not in u_cols:
        raise ValueError(f"{ui_name} not found in 'Unit process & Utilities' header. Found: {list(u_cols)}")
    u_col = u_cols[ui_name]

    print(f"\n=== Computing {ui_name}_Midpoint ===")

    # Collect coefficient maps
    m_rows = _find_row_indices(unit_df, "M_")
    w_rows = _find_row_indices(unit_df, "W_")
    t_rows = _find_row_indices(unit_df, "T_")
    e_rows = _find_row_indices(unit_df, "E_")

    m_coeffs = _get_coeff_vector(unit_df, u_col, m_rows)
    w_coeffs = _get_coeff_vector(unit_df, u_col, w_rows)
    t_coeffs = _get_coeff_vector(unit_df, u_col, t_rows)
    e_coeffs = _get_coeff_vector(unit_df, u_col, e_rows)

    print(f"Material coeffs: {m_coeffs}")
    print(f"Waste coeffs: {w_coeffs}")
    print(f"Transport coeffs: {t_coeffs}")
    print(f"Emission coeffs: {e_coeffs}")

    def _energy_coeff(name: str) -> float:
        rows = _find_row_indices(unit_df, name)
        row = rows.get(name)
        if row is None:
            return 0.0
        val = unit_df.iat[row, u_col]
        return 0.0 if pd.isna(val) else float(val)

    energy_coeffs = {n: _energy_coeff(n) for n in ("Electricity", "Heat", "Steam")}
    print(f"Energy coeffs: {energy_coeffs}")

    material = _compute_materials(xls, m_coeffs)
    waste = _compute_waste(xls, w_coeffs)
    transport = _compute_transport(xls, t_coeffs)
    energy = _compute_energy(xls, energy_coeffs).fillna(0.0)
    emission = _compute_emissions(xls, e_coeffs)

    ui_df = _assemble_ui_frame(material, waste, transport, energy, emission)
    print(f"Finished {ui_name}: result shape {ui_df.shape}")
    return ui_df

def run_lca(path: str, ui_list: Optional[List[str]] = None, write_back: bool = True, make_backup: bool = True) -> Dict[str, pd.DataFrame]:
    """Main entry point. Computes Ui_Midpoint for selected U_* and optionally writes back."""
    print(f"Opening workbook: {path}")
    xls = pd.ExcelFile(path)
    unit_df = _get_unit_sheet(xls)
    u_cols = _discover_u_columns(unit_df)
    if not u_cols:
        raise ValueError("No U_* columns discovered in 'Unit process & Utilities'.")
    chosen = ui_list if ui_list is not None else list(u_cols.keys())
    print(f"U columns discovered: {list(u_cols.keys())}")
    print(f"Processing units: {chosen}")

    results: Dict[str, pd.DataFrame] = {}
    for ui in chosen:
        print(f"\nProcessing {ui}...")
        df = compute_ui_midpoint(xls, ui)
        results[f"{ui}_Midpoint"] = df
        print(f"Stored {ui}_Midpoint with shape {df.shape}")

    if write_back:
        if make_backup:
            backup_path = path + ".bak"
            shutil.copy2(path, backup_path)
            print(f"Backup written to {backup_path}")
        print("Writing results back to workbook")
        with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            for sheet_name, df in results.items():
                print(f"  Writing sheet {sheet_name} with shape {df.shape}")
                df.to_excel(writer, sheet_name=sheet_name, index=False)
    print("run_lca completed")
    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compute U_i midpoint tables for the provided workbook")
    parser.add_argument("workbook", help="Path to the Excel workbook")
    parser.add_argument("--ui", nargs="*", default=None, help="Specific U_i names to compute (default: all)")
    parser.add_argument("--no-write", action="store_true", help="Do not write results back to the workbook")
    args = parser.parse_args()

    run_lca(args.workbook, ui_list=args.ui, write_back=not args.no_write)

# # Save the module so you can download it.
# with open("/mnt/data/lca_pipeline.py", "w", encoding="utf-8") as f:
#     import inspect
#     f.write(inspect.getsource(_get_unit_sheet))


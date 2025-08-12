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
    print("[DEBUG] Reading 'Unit process & Utilities' sheet")
    df = xls.parse(sheet_name="Unit process & Utilities", header=None)
    print(f"[DEBUG] Unit sheet shape: {df.shape}")
    return df

def _discover_u_columns(unit_df: pd.DataFrame) -> Dict[str, int]:
    """
    Return a mapping like {'U_1': 2, 'U_2': 3, ...} from the header row (row 0).
    """
    print("[DEBUG] Discovering U columns in unit sheet header")
    ucols = {}
    for j, val in unit_df.iloc[0].items():
        if isinstance(val, str) and val.startswith("U_"):
            ucols[val] = j
            print(f"[DEBUG] Found U column {val} at position {j}")
    sorted_ucols = dict(sorted(ucols.items(), key=lambda kv: int(kv[0].split("_")[1])))
    print(f"[DEBUG] Discovered U columns: {sorted_ucols}")
    return sorted_ucols

def _find_row_indices(unit_df: pd.DataFrame, prefix: str) -> Dict[str, int]:
    """
    Find rows whose first cell starts with the given prefix, e.g., 'M_', 'W_', 'E_', 'T_'.
    Returns a mapping like {'M_1': row_index, ...}
    """
    print(f"[DEBUG] Searching for rows starting with '{prefix}'")
    out = {}
    for i, val in unit_df.iloc[:, 0].items():
        if isinstance(val, str) and val.startswith(prefix):
            out[val] = i
            print(f"[DEBUG] Found row {i} for key {val}")
    # Sort by numeric suffix if possible
    def suffix(v: str) -> Tuple:
        # For T_ and E_ can be like T_1, E_10, etc.
        try:
            return (int(v.split("_")[1]),)
        except Exception:
            return (v,)
    sorted_out = dict(sorted(out.items(), key=lambda kv: suffix(kv[0])))
    print(f"[DEBUG] Row indices for prefix '{prefix}': {sorted_out}")
    return sorted_out

def _get_coeff_vector(unit_df: pd.DataFrame, u_col: int, index_map: Dict[str, int]) -> Dict[str, float]:
    print(f"[DEBUG] Building coefficient vector for column index {u_col}")
    coeffs = {}
    for k, row in index_map.items():
        val = unit_df.iat[row, u_col]
        coeffs[k] = 0.0 if pd.isna(val) else float(val)
        print(f"[DEBUG] Coefficient for {k}: {coeffs[k]}")
    print(f"[DEBUG] Coefficient vector: {coeffs}")
    return coeffs

def _read_sheet(xls: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    print(f"[DEBUG] Reading sheet '{sheet}'")
    df = xls.parse(sheet_name=sheet)
    print(f"[DEBUG] Sheet '{sheet}' shape: {df.shape}")
    return df

def _ensure_same_index(frames: List[pd.DataFrame]) -> pd.MultiIndex:
    """
    Ensure all frames contain the same KEY_COLS and return a multiindex built from them.
    Throws if keys mismatch.
    """
    print(f"[DEBUG] Ensuring same index for {len(frames)} frames")
    keys = None
    for f in frames:
        if not all(k in f.columns for k in KEY_COLS):
            raise ValueError(f"Sheet missing key columns {KEY_COLS}: has {f.columns.tolist()}")
        idx = pd.MultiIndex.from_frame(f[KEY_COLS])
        if keys is None:
            keys = idx
        else:
            if len(idx) != len(keys) or not idx.equals(keys):
                print("[DEBUG] Index mismatch detected; will build union index")
                keys = None
                break
    if keys is None:
        all_idx = None
        for f in frames:
            idx = pd.MultiIndex.from_frame(f[KEY_COLS])
            all_idx = idx if all_idx is None else all_idx.union(idx)
        print(f"[DEBUG] Union index length: {len(all_idx)}")
        return all_idx
    print(f"[DEBUG] Shared index length: {len(keys)}")
    return keys

def _to_indexed_series(df: pd.DataFrame, value_col: str) -> pd.Series:
    print(f"[DEBUG] Converting DataFrame with shape {df.shape} to Series using column '{value_col}'")
    s = df.set_index(KEY_COLS)[value_col]
    if s.index.has_duplicates:
        print("[DEBUG] Duplicate index entries found; aggregating by sum")
        s = s.groupby(level=list(range(s.index.nlevels))).sum()
    print(f"[DEBUG] Resulting series length: {len(s)}")
    return s

def _sum_aligned(series_list: List[pd.Series]) -> pd.Series:
    """Return the elementwise sum of series, aligning on their indexes."""
    print(f"[DEBUG] Summing {len(series_list)} series")
    if not series_list:
        print("[DEBUG] No series to sum; returning empty")
        return pd.Series(dtype=float)
    if len(series_list) == 1:
        print("[DEBUG] Only one series provided; returning it directly")
        return series_list[0]
    result = pd.concat(series_list, axis=1).fillna(0).sum(axis=1)
    print(f"[DEBUG] Summed series length: {len(result)}")
    return result

def _compute_materials(xls: pd.ExcelFile, m_coeffs: Dict[str, float]) -> pd.Series:
    print(f"[DEBUG] Computing materials with coefficients: {m_coeffs}")
    series_list = []
    for m_name, qty in m_coeffs.items():
        sheet = f"{m_name}_Midpoint"
        print(f"[DEBUG] Processing material sheet '{sheet}' with quantity {qty}")
        df = _read_sheet(xls, sheet)
        s = _to_indexed_series(df, "Impacts per kg/m³")
        print(f"[DEBUG] Material series length for {m_name}: {len(s)}")
        series_list.append(s * qty)
    if not series_list:
        print("[DEBUG] No material sheets found.")
        raise ValueError("No material sheets found.")
    result = _sum_aligned(series_list)
    print(f"[DEBUG] Combined material series length: {len(result)}")
    return result

def _compute_waste(xls: pd.ExcelFile, w_coeffs: Dict[str, float]) -> pd.Series:
    print(f"[DEBUG] Computing waste with coefficients: {w_coeffs}")
    series_list = []
    for w_name, qty in w_coeffs.items():
        sheet = f"{w_name}_Midpoint"
        print(f"[DEBUG] Processing waste sheet '{sheet}' with quantity {qty}")
        df = _read_sheet(xls, sheet)
        s = _to_indexed_series(df, "Impacts per kg/m³")
        print(f"[DEBUG] Waste series length for {w_name}: {len(s)}")
        series_list.append(s * qty)
    if not series_list:
        print("[DEBUG] No waste coefficients provided; attempting template")
        try:
            df = _read_sheet(xls, "W_1_Midpoint")
            template = _to_indexed_series(df, "Impacts per kg/m³")
            print("[DEBUG] Using W_1_Midpoint as zero template")
            return template * 0.0
        except Exception:
            print("[DEBUG] No waste sheets found and no template available")
            raise ValueError("No waste sheets found and no template available.")
    result = _sum_aligned(series_list)
    print(f"[DEBUG] Combined waste series length: {len(result)}")
    return result

def _compute_energy(xls: pd.ExcelFile, e_coeffs: Dict[str, float]) -> pd.Series:
    print(f"[DEBUG] Computing energy with coefficients: {e_coeffs}")
    parts = []
    for name in ("Electricity", "Heat", "Steam"):
        try:
            df = _read_sheet(xls, f"{name}_Midpoint")
            s = _to_indexed_series(df, "Impacts per kWh")
            qty = e_coeffs.get(name, 0.0)
            print(f"[DEBUG] Energy component {name}: qty={qty}, series length={len(s)}")
            parts.append(s * qty)
        except Exception:
            print(f"[DEBUG] Energy sheet for {name} not found; assuming zero")
            pass
    if not parts:
        print("[DEBUG] No energy parts computed; attempting zero template")
        for name in ("Electricity", "Heat", "Steam"):
            try:
                df = _read_sheet(xls, f"{name}_Midpoint")
                tmpl = _to_indexed_series(df, "Impacts per kWh")
                print(f"[DEBUG] Using {name}_Midpoint as zero template")
                return tmpl * 0.0
            except Exception:
                continue
        print("[DEBUG] No energy sheets found at all")
        raise ValueError("No energy sheets found.")
    result = _sum_aligned(parts)
    print(f"[DEBUG] Combined energy series length: {len(result)}")
    return result

def _compute_transport(xls: pd.ExcelFile, t_coeffs: Dict[str, float]) -> pd.Series:
    print(f"[DEBUG] Computing transport with coefficients: {t_coeffs}")
    df = _read_sheet(xls, "Transportation_Midpoint")
    df = df.copy()
    t_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("T_")]
    print(f"[DEBUG] Transport columns present: {t_cols}")
    if not t_cols:
        print("[DEBUG] No transport columns found; attempting zero template")
        try:
            tmpl = _to_indexed_series(_read_sheet(xls, "Electricity_Midpoint"), "Impacts per kWh")
            return tmpl * 0.0
        except Exception:
            m_sheet = next((s for s in xls.sheet_names if s.startswith("M_") and s.endswith("_Midpoint")), None)
            if m_sheet is None:
                print("[DEBUG] Cannot construct transport template; no suitable sheet found")
                raise ValueError("Cannot construct transport template; no suitable sheet found.")
            tmpl = _to_indexed_series(_read_sheet(xls, m_sheet), "Impacts per kg/m³")
            return tmpl * 0.0
    base_idx = pd.MultiIndex.from_frame(df[KEY_COLS])
    vals = pd.DataFrame({c: _to_indexed_series(df, c).reindex(base_idx) for c in t_cols})
    weights = pd.Series({c: float(t_coeffs.get(c, 0.0)) for c in t_cols})
    print(f"[DEBUG] Transport weights: {weights.to_dict()}")
    out = (vals * weights).sum(axis=1)
    out.index = base_idx
    print(f"[DEBUG] Transport series length: {len(out)}")
    return out

def _compute_emissions(xls: pd.ExcelFile, e_coeffs: Dict[str, float]) -> pd.Series:
    print(f"[DEBUG] Computing emissions with coefficients: {e_coeffs}")
    df = _read_sheet(xls, "Emissions_Midpoint")
    df = df.copy()
    e_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("E_")]
    print(f"[DEBUG] Emission columns present: {e_cols}")
    if not e_cols:
        print("[DEBUG] No emission columns found; attempting zero template")
        try:
            tmpl = _to_indexed_series(_read_sheet(xls, "Electricity_Midpoint"), "Impacts per kWh")
            return tmpl * 0.0
        except Exception:
            m_sheet = next((s for s in xls.sheet_names if s.startswith("M_") and s.endswith("_Midpoint")), None)
            if m_sheet is None:
                print("[DEBUG] Cannot construct emissions template; no suitable sheet found")
                raise ValueError("Cannot construct emissions template; no suitable sheet found.")
            tmpl = _to_indexed_series(_read_sheet(xls, m_sheet), "Impacts per kg/m³")
            return tmpl * 0.0
    base_idx = pd.MultiIndex.from_frame(df[KEY_COLS])
    vals = pd.DataFrame({c: _to_indexed_series(df, c).reindex(base_idx) for c in e_cols})
    weights = pd.Series({c: float(e_coeffs.get(c, 0.0)) for c in e_cols})
    print(f"[DEBUG] Emission weights: {weights.to_dict()}")
    out = (vals * weights).sum(axis=1)
    out.index = base_idx
    print(f"[DEBUG] Emission series length: {len(out)}")
    return out


def _assemble_ui_frame(material: pd.Series,
                       waste: pd.Series,
                       transport: pd.Series,
                       energy: pd.Series,
                       emission: pd.Series) -> pd.DataFrame:
    print("[DEBUG] Assembling UI frame from components")
    all_idx = material.index.union(waste.index).union(transport.index).union(energy.index).union(emission.index)
    print(f"[DEBUG] Combined index length: {len(all_idx)}")
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
    df.insert(0, "Impact Categories", [ix[-1] for ix in df.index])
    keys_df = pd.DataFrame(list(all_idx), columns=KEY_COLS)
    df = pd.concat([keys_df.reset_index(drop=True), df.reset_index(drop=True)], axis=1)
    df["Total Impacts per FU"] = df[["Material consumption","Waste","Transportation","Energy use","Emission"]].sum(axis=1)
    ordered = KEY_COLS + ["Total Impacts per FU", "Material consumption","Waste","Transportation","Energy use","Emission"]
    df = df[ordered]
    print(f"[DEBUG] Assembled UI DataFrame shape: {df.shape}")
    return df

def compute_ui_midpoint(xls: pd.ExcelFile, ui_name: str) -> pd.DataFrame:
    """
    Compute the Ui_Midpoint table for one U (e.g., 'U_1').
    Returns a DataFrame with columns KEY_COLS + five subcategories + total.
    """
    print(f"[DEBUG] Computing midpoint for {ui_name}")
    unit_df = _get_unit_sheet(xls)
    u_cols = _discover_u_columns(unit_df)
    if ui_name not in u_cols:
        print(f"[DEBUG] {ui_name} not found among columns {u_cols}")
        raise ValueError(f"{ui_name} not found in 'Unit process & Utilities' header. Found: {list(u_cols)}")
    u_col = u_cols[ui_name]
    print(f"[DEBUG] Using column index {u_col} for {ui_name}")

    m_rows = _find_row_indices(unit_df, "M_")
    w_rows = _find_row_indices(unit_df, "W_")
    t_rows = _find_row_indices(unit_df, "T_")
    e_rows = _find_row_indices(unit_df, "E_")

    m_coeffs = _get_coeff_vector(unit_df, u_col, m_rows)
    w_coeffs = _get_coeff_vector(unit_df, u_col, w_rows)
    t_coeffs = _get_coeff_vector(unit_df, u_col, t_rows)
    e_coeffs = _get_coeff_vector(unit_df, u_col, e_rows)
    print(f"[DEBUG] Coeff maps sizes: M={len(m_coeffs)}, W={len(w_coeffs)}, T={len(t_coeffs)}, E={len(e_coeffs)}")

    material = _compute_materials(xls, m_coeffs)
    waste = _compute_waste(xls, w_coeffs)
    transport = _compute_transport(xls, t_coeffs)
    energy_coeffs = {
        "Electricity": unit_df.iat[_find_row_indices(unit_df, "Electricity").get("Electricity", -1), u_col]
        if "Electricity" in unit_df.iloc[:,0].values else 0.0,
        "Heat": unit_df.iat[_find_row_indices(unit_df, "Heat").get("Heat", -1), u_col]
        if "Heat" in unit_df.iloc[:,0].values else 0.0,
        "Steam": unit_df.iat[_find_row_indices(unit_df, "Steam").get("Steam", -1), u_col]
        if "Steam" in unit_df.iloc[:,0].values else 0.0,
    }
    print(f"[DEBUG] Energy coefficients for {ui_name}: {energy_coeffs}")
    energy = _compute_energy(xls, energy_coeffs).fillna(0.0)

    emission = _compute_emissions(xls, e_coeffs)

    ui_df = _assemble_ui_frame(material, waste, transport, energy, emission)
    print(f"[DEBUG] Completed {ui_name}_Midpoint with shape {ui_df.shape}")
    return ui_df

def run_lca(path: str, ui_list: Optional[List[str]] = None, write_back: bool = True, make_backup: bool = True) -> Dict[str, pd.DataFrame]:
    """Main entry point. Computes Ui_Midpoint for selected U_* and optionally writes back."""
    print(f"[DEBUG] run_lca started for workbook '{path}'")
    xls = pd.ExcelFile(path)
    unit_df = _get_unit_sheet(xls)
    u_cols = _discover_u_columns(unit_df)
    if not u_cols:
        print("[DEBUG] No U_* columns discovered")
        raise ValueError("No U_* columns discovered in 'Unit process & Utilities'.")
    chosen = ui_list if ui_list is not None else list(u_cols.keys())
    print(f"[DEBUG] Will compute U columns: {chosen}")

    results: Dict[str, pd.DataFrame] = {}
    for ui in chosen:
        print(f"[DEBUG] Processing {ui}")
        df = compute_ui_midpoint(xls, ui)
        results[f"{ui}_Midpoint"] = df
        print(f"[DEBUG] {ui}_Midpoint shape: {df.shape}")

    if write_back:
        print(f"[DEBUG] Writing results back to workbook (backup={make_backup})")
        if make_backup:
            backup_path = path + ".bak"
            shutil.copy2(path, backup_path)
            print(f"[DEBUG] Backup created at {backup_path}")
        with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            for sheet_name, df in results.items():
                print(f"[DEBUG] Writing sheet '{sheet_name}' with shape {df.shape}")
                df.to_excel(writer, sheet_name=sheet_name, index=False)
    print("[DEBUG] run_lca finished")
    return results

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Compute U_i midpoint sheets with verbose debug output")
    parser.add_argument("workbook", help="Path to the Excel workbook")
    parser.add_argument("--ui", nargs="*", default=None, help="List of U_* columns to compute")
    parser.add_argument("--no-write", action="store_true", help="Do not write results back to the workbook")
    parser.add_argument("--no-backup", action="store_true", help="Do not create a .bak backup when writing")
    args = parser.parse_args()

    print(f"[DEBUG] CLI arguments: {args}")
    run_lca(
        args.workbook,
        ui_list=args.ui,
        write_back=not args.no_write,
        make_backup=not args.no_backup,
    )
    print("[DEBUG] CLI run completed")


if __name__ == "__main__":
    main()

# # Save the module so you can download it.
# with open("/mnt/data/lca_pipeline.py", "w", encoding="utf-8") as f:
#     import inspect
#     f.write(inspect.getsource(_get_unit_sheet))


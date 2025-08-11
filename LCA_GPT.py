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
from openpyxl import load_workbook

KEY_COLS = ["Time Frame", "SSPs", "RCPs", "Impact Categories"]

def _get_unit_sheet(path: str) -> pd.DataFrame:
    # Read without headers to handle the matrix layout.
    return pd.read_excel(path, sheet_name="Unit process & Utilities", header=None)

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

def _read_sheet(path: str, sheet: str) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet)

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

def _compute_materials(path: str, m_coeffs: Dict[str, float]) -> pd.Series:
    # Each M_k sheet has 'Impacts per kg/m³'
    series_list = []
    for m_name, qty in m_coeffs.items():
        sheet = f"{m_name}_Midpoint"
        df = _read_sheet(path, sheet)
        s = _to_indexed_series(df, "Impacts per kg/m³")
        series_list.append(s * qty)
    if not series_list:
        raise ValueError("No material sheets found.")
    # Align and sum
    out = sum(s.reindex(series_list[0].index.union_many([x.index for x in series_list[1:]]), fill_value=0) for s in series_list)
    return out

def _compute_waste(path: str, w_coeffs: Dict[str, float]) -> pd.Series:
    series_list = []
    for w_name, qty in w_coeffs.items():
        sheet = f"{w_name}_Midpoint"
        df = _read_sheet(path, sheet)
        s = _to_indexed_series(df, "Impacts per kg/m³")
        series_list.append(s * qty)
    if not series_list:
        # If no waste items, return zero series built from a template
        # Use W_1_Midpoint as template if present, else zero later
        try:
            df = _read_sheet(path, "W_1_Midpoint")
            template = _to_indexed_series(df, "Impacts per kg/m³")
            return template * 0.0
        except Exception:
            raise ValueError("No waste sheets found and no template available.")
    out = sum(s.reindex(series_list[0].index.union_many([x.index for x in series_list[1:]]), fill_value=0) for s in series_list)
    return out

def _compute_energy(path: str, e_coeffs: Dict[str, float]) -> pd.Series:
    # Electricity, Heat, Steam each with 'Impacts per kWh'
    parts = []
    for name in ("Electricity", "Heat", "Steam"):
        try:
            df = _read_sheet(path, f"{name}_Midpoint")
            s = _to_indexed_series(df, "Impacts per kWh")
            qty = e_coeffs.get(name, 0.0)
            parts.append(s * qty)
        except Exception:
            # Sheet might be absent; treat as zero
            pass
    if not parts:
        # try to build a zero template from any of the energy sheets
        for name in ("Electricity", "Heat", "Steam"):
            try:
                df = _read_sheet(path, f"{name}_Midpoint")
                tmpl = _to_indexed_series(df, "Impacts per kWh")
                return tmpl * 0.0
            except Exception:
                continue
        raise ValueError("No energy sheets found.")
    out = sum(s.reindex(parts[0].index.union_many([x.index for x in parts[1:]]), fill_value=0) for s in parts)
    return out

def _compute_transport(path: str, t_coeffs: Dict[str, float]) -> pd.Series:
    # Transportation_Midpoint has columns T_1, T_2, ... (if more added later)
    df = _read_sheet(path, "Transportation_Midpoint")
    df = df.copy()
    # Keep only T_* columns present
    t_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("T_")]
    if not t_cols:
        # Build zero template using any sheet previously seen: fallback to electricity or a materials sheet
        try:
            tmpl = _to_indexed_series(_read_sheet(path, "Electricity_Midpoint"), "Impacts per kWh")
            return tmpl * 0.0
        except Exception:
            # Final fallback: first materials sheet
            xls = pd.ExcelFile(path)
            m_sheet = next((s for s in xls.sheet_names if s.startswith("M_") and s.endswith("_Midpoint")), None)
            if m_sheet is None:
                raise ValueError("Cannot construct transport template; no suitable sheet found.")
            tmpl = _to_indexed_series(_read_sheet(path, m_sheet), "Impacts per kg/m³")
            return tmpl * 0.0
    # Build rowwise dot product
    # Align index first
    base_idx = pd.MultiIndex.from_frame(df[KEY_COLS])
    vals = pd.DataFrame({c: _to_indexed_series(df, c).reindex(base_idx) for c in t_cols})
    # Quantity vector for these columns
    weights = pd.Series({c: float(t_coeffs.get(c, 0.0)) for c in t_cols})
    out = (vals * weights).sum(axis=1)
    out.index = base_idx
    return out

def _compute_emissions(path: str, e_coeffs: Dict[str, float]) -> pd.Series:
    # Emissions_Midpoint has columns E_1 .. E_n
    df = _read_sheet(path, "Emissions_Midpoint")
    df = df.copy()
    e_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("E_")]
    if not e_cols:
        # Build zero template
        try:
            tmpl = _to_indexed_series(_read_sheet(path, "Electricity_Midpoint"), "Impacts per kWh")
            return tmpl * 0.0
        except Exception:
            xls = pd.ExcelFile(path)
            m_sheet = next((s for s in xls.sheet_names if s.startswith("M_") and s.endswith("_Midpoint")), None)
            if m_sheet is None:
                raise ValueError("Cannot construct emissions template; no suitable sheet found.")
            tmpl = _to_indexed_series(_read_sheet(path, m_sheet), "Impacts per kg/m³")
            return tmpl * 0.0
    base_idx = pd.MultiIndex.from_frame(df[KEY_COLS])
    vals = pd.DataFrame({c: _to_indexed_series(df, c).reindex(base_idx) for c in e_cols})
    weights = pd.Series({c: float(e_coeffs.get(c, 0.0)) for c in e_cols})
    out = (vals * weights).sum(axis=1)
    out.index = base_idx
    return out

def _template_index(path: str) -> pd.MultiIndex:
    """
    Pick a template sheet to extract the standard key rows.
    Preference: Electricity_Midpoint -> any M_*_Midpoint -> Transportation_Midpoint.
    """
    for sheet, valcol in [("Electricity_Midpoint", "Impacts per kWh"),
                          ("Heat_Midpoint", "Impacts per kWh"),
                          ("Steam_Midpoint", "Impacts per kWh")]:
        try:
            df = _read_sheet(path, sheet)
            return pd.MultiIndex.from_frame(df[KEY_COLS])
        except Exception:
            pass
    # Try any M_* sheet
    xls = pd.ExcelFile(path)
    for s in xls.sheet_names:
        if s.startswith("M_") and s.endswith("_Midpoint"):
            df = _read_sheet(path, s)
            return pd.MultiIndex.from_frame(df[KEY_COLS])
    # Fallback: Transportation_Midpoint
    df = _read_sheet(path, "Transportation_Midpoint")
    return pd.MultiIndex.from_frame(df[KEY_COLS])

def _assemble_ui_frame(path: str,
                       material: pd.Series,
                       waste: pd.Series,
                       transport: pd.Series,
                       energy: pd.Series,
                       emission: pd.Series) -> pd.DataFrame:
    # Build a full index (union) and reindex all series to it
    all_idx = material.index.union(waste.index).union(transport.index).union(energy.index).union(emission.index)
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
    # Put the full key columns from a template
    template_idx = _template_index(path)
    # Build a DataFrame with KEY_COLS from index
    keys_df = pd.DataFrame(list(all_idx), columns=KEY_COLS)
    # Ensure ordering matches idx
    df = pd.concat([keys_df.reset_index(drop=True), df.reset_index(drop=True)], axis=1)
    # Total
    df["Total Impacts per FU"] = df[["Material consumption","Waste","Transportation","Energy use","Emission"]].sum(axis=1)
    # Column order
    ordered = KEY_COLS + ["Total Impacts per FU", "Material consumption","Waste","Transportation","Energy use","Emission"]
    df = df[ordered]
    return df

def compute_ui_midpoint(path: str, ui_name: str) -> pd.DataFrame:
    """
    Compute the Ui_Midpoint table for one U (e.g., 'U_1').
    Returns a DataFrame with columns KEY_COLS + five subcategories + total.
    """
    unit_df = _get_unit_sheet(path)
    u_cols = _discover_u_columns(unit_df)
    if ui_name not in u_cols:
        raise ValueError(f"{ui_name} not found in 'Unit process & Utilities' header. Found: {list(u_cols)}")
    u_col = u_cols[ui_name]

    # Collect coefficient maps
    m_rows = _find_row_indices(unit_df, "M_")
    w_rows = _find_row_indices(unit_df, "W_")
    t_rows = _find_row_indices(unit_df, "T_")
    e_rows = _find_row_indices(unit_df, "E_")

    m_coeffs = _get_coeff_vector(unit_df, u_col, m_rows)
    w_coeffs = _get_coeff_vector(unit_df, u_col, w_rows)
    t_coeffs = _get_coeff_vector(unit_df, u_col, t_rows)
    e_coeffs = _get_coeff_vector(unit_df, u_col, e_rows)

    # Compute each component
    material = _compute_materials(path, m_coeffs)
    waste = _compute_waste(path, w_coeffs)
    transport = _compute_transport(path, t_coeffs)
    energy = _compute_energy(path, {"Electricity": unit_df.iat[_find_row_indices(unit_df, "Electricity").get("Electricity", -1), u_col]
                                               if "Electricity" in unit_df.iloc[:,0].values else 0.0,
                                     "Heat": unit_df.iat[_find_row_indices(unit_df, "Heat").get("Heat", -1), u_col]
                                               if "Heat" in unit_df.iloc[:,0].values else 0.0,
                                     "Steam": unit_df.iat[_find_row_indices(unit_df, "Steam").get("Steam", -1), u_col]
                                               if "Steam" in unit_df.iloc[:,0].values else 0.0})
    # Coerce NaNs to 0
    energy = energy.fillna(0.0)

    emission = _compute_emissions(path, e_coeffs)

    # Assemble
    ui_df = _assemble_ui_frame(path, material, waste, transport, energy, emission)
    return ui_df

def run_lca(path: str, ui_list: Optional[List[str]] = None, write_back: bool = True, make_backup: bool = True) -> Dict[str, pd.DataFrame]:
    """
    Main entry point. Computes Ui_Midpoint for all discovered U_* (or a provided subset).
    Returns a dict mapping sheet_name -> DataFrame.
    If write_back=True, updates/creates 'U_i_Midpoint' sheets in-place (optionally after making a .bak copy).
    """
    unit_df = _get_unit_sheet(path)
    u_cols = _discover_u_columns(unit_df)
    if not u_cols:
        raise ValueError("No U_* columns discovered in 'Unit process & Utilities'.")
    chosen = ui_list if ui_list is not None else list(u_cols.keys())

    results: Dict[str, pd.DataFrame] = {}
    for ui in chosen:
        df = compute_ui_midpoint(path, ui)
        results[f"{ui}_Midpoint"] = df

    if write_back:
        if make_backup:
            backup_path = path + ".bak"
            shutil.copy2(path, backup_path)
        # Write to workbook (overwrite or create U_i_Midpoint sheets)
        with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            for sheet_name, df in results.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
    return results

# # Save the module so you can download it.
# with open("/mnt/data/lca_pipeline.py", "w", encoding="utf-8") as f:
#     import inspect
#     f.write(inspect.getsource(_get_unit_sheet))


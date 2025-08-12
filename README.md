# LCA Automation

This repository automates computation of `U_i_Midpoint` tables in the provided lifecycle assessment workbook.
The `LCA_GPT.py` module reads the "Unit process & Utilities" sheet and combines it with the material, waste,
energy, transport, and emission midpoint sheets to produce per-unit impact tables.

The module emits verbose progress messages showing shapes of loaded sheets and results so you can track the
calculation.

## Usage

### Command line

```
python LCA_GPT.py 20250408-dsRNA\ in\ vitro\ synthesis-LCA\ calculation.xlsx --ui U_1 U_2
```

By default results are written back into the workbook; pass `--no-write` to skip writing.

Run `python LCA_GPT.py --help` to see all options.

### From Python

```python
import LCA_GPT as lca
results = lca.run_lca('20250408-dsRNA in vitro synthesis-LCA calculation.xlsx', ui_list=['U_1'], write_back=False)
for name, df in results.items():
    print(name, df.shape)
```

## Testing

A simple smoke test script is provided:

```
python test_LCA.py
```

It will execute the pipeline and print the shape of each generated sheet.

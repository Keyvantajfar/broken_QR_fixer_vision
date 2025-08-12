# LCA Automation

Verbose tools for computing life cycle assessment (LCA) midpoint values from an Excel workbook.
The script prints shapes, sizes, and progress information at every step to make debugging easy.

## Requirements

Install dependencies first:

```
pip install pandas openpyxl
```

## Usage

Compute all `U_i_Midpoint` sheets and write them back to the workbook with verbose output:

```
python LCA_GPT.py 20250408-dsRNA\ in\ vitro\ synthesis-LCA\ calculation.xlsx
```

Show CLI options:

```
python LCA_GPT.py -h
```

Run the included test script:

```
python test_LCA.py
```

Each command prints detailed debug information so you can follow the shapes and steps being executed.

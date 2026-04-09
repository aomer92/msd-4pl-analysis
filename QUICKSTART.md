# MSD 4PL Analysis Tool — Quick Start Guide

## Installation

### Windows
1. Download and unzip `MSD_4PL_Analysis_v1.0.zip`
2. Navigate into the folder
3. Double-click `MSD_4PL_Analysis.exe`
4. The application window will open

### macOS
1. Download and unzip `MSD_4PL_Analysis_v1.0.zip`
2. Open Finder and navigate into the folder
3. Double-click `MSD_4PL_Analysis`
4. If prompted, click "Open" to allow the application
5. The application window will open

### Linux
1. Download and unzip `MSD_4PL_Analysis_v1.0.zip`
2. Open Terminal in the folder
3. Run: `./MSD_4PL_Analysis`
4. The application window will open

---

## Using the Tool

### Step 1: Select Files
- **MSD Data File**: Click the "Browse" button and select your `.txt` file from the MSD instrument
- **Plate Map CSV**: Click the "Browse" button and select your plate map `.csv` file
- **Output Excel**: Click the "Browse" button and choose where to save the results (will be saved as `.xlsx`)

### Step 2: Configure Options (Optional)

#### Spots per Well
- Leave blank to auto-detect from the MSD file
- Or enter: `1`, `4`, or `10` to override

#### Units
- Enter units for interpolated concentrations (e.g., `pg/mL`, `ng/mL`)
- Leave blank for no units

#### %CV Threshold
- Default: `25` (highlights in green if %CV ≤ 25, red if > 25)
- Change to customize the highlighting threshold

#### LLOQ Method
- **Current (mean + 10×SD)**: Standard method, more conservative
- **3× Blank Mean**: Alternative, less conservative (useful when variance is high)

#### Dilution Factors
- Leave blank if no dilution was applied
- Or enter comma-separated values for each plate (e.g., `1,2,1` for three plates)
- The corrected concentrations will appear in the `All Unknowns` sheet

### Step 3: Load Previous Settings (Optional)

If you've run the tool before:
1. Click the **"Load Last Run"** button
2. All previous settings will be restored
3. You can modify any settings before running

### Step 4: Run the Analysis

Click the **"Run Analysis"** button to start the analysis. This will:
1. Parse your MSD data
2. Fit 4PL curves to each spot/analyte
3. Interpolate unknown concentrations
4. Generate an Excel workbook with results
5. A summary will be printed in the console

---

## Output Files

The Excel workbook contains:

### Summary Sheet
- 4PL fit parameters (a, b, c, d) for each curve
- LLOQ signal and concentration
- R² values (quality of fit)
- Status: Good (R² ≥ 0.99), Acceptable (R² ≥ 0.95), or Poor
- Overlay chart showing all fitted curves

### Per-Spot Detail Sheets
- Standard curve data with fitted signals
- Blanks/background measurements
- Interpolated unknowns with range flags
- Individual standard curve plot

### All Unknowns Sheet
- Summary of all unknown samples
- Average signal and concentration
- Dilution-corrected concentration (if applicable)
- %CV (coefficient of variation) with color coding
- Range status: In Range, < LLOQ, > ULOQ

### MSD Data Sheet
- Original MSD instrument output (for reference)

### Plate Map Sheets
- Your plate map configuration (for reference)

---

## Tips & Best Practices

### File Preparation

**MSD Data File (.txt)**
- Export directly from MSD Discovery Workbench
- Should contain plate number, spots per well, and all detector signals
- Supports multi-plate files

**Plate Map CSV**
- Use the provided template or create your own
- Layout: rows A-H (or A-P for 384-well plates), columns 1-12 (or 1-24)
- Standards: enter the concentration value (e.g., `800000`, `1000`)
- Unknowns: enter a sample name (e.g., `fCtx`, `Sample_1`)
- Blanks: use keywords like `Buffer Only`, `Blank`, `Background`, or `0`
- Multiple curves: prefix values with group name (e.g., `GroupA:800000`)

### Interpreting Results

**R² Value**
- Closer to 1.0 = better fit
- ≥ 0.99 = Excellent
- ≥ 0.95 = Acceptable
- < 0.95 = Poor (consider reviewing your data)

**LLOQ & ULOQ**
- **LLOQ** (Lower Limit of Quantitation): Lowest detectable concentration
- **ULOQ** (Upper Limit of Quantitation): Highest measurable concentration
- Samples outside these ranges are flagged

**%CV (Coefficient of Variation)**
- Measures consistency across replicate measurements
- Green (≤ threshold): Good consistency
- Red (> threshold): High variability, may need investigation

### Common Issues

| Problem | Solution |
|---------|----------|
| Missing buttons in GUI | Window may be too small; try resizing the window |
| Spots per well incorrect | Leave blank to auto-detect, or verify MSD file format |
| LLOQ values seem off | Try the "3× Blank Mean" method; compare with your standards |
| High %CV values | Check for outlier measurements or sample preparation issues |

---

## Keyboard Shortcuts

- **Tab**: Move between fields
- **Enter**: Focus the "Run Analysis" button (then press Enter to run)
- **Ctrl+A** or **Cmd+A**: Select all text in a field

---

## Need Help?

Refer to:
- **README.md**: Full documentation and technical details
- **README_DISTRIBUTION.md**: Building and distributing the tool
- **Plate Map Examples**: Contact the tool administrator for sample files

---

**Version**: 1.0.0  
**Last Updated**: April 2026

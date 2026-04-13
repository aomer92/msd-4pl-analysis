#!/Users/amromer/msd_env/bin/python
#HOW TO RUN 
# python3 msd_4pl_analysis.py --msd 185-008_ControlVariability/23N3QAYE23_2026-04-01-082642.txt --platemap 185-008_ControlVariability/AssayPlateMap.csv --output 185-008_ControlVariability/results.xlsx
"""
MSD 4PL Analysis Tool
=====================
Parses MSD instrument .txt files (1, 4, or 10 spots per well; multi-plate),
fits 4-parameter logistic regression per analyte/spot, interpolates unknown
concentrations, and outputs a formatted Excel workbook with standard curves.

USAGE
-----
  Interactive mode (single-page GUI):
    python3 msd_4pl_analysis.py
    python3 msd_4pl_analysis.py --gui

  Command-line mode:
    python3 msd_4pl_analysis.py --msd <data.txt> --platemap <map.csv> --output <results.xlsx>
    python3 msd_4pl_analysis.py --msd <data.txt> --platemap <map.csv> --output <results.xlsx> --spots 4 --units pg/mL --cv-threshold 30 --lloq-method 3xblank --dilution-factors 1,2,1

  Running with no arguments or with --gui opens a single-page GUI to configure
  all options and select files.

  --msd              MSD instrument .txt data file (supports multi-plate files)
  --platemap         Plate map CSV in grid format (see below)
  --output           Output Excel file path (default: msd_4pl_results.xlsx)
  --spots            Override spots per well: 1, 4, or 10 (auto-detected if omitted)
  --units            Optional units string to append to interpolated concentration headers
  --cv-threshold     Optional %CV threshold for All Unknowns highlight (default 25)
  --lloq-method      LLOQ calculation method: 'current' (mean+10*SD) or '3xblank' (3x blank mean)
  --dilution-factors Optional per-plate dilution factors as comma-separated values (e.g. 1,2,1)
  --gui              Force interactive GUI mode
  --rerun            Rerun the last analysis with saved parameters

PLATE MAP FORMAT
----------------
The plate map is a CSV in 96-well grid layout. Row letters (A-H) are the first
column; column numbers (1-12) are the header row.

  Example:
    ,1,2,3,4,5,6,7,8,9,10,11,12
    A,800000,800000,fCtx,mCtx,Cd,Put,Hp,fCtx,mCtx,Cd,Put,Hp
    B,200000,200000,fCtx,mCtx,Cd,Put,Hp,fCtx,mCtx,Cd,Put,Hp
    ...
    H,Buffer Only,Buffer Only,HQC,MQC,LQC,,,,,,,

CELL CLASSIFICATION RULES
--------------------------
Each cell in the plate map is classified automatically based on its content:

  Standard   — Cell contains a PURELY NUMERIC value (integer or decimal).
               The number is used as the known concentration for curve fitting.
               Commas are stripped before parsing (e.g. "3,125" → 3125).
               Examples: 800000, 781.25, 3125, 0.61

  Unknown    — Cell contains ANY TEXT or a MIX of text and numbers.
               Treated as an unknown sample; the cell value becomes the sample name.
               The concentration will be interpolated from the fitted 4PL curve.
               Examples: fCtx, mCtx, HQC, Sample_3, STD-1, 800000 pg/ml

  Blank      — Cell matches one of these keywords (case-insensitive):
               "Buffer Only", "Blank", "Buffer", "BG", "Background", "0"
               Blanks are included in the curve fit at concentration = 0 and used
               to calculate LLOQ (mean + 10 × SD of blank signals).

  Empty      — Cell is empty or contains only whitespace. Skipped entirely.

  NOTE: There is no requirement for standards or samples to be in specific
  wells or orientations. Standards can be scattered anywhere on the plate.
  The only requirement is at least 4 unique standard concentrations per
  curve for a successful 4PL fit.

  CAUTION: A sample name that is purely numeric (e.g. a sample ID "12345")
  will be misclassified as a standard at concentration 12345. To avoid this,
  include at least one non-numeric character in sample names (e.g. "S-12345").

GROUP PREFIX (MULTIPLE CURVES PER PLATE)
----------------------------------------
To run multiple independent standard curves on the same plate, prefix cell
values with a group tag followed by a colon:

    GroupName:value

  Examples:
    CurveA:800000      → Standard at 800000, assigned to group "CurveA"
    CurveA:fCtx        → Unknown sample "fCtx", assigned to group "CurveA"
    CurveB:500000      → Standard at 500000, assigned to group "CurveB"
    CurveB:SampleX     → Unknown sample "SampleX", assigned to group "CurveB"

  Each group gets its own independent 4PL fit, LLOQ/ULOQ, and Excel sheet.
  Blanks WITHOUT a group prefix are shared across all groups automatically.
  Cells without any prefix belong to a single default group (backward compatible).

  NOTE: The group prefix can be up to 20 characters. The colon ":" is the
  delimiter, so avoid colons in sample names unless using the group feature.
  A prefix like "1:2" would be interpreted as group "1", value "2".

MULTI-PLATE SUPPORT
-------------------
Multiple plate maps can be stacked vertically in a single CSV, separated by
one or more blank rows. Each block is assigned a plate number (1, 2, 3, ...)
in order and matched to the corresponding plate in the MSD file.

  Example (two plates):
    ,1,2,...,12
    A,800000,...
    ...H,...
                        ← blank row separates plates
    ,1,2,...,12         ← optional repeated header
    A,500000,...
    ...H,...

  If only one plate map is provided but the MSD file contains multiple plates,
  the single map is reused for all plates.

OUTPUT
------
The Excel workbook contains:
  - Summary sheet: 4PL parameters, LLOQ, R² for all spots/groups, plus an
    overlay chart showing all fitted curves on one plot.
  - Per-spot sheets: Detailed standard curve data, blanks, interpolated
    unknowns with ULOQ/LLOQ flags, and an MSD-style log-log chart with
    detection range bands.
  - All Unknowns sheet: Consolidated table of all unknown samples grouped
    by sample name, with averaged signals and concentrations.
"""

import re, sys, argparse, os, tempfile, json, subprocess, platform

LAST_RUN_PATH = os.path.join(os.path.expanduser('~'), '.msd_4pl_last_run.json')
MAX_RUN_HISTORY = 5

def _load_run_history():
    """Return list of up to MAX_RUN_HISTORY prior run dicts, newest first.
    Handles legacy single-dict format transparently."""
    try:
        with open(LAST_RUN_PATH, 'r') as f:
            data = json.load(f)
        if isinstance(data, dict):          # legacy single-entry
            return [data]
        return data[:MAX_RUN_HISTORY]
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def _save_run_to_history(entry):
    """Prepend entry to the run history list and trim to MAX_RUN_HISTORY."""
    from datetime import datetime
    entry.setdefault('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M'))
    history = _load_run_history()
    history.insert(0, entry)
    history = history[:MAX_RUN_HISTORY]
    with open(LAST_RUN_PATH, 'w') as f:
        json.dump(history, f, indent=2)

def _run_label(entry):
    """Short human-readable label for a run history entry."""
    ts  = entry.get('timestamp', '')
    msd = os.path.basename(entry.get('msd') or '') or '—'
    out = os.path.basename(entry.get('output') or '') or '—'
    return f"{ts}  |  {msd}  →  {out}"

from io import StringIO
from collections import defaultdict
try:
    import numpy as np
    import pandas as pd
    from scipy.optimize import curve_fit
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.chart import ScatterChart, Reference, Series
    from openpyxl.drawing.image import Image as XlImage
    from openpyxl.utils import get_column_letter
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ModuleNotFoundError as e:
    missing = str(e).split("'")[1] if "'" in str(e) else str(e)
    print(f"Missing required Python package: {missing}")
    print("Install dependencies with:")
    print("  python3 -m pip install numpy pandas scipy openpyxl matplotlib")
    sys.exit(1)
import warnings
warnings.filterwarnings('ignore')


# ═══════════════════════════════════════════════════════════════════════════════
# 4PL MODEL
# ═══════════════════════════════════════════════════════════════════════════════

QC_LEVELS = ["ULOQ", "HQC", "MQC", "LQC", "LLOQ"]

def _identify_qc_level(sample_name):
    """Return which QC level (ULOQ/HQC/MQC/LQC/LLOQ) this sample represents, or None."""
    upper = sample_name.upper()
    for level in QC_LEVELS:
        if level in upper:
            return level
    return None


def four_pl(x, a, b, c, d):
    """4PL: a=min asymptote, b=Hill slope, c=inflection (EC50), d=max asymptote"""
    return d + (a - d) / (1.0 + (x / c) ** b)

def inverse_4pl(y, a, b, c, d):
    """Solve 4PL for x given y."""
    denom = y - d
    if denom == 0:
        return np.nan
    ratio = (a - d) / denom - 1.0
    if ratio <= 0:
        return np.nan
    return c * (ratio ** (1.0 / b))

def fit_4pl(conc, signal):
    """Fit 4PL to standards and blanks. Returns (popt, r2) or (None, None)."""
    conc, signal = np.asarray(conc, float), np.asarray(signal, float)
    mask = (conc >= 0) & np.isfinite(signal)
    conc, signal = conc[mask], signal[mask]
    if len(conc) < 4:
        return None, None
    a0, d0 = np.min(signal), np.max(signal)
    c0, b0 = np.median(conc), 1.0
    try:
        popt, _ = curve_fit(four_pl, conc, signal, p0=[a0, b0, c0, d0],
                            sigma=np.clip(signal, 1e-3, None), absolute_sigma=True,
                            maxfev=1000,
                            bounds=([-np.inf, -np.inf, 1e-15, -np.inf],
                                    [np.inf, np.inf, np.inf, np.inf]))
        ss_res = np.sum((signal - four_pl(conc, *popt)) ** 2)
        ss_tot = np.sum((signal - np.mean(signal)) ** 2)
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
        return popt, r2
    except Exception:
        return None, None


# ═══════════════════════════════════════════════════════════════════════════════
# CHART GENERATION (MSD Discovery Workbench style)
# ═══════════════════════════════════════════════════════════════════════════════

def generate_std_curve_chart(res, tmp_dir, lloq_method='current'):
    """
    Generate a log-log standard curve plot matching MSD Discovery Workbench style.
    Returns path to saved PNG image, or None if curve fit failed.
    """
    params = res.get('params')
    standards = res.get('standards', [])
    blanks = res.get('blanks', [])
    if params is None or not standards:
        return None

    a, b, c, d = params
    plate = res['plate']
    spot = res['spot']
    group = res.get('group', '')

    # Collect standard data
    std_concs = np.array([s['conc'] for s in standards if s['conc'] > 0])
    std_sigs = np.array([s['signal'] for s in standards if s['conc'] > 0])
    if len(std_concs) == 0:
        return None

    # Calculate LLOQ signal from blanks
    lloq_sig = None
    if blanks:
        bsigs = [bl['signal'] for bl in blanks if np.isfinite(bl['signal'])]
        lloq_sig = calculate_lloq_signal(bsigs, lloq_method)

    # ULOQ signal = fitted signal at highest standard concentration
    uloq_conc = np.max(std_concs)
    uloq_sig = four_pl(uloq_conc, *params)

    # LLOQ concentration from signal
    lloq_conc = None
    if lloq_sig is not None:
        try:
            lloq_conc = inverse_4pl(lloq_sig, *params)
            if not (np.isfinite(lloq_conc) and lloq_conc > 0):
                lloq_conc = None
        except:
            lloq_conc = None

    # Generate smooth fitted curve
    conc_min = np.min(std_concs) * 0.3
    conc_max = np.max(std_concs) * 3
    x_smooth = np.logspace(np.log10(conc_min), np.log10(conc_max), 500)
    y_smooth = four_pl(x_smooth, *params)

    # ── Plot ──────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(8, 5.5))

    # Detection range shading
    if lloq_sig is not None and uloq_sig is not None:
        ax.axhspan(lloq_sig, uloq_sig, alpha=0.08, color='#2244AA', zorder=0)
        # "In Detection Range" label at left edge, vertically centered
        mid_sig = np.sqrt(lloq_sig * uloq_sig)  # geometric mean for log scale
        ax.text(conc_min * 0.35, mid_sig, 'In Detection Range',
                fontsize=7.5, color='#1a3a8a', style='italic', ha='left', va='center')

    # ULOQ line (top)
    if uloq_sig is not None:
        ax.axhline(y=uloq_sig, color='#1a3a8a', linestyle=':', linewidth=1.2, zorder=1)
        ax.text(conc_min * 0.35, uloq_sig * 1.25, 'Above Detection Range',
                fontsize=7.5, color='#1a3a8a', style='italic', ha='left', va='bottom')

    # LLOQ line (bottom)
    if lloq_sig is not None:
        ax.axhline(y=lloq_sig, color='#1a3a8a', linestyle=':', linewidth=1.2, zorder=1)
        ax.text(conc_max * 1.5, lloq_sig * 0.80, 'Below Detection Range',
                fontsize=7.5, color='#1a3a8a', style='italic', ha='right', va='top')

    # Fitted curve
    ax.plot(x_smooth, y_smooth, '-', color='#1a3a8a', linewidth=1.5, zorder=3)

    # Observed data points
    ax.scatter(std_concs, std_sigs, s=35, color='#1a3a8a', zorder=4,
               edgecolors='#0a2060', linewidths=0.5, label='Standards')

    # Log-log scale
    ax.set_xscale('log')
    ax.set_yscale('log')

    # Axis limits — leave room for labels
    ax.set_xlim(conc_min * 0.25, conc_max * 4)
    all_sigs = list(std_sigs)
    if lloq_sig is not None:
        all_sigs.append(lloq_sig)
    if uloq_sig is not None:
        all_sigs.append(uloq_sig)
    sig_min = min(s for s in all_sigs if s > 0) * 0.4
    sig_max = max(all_sigs) * 3
    ax.set_ylim(sig_min, sig_max)

    # Axis formatting
    ax.set_xlabel('Concentration', fontsize=10, fontweight='bold')
    ax.set_ylabel('Signal', fontsize=10, fontweight='bold')

    title_str = f"Plate {plate}, Spot {spot}"
    if group:
        title_str += f" — {group}"
    ax.set_title(title_str, fontsize=11, fontweight='bold', pad=10)

    # Tick formatting — show 10^n style
    for axis in [ax.xaxis, ax.yaxis]:
        axis.set_major_formatter(ticker.LogFormatterSciNotation())

    ax.tick_params(which='both', direction='in', top=True, right=True)
    ax.grid(True, which='major', alpha=0.15, linewidth=0.5)

    # Info box with Calc. Low / High
    info_lines = []
    if lloq_conc is not None:
        info_lines.append(f"Calc. Low    {lloq_conc:.2f}")
    info_lines.append(f"Calc. High   {uloq_conc:.0f}")
    if info_lines:
        box_text = '\n'.join(info_lines)
        props = dict(boxstyle='round,pad=0.4', facecolor='white', edgecolor='#666666', alpha=0.9)
        ax.text(0.98, 0.98, box_text, transform=ax.transAxes, fontsize=8,
                verticalalignment='top', horizontalalignment='right',
                bbox=props, family='monospace')

    # Legend
    ax.legend(loc='lower right', fontsize=8, framealpha=0.9)

    plt.tight_layout()

    # Save
    fname = f"chart_P{plate}_S{spot}{'_' + group if group else ''}.png"
    fpath = os.path.join(tmp_dir, fname)
    fig.savefig(fpath, dpi=180, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    return fpath


def generate_overlay_chart(results, tmp_dir, qc_overlay_points=None, qc_expected_concentrations=None):
    """
    Generate an overlay plot showing all fitted standard curves on one chart.
    Each spot/group gets its own color. QC points (if provided) are overlaid
    as star markers using their corrected concentration and original signal.
    Expected QC concentrations (if provided) are shown as ±30% vertical bands.
    Returns path to saved PNG.
    """
    fitted = [r for r in results if r.get('params') is not None]
    if not fitted:
        return None

    fig, ax = plt.subplots(figsize=(12, 7.5))

    cmap = plt.cm.get_cmap('tab10')
    colors = [cmap(i % 10) for i in range(len(fitted))]

    global_conc_min = np.inf
    global_conc_max = 0

    for idx, res in enumerate(fitted):
        params = res['params']
        standards = res.get('standards', [])
        std_concs = np.array([s['conc'] for s in standards if s['conc'] > 0])
        std_sigs = np.array([s['signal'] for s in standards if s['conc'] > 0])
        if len(std_concs) == 0:
            continue

        color = colors[idx]
        spot = res['spot']
        group = res.get('group', '')
        plate = res['plate']
        label = f"Spot {spot}"
        if group:
            label += f" ({group})"
        if len(set(r['plate'] for r in fitted)) > 1:
            label = f"P{plate} " + label

        cmin, cmax = np.min(std_concs), np.max(std_concs)
        global_conc_min = min(global_conc_min, cmin)
        global_conc_max = max(global_conc_max, cmax)

        # Smooth fitted curve
        x_smooth = np.logspace(np.log10(cmin * 0.3), np.log10(cmax * 3), 400)
        y_smooth = four_pl(x_smooth, *params)
        ax.plot(x_smooth, y_smooth, '-', color=color, linewidth=1.5, label=label, zorder=3)

        # Observed points
        ax.scatter(std_concs, std_sigs, s=25, color=color, zorder=4,
                   edgecolors='black', linewidths=0.3, alpha=0.8)

    # Shared QC level color palette
    qc_cmap = plt.cm.get_cmap('Set1')
    qc_level_colors = {level: qc_cmap(i % 9) for i, level in enumerate(QC_LEVELS)}

    # ±30% expected concentration band (single value, drawn before QC points)
    if qc_expected_concentrations is not None and np.isfinite(qc_expected_concentrations) and qc_expected_concentrations > 0:
        exp_conc = qc_expected_concentrations
        lo, hi = exp_conc * 0.70, exp_conc * 1.30
        ax.axvspan(lo, hi, alpha=0.15, color='steelblue', zorder=1,
                   label=f"QC ±30% ({exp_conc:.3g})")
        ax.axvline(exp_conc, color='steelblue', linewidth=1.0, linestyle='--', zorder=2)
        # Expand axis range to include band
        global_conc_min = min(global_conc_min, lo)
        global_conc_max = max(global_conc_max, hi)

    # QC overlay points (corrected conc vs original signal)
    if qc_overlay_points:
        plotted_levels = set()
        for pt in qc_overlay_points:
            conc = pt.get('corrected_conc')
            sig = pt.get('signal')
            level = pt.get('level', pt.get('sample_name', ''))
            if conc is None or sig is None:
                continue
            if not (np.isfinite(conc) and np.isfinite(sig) and conc > 0 and sig > 0):
                continue
            qc_color = qc_level_colors.get(level, 'black')
            lbl = f"QC: {level}" if level not in plotted_levels else None
            ax.scatter(conc, sig, s=120, marker='*', color=qc_color, zorder=6,
                       edgecolors='black', linewidths=0.5, label=lbl, alpha=0.95)
            plotted_levels.add(level)
            # Expand axis range to include QC points
            global_conc_min = min(global_conc_min, conc)
            global_conc_max = max(global_conc_max, conc)

    ax.set_xscale('log')
    ax.set_yscale('log')

    ax.set_xlim(global_conc_min * 0.2, global_conc_max * 5)
    ax.set_xlabel('Concentration', fontsize=11, fontweight='bold')
    ax.set_ylabel('Signal', fontsize=11, fontweight='bold')
    ax.set_title('All Standard Curves — Overlay', fontsize=13, fontweight='bold', pad=12)

    for axis in [ax.xaxis, ax.yaxis]:
        axis.set_major_formatter(ticker.LogFormatterSciNotation())

    ax.tick_params(which='both', direction='in', top=True, right=True)
    ax.grid(True, which='major', alpha=0.15, linewidth=0.5)

    n_qc_bands = 1 if (qc_expected_concentrations is not None) else 0
    n_qc_pts  = len(set(pt['level'] for pt in (qc_overlay_points or []))) if qc_overlay_points else 0
    n_series = len(fitted) + n_qc_bands + n_qc_pts
    if n_series <= 6:
        ax.legend(loc='lower right', fontsize=8, framealpha=0.9)
    else:
        ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), fontsize=7.5,
                  framealpha=0.9, ncol=1 + n_series // 15)
        fig.subplots_adjust(right=0.78)

    plt.tight_layout()

    fpath = os.path.join(tmp_dir, 'overlay_all_curves.png')
    fig.savefig(fpath, dpi=180, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    return fpath




def parse_msd_file(filepath):
    """
    Parse MSD .txt file. Handles 1, 4, or 10 spots per well and multi-plate.
    Returns: list of dicts [{plate_num, spots_per_well, data: {well_id: [signal_per_spot]}}]
    """
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()

    plate_sections = re.split(r'(?=Plate\s*#\s*:)', content)
    if len(plate_sections) == 1 and 'Plate #' not in plate_sections[0][:200]:
        plate_sections = [content]

    plates = []
    for section in plate_sections:
        if '==========Data==' not in section:
            continue

        m_plate = re.search(r'Plate\s*#\s*:\s*(\d+)', section)
        plate_num = int(m_plate.group(1)) if m_plate else len(plates) + 1

        m_spots = re.search(r'Spots Per Well\s*:\s*(\d+)', section)
        n_spots = int(m_spots.group(1)) if m_spots else 1

        data_start = section.index('==========Data==')
        rest = section[data_start:]
        lines = rest.split('\n')

        data_lines = []
        started = False
        for line in lines:
            if '==========Data==' in line:
                started = True
                continue
            if started and '==========' in line:
                break
            if started:
                data_lines.append(line)

        well_data = {}
        current_row = None

        for line in data_lines:
            raw = line.rstrip()
            if not raw.strip():
                current_row = None
                continue

            parts = raw.split('\t')
            label = parts[0].strip()

            if label and len(label) == 1 and label.isalpha():
                current_row = label.upper()

            if current_row is None:
                continue

            vals = []
            for p in parts[1:]:
                p = p.strip()
                if p:
                    try:
                        vals.append(float(p))
                    except ValueError:
                        pass

            if not vals:
                continue

            for ci, v in enumerate(vals):
                well = f"{current_row}{ci + 1}"
                if well not in well_data:
                    well_data[well] = []
                well_data[well].append(v)

        plates.append({
            'plate_num': plate_num,
            'spots_per_well': n_spots,
            'data': well_data
        })

    return plates


# ═══════════════════════════════════════════════════════════════════════════════
# PLATE MAP PARSER (GRID FORMAT)
# ═══════════════════════════════════════════════════════════════════════════════

def parse_plate_map_grid(filepath):
    """
    Parse grid-format plate map CSV. Supports multiple plates stacked
    vertically, separated by blank rows.

    Single plate:
        ,1,2,3,...,12
        A,800000,800000,fCtx,...
        B,200000,200000,fCtx,...
        ...H,...

    Multiple plates (blank row separates each):
        ,1,2,3,...,12
        A,800000,800000,fCtx,...
        ...H,...
                                    ← blank row
        ,1,2,3,...,12               ← optional repeated header
        A,500000,500000,Sample,...
        ...H,...

    Returns: dict {plate_number: [entries]}
      where each entry = {well, sample_type, concentration, sample_name}
    """
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        raw_lines = f.readlines()

    # Split into plate blocks on blank lines
    blocks = []
    raw_blocks = {}
    current_block = []
    for line in raw_lines:
        stripped = line.strip().replace(',', '').strip()
        if stripped == '':
            if current_block:
                blocks.append(current_block)
                current_block = []
        else:
            current_block.append(line)
    if current_block:
        blocks.append(current_block)

    # Parse each block as a plate grid
    all_plates = {}
    for plate_idx, block_lines in enumerate(blocks):
        plate_num = plate_idx + 1
        text = ''.join(block_lines)

        try:
            df = pd.read_csv(StringIO(text), index_col=0, dtype=str,
                             on_bad_lines='skip', sep=',', skipinitialspace=True)
        except Exception:
            # Fallback: strip trailing commas and retry
            cleaned = '\n'.join(l.rstrip().rstrip(',') for l in block_lines)
            try:
                df = pd.read_csv(StringIO(cleaned), index_col=0, dtype=str)
            except Exception:
                continue

        df.index = df.index.astype(str).str.strip().str.upper()
        df.columns = [str(c).strip() for c in df.columns]

        # Skip blocks that don't look like plate grids (need row letters A-H/A-P)
        valid_rows = [r for r in df.index if re.match(r'^[A-P]$', r)]
        if not valid_rows:
            continue

        entries = []
        for row_letter in valid_rows:
            for col_str in df.columns:
                raw = df.loc[row_letter, col_str]
                val = str(raw).strip() if pd.notna(raw) else ''
                well = f"{row_letter}{col_str}"

                if val == '' or val.lower() == 'nan':
                    entries.append({'well': well, 'sample_type': 'Empty',
                                    'concentration': np.nan, 'sample_name': '',
                                    'group': '_default'})
                    continue

                # Extract group prefix if present (e.g. "A:800000" → group="A", val="800000")
                group = '_default'
                if ':' in val:
                    parts = val.split(':', 1)
                    candidate_group = parts[0].strip()
                    candidate_val = parts[1].strip()
                    # Accept prefix if it looks like a short tag (not a full path or URL)
                    if len(candidate_group) <= 20 and candidate_val:
                        group = candidate_group
                        val = candidate_val

                if val.lower() in ('buffer only', 'blank', 'buffer', 'bg', 'background', '0'):
                    entries.append({'well': well, 'sample_type': 'Blank',
                                    'concentration': 0, 'sample_name': val,
                                    'group': group})
                    continue

                try:
                    conc = float(val.replace(',', ''))
                    entries.append({'well': well, 'sample_type': 'Standard',
                                    'concentration': conc, 'sample_name': f'STD ({conc})',
                                    'group': group})
                    continue
                except ValueError:
                    pass

                entries.append({'well': well, 'sample_type': 'Unknown',
                                'concentration': np.nan, 'sample_name': val,
                                'group': group})

        all_plates[plate_num] = entries
        raw_blocks[plate_num] = block_lines
        groups = set(e['group'] for e in entries if e['group'] != '_default')
        group_str = f" | Groups: {', '.join(sorted(groups))}" if groups else ""
        print(f"  Plate {plate_num}: {sum(1 for e in entries if e['sample_type']=='Standard')} stds, "
              f"{sum(1 for e in entries if e['sample_type']=='Unknown')} unknowns, "
              f"{sum(1 for e in entries if e['sample_type']=='Blank')} blanks{group_str}")

    return all_plates, raw_blocks


def normalize_well(w):
    w = str(w).strip().upper()
    m = re.match(r'^([A-P])0*(\d+)$', w)
    return f"{m.group(1)}{int(m.group(2))}" if m else w


def parse_plate_dilution_factors(raw_value, n_plates):
    if raw_value is None:
        return {}
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, (int, float)):
        return {i + 1: float(raw_value) for i in range(n_plates)}

    if isinstance(raw_value, (list, tuple)):
        parts = [str(p).strip() for p in raw_value if str(p).strip() != '']
    else:
        parts = [p.strip() for p in str(raw_value).split(',') if p.strip() != '']

    if not parts:
        return {}

    try:
        values = [float(p) for p in parts]
    except ValueError:
        raise ValueError("Invalid dilution factors. Use numbers separated by commas.")

    if len(values) == 1:
        return {i + 1: values[0] for i in range(n_plates)}
    if len(values) == n_plates:
        return {i + 1: v for i, v in enumerate(values)}

    raise ValueError(f"Expected 1 or {n_plates} dilution factor(s), got {len(values)}.")


def calculate_lloq_signal(signals, lloq_method='current'):
    if not signals:
        return None
    values = [s for s in signals if np.isfinite(s)]
    if not values:
        return None
    mean_sig = np.mean(values)
    if lloq_method == '3xblank':
        return mean_sig * 3
    if len(values) > 1:
        return mean_sig + 10 * np.std(values, ddof=1)
    return None


def parse_total_protein_csv(filepath):
    """
    Parse a total protein CSV file. Expects columns:
      'External Animal Number', 'Tissue Type', 'Total Protein Result'
    Multiple rows with the same (animal, tissue) are kept in order of appearance.
    Returns dict {(animal_str, tissue_str): [val1, val2, ...]}
    """
    df = pd.read_csv(filepath, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    required = {'External Animal Number', 'Tissue Type', 'Total Protein Result'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Total protein CSV missing columns: {missing}")
    tp_map = {}
    for _, row in df.iterrows():
        animal = str(row['External Animal Number']).strip()
        tissue = str(row['Tissue Type']).strip()
        try:
            val = float(row['Total Protein Result'])
        except (ValueError, TypeError):
            continue
        key = (animal, tissue)
        if key not in tp_map:
            tp_map[key] = []
        tp_map[key].append(val)
    return tp_map


def _extract_animal_tissue(sample_name):
    """
    Flexible extraction of animal number and tissue from a sample name.
    - Strips any trailing _suffix (e.g. _P1, _rep2) before parsing
    - Splits by '-' and scans segments regardless of order or extras:
        Animal → first purely numeric segment        (e.g. '1001')
        Tissue → longest purely alphabetic segment   (e.g. 'fCtx' beats 'XX')
    - Returns (None, None) if no animal number found (e.g. HQC, Buffer Only)
    Handles any ordering or extra segments:
        fCtx-1001_P1, 1001-fCtx, fCtx-XX-1001, 1001-XX-fCtx, etc.
    """
    base = sample_name.strip().split('_')[0]   # drop _P1, _rep2, etc.
    segments = base.split('-')
    animal = next((s for s in segments if s.isdigit()), None)
    if animal is None:
        return None, None   # no animal ID → QC or non-sample name
    alpha_segs = [s for s in segments if s.isalpha()]
    tissue = max(alpha_segs, key=len) if alpha_segs else None
    return animal, tissue


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEL OUTPUT
# ═══════════════════════════════════════════════════════════════════════════════

HEADER_FILL = PatternFill('solid', fgColor='2F5496')
HEADER_FONT = Font(bold=True, color='FFFFFF', name='Arial', size=10)
DATA_FONT = Font(name='Arial', size=10)
BOLD_FONT = Font(bold=True, name='Arial', size=10)
SECTION_FONT = Font(bold=True, name='Arial', size=12, color='2F5496')
THIN_BORDER = Border(
    left=Side('thin', color='B4B4B4'), right=Side('thin', color='B4B4B4'),
    top=Side('thin', color='B4B4B4'), bottom=Side('thin', color='B4B4B4'))
STD_FILL = PatternFill('solid', fgColor='E2EFDA')
UNK_FILL = PatternFill('solid', fgColor='FFF2CC')
BLANK_FILL = PatternFill('solid', fgColor='F2F2F2')
CV_GOOD_FILL = PatternFill('solid', fgColor='D9EAD3')
CV_BAD_FILL = PatternFill('solid', fgColor='F8CBAD')
PASS_FONT = Font(name='Arial', size=10, color='006100')
WARN_FONT = Font(name='Arial', size=10, color='9C5700')
FAIL_FONT = Font(name='Arial', size=10, color='9C0006')


def _style_row(ws, row, max_col, fill=None, font=DATA_FONT):
    for c in range(1, max_col + 1):
        cell = ws.cell(row=row, column=c)
        cell.font = font
        cell.border = THIN_BORDER
        cell.alignment = Alignment(horizontal='center')
        if fill:
            cell.fill = fill

def _header_row(ws, row, headers):
    for ci, h in enumerate(headers, 1):
        ws.cell(row=row, column=ci, value=h)
    for c in range(1, len(headers) + 1):
        cell = ws.cell(row=row, column=c)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal='center', wrap_text=True)
        cell.border = THIN_BORDER

def _section_title(ws, row, title, span=5):
    ws.cell(row=row, column=1, value=title).font = SECTION_FONT
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=span)


def create_output(results, output_path, msd_path, raw_plate_blocks, units=None, cv_threshold=25, plate_dilution_factors=None, lloq_method='current', total_protein_map=None, qc_dilution_factors=None, qc_expected_concentrations=None):
    wb = Workbook()
    wb.remove(wb.active)
    tmp_dir = tempfile.mkdtemp(prefix='msd_charts_')

    # Pre-collect QC overlay points (corrected conc + signal) for overlay chart
    qc_overlay_points = []   # used by chart
    qc_summary_rows = []     # used by Summary sheet table
    if qc_dilution_factors:
        qc_groups = defaultdict(list)
        for res in results:
            for unk in res.get('unknowns', []):
                sname = unk.get('sample_name', '')
                level = _identify_qc_level(sname)
                if level and level in qc_dilution_factors:
                    key = (sname, res.get('group', ''), res['plate'])
                    qc_groups[key].append({'signal': unk['signal'], 'interp_conc': unk['interp_conc'], 'level': level})
        for (sname, grp, plate), entries in sorted(qc_groups.items()):
            sigs = [e['signal'] for e in entries if np.isfinite(e['signal'])]
            concs = [e['interp_conc'] for e in entries if np.isfinite(e['interp_conc'])]
            level = entries[0]['level']
            qc_factor = qc_dilution_factors[level]
            avg_sig = np.mean(sigs) if sigs else np.nan
            avg_conc = np.mean(concs) if concs else np.nan
            corrected = avg_conc * qc_factor if np.isfinite(avg_conc) else np.nan
            recovery = (corrected / qc_expected_concentrations * 100
                        if qc_expected_concentrations and np.isfinite(corrected) else np.nan)
            row = {'sample_name': sname, 'level': level, 'plate': plate, 'group': grp,
                   'avg_signal': avg_sig, 'corrected_conc': corrected, 'recovery': recovery}
            qc_summary_rows.append(row)
            if np.isfinite(corrected) and np.isfinite(avg_sig) and corrected > 0 and avg_sig > 0:
                qc_overlay_points.append({**row, 'signal': avg_sig})

    # Pre-generate all charts before Excel writing (sequential — matplotlib mathtext is not thread-safe)
    overlay_path = generate_overlay_chart(
        results, tmp_dir,
        qc_overlay_points if qc_overlay_points else None,
        qc_expected_concentrations if qc_expected_concentrations else None
    )
    chart_map = {id(res): generate_std_curve_chart(res, tmp_dir, lloq_method) for res in results}

    unit_suffix = f" ({units})" if units else ""
    interp_header = f"Interp. Conc.{unit_suffix}"
    avg_interp_header = f"Avg Interp. Conc.{unit_suffix}"
    corrected_header = f"Corrected Avg Interp. Conc.{unit_suffix}"
    cv_threshold = float(cv_threshold) if cv_threshold is not None else 25.0
    plate_dilution_factors = plate_dilution_factors or {}
    lloq_method = lloq_method or 'current'

    # ── Summary Sheet ─────────────────────────────────────────────────
    ws = wb.create_sheet("Summary")
    headers = ["Plate", "Spot", "Group", "Min (a)", "Hill Slope (b)", "EC50 (c)", "Max (d)", "LLOQ Signal", "LLOQ Conc", "R²", "Flags", "Status"]
    _header_row(ws, 1, headers)

    for ri, res in enumerate(results, 2):
        vals = [res['plate'], res['spot'], res.get('group', '')]
        if res['params'] is not None:
            a, b, c, d = res['params']
            vals += [round(a, 2), round(b, 4), round(c, 4), round(d, 2)]
        else:
            vals += ["N/A"] * 4

        # Calculate LLOQ
        lloq_sig_val = "N/A"
        lloq_conc_val = "N/A"
        lloq_sig = res.get('lloq_sig')
        if lloq_sig is not None:
            lloq_sig_val = round(lloq_sig, 1)
            if res['params'] is not None:
                try:
                    lloq_conc = inverse_4pl(lloq_sig, *res['params'])
                    if np.isfinite(lloq_conc) and lloq_conc > 0:
                        lloq_conc_val = round(lloq_conc, 4)
                except:
                    pass
        vals.append(lloq_sig_val)
        vals.append(lloq_conc_val)

        if res['params'] is not None:
            vals += [round(res['r2'], 6)]
            flag_text = "No standards" if res.get('no_standards') else ""
            vals.append(flag_text)
            vals.append("Good" if res['r2'] >= 0.99 else ("Acceptable" if res['r2'] >= 0.95 else "Poor"))
        else:
            vals += ["N/A", "", "Failed"]

        for ci, v in enumerate(vals, 1):
            ws.cell(row=ri, column=ci, value=v)
        status = ws.cell(row=ri, column=12)
        status.font = PASS_FONT if status.value == "Good" else (WARN_FONT if status.value == "Acceptable" else FAIL_FONT)
        _style_row(ws, ri, len(headers))

    for ci in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(ci)].width = 16

    # ── QC Recovery Table on Summary sheet ───────────────────────────
    next_row = len(results) + 3
    if qc_summary_rows:
        _section_title(ws, next_row, "QC Recovery")
        next_row += 1
        qc_h = ["Sample Name", "Level", "Plate", "Group", "Avg Signal",
                corrected_header, "Expected Conc.", "% Recovery"]
        _header_row(ws, next_row, qc_h)
        next_row += 1
        for qr in qc_summary_rows:
            ws.cell(row=next_row, column=1, value=qr['sample_name'])
            ws.cell(row=next_row, column=2, value=qr['level'])
            ws.cell(row=next_row, column=3, value=qr['plate'])
            ws.cell(row=next_row, column=4, value=qr['group'] or "")
            sig_cell = ws.cell(row=next_row, column=5,
                               value=round(qr['avg_signal'], 1) if np.isfinite(qr['avg_signal']) else "N/A")
            sig_cell.number_format = '#,##0'
            corr_cell = ws.cell(row=next_row, column=6,
                                value=round(qr['corrected_conc'], 4) if np.isfinite(qr['corrected_conc']) else "N/A")
            corr_cell.number_format = '#,##0.0000'
            exp_cell = ws.cell(row=next_row, column=7,
                               value=qc_expected_concentrations if qc_expected_concentrations else "")
            if qc_expected_concentrations:
                exp_cell.number_format = '#,##0.0###'
            rec_cell = ws.cell(row=next_row, column=8)
            if np.isfinite(qr['recovery']):
                rec_cell.value = round(qr['recovery'], 1)
                rec_cell.number_format = '0.0'
                rec_cell.font = PASS_FONT if 70.0 <= qr['recovery'] <= 130.0 else FAIL_FONT
            else:
                rec_cell.value = "N/A"
            _style_row(ws, next_row, len(qc_h))
            next_row += 1
        for ci, w in enumerate([18, 10, 8, 10, 14, 24, 16, 12], 1):
            ws.column_dimensions[get_column_letter(ci)].width = w
        next_row += 1  # blank row before chart

    # Overlay chart of all curves on Summary sheet (pre-generated above)
    if overlay_path:
        overlay_row = next_row + 1
        img = XlImage(overlay_path)
        img.width = 900
        img.height = 560
        ws.add_image(img, f"A{overlay_row}")

    # ── Per-Spot Detail Sheets ────────────────────────────────────────
    added_plates = set()
    for res in results:
        spot, plate = res['spot'], res['plate']
        group = res.get('group', '')
        g_suffix = f"_{group}" if group else ""
        sname = f"P{plate}_S{spot}{g_suffix}"[:31]
        ws = wb.create_sheet(sname)
        row = 1

        title_str = f"4PL Curve Fit — Plate {plate}, Spot {spot}"
        if group:
            title_str += f", Group {group}"
        _section_title(ws, row, title_str)
        row += 1
        param_names = ["Min (a)", "Hill Slope (b)", "EC50 (c)", "Max (d)", "R²"]
        for i, pname in enumerate(param_names):
            ws.cell(row=row, column=1, value=pname).font = BOLD_FONT
            ws.cell(row=row, column=1).border = THIN_BORDER
            if res['params'] is not None:
                val = res['params'][i] if i < 4 else res['r2']
                ws.cell(row=row, column=2, value=round(val, 6))
            else:
                ws.cell(row=row, column=2, value="N/A")
            ws.cell(row=row, column=2).font = DATA_FONT
            ws.cell(row=row, column=2).border = THIN_BORDER
            ws.cell(row=row, column=2).number_format = '0.000000'
            row += 1

        # Standards table (grouped by concentration = mean of replicates)
        row += 1
        _section_title(ws, row, "Standard Curve Data")
        row += 1
        _header_row(ws, row, ["Well(s)", "Concentration", "Mean Signal", "Fitted Signal", "% Recovery"])
        row += 1

        std_groups = {}
        for s in res.get('standards', []):
            key = s['conc']
            if key not in std_groups:
                std_groups[key] = {'wells': [], 'signals': [], 'conc': key}
            std_groups[key]['wells'].append(s['well'])
            std_groups[key]['signals'].append(s['signal'])

        for sg in sorted(std_groups.values(), key=lambda x: x['conc'], reverse=True):
            mean_sig = np.mean(sg['signals'])
            fitted = four_pl(sg['conc'], *res['params']) if res['params'] is not None else None
            recovery = (fitted / mean_sig * 100) if fitted and mean_sig != 0 else None

            ws.cell(row=row, column=1, value=', '.join(sg['wells']))
            ws.cell(row=row, column=2, value=sg['conc'])
            ws.cell(row=row, column=2).number_format = '#,##0.00'
            ws.cell(row=row, column=3, value=round(mean_sig, 1))
            ws.cell(row=row, column=3).number_format = '#,##0.0'
            ws.cell(row=row, column=4, value=round(fitted, 1) if fitted else "N/A")
            ws.cell(row=row, column=4).number_format = '#,##0.0'
            if recovery:
                ws.cell(row=row, column=5, value=round(recovery, 1))
                ws.cell(row=row, column=5).number_format = '0.0'
            _style_row(ws, row, 5, fill=STD_FILL)
            row += 1

        # Individual standard points data (kept in columns G-I for reference)
        ind_start = row + 1
        ws.cell(row=ind_start, column=7, value="Conc").font = BOLD_FONT
        ws.cell(row=ind_start, column=8, value="Signal").font = BOLD_FONT
        ws.cell(row=ind_start, column=9, value="Fitted").font = BOLD_FONT
        irow = ind_start + 1
        for s in sorted(res.get('standards', []), key=lambda x: x['conc']):
            if s['conc'] > 0 and s['signal'] > 0:
                ws.cell(row=irow, column=7, value=s['conc'])
                ws.cell(row=irow, column=8, value=s['signal'])
                if res['params'] is not None:
                    fitted_val = four_pl(s['conc'], *res['params'])
                    if fitted_val > 0:
                        ws.cell(row=irow, column=9, value=round(fitted_val, 1))
                irow += 1

        # Blanks
        if res.get('blanks'):
            row += 2
            _section_title(ws, row, "Blanks / Background", 3)
            row += 1
            _header_row(ws, row, ["Well", "Sample Name", "Signal"])
            row += 1
            for bl in res['blanks']:
                ws.cell(row=row, column=1, value=bl['well'])
                ws.cell(row=row, column=2, value=bl.get('sample_name', ''))
                ws.cell(row=row, column=3, value=bl['signal'])
                _style_row(ws, row, 3, fill=BLANK_FILL)
                row += 1

        # Unknowns
        row += 2
        _section_title(ws, row, "Interpolated Unknowns")
        row += 1
        _header_row(ws, row, ["Well", "Sample Name", "Signal", interp_header, "Flag"])
        row += 1

        std_concs = [s['conc'] for s in res.get('standards', []) if s['conc'] > 0]
        uloq = max(std_concs) if std_concs else None
        lloq = min(std_concs) if std_concs else None

        # Use pre-computed LLOQ signal for this spot
        lloq_sig = res.get('lloq_sig')

        for unk in res.get('unknowns', []):
            ws.cell(row=row, column=1, value=unk['well'])
            ws.cell(row=row, column=2, value=unk.get('sample_name', ''))
            ws.cell(row=row, column=3, value=unk['signal'])
            ws.cell(row=row, column=3).number_format = '#,##0'
            c_val = unk['interp_conc']
            if c_val is not None and np.isfinite(c_val):
                ws.cell(row=row, column=4, value=round(c_val, 4))
                ws.cell(row=row, column=4).number_format = '#,##0.0000'
                # Check signal against LLOQ signal threshold first
                if lloq_sig and unk['signal'] < lloq_sig:
                    ws.cell(row=row, column=5, value="< LLOQ")
                    ws.cell(row=row, column=5).font = WARN_FONT
                elif uloq and c_val > uloq:
                    ws.cell(row=row, column=5, value="> ULOQ")
                    ws.cell(row=row, column=5).font = WARN_FONT
                elif lloq and c_val < lloq:
                    ws.cell(row=row, column=5, value="< LLOQ")
                    ws.cell(row=row, column=5).font = WARN_FONT
                else:
                    ws.cell(row=row, column=5, value="In Range")
                    ws.cell(row=row, column=5).font = PASS_FONT
            else:
                ws.cell(row=row, column=4, value="N/A")
                ws.cell(row=row, column=5, value="Out of Range")
                ws.cell(row=row, column=5).font = FAIL_FONT
            _style_row(ws, row, 5, fill=UNK_FILL)
            row += 1

        # Chart — matplotlib image (pre-generated in parallel)
        chart_path = chart_map.get(id(res))
        if chart_path:
            row += 2
            img = XlImage(chart_path)
            img.width = 680
            img.height = 470
            ws.add_image(img, f"A{row}")

        # Plate Map — add to the first sheet for each plate
        if plate not in added_plates and plate in raw_plate_blocks:
            row += 20  # Leave space after chart
            _section_title(ws, row, f"Plate {plate} Map")
            row += 1
            block_lines = raw_plate_blocks[plate]
            for ri, line in enumerate(block_lines, 1):
                parts = line.strip().split(',')
                for ci, part in enumerate(parts, 1):
                    ws.cell(row=row + ri - 1, column=ci, value=part.strip())
            # Adjust row counter
            row += len(block_lines)
            added_plates.add(plate)

        for ci, w in enumerate([14, 18, 14, 16, 14, 2, 14, 14, 14], 1):
            ws.column_dimensions[get_column_letter(ci)].width = w

    # ── All Unknowns Combined ─────────────────────────────────────────
    ws_all = wb.create_sheet("All Unknowns")
    all_h = ["Sample Name", "Animal", "Tissue", "Plate", "Wells", "Avg Signal", avg_interp_header,
             "%CV", "Flag", "Dilution Factor", corrected_header, "Total Protein", "Normalized Protein Concentration"]
    _header_row(ws_all, 1, all_h)
    arow = 2
    # Track how many TP values have been consumed per (animal, tissue) key
    tp_index = defaultdict(int)

    # Collect all standards for uloq/lloq
    all_std_concs = set()
    for res in results:
        for s in res.get('standards', []):
            if s['conc'] > 0:
                all_std_concs.add(s['conc'])
    uloq = max(all_std_concs) if all_std_concs else None
    lloq = min(all_std_concs) if all_std_concs else None

    # Use pre-computed LLOQ signals; take the mean across all spots as a global threshold
    cached_lloq_sigs = [r['lloq_sig'] for r in results if r.get('lloq_sig') is not None]
    all_lloq_sig = float(np.mean(cached_lloq_sigs)) if cached_lloq_sigs else None

    # Group unknowns by (sample_name, group, plate)
    unknown_groups = defaultdict(list)
    for res in results:
        curve_group = res.get('group', '')
        plate = res['plate']
        for unk in res.get('unknowns', []):
            sample_name = unk.get('sample_name', '')
            key = (sample_name, curve_group, plate)
            unknown_groups[key].append({
                'well': unk['well'],
                'signal': unk['signal'],
                'interp_conc': unk['interp_conc']
            })

    for (sample_name, curve_group, plate) in sorted(unknown_groups.keys()):
        if _identify_qc_level(sample_name):
            continue  # QC samples reported in Summary sheet QC Recovery table
        group = unknown_groups[(sample_name, curve_group, plate)]
        signals = [g['signal'] for g in group if np.isfinite(g['signal'])]
        concs = [g['interp_conc'] for g in group if np.isfinite(g['interp_conc'])]
        avg_signal = np.mean(signals) if signals else np.nan
        avg_conc = np.mean(concs) if concs else np.nan
        wells = ', '.join(sorted(g['well'] for g in group))

        # Determine dilution factor: QC-specific factor takes priority over plate factor
        qc_level = _identify_qc_level(sample_name) if qc_dilution_factors else None
        if qc_level and qc_level in (qc_dilution_factors or {}):
            factor = qc_dilution_factors[qc_level]
            is_qc_factor = True
        else:
            factor = plate_dilution_factors.get(plate, 1.0)
            is_qc_factor = plate in plate_dilution_factors

        corrected_conc = avg_conc * factor if np.isfinite(avg_conc) else np.nan

        flag = ""
        if np.isfinite(avg_signal) and all_lloq_sig and avg_signal < all_lloq_sig:
            flag = "< LLOQ"
        elif np.isfinite(avg_conc):
            if uloq and avg_conc > uloq:
                flag = "> ULOQ"
            elif lloq and avg_conc < lloq:
                flag = "< LLOQ"
            else:
                flag = "In Range"
        else:
            flag = "Out of Range"

        cv = np.nan
        if len(concs) > 1 and np.isfinite(avg_conc) and avg_conc != 0:
            cv = np.std(concs, ddof=1) / avg_conc * 100

        animal, tissue = _extract_animal_tissue(sample_name)
        ws_all.cell(row=arow, column=1, value=sample_name)
        ws_all.cell(row=arow, column=2, value=animal or "")
        ws_all.cell(row=arow, column=3, value=tissue or "")
        ws_all.cell(row=arow, column=4, value=plate)
        ws_all.cell(row=arow, column=5, value=wells)
        ws_all.cell(row=arow, column=6, value=round(avg_signal, 1) if np.isfinite(avg_signal) else "N/A")
        ws_all.cell(row=arow, column=6).number_format = '#,##0'
        ws_all.cell(row=arow, column=7, value=round(avg_conc, 4) if np.isfinite(avg_conc) else "N/A")
        ws_all.cell(row=arow, column=7).number_format = '#,##0.0000'
        # %CV (col 8)
        cv_cell = ws_all.cell(row=arow, column=8)
        cv_cell.value = round(cv, 1) if np.isfinite(cv) else "N/A"
        cv_cell.number_format = '0.0'
        if np.isfinite(cv):
            cv_cell.fill = CV_BAD_FILL if cv > cv_threshold else CV_GOOD_FILL
        # Flag (col 9)
        ws_all.cell(row=arow, column=9, value=flag)
        cell_flag = ws_all.cell(row=arow, column=9)
        cell_flag.font = PASS_FONT if flag == "In Range" else (WARN_FONT if flag in ["> ULOQ", "< LLOQ"] else FAIL_FONT)
        # Dilution Factor (col 10)
        df_cell = ws_all.cell(row=arow, column=10)
        has_factor = is_qc_factor or (plate in plate_dilution_factors)
        df_cell.value = factor if has_factor else ""
        if has_factor:
            df_cell.number_format = '0.###'
        # Corrected Avg Interp. Conc. (col 11)
        corrected_cell = ws_all.cell(row=arow, column=11)
        corrected_cell.value = round(corrected_conc, 4) if np.isfinite(corrected_conc) else "N/A"
        corrected_cell.number_format = '#,##0.0000'
        # Total Protein (col 12) — consume values in order of appearance per (animal, tissue)
        tp_val = None
        tp_cell = ws_all.cell(row=arow, column=12)
        if total_protein_map and animal:
            tp_key = (animal, tissue)
            tp_list = total_protein_map.get(tp_key, [])
            idx = tp_index[tp_key]
            if idx < len(tp_list):
                tp_val = tp_list[idx]
                tp_index[tp_key] += 1
                tp_cell.value = round(tp_val, 4)
                tp_cell.number_format = '0.0000'
        # Normalized Protein Concentration (col 13)
        norm_cell = ws_all.cell(row=arow, column=13)
        if tp_val is not None and np.isfinite(corrected_conc) and tp_val != 0:
            norm_cell.value = round(corrected_conc / tp_val, 6)
            norm_cell.number_format = '0.000000'
        _style_row(ws_all, arow, len(all_h))
        arow += 1

    for ci in range(1, len(all_h) + 1):
        ws_all.column_dimensions[get_column_letter(ci)].width = 20

    # ── MSD Data Sheet ──────────────────────────────────────────────────────
    ws_msd = wb.create_sheet("MSD Data")
    with open(msd_path, 'r', encoding='utf-8', errors='replace') as f:
        msd_content = f.read()
    ws_msd.cell(row=1, column=1, value=msd_content)
    ws_msd.cell(row=1, column=1).alignment = Alignment(wrap_text=True, horizontal='left', vertical='top')
    ws_msd.column_dimensions['A'].width = 100

    wb.save(output_path)
    print(f"Saved: {output_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def _open_file(path):
    """Open a file with the system default application (cross-platform)."""
    try:
        if platform.system() == 'Darwin':
            subprocess.Popen(['open', path])
        elif platform.system() == 'Windows':
            os.startfile(path)
        else:
            subprocess.Popen(['xdg-open', path])
    except Exception as e:
        print(f"Note: could not auto-open file: {e}")

def run_analysis(msd_path, platemap_path, output_path, spots_override=None, units=None, cv_threshold=25, dilution_factors=None, lloq_method='current', total_protein_path=None, qc_dilution_factors=None, qc_expected_concentrations=None):
    print("=" * 60)
    print("MSD 4PL ANALYSIS")
    print("=" * 60)

    print(f"\nParsing MSD file: {msd_path}")
    plates = parse_msd_file(msd_path)
    for p in plates:
        n = spots_override if spots_override else p['spots_per_well']
        p['spots_per_well'] = n
        print(f"  Plate {p['plate_num']}: {len(p['data'])} wells x {n} spots")

    print(f"\nParsing plate map: {platemap_path}")
    plate_maps, raw_plate_blocks = parse_plate_map_grid(platemap_path)
    n_plate_maps = len(plate_maps)
    print(f"  Found {n_plate_maps} plate map(s)")

    if len(plates) != n_plate_maps:
        print(f"Error: MSD file contains {len(plates)} plate(s), but plate map contains {n_plate_maps} plate map(s).\n" \
              "MSD and plate map files must have the same number of plates.")
        sys.exit(1)

    try:
        plate_dilution_factors = parse_plate_dilution_factors(dilution_factors, len(plates))
    except ValueError as e:
        print(f"Error parsing dilution factors: {e}")
        sys.exit(1)

    # Build per-plate well lookups
    plate_well_maps = {}
    for pm_num, entries in plate_maps.items():
        plate_well_maps[pm_num] = {normalize_well(e['well']): e for e in entries}

    results = []
    for plate_data in plates:
        pnum = plate_data['plate_num']
        n_spots = plate_data['spots_per_well']
        wd = plate_data['data']

        # Match plate map: use matching plate number, or fall back to plate 1 if only one map
        if pnum in plate_well_maps:
            well_map = plate_well_maps[pnum]
        elif n_plate_maps == 1:
            well_map = list(plate_well_maps.values())[0]
            print(f"\n  (Using single plate map for MSD Plate {pnum})")
        else:
            print(f"\n  ⚠ No plate map found for MSD Plate {pnum} — skipping")
            continue

        for spot_idx in range(n_spots):
            spot_num = spot_idx + 1

            # Collect all well data for this spot, tagged with group
            spot_wells = []
            for well_id, spot_signals in wd.items():
                nw = normalize_well(well_id)
                if spot_idx >= len(spot_signals):
                    continue
                signal = spot_signals[spot_idx]
                info = well_map.get(nw)
                if info is None:
                    continue
                spot_wells.append({
                    'well': nw, 'signal': signal,
                    'sample_type': info['sample_type'],
                    'concentration': info.get('concentration', np.nan),
                    'sample_name': info.get('sample_name', ''),
                    'group': info.get('group', '_default')
                })

            # Determine unique groups on this plate
            groups = sorted(set(w['group'] for w in spot_wells if w['group'] != '_default'))
            if not groups:
                groups = ['_default']

            for group in groups:
                group_label = group if group != '_default' else ''
                label = f"Plate {pnum}, Spot {spot_num}" + (f", Group {group}" if group_label else "")
                print(f"\n── {label} ──")

                # Partition: standards/unknowns/blanks for this group
                # Blanks with '_default' group are shared across all groups
                standards, unknowns, blanks = [], [], []
                for w in spot_wells:
                    wg = w['group']
                    stype = w['sample_type']

                    if stype == 'Blank' and (wg == group or wg == '_default'):
                        blanks.append({'well': w['well'], 'signal': w['signal'],
                                       'sample_name': w['sample_name']})
                    elif wg != group:
                        continue
                    elif stype == 'Standard':
                        conc = w['concentration']
                        if pd.notna(conc) and conc > 0:
                            standards.append({'well': w['well'], 'conc': conc, 'signal': w['signal']})
                    elif stype == 'Unknown':
                        unknowns.append({'well': w['well'], 'signal': w['signal'],
                                         'sample_name': w['sample_name']})

                no_standards = not bool(standards)
                if no_standards:
                    print("  ⚠ No standards detected for this curve")
                    params, r2 = None, None
                else:
                    conc_list = [s['conc'] for s in standards] + [0] * len(blanks)
                    signal_list = [s['signal'] for s in standards] + [b['signal'] for b in blanks]
                    params, r2 = fit_4pl(conc_list, signal_list)

                if params is not None:
                    a, b, c, d = params
                    print(f"  a={a:.1f}  b={b:.4f}  c={c:.2f}  d={d:.1f}  R²={r2:.6f}")
                    for u in unknowns:
                        try:
                            u['interp_conc'] = inverse_4pl(u['signal'], *params)
                        except:
                            u['interp_conc'] = np.nan
                else:
                    if not no_standards:
                        print("  ⚠ Curve fit FAILED")
                    for u in unknowns:
                        u['interp_conc'] = np.nan

                unknowns.sort(key=lambda x: (x['well'][0], int(re.search(r'\d+', x['well']).group())))

                blank_sigs = [b['signal'] for b in blanks if np.isfinite(b['signal'])]
                lloq_sig_cached = calculate_lloq_signal(blank_sigs, lloq_method)
                results.append({
                    'plate': pnum, 'spot': spot_num, 'group': group_label,
                    'params': params, 'r2': r2,
                    'standards': sorted(standards, key=lambda x: x['conc']),
                    'unknowns': unknowns, 'blanks': blanks,
                    'no_standards': no_standards,
                    'lloq_sig': lloq_sig_cached
                })

    missing = [r for r in results if r.get('no_standards')]
    if missing:
        print("Error: Standards are missing for one or more curves. Analysis cannot continue.")
        for r in missing:
            label = f"Plate {r['plate']}, Spot {r['spot']}"
            if r.get('group'):
                label += f", Group {r['group']}"
            print(f"  - {label}")
        sys.exit(1)

    # Parse total protein CSV if provided
    total_protein_map = None
    if total_protein_path:
        try:
            total_protein_map = parse_total_protein_csv(total_protein_path)
            print(f"\nLoaded total protein data: {len(total_protein_map)} animal/tissue entries")
        except Exception as e:
            print(f"Warning: could not load total protein CSV: {e}")

    print(f"\n{'=' * 60}")
    print(f"Generating Excel: {output_path}")
    create_output(results, output_path, msd_path, raw_plate_blocks, units, cv_threshold, plate_dilution_factors, lloq_method, total_protein_map, qc_dilution_factors, qc_expected_concentrations)
    print("Done!")
    _open_file(output_path)

    # Save last run parameters
    last_args = {
        'msd': msd_path,
        'platemap': platemap_path,
        'output': output_path,
        'spots': spots_override,
        'units': units,
        'cv_threshold': cv_threshold,
        'dilution_factors': list(plate_dilution_factors.values()) if plate_dilution_factors else None,
        'lloq_method': lloq_method,
        'total_protein': total_protein_path,
        'qc_dilution_factors': qc_dilution_factors,
        'qc_expected_concentrations': qc_expected_concentrations
    }
    _save_run_to_history(last_args)


def run_interactive():
    """Launch a single-page GUI for configuring all analysis options."""
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
    import json
    import os

    def browse_file(var, title, filetypes):
        filename = filedialog.askopenfilename(title=title, filetypes=filetypes)
        if filename:
            var.set(filename)

    def browse_save(var, title, defaultextension, filetypes, initialfile):
        filename = filedialog.asksaveasfilename(title=title, defaultextension=defaultextension, filetypes=filetypes, initialfile=initialfile)
        if filename:
            var.set(filename)

    def _apply_run_entry(entry):
        """Populate all form fields from a history entry dict."""
        msd_var.set(entry.get('msd', ''))
        platemap_var.set(entry.get('platemap', ''))
        output_var.set(entry.get('output', 'msd_4pl_results.xlsx'))
        spots_var.set(str(entry.get('spots') or ''))
        units_var.set(entry.get('units') or '')
        cv_threshold_var.set(str(entry.get('cv_threshold') or '25'))
        lloq_method_var.set(entry.get('lloq_method') or 'current')
        dilution_factors_var.set(
            ','.join(str(x) for x in entry['dilution_factors'])
            if entry.get('dilution_factors') else '')
        total_protein_var.set(entry.get('total_protein') or '')
        saved_qc = entry.get('qc_dilution_factors') or {}
        for level in QC_LEVELS:
            qc_df_vars[level].set(str(saved_qc[level]) if saved_qc.get(level) is not None else '')
        saved_exp = entry.get('qc_expected_concentrations')
        qc_exp_var.set(str(saved_exp) if saved_exp is not None else '')

    def load_selected_run():
        sel = history_lb.curselection()
        if not sel:
            messagebox.showinfo("No Selection", "Please click a run in the list to select it.")
            return
        idx = sel[0]
        history = _load_run_history()
        if idx >= len(history):
            return
        _apply_run_entry(history[idx])

    def run():
        msd_path = msd_var.get().strip()
        platemap_path = platemap_var.get().strip()
        output_path = output_var.get().strip()
        spots_override = spots_var.get().strip()
        units = units_var.get().strip()
        cv_threshold = cv_threshold_var.get().strip()
        lloq_method = lloq_method_var.get()
        dilution_factors = dilution_factors_var.get().strip()
        total_protein_path = total_protein_var.get().strip()

        if not msd_path or not platemap_path or not output_path:
            messagebox.showerror("Error", "Please select MSD file, plate map, and output location.")
            return

        spots_override = int(spots_override) if spots_override and spots_override in ('1', '4', '10') else None
        units = units if units else None
        cv_threshold = float(cv_threshold) if cv_threshold else None
        dilution_factors = dilution_factors if dilution_factors else None
        total_protein_path = total_protein_path if total_protein_path else None

        # Collect QC dilution factors
        qc_dilution_factors = {}
        for level in QC_LEVELS:
            val_str = qc_df_vars[level].get().strip()
            if val_str:
                try:
                    qc_dilution_factors[level] = float(val_str)
                except ValueError:
                    messagebox.showerror("Error", f"Invalid QC dilution factor for {level}: '{val_str}'")
                    return
        qc_dilution_factors = qc_dilution_factors if qc_dilution_factors else None

        # Collect single expected QC concentration
        qc_expected_concentrations = None
        val_str = qc_exp_var.get().strip()
        if val_str:
            try:
                qc_expected_concentrations = float(val_str)
            except ValueError:
                messagebox.showerror("Error", f"Invalid expected QC concentration: '{val_str}'")
                return

        root.destroy()

        print(f"\nMSD file:   {msd_path}")
        print(f"Plate map:  {platemap_path}")
        print(f"Output:     {output_path}")
        if spots_override:
            print(f"Spots:      {spots_override} (manual override)")
        else:
            print("Spots:      auto-detect")
        if units:
            print(f"Units:      {units}")
        if cv_threshold is not None:
            print(f"CV threshold: {cv_threshold}")
        if dilution_factors:
            print(f"Dilution factors: {dilution_factors}")
        print(f"LLOQ method: {lloq_method}")
        if qc_dilution_factors:
            print(f"QC dilution factors: {qc_dilution_factors}")
        if qc_expected_concentrations:
            print(f"QC expected concentrations: {qc_expected_concentrations}")

        run_analysis(msd_path, platemap_path, output_path, spots_override, units, cv_threshold, dilution_factors, lloq_method, total_protein_path, qc_dilution_factors, qc_expected_concentrations)

    # ── Window setup ───────────────────────────────────────────────────
    root = tk.Tk()
    root.title("MSD 4PL Analysis Tool")
    root.geometry("860x720")
    root.minsize(760, 620)
    root.resizable(True, True)

    # ── Variables ──────────────────────────────────────────────────────
    msd_var = tk.StringVar()
    platemap_var = tk.StringVar()
    output_var = tk.StringVar(value="msd_4pl_results.xlsx")
    spots_var = tk.StringVar()
    units_var = tk.StringVar()
    cv_threshold_var = tk.StringVar(value="25")
    lloq_method_var = tk.StringVar(value="current")
    dilution_factors_var = tk.StringVar()
    total_protein_var = tk.StringVar()
    qc_df_vars = {level: tk.StringVar() for level in QC_LEVELS}
    qc_exp_var = tk.StringVar()

    # ── Header banner ──────────────────────────────────────────────────
    SLATE  = '#3a506b'   # soft slate-blue — clean, not corporate-heavy
    SLATE_LIGHT = '#c8d8e8'
    header_canvas = tk.Canvas(root, height=58, bg=SLATE, highlightthickness=0)
    header_canvas.pack(fill=tk.X, side=tk.TOP)
    header_canvas.create_text(18, 20, anchor='w', text='MSD 4PL Analysis Tool',
                              fill='white', font=('Helvetica', 16, 'bold'))
    header_canvas.create_text(18, 42, anchor='w',
                              text='4-Parameter Logistic Curve Fitting  ·  Quantitative Analysis',
                              fill=SLATE_LIGHT, font=('Helvetica', 10))

    # Thin accent rule below header
    tk.Canvas(root, height=2, bg='#7ba7bc', highlightthickness=0).pack(fill=tk.X)

    # ── Main content area (expands with window) ────────────────────────
    outer = ttk.Frame(root, padding='12 10 12 6')
    outer.pack(fill=tk.BOTH, expand=True)
    outer.columnconfigure(0, weight=1)

    # helper: consistent row padding inside LabelFrames
    _rp = {'pady': 4, 'padx': 2}

    # ── Input Files ────────────────────────────────────────────────────
    files_lf = ttk.LabelFrame(outer, text='Input Files', padding='10 6')
    files_lf.pack(fill=tk.X, pady=(0, 8))
    files_lf.columnconfigure(1, weight=1)

    def _file_row(parent, row, label, var, btn_cmd, btn2_cmd=None):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky=tk.W,
                                           padx=(0, 10), **_rp)
        ttk.Entry(parent, textvariable=var, width=54).grid(row=row, column=1,
                                                            sticky=tk.EW, **_rp)
        ttk.Button(parent, text='Browse…', command=btn_cmd, width=8).grid(
            row=row, column=2, padx=(6, 0), **_rp)

    _file_row(files_lf, 0, 'MSD Data File:', msd_var,
              lambda: browse_file(msd_var, 'Select MSD Data File',
                                  [('MSD Text Files', '*.txt'), ('All Files', '*.*')]))
    _file_row(files_lf, 1, 'Plate Map CSV:', platemap_var,
              lambda: browse_file(platemap_var, 'Select Plate Map CSV',
                                  [('CSV Files', '*.csv'), ('All Files', '*.*')]))
    _file_row(files_lf, 2, 'Output Excel:', output_var,
              lambda: browse_save(output_var, 'Save Results As', '.xlsx',
                                  [('Excel Files', '*.xlsx')], 'msd_4pl_results.xlsx'))

    # ── Analysis Options ───────────────────────────────────────────────
    opts_lf = ttk.LabelFrame(outer, text='Analysis Options', padding='10 6')
    opts_lf.pack(fill=tk.X, pady=(0, 8))

    # Row 0 — Spots | Units
    ttk.Label(opts_lf, text='Spots per Well:').grid(row=0, column=0, sticky=tk.W, **_rp)
    spots_e = ttk.Entry(opts_lf, textvariable=spots_var, width=8)
    spots_e.grid(row=0, column=1, sticky=tk.W, padx=(0, 20), **_rp)
    ttk.Label(opts_lf, text='1, 4, 10 or blank', foreground='grey').grid(
        row=0, column=2, sticky=tk.W, padx=(0, 30), **_rp)
    ttk.Label(opts_lf, text='Units:').grid(row=0, column=3, sticky=tk.W, **_rp)
    ttk.Entry(opts_lf, textvariable=units_var, width=14).grid(
        row=0, column=4, sticky=tk.W, **_rp)
    ttk.Label(opts_lf, text='e.g. pg/mL', foreground='grey').grid(
        row=0, column=5, sticky=tk.W, padx=(4, 0), **_rp)

    # Row 1 — %CV | Dilution Factors
    ttk.Label(opts_lf, text='%CV Threshold:').grid(row=1, column=0, sticky=tk.W, **_rp)
    ttk.Entry(opts_lf, textvariable=cv_threshold_var, width=8).grid(
        row=1, column=1, sticky=tk.W, padx=(0, 20), **_rp)
    ttk.Label(opts_lf, text='Plate Dilution Factors:').grid(
        row=1, column=3, sticky=tk.W, **_rp)
    ttk.Entry(opts_lf, textvariable=dilution_factors_var, width=22).grid(
        row=1, column=4, columnspan=2, sticky=tk.W, **_rp)

    # Row 2 — LLOQ Method
    ttk.Label(opts_lf, text='LLOQ Method:').grid(row=2, column=0, sticky=tk.W, **_rp)
    lloq_inner = ttk.Frame(opts_lf)
    lloq_inner.grid(row=2, column=1, columnspan=5, sticky=tk.W, **_rp)
    ttk.Radiobutton(lloq_inner, text='Mean + 10×SD (current)',
                    variable=lloq_method_var, value='current').pack(side=tk.LEFT)
    ttk.Radiobutton(lloq_inner, text='3× Blank Mean',
                    variable=lloq_method_var, value='3xblank').pack(side=tk.LEFT, padx=(16, 0))

    # Row 3 — Total Protein (spans full width)
    ttk.Label(opts_lf, text='Total Protein CSV:').grid(row=3, column=0, sticky=tk.W, **_rp)
    ttk.Entry(opts_lf, textvariable=total_protein_var, width=46).grid(
        row=3, column=1, columnspan=4, sticky=tk.W, **_rp)
    ttk.Button(opts_lf, text='Browse…', width=8,
               command=lambda: browse_file(
                   total_protein_var, 'Select Total Protein CSV',
                   [('CSV Files', '*.csv'), ('All Files', '*.*')])).grid(
        row=3, column=5, padx=(6, 0), **_rp)

    # ── QC Controls ────────────────────────────────────────────────────
    qc_lf = ttk.LabelFrame(outer, text='QC Controls  (optional — samples containing ULOQ / HQC / MQC / LQC / LLOQ)',
                            padding='10 6')
    qc_lf.pack(fill=tk.X, pady=(0, 8))

    # Level headers
    for ci, level in enumerate(QC_LEVELS):
        ttk.Label(qc_lf, text=level, font=('TkDefaultFont', 9, 'bold'),
                  anchor='center').grid(row=0, column=ci + 1, padx=8, pady=(0, 2))

    # Dilution factor row
    ttk.Label(qc_lf, text='Dilution Factor:').grid(row=1, column=0, sticky=tk.W,
                                                    padx=(0, 10), pady=2)
    for ci, level in enumerate(QC_LEVELS):
        ttk.Entry(qc_lf, textvariable=qc_df_vars[level], width=9).grid(
            row=1, column=ci + 1, padx=8, pady=2)

    # Expected concentration row
    exp_inner = ttk.Frame(qc_lf)
    exp_inner.grid(row=2, column=0, columnspan=6, sticky=tk.W, pady=(6, 2))
    ttk.Label(exp_inner, text='Expected Conc. (all QC):').pack(side=tk.LEFT, padx=(0, 8))
    ttk.Entry(exp_inner, textvariable=qc_exp_var, width=12).pack(side=tk.LEFT)
    ttk.Label(exp_inner, text='  ·  ±30% acceptance band plotted on overlay chart',
              foreground='grey').pack(side=tk.LEFT, padx=(6, 0))

    # ── Previous Runs (expands vertically when window is resized) ──────
    hist_lf = ttk.LabelFrame(outer, text='Previous Runs', padding='10 6')
    hist_lf.pack(fill=tk.BOTH, expand=True, pady=(0, 8))
    hist_lf.columnconfigure(0, weight=1)
    hist_lf.rowconfigure(0, weight=1)

    lb_outer = ttk.Frame(hist_lf)
    lb_outer.grid(row=0, column=0, sticky=tk.NSEW)
    lb_outer.columnconfigure(0, weight=1)
    lb_outer.rowconfigure(0, weight=1)

    history_lb = tk.Listbox(lb_outer, height=4, activestyle='none',
                            selectmode=tk.SINGLE, font=('TkFixedFont', 9),
                            relief='solid', borderwidth=1, highlightthickness=0,
                            selectbackground='#7ba7bc', selectforeground='white',
                            bg='white')
    history_lb.grid(row=0, column=0, sticky=tk.NSEW)

    lb_scroll = ttk.Scrollbar(lb_outer, orient=tk.VERTICAL, command=history_lb.yview)
    lb_scroll.grid(row=0, column=1, sticky=tk.NS)
    history_lb.configure(yscrollcommand=lb_scroll.set)

    _history_data = _load_run_history()
    for entry in _history_data:
        history_lb.insert(tk.END, f'  {_run_label(entry)}')
    if not _history_data:
        history_lb.insert(tk.END, '  (no previous runs yet)')
        history_lb.configure(state=tk.DISABLED)

    history_lb.bind('<Double-Button-1>', lambda _e: load_selected_run())

    hist_btn_row = ttk.Frame(hist_lf)
    hist_btn_row.grid(row=1, column=0, sticky=tk.EW, pady=(6, 0))
    ttk.Label(hist_btn_row, text='Double-click or select then click Load →',
              foreground='grey').pack(side=tk.LEFT)
    ttk.Button(hist_btn_row, text='Load Selected Run',
               command=load_selected_run).pack(side=tk.RIGHT)

    # ── Action buttons ─────────────────────────────────────────────────
    sep = ttk.Separator(outer, orient='horizontal')
    sep.pack(fill=tk.X, pady=(2, 10))

    btn_row = ttk.Frame(outer)
    btn_row.pack(fill=tk.X)
    ttk.Button(btn_row, text='Cancel',
               command=root.destroy).pack(side=tk.RIGHT, padx=(6, 0))
    ttk.Button(btn_row, text='▶  Run Analysis',
               command=run, default='active').pack(side=tk.RIGHT)

    root.mainloop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MSD 4PL Analysis Tool')
    parser.add_argument('--msd', required=False, default=None, help='MSD .txt data file')
    parser.add_argument('--platemap', required=False, default=None, help='Plate map CSV (grid format)')
    parser.add_argument('--output', default='msd_4pl_results.xlsx', help='Output Excel file')
    parser.add_argument('--spots', type=int, choices=[1, 4, 10], default=None,
                        help='Override spots per well (auto-detected if omitted)')
    parser.add_argument('--units', default=None,
                        help='Optional units string to append to interpolated concentration headers')
    parser.add_argument('--cv-threshold', type=float, default=None,
                        help='Optional %%CV threshold for All Unknowns highlight (default 25)')
    parser.add_argument('--lloq-method', choices=['current', '3xblank'], default='current',
                        help='LLOQ calculation method: current mean+10*SD or 3x blank mean')
    parser.add_argument('--dilution-factors', default=None,
                        help='Optional per-plate dilution factors as comma-separated values (e.g. 1,2,1)')
    parser.add_argument('--total-protein', default=None,
                        help='Optional total protein CSV for normalisation (External Animal Number + Tissue Type)')
    parser.add_argument('--gui', action='store_true', help='Launch interactive file picker dialogs')
    parser.add_argument('--rerun', action='store_true', help='Rerun the last analysis with saved parameters')
    args = parser.parse_args()

    if args.rerun:
        history = _load_run_history()
        if not history:
            print("No previous run found. Use --msd and --platemap or --gui.")
            sys.exit(1)
        last_args = history[0]
        args.msd = last_args.get('msd')
        args.platemap = last_args.get('platemap')
        args.output = last_args.get('output', 'msd_4pl_results.xlsx')
        args.spots = last_args.get('spots')
        args.units = last_args.get('units')
        args.cv_threshold = last_args.get('cv_threshold', 25)
        args.lloq_method = last_args.get('lloq_method', 'current')
        args.dilution_factors = last_args.get('dilution_factors')
        args.total_protein = last_args.get('total_protein')
        args.gui = False
        print(f"Rerunning: {_run_label(last_args)}")
        print(f"  MSD: {args.msd}")
        print(f"  Plate map: {args.platemap}")
        print(f"  Output: {args.output}")

    # No args or --gui → open GUI (default when double-clicked as .app)
    if args.gui or (not args.msd and not args.platemap and not args.rerun):
        run_interactive()
    elif args.msd and args.platemap:
        run_analysis(args.msd, args.platemap, args.output, args.spots, args.units, args.cv_threshold, args.dilution_factors, args.lloq_method, args.total_protein)
    else:
        print("Error: provide both --msd and --platemap, or use --gui for interactive mode.")
        parser.print_help()


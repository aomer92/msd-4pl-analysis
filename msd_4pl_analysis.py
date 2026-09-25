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

import re, sys, argparse, os, tempfile, json, subprocess, platform, functools, multiprocessing, shutil
import copy
import threading, urllib.request
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

__version__ = "1.17.0"

# ── Auto-update check ─────────────────────────────────────────────────────────
_GITHUB_REPO  = "aomer92/msd-4pl-analysis"
_RELEASES_URL = f"https://api.github.com/repos/{_GITHUB_REPO}/releases/latest"
_DOWNLOAD_URL = f"https://github.com/{_GITHUB_REPO}/releases/latest"

_EMAIL_REGISTER_URL = f"https://github.com/{_GITHUB_REPO}/subscription"  # GitHub Watch page
_FORMSPREE_ENDPOINT = ""   # optional: set to your Formspree form ID (e.g. "xyzabcde")
#   → create a free form at https://formspree.io, paste the 8-char ID above
#   → submissions arrive in your email; you maintain the mailing list manually

def _parse_version(v):
    """'1.2.3' → (1, 2, 3).  Returns (0,) on bad input."""
    try:
        return tuple(int(x) for x in str(v).lstrip('v').split('.'))
    except Exception:
        return (0,)

def _fetch_latest_release():
    """Return dict with keys: tag, html_url, assets  — or None on failure.
    assets is {platform_key: browser_download_url} where platform_key is
    'windows' or 'macos'."""
    try:
        req = urllib.request.Request(
            _RELEASES_URL,
            headers={"Accept": "application/vnd.github+json",
                     "User-Agent": f"MSD-4PL-Analysis/{__version__}"},
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())
        tag      = data.get("tag_name", "")
        html_url = data.get("html_url", _DOWNLOAD_URL)
        assets   = {}
        for a in data.get("assets", []):
            name = a.get("name", "").lower()
            url  = a.get("browser_download_url", "")
            if "windows" in name:
                assets["windows"] = url
            elif "macos" in name or "mac" in name:
                assets["macos"] = url
        return {"tag": tag, "html_url": html_url, "assets": assets}
    except Exception:
        return None

# ── backward-compat shim used by the banner code ─────────────────────────────
def _fetch_latest_version():
    """Return (tag_str, html_url) or (None, None)."""
    r = _fetch_latest_release()
    if r:
        return r["tag"], r["html_url"]
    return None, None

# ── Auto-install update ───────────────────────────────────────────────────────


def _platform_asset_key():
    """Return 'windows' or 'macos' based on current OS."""
    if sys.platform == "win32":
        return "windows"
    if sys.platform == "darwin":
        return "macos"
    return None

def _current_exe_path():
    """Return the path to the running executable (works frozen + plain Python)."""
    if getattr(sys, 'frozen', False):
        return sys.executable          # PyInstaller frozen app
    return os.path.abspath(sys.argv[0])

def _download_file(url, dest_path, progress_cb=None):
    """Download url → dest_path.  progress_cb(bytes_done, total) called periodically."""
    req = urllib.request.Request(
        url, headers={"User-Agent": f"MSD-4PL-Analysis/{__version__}"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        total = int(resp.headers.get("Content-Length", 0))
        done  = 0
        chunk = 65536
        with open(dest_path, "wb") as f:
            while True:
                buf = resp.read(chunk)
                if not buf:
                    break
                f.write(buf)
                done += len(buf)
                if progress_cb:
                    progress_cb(done, total)

def _install_update_windows(zip_path, new_exe_name="MSD 4PL Analysis.exe"):
    """Extract new exe from zip, write a swap .bat, exit.
    The .bat waits for this process to end, copies, relaunches."""
    import zipfile as _zf
    stage_dir = os.path.join(tempfile.gettempdir(), "_msd_update_stage")
    os.makedirs(stage_dir, exist_ok=True)

    # Extract zip, look for the exe
    with _zf.ZipFile(zip_path) as z:
        z.extractall(stage_dir)

    # Find the exe in the extracted tree
    new_exe = None
    for root_d, _dirs, files in os.walk(stage_dir):
        for fname in files:
            if fname.lower().endswith(".exe"):
                new_exe = os.path.join(root_d, fname)
                break
        if new_exe:
            break
    if not new_exe:
        raise FileNotFoundError("No .exe found in update zip")

    current_exe = _current_exe_path()
    bat_path = os.path.join(tempfile.gettempdir(), "_msd_updater.bat")
    bat_content = (
        "@echo off\n"
        ":: Wait 3 s for the old process to fully exit\n"
        "timeout /t 3 /nobreak >nul\n"
        f'copy /Y "{new_exe}" "{current_exe}"\n'
        f'start "" "{current_exe}"\n'
        ":: Clean up staging folder\n"
        f'rmdir /S /Q "{stage_dir}"\n'
        "del \"%~f0\"\n"
    )
    with open(bat_path, "w") as f:
        f.write(bat_content)

    # Launch the batch file hidden, then let GUI code call sys.exit()
    subprocess.Popen(
        ["cmd.exe", "/c", bat_path],
        creationflags=subprocess.CREATE_NO_WINDOW,
        close_fds=True,
    )

def _install_update_macos(zip_path):
    """Extract new .app from zip, replace current .app bundle, relaunch."""
    import zipfile as _zf
    stage_dir = os.path.join(tempfile.gettempdir(), "_msd_update_stage")
    os.makedirs(stage_dir, exist_ok=True)

    with _zf.ZipFile(zip_path) as z:
        z.extractall(stage_dir)

    # Find the .app in the extracted tree
    new_app = None
    for item in os.listdir(stage_dir):
        if item.endswith(".app"):
            new_app = os.path.join(stage_dir, item)
            break
    if not new_app:
        raise FileNotFoundError("No .app bundle found in update zip")

    # Find current .app bundle (go up from sys.executable until we hit .app)
    current_exe = _current_exe_path()
    current_app = current_exe
    for _ in range(10):
        if current_app.endswith(".app"):
            break
        current_app = os.path.dirname(current_app)
    else:
        # Fallback: install alongside the script
        current_app = os.path.join(os.path.dirname(current_exe),
                                   os.path.basename(new_app))

    # Write a tiny shell script that waits, replaces, and relaunches
    sh_path = os.path.join(tempfile.gettempdir(), "_msd_updater.sh")
    sh_content = (
        "#!/bin/bash\n"
        "sleep 3\n"
        f'rm -rf "{current_app}"\n'
        f'cp -R "{new_app}" "{current_app}"\n'
        f'open "{current_app}"\n'
        f'rm -rf "{stage_dir}"\n'
        f'rm -- "$0"\n'
    )
    with open(sh_path, "w") as f:
        f.write(sh_content)
    os.chmod(sh_path, 0o755)
    subprocess.Popen(["/bin/bash", sh_path], close_fds=True)

def _register_email_formspree(email):
    """POST email to Formspree endpoint.  Returns (ok:bool, msg:str)."""
    if not _FORMSPREE_ENDPOINT:
        return False, "No Formspree endpoint configured"
    try:
        payload = json.dumps({"email": email,
                              "version": __version__,
                              "platform": sys.platform}).encode()
        req = urllib.request.Request(
            f"https://formspree.io/f/{_FORMSPREE_ENDPOINT}",
            data=payload,
            headers={"Content-Type": "application/json",
                     "Accept": "application/json",
                     "User-Agent": f"MSD-4PL-Analysis/{__version__}"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
        if result.get("ok"):
            return True, "Registered successfully"
        return False, result.get("error", "Unknown error")
    except Exception as exc:
        return False, str(exc)

LAST_RUN_PATH = os.path.join(os.path.expanduser('~'), '.msd_4pl_last_run.json')
MAX_RUN_HISTORY = 5

# ── Analysis thresholds ───────────────────────────────────────────────────────
R2_GOOD         = 0.99   # R² ≥ this → "Good" curve fit
R2_ACCEPTABLE   = 0.95   # R² ≥ this → "Acceptable" curve fit (else "Poor")
QC_RECOVERY_LOW  = 70.0  # % recovery below this → QC failure
QC_RECOVERY_HIGH = 130.0 # % recovery above this → QC failure
DEFAULT_CV_THRESHOLD = 25.0  # %CV above this → flagged

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
    """Prepend entry to the run history list and trim to MAX_RUN_HISTORY.
    If an entry with the same (msd, platemap, output) already exists it is
    replaced rather than duplicated — re-runs update in place."""
    from datetime import datetime
    entry.setdefault('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M'))
    history = _load_run_history()
    # Remove any existing entry for the same experiment
    key = (entry.get('msd'), entry.get('platemap'), entry.get('output'))
    history = [h for h in history
               if (h.get('msd'), h.get('platemap'), h.get('output')) != key]
    history.insert(0, entry)
    history = history[:MAX_RUN_HISTORY]
    with open(LAST_RUN_PATH, 'w') as f:
        json.dump(history, f, indent=2)

def _run_label(entry):
    """Short human-readable label for a run history entry."""
    status = entry.get('status', '')
    icon = '✓' if status == 'pass' else ('✗' if status == 'fail' else ' ')
    ts  = entry.get('timestamp', '')
    msd = os.path.basename(entry.get('msd') or '') or '—'
    out = os.path.basename(entry.get('output') or '') or '—'
    return f"{icon}  {ts}  |  {msd}  →  {out}"

from io import StringIO
from collections import defaultdict

# Threading primitives for safe lazy dependency loading.
# _deps_lock  — ensures only one thread performs the actual imports.
# _deps_ready — set only after ALL imports and globals assignments are done;
#               acts as the fast-path guard so concurrent callers never see
#               a partially-initialised globals() (e.g. np present, Workbook absent).
_deps_lock  = threading.Lock()
_deps_ready = threading.Event()

def _use_stable_mpl_cache():
    """Point matplotlib at a persistent cache directory before it is imported.

    PyInstaller's runtime hook sets MPLCONFIGDIR to a throwaway temp directory,
    so the frozen app rebuilds matplotlib's font cache from scratch on every
    launch — and, because the chart pool spawns fresh interpreters, once per
    worker as well. On a 12-plate run that was nine rebuilds and the dominant
    cost of the whole analysis. Re-pointing it at a stable per-user directory
    makes the cache survive both the worker spawn and the next launch.

    Must run before `import matplotlib`; matplotlib reads this at import time.
    """
    try:
        base = os.path.join(os.path.expanduser('~'), '.msd_4pl_analysis', 'mpl-cache')
        os.makedirs(base, exist_ok=True)
        if not os.access(base, os.W_OK):
            return
        os.environ['MPLCONFIGDIR'] = base
    except OSError:
        pass   # read-only home, locked-down profile — fall back to the default


def _ensure_deps():
    """Lazy-load all heavy analysis dependencies the first time an analysis runs.
    Keeps GUI startup near-instant (only stdlib loads at launch).

    Thread-safe: uses double-checked locking so that a background preload thread
    and the main analysis thread cannot race and leave globals() half-populated.
    """
    if _deps_ready.is_set():
        return
    with _deps_lock:
        if _deps_ready.is_set():   # another thread finished while we waited
            return
    g = globals()
    _use_stable_mpl_cache()
    try:
        import numpy as np;          g['np'] = np
        import pandas as pd;         g['pd'] = pd
        from scipy.optimize import curve_fit; g['curve_fit'] = curve_fit
        from openpyxl import Workbook; g['Workbook'] = Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.styles.cell_style import StyleArray
        g.update(Font=Font, PatternFill=PatternFill, Alignment=Alignment,
                 Border=Border, Side=Side, StyleArray=StyleArray)
        from openpyxl.drawing.image import Image as XlImage; g['XlImage'] = XlImage
        from openpyxl.utils import get_column_letter; g['get_column_letter'] = get_column_letter
        import matplotlib; matplotlib.use('Agg'); g['matplotlib'] = matplotlib
        import matplotlib.pyplot as plt;   g['plt'] = plt
        import matplotlib.ticker as ticker; g['ticker'] = ticker
        import warnings; warnings.filterwarnings('ignore')
    except ModuleNotFoundError as e:
        missing = str(e).split("'")[1] if "'" in str(e) else str(e)
        msg = (f"Missing required package: {missing}\n"
               f"Install with: python3 -m pip install numpy pandas scipy openpyxl matplotlib")
        print(msg)
        raise RuntimeError(msg) from e

    # Openpyxl style constants (constructed once, reused across all sheets)
    g['HEADER_FILL'] = PatternFill('solid', fgColor='2F5496')
    g['HEADER_FONT'] = Font(bold=True, color='FFFFFF', name='Arial', size=10)
    g['DATA_FONT']   = Font(name='Arial', size=10)
    g['CENTER_ALIGN']= Alignment(horizontal='center')
    g['BOLD_FONT']   = Font(bold=True, name='Arial', size=10)
    g['SECTION_FONT']= Font(bold=True, name='Arial', size=12, color='2F5496')
    g['THIN_BORDER'] = Border(
        left=Side('thin', color='B4B4B4'), right=Side('thin', color='B4B4B4'),
        top=Side('thin', color='B4B4B4'), bottom=Side('thin', color='B4B4B4'))
    g['STD_FILL']     = PatternFill('solid', fgColor='E2EFDA')
    g['UNK_FILL']     = PatternFill('solid', fgColor='FFF2CC')
    g['BLANK_FILL']   = PatternFill('solid', fgColor='F2F2F2')
    g['CV_GOOD_FILL'] = PatternFill('solid', fgColor='D9EAD3')
    g['CV_BAD_FILL']  = PatternFill('solid', fgColor='F8CBAD')
    g['PASS_FONT']    = Font(name='Arial', size=10, color='006100')
    g['WARN_FONT']    = Font(name='Arial', size=10, color='9C5700')
    g['FAIL_FONT']    = Font(name='Arial', size=10, color='9C0006')
    # Signal that ALL globals are now populated — unblocks any concurrent caller.
    _deps_ready.set()


# ═══════════════════════════════════════════════════════════════════════════════
# 4PL MODEL
# ═══════════════════════════════════════════════════════════════════════════════

QC_LEVELS = ["ULOQ", "HQC", "MQC", "LQC", "LLOQ"]

@functools.lru_cache(maxsize=None)
def _identify_qc_level(sample_name):
    """Return which QC level this sample represents, or None.

    Checks the canonical levels (ULOQ/HQC/MQC/LQC/LLOQ) first. If none match
    but the name still contains "QC" anywhere (e.g. '1XQC-1', '2XQC-2' —
    custom per-study QC pool naming), the '-'/'_'-delimited token containing
    "QC" is used as the level identifier, with any trailing pool/replicate
    suffix (the '-1'/'-2' after the token) stripped away — so '1XQC-1' and
    '1XQC-2' both resolve to level '1XQC', letting one dilution factor apply
    to every pool/replicate sharing that QC concentration.
    """
    upper = sample_name.upper()
    for level in QC_LEVELS:
        if level in upper:
            return level
    if 'QC' in upper:
        for tok in re.split(r'[-_]', sample_name):
            if 'QC' in tok.upper():
                return tok.upper()
    return None


def _parse_groups_only(filepath):
    """Lightweight stdlib-only group + QC detection from a plate map CSV.

    Returns (groups_found, grp_qc_levels) without importing pandas/numpy:
      groups_found  : set of group name strings (excludes '_default')
      grp_qc_levels : dict {group: set_of_qc_level_strings}
    """
    import csv as _csv
    groups_found = set()
    grp_qc = {}   # {group: set()}

    _BLANK_VALS = {'buffer only', 'blank', 'buffer', 'bg', 'background', '0'}

    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            reader = _csv.reader(f)
            for row in reader:
                # Skip blank rows and header rows (first cell empty = column-number row)
                if not row or all(c.strip() == '' for c in row):
                    continue
                for cell in row[1:]:   # column 0 is the row-letter index
                    cell = cell.strip()
                    if not cell:
                        continue

                    # Parse group prefix  e.g. "TreatA&TreatB:SampleX" or "Group1:800000"
                    raw_val = cell
                    cell_groups = ['_default']
                    if ':' in cell:
                        prefix, rest = cell.split(':', 1)
                        prefix = prefix.strip()
                        rest   = rest.strip()
                        if rest and len(prefix) <= 40 and prefix:
                            sub = [g.strip() for g in prefix.split('&') if g.strip()]
                            if sub:
                                cell_groups = sub
                                raw_val = rest

                    # Determine sample type from the value
                    is_blank   = raw_val.lower() in _BLANK_VALS
                    is_numeric = False
                    try:
                        float(raw_val.replace(',', ''))
                        is_numeric = True
                    except ValueError:
                        pass
                    sample_name = '' if (is_blank or is_numeric) else raw_val

                    for g in cell_groups:
                        if g == '_default':
                            continue
                        groups_found.add(g)
                        if g not in grp_qc:
                            grp_qc[g] = set()
                        if sample_name:
                            level = _identify_qc_level(sample_name)
                            if level:
                                grp_qc[g].add(level)

    except Exception:
        pass   # caller handles empty result gracefully

    return groups_found, grp_qc


def four_pl(x, a, b, c, d):
    """4PL: a=min asymptote, b=Hill slope, c=inflection (EC50), d=max asymptote"""
    return d + (a - d) / (1.0 + (x / c) ** b)

def inverse_4pl(y, a, b, c, d):
    """Solve 4PL for x given y."""
    if abs(b) < 1e-9:          # degenerate Hill slope → undefined inverse
        return np.nan
    denom = y - d
    if abs(denom) < 1e-9:   # near-zero: signal ≈ upper asymptote, inversion undefined
        return np.nan
    ratio = (a - d) / denom - 1.0
    if ratio <= 0:
        return np.nan
    return c * (ratio ** (1.0 / b))

def fit_4pl(conc, signal):
    """
    Fit 4PL model with 1/y² weighted least-squares.

    Weighting: σ_i = y_i so the optimiser minimises Σ[(y_i − f(x_i)) / y_i]²,
    i.e. it minimises relative (percentage) residuals — the correct criterion for
    assay signals that span orders of magnitude with roughly constant CV.
    absolute_sigma=False means σ values define relative importance only and are not
    assumed to be true measurement standard deviations.

    Blanks (conc=0) are included; they anchor the lower asymptote (a parameter).
    At x=0 the model returns a, so including blanks constrains a ≈ blank signal.

    R² is computed on the same 1/y² weighted scale as the fit.
    Falls back to unweighted fit if the weighted optimisation fails.

    Returns (popt, weighted_r2) or (None, None).
    """
    conc  = np.asarray(conc,   float)
    signal = np.asarray(signal, float)

    # Require: non-negative concentration, finite & strictly positive signal
    # (signal > 0 required for 1/y² weights; MSD blanks always have positive signal)
    mask = (conc >= 0) & np.isfinite(signal) & (signal > 0)
    c_fit = conc[mask]
    s_fit = signal[mask]

    if len(c_fit) < 4:
        return None, None

    # Initial parameter guesses
    a0 = float(np.min(s_fit))                          # lower asymptote
    d0 = float(np.max(s_fit))                          # upper asymptote
    pos = c_fit[c_fit > 0]
    # Geometric mean of positive concentrations → midpoint on log scale (better than median)
    c0 = float(np.exp(np.mean(np.log(pos)))) if len(pos) > 0 else 1.0
    b0 = 1.0                                           # Hill slope

    # Hill slope bounded to physically meaningful range; c bounded > 0
    bounds = ([-np.inf, 0.01, 1e-15, -np.inf],
              [ np.inf, 20.0,  np.inf,  np.inf])

    def _weighted_r2(params):
        """1/y² weighted R²: consistent with the fitting criterion."""
        y_pred  = four_pl(c_fit, *params)
        w       = 1.0 / s_fit ** 2
        y_wmean = np.average(s_fit, weights=w)
        ss_res  = np.sum(w * (s_fit - y_pred)  ** 2)
        ss_tot  = np.sum(w * (s_fit - y_wmean) ** 2)
        return float(1.0 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    # ── Primary: 1/y² weighted fit ──────────────────────────────────────────
    # σ_i = y_i → minimises Σ(relative_residual²)
    try:
        popt, _ = curve_fit(
            four_pl, c_fit, s_fit,
            p0=[a0, b0, c0, d0],
            sigma=np.clip(s_fit, 1e-3, None),   # 1/y² weighting; clamp avoids near-zero σ → ∞ weight instability
            absolute_sigma=False,   # σ defines relative weights, not true std-devs
            maxfev=5000,
            bounds=bounds,
        )
        return popt, _weighted_r2(popt)
    except (RuntimeError, ValueError):
        pass  # RuntimeError = maxfev exceeded or no convergence; ValueError = bad input

    # ── Fallback: unweighted fit ─────────────────────────────────────────────
    try:
        popt, _ = curve_fit(
            four_pl, c_fit, s_fit,
            p0=[a0, b0, c0, d0],
            maxfev=5000,
            bounds=bounds,
        )
        return popt, _weighted_r2(popt)
    except (RuntimeError, ValueError):
        return None, None


# ═══════════════════════════════════════════════════════════════════════════════
# CHART GENERATION (MSD Discovery Workbench style)
# ═══════════════════════════════════════════════════════════════════════════════

def generate_std_curve_chart(res, tmp_dir, lloq_method='current', units=None):
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
    unit_suffix = f" ({units})" if units else ""

    # Collect standard data — all individual replicates (including finite-only)
    std_pts = [(s['conc'], s['signal']) for s in standards
               if s['conc'] > 0 and np.isfinite(s['signal'])]
    if not std_pts:
        return None
    rep_concs = np.array([p[0] for p in std_pts])
    rep_sigs  = np.array([p[1] for p in std_pts])

    # Aggregate replicates per unique concentration → mean ± SD for error bars
    _by_conc = defaultdict(list)
    for c_val, s_val in std_pts:
        _by_conc[c_val].append(s_val)
    agg_concs = np.array(sorted(_by_conc))
    agg_means = np.array([np.mean(_by_conc[c_val]) for c_val in agg_concs])
    agg_sds   = np.array([np.std(_by_conc[c_val], ddof=0) for c_val in agg_concs])
    agg_ns    = np.array([len(_by_conc[c_val]) for c_val in agg_concs])
    # Only draw error bar where n > 1 and SD > 0
    yerr_vals = np.where((agg_ns > 1) & (agg_sds > 0), agg_sds, np.nan)

    # Calculate LLOQ signal from blanks (use cached value if available)
    lloq_sig = res.get('lloq_sig')
    if lloq_sig is None and blanks:
        bsigs = [bl['signal'] for bl in blanks if np.isfinite(bl['signal'])]
        lloq_sig = calculate_lloq_signal(bsigs, lloq_method)

    # ULOQ signal = fitted signal at highest standard concentration
    uloq_conc = np.max(agg_concs)
    uloq_sig = four_pl(uloq_conc, *params)

    # LLOQ concentration from signal
    lloq_conc = None
    if lloq_sig is not None:
        try:
            lloq_conc = inverse_4pl(lloq_sig, *params)
            if not (np.isfinite(lloq_conc) and lloq_conc > 0):
                lloq_conc = None
        except (ValueError, ZeroDivisionError, OverflowError):
            lloq_conc = None

    # Generate smooth fitted curve
    conc_min = np.min(agg_concs) * 0.3
    conc_max = np.max(agg_concs) * 3
    x_smooth = np.logspace(np.log10(conc_min), np.log10(conc_max), 200)
    y_smooth = four_pl(x_smooth, *params)

    # ── Plot ──────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(8, 5.5))

    # Detection range shading
    if lloq_sig is not None and uloq_sig is not None:
        ax.axhspan(lloq_sig, uloq_sig, alpha=0.08, color='#2244AA', zorder=0)
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

    # Individual replicate dots (small, semi-transparent) behind the mean markers
    has_reps = len(rep_concs) > len(agg_concs)
    if has_reps:
        ax.scatter(rep_concs, rep_sigs, s=14, color='#6688cc', alpha=0.45,
                   zorder=3, linewidths=0)

    # Mean markers with SD error bars (error bar hidden when n=1 or SD=0)
    ax.errorbar(agg_concs, agg_means, yerr=yerr_vals,
                fmt='o', color='#1a3a8a', markersize=5.5,
                ecolor='#1a3a8a', elinewidth=1.2, capsize=3.5, capthick=1.2,
                zorder=4, label='Standards (mean ± SD)' if has_reps else 'Standards')

    # Log-log scale
    ax.set_xscale('log')
    ax.set_yscale('log')

    # Axis limits — leave room for labels
    ax.set_xlim(conc_min * 0.25, conc_max * 4)
    all_sigs = list(rep_sigs)
    if lloq_sig is not None:
        all_sigs.append(lloq_sig)
    if uloq_sig is not None:
        all_sigs.append(uloq_sig)
    sig_min = min(s for s in all_sigs if s > 0) * 0.4
    sig_max = max(all_sigs) * 3
    ax.set_ylim(sig_min, sig_max)

    # Axis labels — include units on x-axis when provided
    ax.set_xlabel(f'Concentration{unit_suffix}', fontsize=10, fontweight='bold')
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
    box_text = '\n'.join(info_lines)
    props = dict(boxstyle='round,pad=0.4', facecolor='white', edgecolor='#666666', alpha=0.9)
    ax.text(0.98, 0.98, box_text, transform=ax.transAxes, fontsize=8,
            verticalalignment='top', horizontalalignment='right',
            bbox=props, family='monospace')

    # Legend
    ax.legend(loc='lower right', fontsize=8, framealpha=0.9)

    plt.tight_layout()

    # Save — sanitize name components so characters like '/' don't split the path
    def _safe(s):
        return re.sub(r'[^\w\-.]', '_', str(s))
    fname = f"chart_P{_safe(plate)}_S{_safe(spot)}{'_' + _safe(group) if group else ''}.png"
    fpath = os.path.join(tmp_dir, fname)
    fig.savefig(fpath, dpi=96, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    return fpath


def generate_overlay_chart(results, tmp_dir, qc_overlay_points=None,
                           qc_expected_concentrations=None, units=None):
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
    unit_suffix = f" ({units})" if units else ""

    cmap = plt.colormaps['tab10']

    # Build stable group→color map (first-seen order) so bands match curves
    _grp_color_map = {}
    _ci = 0
    for res in fitted:
        g = res.get('group', '') or ''
        if g not in _grp_color_map:
            _grp_color_map[g] = cmap(_ci % 10)
            _ci += 1
    colors = [_grp_color_map.get(res.get('group', '') or '', cmap(i % 10))
              for i, res in enumerate(fitted)]

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
        x_smooth = np.logspace(np.log10(cmin * 0.3), np.log10(cmax * 3), 200)
        y_smooth = four_pl(x_smooth, *params)
        ax.plot(x_smooth, y_smooth, '-', color=color, linewidth=1.5, label=label, zorder=3)

        # Observed points
        ax.scatter(std_concs, std_sigs, s=25, color=color, zorder=4,
                   edgecolors='black', linewidths=0.3, alpha=0.8)

    # Shared QC level color palette
    qc_cmap = plt.colormaps['Set1']
    qc_level_colors = {level: qc_cmap(i % 9) for i, level in enumerate(QC_LEVELS)}

    # Per-group ±30% expected concentration bands, color-matched to each group's curve
    _exp_dict = qc_expected_concentrations if isinstance(qc_expected_concentrations, dict) else (
        {} if qc_expected_concentrations is None else {'': qc_expected_concentrations})
    _n_qc_bands = 0
    for _grp, _exp_conc in _exp_dict.items():
        if _exp_conc is None or not np.isfinite(float(_exp_conc)) or float(_exp_conc) <= 0:
            continue
        _exp_conc = float(_exp_conc)
        _band_clr = _grp_color_map.get(_grp, 'steelblue')
        _lo, _hi = _exp_conc * 0.70, _exp_conc * 1.30
        _lbl = f"{_grp} ±30% ({_exp_conc:.3g})" if _grp and _grp != '_default' else f"QC ±30% ({_exp_conc:.3g})"
        ax.axvspan(_lo, _hi, alpha=0.15, color=_band_clr, zorder=1, label=_lbl)
        ax.axvline(_exp_conc, color=_band_clr, linewidth=1.0, linestyle='--', zorder=2)
        global_conc_min = min(global_conc_min, _lo)
        global_conc_max = max(global_conc_max, _hi)
        _n_qc_bands += 1

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
    ax.set_xlabel(f'Concentration{unit_suffix}', fontsize=11, fontweight='bold')
    ax.set_ylabel('Signal', fontsize=11, fontweight='bold')
    ax.set_title('All Standard Curves — Overlay', fontsize=13, fontweight='bold', pad=12)

    for axis in [ax.xaxis, ax.yaxis]:
        axis.set_major_formatter(ticker.LogFormatterSciNotation())

    ax.tick_params(which='both', direction='in', top=True, right=True)
    ax.grid(True, which='major', alpha=0.15, linewidth=0.5)

    n_qc_pts  = len(set(pt['level'] for pt in (qc_overlay_points or []))) if qc_overlay_points else 0
    n_series = len(fitted) + _n_qc_bands + n_qc_pts
    if n_series <= 6:
        ax.legend(loc='lower right', fontsize=8, framealpha=0.9)
    else:
        ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), fontsize=7.5,
                  framealpha=0.9, ncol=1 + n_series // 15)
        fig.subplots_adjust(right=0.78)

    plt.tight_layout()

    fpath = os.path.join(tmp_dir, 'overlay_all_curves.png')
    fig.savefig(fpath, dpi=96, bbox_inches='tight', facecolor='white')
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
                        # OFL / Error / non-numeric flags from the instrument:
                        # append NaN to preserve column alignment rather than
                        # silently dropping the token and shifting subsequent values.
                        vals.append(np.nan)

            if not vals or all(np.isnan(v) for v in vals):
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
                # Supports multi-group for standards: "HTT1&HTT2:800000" → groups ["HTT1","HTT2"]
                groups_for_well = ['_default']
                if ':' in val:
                    parts = val.split(':', 1)
                    candidate_group = parts[0].strip()
                    candidate_val = parts[1].strip()
                    # Accept prefix if it looks like a short tag (not a full path or URL)
                    if len(candidate_group) <= 40 and candidate_val:
                        # Split on & to allow shared standards across multiple groups
                        sub_groups = [g.strip() for g in candidate_group.split('&') if g.strip()]
                        if sub_groups:
                            groups_for_well = sub_groups
                        val = candidate_val

                for group in groups_for_well:
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
    print("  ⚠ LLOQ not computed: only one finite blank replicate available "
          "(mean + 10×SD requires ≥2). Use '3×Blank Mean' method for single blanks.")
    return None


# Back-calculated calibrator accuracy tolerances (%RE). The lowest and highest
# calibrator levels — the ones that define the quantifiable range — are allowed
# the wider tolerance, matching standard bioanalytical practice.
CAL_RE_TOLERANCE = 20.0
CAL_RE_TOLERANCE_ANCHOR = 25.0

# Hill slope outside this range usually means the fit latched onto something
# other than the intended sigmoid (wrong analyte, saturated plate, bad dilution).
# Bounds set from the curves in this repository: the median slope is 0.95 and
# the 10th/90th percentiles are 0.75/3.3, so this flags roughly the outer tenth
# rather than a routine assay.
HILL_SLOPE_RANGE = (0.5, 3.0)


def compute_calibrator_accuracy(res, tolerance=CAL_RE_TOLERANCE,
                                anchor_tolerance=CAL_RE_TOLERANCE_ANCHOR):
    """Back-calculate each calibrator level through its own fitted curve.

    For every nominal standard concentration, the replicate signals are averaged
    and read back through the inverse 4PL. The relative error

        %RE = (back-calculated − nominal) / nominal × 100

    says whether the curve actually reproduces the standards it was fitted to —
    the criterion that establishes a working range. It is independent of R²,
    which can look excellent while individual levels are well outside tolerance.

    The quantifiable range is derived the way the criterion intends: the LLOQ and
    ULOQ are the *outermost levels that pass at the anchor tolerance*, and only
    levels strictly between them are held to the tighter interior tolerance.
    Assigning the anchor tolerance by position in the calibrator series instead
    would hold whichever level becomes the range end to the interior tolerance
    whenever the outermost standard fails — which reports a narrower range than
    the data supports (it did so on 11 of the 166 curves in this repository,
    usually by a whole dilution step).

    Levels outside that range are reported as `outside`, not as failures. A
    calibrator below the LLOQ has not failed a test; it is below the range the
    test established, and counting it as a failure overstates how bad a curve is
    — across this repository 70% of calibrators below the blank-derived LLOQ are
    out of tolerance versus 10% above it, because %RE is measured on
    concentration while the curve is nearly flat in signal down there, so a small
    signal error maps to an enormous concentration error.

    Returns a dict:
        levels      per-level dicts, ascending by nominal concentration:
                    conc, n, mean_signal, back_calc, re_pct, quantifiable,
                    status ('pass' | 'fail' | 'outside'), tolerance, passed
        lloq/uloq   the quantifiable range (None when no level anchors it)
        n_in_range  levels from lloq to uloq inclusive
        n_pass      in-range levels within tolerance
        n_failed    in-range levels out of tolerance (holes in the range)
        n_outside   levels below the LLOQ or above the ULOQ
        n_below / n_above   how those split
        interior_gap  True when the passing levels are not contiguous
        single_level  True when the range rests on one calibrator
    Returns None when there is no fit or no standards to back-calculate.
    """
    params = res.get('params')
    standards = res.get('standards') or []
    if params is None or not standards:
        return None

    by_conc = defaultdict(list)
    for st in standards:
        c = st.get('conc')
        sig = st.get('signal')
        if c is not None and c > 0 and np.isfinite(sig):
            by_conc[float(c)].append(float(sig))
    if not by_conc:
        return None

    concs = sorted(by_conc)
    levels = []
    for c in concs:
        sigs = by_conc[c]
        mean_sig = float(np.mean(sigs))
        try:
            back = inverse_4pl(mean_sig, *params)
        except (ValueError, ZeroDivisionError, OverflowError):
            back = np.nan
        if back is not None and np.isfinite(back):
            re_pct = (back - c) / c * 100.0
        else:
            back, re_pct = np.nan, np.nan
        # Near an asymptote the inverse is numerically meaningless rather than
        # merely large — %RE runs to 1e70 and beyond. Such a level can still
        # fail, but its number is not worth printing.
        quantifiable = bool(np.isfinite(re_pct)) and abs(re_pct) <= 1e4
        levels.append({'conc': c, 'n': len(sigs), 'mean_signal': mean_sig,
                       'back_calc': float(back) if np.isfinite(back) else None,
                       're_pct': float(re_pct) if np.isfinite(re_pct) else None,
                       'quantifiable': quantifiable,
                       'status': 'outside', 'tolerance': anchor_tolerance,
                       'passed': False})

    def _within(level, tol):
        r = level['re_pct']
        return r is not None and np.isfinite(r) and abs(r) <= tol

    # Range ends first: the outermost levels that clear the anchor tolerance.
    lo_i = next((i for i, lv in enumerate(levels) if _within(lv, anchor_tolerance)), None)
    hi_i = next((i for i in range(len(levels) - 1, -1, -1)
                 if _within(levels[i], anchor_tolerance)), None)

    if lo_i is None:
        for lv in levels:
            lv['tolerance'] = anchor_tolerance
            lv['status'] = 'outside'
        return {'levels': levels, 'lloq': None, 'uloq': None,
                'n_in_range': 0, 'n_pass': 0, 'n_failed': 0,
                'n_outside': len(levels), 'n_below': 0, 'n_above': 0,
                'interior_gap': False, 'single_level': False}

    for i, lv in enumerate(levels):
        if i < lo_i or i > hi_i:
            lv['status'] = 'outside'
            lv['tolerance'] = anchor_tolerance
        else:
            tol = anchor_tolerance if i in (lo_i, hi_i) else tolerance
            lv['tolerance'] = tol
            lv['status'] = 'pass' if _within(lv, tol) else 'fail'
        lv['passed'] = lv['status'] == 'pass'

    in_range = levels[lo_i:hi_i + 1]
    n_fail = sum(1 for lv in in_range if lv['status'] == 'fail')
    return {'levels': levels,
            'lloq': levels[lo_i]['conc'], 'uloq': levels[hi_i]['conc'],
            'n_in_range': len(in_range),
            'n_pass': sum(1 for lv in in_range if lv['status'] == 'pass'),
            'n_failed': n_fail,
            'n_outside': len(levels) - len(in_range),
            'n_below': lo_i, 'n_above': len(levels) - 1 - hi_i,
            'interior_gap': n_fail > 0,
            'single_level': lo_i == hi_i}


def compute_curve_flags(res, accuracy=None):
    """Short, human-readable warnings about a fitted standard curve.

    These are advisories shown next to the fit, not gates — nothing is dropped
    or corrected on their account.
    """
    flags = []
    if res.get('no_standards'):
        return ["No standards"]
    params = res.get('params')
    if params is None:
        return ["Fit failed"]

    b = params[1]
    lo, hi = HILL_SLOPE_RANGE
    if not (lo <= b <= hi):
        flags.append(f"Hill slope {b:.2f}")

    # Two conditions were measured across every dataset here and deliberately
    # left unflagged, because in these assays they describe the norm rather than
    # an anomaly: the top calibrator sitting below the fitted plateau (median
    # top-signal/d is 0.2) and the blank-derived LLOQ landing above the lowest
    # calibrator (median ratio 3.5). Both are carried instead by the
    # accuracy-based LLOQ/ULOQ columns, which state the quantifiable range
    # directly rather than by inference.
    # The plain count of calibrators outside tolerance is a column, not a flag:
    # 72% of the curves in this repository have at least one, so flagging it
    # would say nothing. What is worth flagging is structure — a quantifiable
    # range narrower than the calibrators that were run, or one with a hole in it.
    if accuracy and accuracy['lloq'] is not None:
        # Calibrators outside the quantifiable range are stated as a count, not
        # as a failure — they sit below the LLOQ the curve itself established.
        if accuracy['n_below']:
            flags.append(f"{accuracy['n_below']} std below range")
        if accuracy['n_above']:
            flags.append(f"{accuracy['n_above']} std above range")
        if accuracy['interior_gap']:
            flags.append("Non-contiguous range")
        if accuracy['single_level']:
            flags.append("Range rests on one calibrator")
    elif accuracy:
        flags.append("No calibrator within tolerance")

    return flags


def parse_total_protein_csv(filepath):
    """Parse a total protein CSV file.

    Accepts two formats:

    Simple (3-column minimum):
        External Animal Number, Tissue Type, Total Protein Result

    ELISA Results export (superset — extra columns are ignored):
        In Vivo Study, Animal Sample Lookup, Lysate, External Animal Number,
        Group Number, Sample Number, Tissue Type, Total Protein Result, ...

    When a 'Sample Number' column is present its value is used directly as
    the 1-based replicate key (Sample Number 1 → _P1, 2 → _P2, etc.).
    When it is absent the position within each (animal, tissue) group is
    used instead (first row → 1, second row → 2, …).

    Rows with non-numeric or blank Total Protein Result are skipped.

    Returns a 3-tuple:
      tp_map           {(animal_str, tissue_str): {sample_num_int: float}}
                         where sample_num_int is 1-based (matches _P1, _P2, …).
      animal_tissue_map {animal_str: tissue_str}   first tissue seen per animal.
      animal_group_map  {animal_str: group_str}    in-vivo 'Group Number' per animal.
    """
    df = pd.read_csv(filepath, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    required = {'External Animal Number', 'Tissue Type', 'Total Protein Result'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Total protein CSV missing required columns: {missing}\n"
            f"Found columns: {list(df.columns)}")
    has_sample_num = 'Sample Number' in df.columns
    has_group_num = 'Group Number' in df.columns
    tp_map = {}
    animal_group_map: dict[str, str] = {}
    position_counter = {}   # {(animal, tissue): next_position} — used when no Sample Number col
    for _, row in df.iterrows():
        animal = str(row['External Animal Number']).strip()
        tissue = str(row['Tissue Type']).strip()
        if not animal or animal.lower() in ('nan', ''):
            continue
        # Capture the in-vivo Group Number for this animal (first non-blank wins).
        # Independent of Total Protein Result so group # is available even when
        # the protein value is blank/#VALUE!.
        if has_group_num and animal not in animal_group_map:
            grp = str(row['Group Number']).strip()
            if grp and grp.lower() not in ('nan', ''):
                animal_group_map[animal] = grp
        try:
            val = float(row['Total Protein Result'])
        except (ValueError, TypeError):
            continue   # blank, #VALUE!, or non-numeric — skip row
        key = (animal, tissue)
        if key not in tp_map:
            tp_map[key] = {}
        if has_sample_num:
            try:
                snum = int(float(str(row['Sample Number']).strip()))
            except (ValueError, TypeError):
                continue   # unparseable sample number — skip row
        else:
            snum = position_counter.get(key, 1)
            position_counter[key] = snum + 1
        tp_map[key][snum] = val
    # Build a secondary map: animal → tissue (from CSV's Tissue Type column).
    # Used to enrich display tissue and TP-key lookup for samples whose names
    # contain no tissue suffix (e.g. 'Rn2541' style).
    # First occurrence per animal wins (deterministic for the common case where
    # each animal appears under exactly one tissue type).
    animal_tissue_map: dict[str, str] = {}
    for (ani, tis) in tp_map:
        if ani not in animal_tissue_map and tis:
            animal_tissue_map[ani] = tis
    return tp_map, animal_tissue_map, animal_group_map


def _natural_well_key(well):
    """Sort key for well IDs in natural order: A1, A2, ..., A12, B1, ... (not A1, A10, A2)."""
    m = re.search(r'\d+', well)
    return (well[0], int(m.group()) if m else 0)


def _extract_replicate_index(sample_name):
    """Return the 0-based replicate index from a replicate suffix, or None.

    Recognised formats (1-based in name → 0-based return):
        'fCtx-1001_P1'           → 0   (_P{n} / _R{n} old format)
        'fCtx-1001_P2'           → 1
        '185-008-1001-fCtx-1'    → 0   (trailing -{n} after non-digit, new format)
        '185-008-1001-SC-C-2'    → 1
        'fCtx-1001'              → None  (no suffix — fall back to sequential counter)

    The lookbehind (?<![0-9]) prevents matching the animal number itself
    (e.g. the -1001 in 'fCtx-1001' is preceded by a digit so it won't fire).
    """
    s = sample_name.strip()
    # Old format: _P1, _R2 etc.
    m = re.search(r'_[PpRr](\d+)$', s)
    if m:
        return int(m.group(1)) - 1
    # New format: trailing -1 or -2 after a non-digit character (e.g. tissue letter)
    m = re.search(r'(?<![0-9])-([0-9]{1,2})$', s)
    if m:
        return int(m.group(1)) - 1
    # Underscore-delimited trailing replicate, e.g. ATLAS189_fCTX_1001_1 → 0
    m = re.search(r'_([0-9]{1,2})$', s)
    if m:
        return int(m.group(1)) - 1
    return None


def _extract_animal_tissue(sample_name):
    """Flexible extraction of animal number and tissue from a sample name.

    Supported formats:
        fCtx-1001_P1                 → ('1001', 'fCtx')      old simple format
        1001-fCtx                    → ('1001', 'fCtx')
        185-008-1001-fCtx-1          → ('1001', 'fCtx')      new study-prefixed format
        185-008-1001-SC-C-1          → ('1001', 'SC-C')      compound tissue
        185-008-7001-SC-T-2          → ('7001', 'SC-T')
        185-008-1001-SC-L-1          → ('1001', 'SC-L')
        1001-C5-L                    → ('1001', 'C5-L')      spinal level + side
        7502A-T6-R                   → ('7502A', 'T6-R')
        ATLAS189_fCTX_1001_1         → ('ATLAS189-1001', 'fCTX')  study_tissue_animal_rep
        197-1001_fCTX_r1             → ('197-1001', 'fCTX')  study-animal_tissue_r{rep}
        197-7502A_fCTX_r1            → ('197-7502A', 'fCTX')

    Strategy:
      1. Strip trailing replicate suffix (_P1/_R1 or -1/-2 after a non-digit).
      2. Split by '-'; find all purely-numeric segments.
      3. Animal = LONGEST purely-numeric segment (disambiguates study prefix
         '185' from animal '1001' — animal numbers are typically 4 digits).
      4. Tissue = all non-numeric segments that follow the animal segment,
         joined with '-'.  If none follow, use segments that precede it
         (backward compat with 'fCtx-1001' ordering).

    Returns (None, None) if no purely-numeric segment found (QC, blanks, etc.).
    """
    s = sample_name.strip()
    # Strip trailing replicate suffix before splitting
    s = re.sub(r'_[PpRr]\d+$', '', s)                   # _P1, _R2 etc.
    s = re.sub(r'(?<![0-9])-([0-9]{1,2})$', '', s)      # -1, -2 after tissue letter

    # ID-like: purely numeric (1001, 185) OR optional leading letters + digits (M001, F1234, Rn1868)
    _id_pat = re.compile(r'^[A-Za-z]*\d+$')

    # Special case: "{study}-{animal}_{tissue}", e.g. 197-1001_fCTX (left after
    # a trailing _r1/_r2 replicate suffix is stripped above, from
    # "197-1001_fCTX_r1"). Study and animal are hyphen-joined, tissue follows
    # an underscore — a mixed delimiter shape neither the hyphen-only nor
    # underscore-only path below can parse (hyphen-split fuses "1001_fCTX"
    # into one segment; underscore-split fuses "197-1001" into one segment).
    # Animal numbers are not guaranteed unique across studies sharing one
    # plate map, so the study prefix is kept in the returned animal id
    # (matches the study_tissue_animal_rep precedent below).
    _m = re.match(r'^([A-Za-z0-9]+)-([0-9]+[A-Za-z]*)_([A-Za-z]+)$', s)
    if _m:
        return f"{_m.group(1)}-{_m.group(2)}", _m.group(3)

    def _try_parse(segs, tissue_joiner='-'):
        """Apply the same animal/tissue extraction logic to a list of segments."""
        first_alpha_idx = next(
            (i for i, seg in enumerate(segs) if re.match(r'^[A-Za-z]+$', seg)),
            None
        )
        if first_alpha_idx is not None and first_alpha_idx > 0:
            animal = segs[first_alpha_idx - 1]
            if not _id_pat.match(animal):
                return None, None
            tissue_parts = segs[first_alpha_idx:]
            tissue = tissue_joiner.join(tissue_parts) if tissue_parts else None
            return animal, tissue
        elif first_alpha_idx == 0:
            id_segs = [(i, seg) for i, seg in enumerate(segs)
                       if i > 0 and _id_pat.match(seg)]
            if not id_segs:
                return None, None
            animal_idx, animal = id_segs[0]
            before = [seg for i, seg in enumerate(segs) if i < animal_idx]
            tissue = tissue_joiner.join(before) if before else None
            return animal, tissue
        else:
            if len(segs) == 1 and _id_pat.match(segs[0]):
                return segs[0], None   # plain animal ID, e.g. 'Rn2541'
            return None, None

    # First try hyphen-delimited (existing formats: 185-008-1001-fCtx, fCtx-1001, Rn2541)
    segments = s.split('-')

    # Special case: trailing "{level}-{side}" spinal-cord tissue pair, e.g.
    # 1001-C5-L, 7502A-T6-R. 'level' (C5/T6/L4/L5/S1/...) is a short anatomical
    # code — single letter + 1-2 digits — which matches _id_pat just like an
    # animal ID does, so the general rule below (which looks for the first
    # PURELY-alpha segment as the tissue boundary) would mistake the level
    # code for the animal and the trailing single-letter side for the tissue,
    # colliding every animal that shares a level+side onto one lookup key.
    # Must be checked first since it overrides that general rule.
    _level_pat = re.compile(r'^[A-Za-z]\d{1,2}$')
    if (len(segments) >= 3 and re.match(r'^[LR]$', segments[-1])
            and _level_pat.match(segments[-2]) and _id_pat.match(segments[-3])):
        return segments[-3], f"{segments[-2]}-{segments[-1]}"

    animal, tissue = _try_parse(segments, tissue_joiner='-')
    if animal is not None:
        return animal, tissue

    # Fall back to underscore-delimited (e.g. ATLAS163_Rn1868_fCtx → Rn1868, fCtx)
    # Only attempt when there are multiple underscore-separated parts, so that a plain
    # animal ID like 'Rn2541' (already handled above) isn't re-tried unnecessarily.
    us_segments = s.split('_')

    # Special case: "{study}_{tissue}_{animal}_{rep}", e.g. ATLAS189_fCTX_1001_1
    # — tissue precedes animal (reversed from every other supported underscore
    # convention), with a trailing 1-2 digit replicate. The general rule below
    # assumes the animal immediately precedes the tissue, so on this ordering
    # it would take the study prefix as the animal instead. Animal numbers are
    # not globally unique across studies (the same "1001" can exist in two
    # different studies sharing one plate map), so the study prefix is kept
    # as part of the returned animal id ("ATLAS189-1001") rather than discarded.
    if (len(us_segments) == 4 and re.match(r'^[0-9]{1,2}$', us_segments[3])
            and re.match(r'^[A-Za-z]+$', us_segments[1]) and _id_pat.match(us_segments[2])):
        return f"{us_segments[0]}-{us_segments[2]}", us_segments[1]

    if len(us_segments) > 1:
        animal, tissue = _try_parse(us_segments, tissue_joiner='_')
        if animal is not None:
            return animal, tissue

    return None, None


def _infer_group_number(sample_name):
    """Infer the in-vivo study Group Number from a 4-5 digit run found
    anywhere in a sample name.

    Convention observed across NHP study animal numbering (e.g. ATLAS197):
    the animal ID is a 4-5 digit number where the group is the leading 1-2
    digits and the trailing 3 digits identify the individual within that
    group — e.g. 1001/1501/1502 -> group 1, 2001/2501/2502 -> group 2, ...,
    13001/13501/13502 -> group 13. The digit run can appear anywhere in the
    name (a study prefix like "197-" or a trailing letter/tissue suffix like
    "7502A"/"1001_fCTX" doesn't block it) — only a run of digits immediately
    adjacent to other digits is excluded, so a 4-5 digit match is never a
    fragment of a longer number. When more than one 4-5 digit run appears,
    the LAST one in the name is used (the animal ID is consistently the
    right-most numeric component in every convention seen so far).

    This can misfire on ID conventions where the group isn't encoded in the
    number at all (e.g. 'Rn2541'-style IDs, whose real groups come from study
    design, not from the digits) — infer_animal_group_map's caller validates
    the resulting group set (must start at 1, no gaps) specifically to catch
    that kind of misapplication and warn rather than silently mislabel data.

    Returns the group number as a string (e.g. '1', '13'), or None if no
    4-5 digit run is present.
    """
    if not sample_name:
        return None
    matches = re.findall(r'(?<!\d)\d{4,5}(?!\d)', sample_name)
    if not matches:
        return None
    digits = matches[-1]
    group = digits[:-3]
    return group if group else None


def infer_animal_group_map(results):
    """Build {animal: group_number_str} by inferring from every unique
    animal ID found in `results`' unknown sample names (see
    _infer_group_number). Used as a fallback/supplement when no total-protein
    CSV (or an incomplete one) supplies explicit Group Numbers.
    """
    inferred = {}
    for res in results:
        for u in res.get('unknowns', []):
            sname = u.get('sample_name', '')
            if not sname:
                continue
            animal, _tissue = _extract_animal_tissue(sname)
            if not animal or animal in inferred:
                continue
            grp = _infer_group_number(sname)
            if grp:
                inferred[animal] = grp
    return inferred


def _validate_group_sequence(animal_group_map):
    """Sanity-check that a Group Number assignment (inferred or from CSV) is
    sequential starting at 1 — i.e. groups {1, 2, ..., N} with no gaps.

    Real study designs always start dosing groups at 1 and number them
    consecutively, so a missing Group 1 or a gap in the sequence is a strong
    signal the inference (or the source CSV) doesn't actually match this
    dataset's convention. Returns a human-readable warning string, or None
    if the sequence looks correct (or there's nothing numeric to check).
    """
    if not animal_group_map:
        return None
    groups = set()
    for g in animal_group_map.values():
        s = str(g).strip()
        if s.isdigit():
            groups.add(int(s))
    if not groups:
        return None
    groups = sorted(groups)
    problems = []
    if groups[0] != 1:
        problems.append(f"no animals were assigned to Group 1 (lowest group found: {groups[0]})")
    missing = [g for g in range(1, groups[-1] + 1) if g not in groups]
    if missing:
        shown = ', '.join(str(m) for m in missing[:20])
        if len(missing) > 20:
            shown += f", … and {len(missing) - 20} more"
        problems.append(f"missing group number(s): {shown}")
    if not problems:
        return None
    return ("Group Number sequence looks incomplete — " + "; ".join(problems) +
            f". Groups found: {groups}. If this is unexpected, double-check the "
            f"'Infer Study Group from Animal Number' option and/or the total "
            f"protein CSV's Group Number column.")


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEL OUTPUT
# ═══════════════════════════════════════════════════════════════════════════════



# Characters illegal in XML 1.0 (the format used by .xlsx).
# Plate-reader exports sometimes embed null bytes or other control chars
# that pass through Python string I/O but cause openpyxl to emit invalid
# XML — Excel then shows a "repair" prompt when opening the file.
# Illegal ranges: U+0000–U+0008, U+000B, U+000C, U+000E–U+001F, U+FFFE, U+FFFF
_XML_ILLEGAL = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f￾￿]')

def _safe_str(s):
    """Strip XML-1.0-illegal characters from a string before writing to a cell.

    Returns None (not empty string) when the result would be empty.
    openpyxl writes empty-string values as ``<c t="inlineStr" />`` — a cell
    that claims inline-string type but carries no ``<is>`` child.  That
    violates the OOXML schema and triggers Excel's 'repair' dialog.
    Returning None causes openpyxl to emit a plain empty cell instead.
    """
    if not isinstance(s, str):
        s = str(s)
    cleaned = _XML_ILLEGAL.sub('', s)
    return cleaned if cleaned else None


def _xv(val, fallback="N/A"):
    """Convert a value to a Python-native type safe for openpyxl cells.

    openpyxl serialises cell values as XML 1.0.  Three classes of value
    corrupt that XML:

      1. numpy.nan / float('nan') / numpy.inf — written as the literal
         string "nan" or "inf" which is invalid for numeric cell types.
      2. numpy scalar types (numpy.float64, numpy.int64, …) — some
         openpyxl versions do not recognise these as numbers and write
         them as strings or fail entirely.
      3. Strings containing XML-1.0-illegal characters (null bytes,
         control characters \x00-\x08, \x0b, \x0c, \x0e-\x1f) which
         appear in raw plate-reader export files.

    This function:
      • For None      → returns ``fallback``
      • For bool      → returns the bool unchanged (True/False are valid)
      • For str       → strips XML-illegal characters and returns the str
      • For numerics  → converts to Python-native float; returns
                        ``fallback`` for NaN, ±Inf
    """
    if val is None:
        return fallback
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return _safe_str(val)
    try:
        f = float(val)
        if f != f or abs(f) == float('inf'):   # NaN or ±Inf
            return fallback
        # Return as Python int when it is a whole number (cleaner Excel display)
        if f == int(f) and abs(f) < 2**53:
            return int(f)
        return f
    except (TypeError, ValueError):
        return fallback


def _row_style_array(ws, fill, font):
    """Resolve (font, THIN_BORDER, centred alignment, fill) into a single
    StyleArray, cached per workbook.

    Assigning cell.font / .fill / .border / .alignment one cell at a time makes
    openpyxl re-hash each style object into the workbook's indexed style tables
    for every cell — on a 12-plate run that was ~84k Alignment objects and the
    single largest cost in Excel writing. Resolving the ids once and cloning the
    resulting StyleArray is ~21x faster for the same output.

    The cache lives on the workbook because style ids are workbook-scoped: a
    module-level cache would hand stale ids to the next analysis in a GUI session.
    """
    cache = getattr(ws.parent, '_msd_style_cache', None)
    if cache is None:
        cache = ws.parent._msd_style_cache = {}
    key = (id(font), id(fill))
    sa = cache.get(key)
    if sa is None:
        wb = ws.parent
        sa = StyleArray()
        sa.fontId      = wb._fonts.add(font)
        sa.borderId    = wb._borders.add(THIN_BORDER)
        sa.alignmentId = wb._alignments.add(CENTER_ALIGN)
        if fill:
            sa.fillId  = wb._fills.add(fill)
        cache[key] = sa
    return sa


def _style_row(ws, row, max_col, fill=None, font=None):
    """Apply the standard data-row style (font, thin border, centred) to a row.

    Callers set number_format, per-cell fills such as the %CV pass/fail colour,
    and per-cell fonts such as the green/amber/red Status colours *before*
    calling this, so anything the caller set explicitly is carried over rather
    than flattened by the row style. Passing `fill` or `font` here states a
    row-wide choice, which does override the per-cell one.
    """
    row_font = DATA_FONT if font is None else font
    sa = _row_style_array(ws, fill, row_font)
    for c in range(1, max_col + 1):
        cell = ws.cell(row=row, column=c)
        prev = cell._style
        num_fmt_id = prev.numFmtId if prev is not None else 0
        prev_fill_id = prev.fillId if prev is not None else 0
        prev_font_id = prev.fontId if prev is not None else 0
        cell._style = copy.copy(sa)
        if num_fmt_id:
            cell._style.numFmtId = num_fmt_id
        if fill is None and prev_fill_id:
            cell._style.fillId = prev_fill_id
        if font is None and prev_font_id:
            cell._style.fontId = prev_font_id

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


def _physical_cpu_count():
    """Return physical core count (not hyperthreads) for optimal ProcessPoolExecutor sizing.
    Falls back to logical_count // 2 if the OS query fails."""
    try:
        if sys.platform == 'darwin':
            n = int(subprocess.check_output(
                ['sysctl', '-n', 'hw.physicalcpu'], stderr=subprocess.DEVNULL))
            return max(1, n)
        if sys.platform.startswith('linux'):
            pairs = set()
            phys = core = None
            with open('/proc/cpuinfo') as _f:
                for _line in _f:
                    if _line.startswith('physical id'):
                        phys = _line.split(':', 1)[1].strip()
                    elif _line.startswith('core id'):
                        core = _line.split(':', 1)[1].strip()
                    elif _line.strip() == '' and phys is not None and core is not None:
                        pairs.add((phys, core)); phys = core = None
            if pairs:
                return max(1, len(pairs))
        if sys.platform == 'win32':
            out = subprocess.check_output(
                ['wmic', 'cpu', 'get', 'NumberOfCores', '/value'],
                stderr=subprocess.DEVNULL).decode()
            cores = [int(l.split('=')[1]) for l in out.splitlines()
                     if l.startswith('NumberOfCores=') and l.split('=')[1].strip().isdigit()]
            if cores:
                return max(1, sum(cores))
    except Exception:
        pass
    return max(1, (multiprocessing.cpu_count() or 2) // 2)


def _worker_init():
    """Pre-warm matplotlib in each worker process so the first task doesn't pay
    the full import + font-cache cost.  Must be module-level to be picklable."""
    _use_stable_mpl_cache()   # before matplotlib imports in this fresh interpreter
    import matplotlib as _mpl
    _mpl.use('Agg')
    import matplotlib.pyplot      # loads font manager and mathtext
    import matplotlib.ticker      # loads ticker formatters
    import warnings
    warnings.filterwarnings('ignore')


# Below this many charts the ProcessPoolExecutor costs more to start and tear
# down (~0.6 s measured, independent of worker count) than it saves: a single
# standard-curve chart renders in ~0.28 s. Single- and double-plate runs are the
# common case, so they take the sequential path.
_CHART_POOL_MIN_TASKS = 4


def _chart_worker(args):
    """Module-level wrapper so ProcessPoolExecutor can pickle the call.

    Handles both chart kinds so the overlay — the single most expensive figure —
    renders alongside the per-spot charts instead of serially before them.
    """
    # Subprocess workers get a fresh Python process — globals() is empty.
    # _ensure_deps() injects numpy / matplotlib / etc. so chart functions
    # can reference plt, np, etc. as module-level names.
    _ensure_deps()
    kind = args[0]
    if kind == 'overlay':
        _, results, tmp_dir, qc_pts, qc_exp, units = args
        return 'overlay', generate_overlay_chart(results, tmp_dir, qc_pts, qc_exp,
                                                 units=units)
    _, res, tmp_dir, lloq_method, units = args
    path = generate_std_curve_chart(res, tmp_dir, lloq_method, units=units)
    # Return a stable key (not id(res) — memory addresses differ across processes)
    return (res['plate'], res['spot'], res.get('group', '')), path


def _render_all_charts(results, tmp_dir, lloq_method, units,
                       qc_overlay_points, qc_expected_concentrations):
    """Render the overlay chart and every per-spot standard-curve chart.

    Returns (overlay_path, chart_map). Uses a process pool when there is enough
    work to amortise its start-up cost, and falls back to sequential rendering if
    the pool cannot be created at all (e.g. frozen-app edge cases).
    """
    tasks = [('overlay', results, tmp_dir,
              qc_overlay_points or None, qc_expected_concentrations or None, units)]
    tasks += [('spot', res, tmp_dir, lloq_method, units) for res in results]

    def _sequential():
        overlay = generate_overlay_chart(
            results, tmp_dir, qc_overlay_points or None,
            qc_expected_concentrations or None, units=units)
        return overlay, {(r['plate'], r['spot'], r.get('group', '')):
                         generate_std_curve_chart(r, tmp_dir, lloq_method, units=units)
                         for r in results}

    if len(tasks) < _CHART_POOL_MIN_TASKS:
        return _sequential()

    try:
        # Use physical cores (not hyperthreads) — chart rendering is CPU-bound
        # and hyperthreads add context-switch overhead without throughput gain.
        # Cap at 8 to avoid excessive memory pressure on large-core machines.
        _workers = min(len(tasks), _physical_cpu_count(), 8)
        with ProcessPoolExecutor(max_workers=_workers,
                                 initializer=_worker_init) as _pool:
            _raw = list(_pool.map(_chart_worker, tasks))
    except Exception:
        return _sequential()

    overlay_path = None
    chart_map = {}
    for key, path in _raw:
        if key == 'overlay':
            overlay_path = path
        else:
            chart_map[key] = path
    return overlay_path, chart_map


def _aggregate_unknowns(results):
    """Single-pass aggregation of all unknown wells across every result.

    Returns two dicts, both keyed by (sample_name, group, plate, spot):

    unk_data  — non-QC samples:
        wells, signals (finite), concs (finite interp), reps (one dict per well:
        {'well','signal','conc'} — signal/conc may be non-finite, unlike the
        filtered signals/concs lists, so reps is the source for well-paired
        per-replicate display), spot, group, plate,
        lloq_sig, uloq_conc, lloq_conc, params

    qc_data   — QC samples (identified by _identify_qc_level):
        wells, signals (finite), concs (finite interp), spot, group, plate

    Computing both in a single pass avoids iterating over results twice.
    The key includes spot so that multiplex plates keep analytes separate.
    """
    unk_data = defaultdict(lambda: {
        'wells': [], 'signals': [], 'concs': [], 'reps': [],
        'spot': None, 'group': '', 'plate': None,
        'lloq_sig': None, 'uloq_conc': None, 'lloq_conc': None, 'params': None,
    })
    qc_data = defaultdict(lambda: {
        'wells': [], 'signals': [], 'concs': [],
        'spot': None, 'group': '', 'plate': None,
    })

    for res in results:
        plate  = res['plate']
        spot   = res['spot']
        group  = res.get('group', '')
        params = res.get('params')
        lloq_sig  = res.get('lloq_sig')
        std_concs = [s['conc'] for s in res.get('standards', []) if s['conc'] > 0]
        uloq_conc = max(std_concs) if std_concs else None
        # Back-calculated LLOQ concentration (more precise than min calibrator)
        lloq_conc = None
        if lloq_sig is not None and params is not None:
            try:
                lc = inverse_4pl(lloq_sig, *params)
                lloq_conc = lc if np.isfinite(lc) and lc > 0 else None
            except Exception:
                pass

        for unk in res.get('unknowns', []):
            sname = unk['sample_name']
            key   = (sname, group, plate, spot)   # spot in key keeps analytes separate on multiplex plates
            if _identify_qc_level(sname):
                d = qc_data[key]
                d['spot'] = spot; d['group'] = group; d['plate'] = plate
            else:
                d = unk_data[key]
                d['spot'] = spot; d['group'] = group; d['plate'] = plate
                # Per-curve thresholds — first non-None wins (all unknowns in the
                # same (sample, group, plate, spot) share the same curve)
                if d['lloq_sig']  is None: d['lloq_sig']  = lloq_sig
                if d['uloq_conc'] is None: d['uloq_conc'] = uloq_conc
                if d['lloq_conc'] is None: d['lloq_conc'] = lloq_conc
                if d['params']    is None: d['params']    = params

            d['wells'].append(unk['well'])
            if not _identify_qc_level(sname):
                d['reps'].append({'well': unk['well'], 'signal': unk['signal'],
                                  'conc': unk['interp_conc']})
            if np.isfinite(unk['signal']):
                d['signals'].append(unk['signal'])
            if np.isfinite(unk['interp_conc']):
                d['concs'].append(unk['interp_conc'])

    return unk_data, qc_data


def _resolve_dilution_factor(sample_name, group, plate,
                             qc_dilution_factors=None,
                             group_dilution_factors=None,
                             plate_dilution_factors=None):
    """Return the dilution factor for a sample using priority: QC > group > plate.

    Returns the resolved factor (float, default 1.0).
    """
    plate_dilution_factors = plate_dilution_factors or {}
    grp_key = group if group and group != '_default' else ''

    # QC level takes highest priority when a matching factor exists
    if qc_dilution_factors:
        level = _identify_qc_level(sample_name)
        if level:
            grp_qc = qc_dilution_factors.get(grp_key, {})
            if level in grp_qc:
                return grp_qc[level]

    # Group-level factor next
    if grp_key and group_dilution_factors and grp_key in group_dilution_factors:
        return group_dilution_factors[grp_key]

    # Fall back to per-plate factor
    return plate_dilution_factors.get(plate, 1.0)


def _compute_qc_summary(results, qc_dilution_factors, qc_expected_concentrations):
    """Aggregate QC replicate wells and compute corrected concentrations and recovery.

    Returns (qc_summary_rows, qc_overlay_points) — both lists of dicts.
    Called by both create_output (Excel) and generate_html_report (HTML) to
    avoid duplicating this logic.
    """
    qc_summary_rows = []
    qc_overlay_points = []
    if not qc_dilution_factors:
        return qc_summary_rows, qc_overlay_points

    qc_groups = defaultdict(list)
    for res in results:
        grp = res.get('group', '') or ''
        group_qc = qc_dilution_factors.get(grp, {})
        for unk in res.get('unknowns', []):
            sname = unk.get('sample_name', '')
            level = _identify_qc_level(sname)
            if level and level in group_qc:
                key = (sname, grp, res['plate'])
                qc_groups[key].append({
                    'signal': unk['signal'],
                    'interp_conc': unk['interp_conc'],
                    'level': level,
                })

    for (sname, grp, plate), entries in sorted(qc_groups.items()):
        sigs  = [e['signal']       for e in entries if np.isfinite(e['signal'])]
        concs = [e['interp_conc']  for e in entries if np.isfinite(e['interp_conc'])]
        level = entries[0]['level']
        qc_factor = qc_dilution_factors.get(grp, {}).get(level, 1.0)
        avg_sig  = np.mean(sigs)  if sigs  else np.nan
        avg_conc = np.mean(concs) if concs else np.nan
        corrected = avg_conc * qc_factor if np.isfinite(avg_conc) else np.nan
        exp_conc = (
            (qc_expected_concentrations or {}).get(grp)
            if isinstance(qc_expected_concentrations, dict)
            else qc_expected_concentrations
        )
        recovery = (corrected / exp_conc * 100
                    if exp_conc and np.isfinite(corrected) else np.nan)
        row = {
            'sample_name': sname, 'level': level, 'plate': plate, 'group': grp,
            'avg_signal': avg_sig, 'corrected_conc': corrected, 'recovery': recovery,
        }
        qc_summary_rows.append(row)
        if np.isfinite(corrected) and np.isfinite(avg_sig) and corrected > 0 and avg_sig > 0:
            qc_overlay_points.append({**row, 'signal': avg_sig})

    return qc_summary_rows, qc_overlay_points


def create_output(results, output_path, msd_path, raw_plate_blocks, units=None, cv_threshold=25, plate_dilution_factors=None, lloq_method='current', total_protein_map=None, qc_dilution_factors=None, qc_expected_concentrations=None, group_dilution_factors=None, animal_tissue_map=None, animal_group_map=None):
    wb = Workbook()
    wb.remove(wb.active)
    tmp_dir = tempfile.mkdtemp(prefix='msd_charts_')
    try:
        _create_output_inner(wb, tmp_dir, results, output_path, msd_path, raw_plate_blocks,
                             units, cv_threshold, plate_dilution_factors, lloq_method,
                             total_protein_map, qc_dilution_factors, qc_expected_concentrations,
                             group_dilution_factors, animal_tissue_map=animal_tissue_map,
                             animal_group_map=animal_group_map)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _create_output_inner(wb, tmp_dir, results, output_path, msd_path, raw_plate_blocks, units=None, cv_threshold=25, plate_dilution_factors=None, lloq_method='current', total_protein_map=None, qc_dilution_factors=None, qc_expected_concentrations=None, group_dilution_factors=None, animal_tissue_map=None, animal_group_map=None):

    # Pre-collect QC overlay points (corrected conc + signal) for overlay chart
    qc_summary_rows, qc_overlay_points = _compute_qc_summary(
        results, qc_dilution_factors, qc_expected_concentrations)

    # Pre-generate every chart (overlay + per-spot) before Excel writing.
    # Each worker process has its own matplotlib state, so mathtext
    # thread-safety is not an issue.
    overlay_path, chart_map = _render_all_charts(
        results, tmp_dir, lloq_method, units,
        qc_overlay_points, qc_expected_concentrations)

    unit_suffix = f" ({units})" if units else ""
    interp_header = f"Interp. Conc.{unit_suffix}"
    avg_interp_header = f"Avg Interp. Conc.{unit_suffix}"
    corrected_header = f"Corrected Avg Interp. Conc.{unit_suffix}"
    cv_threshold = float(cv_threshold) if cv_threshold is not None else 25.0
    plate_dilution_factors = plate_dilution_factors or {}
    lloq_method = lloq_method or 'current'
    lloq_method_label = "3× Blank Mean" if lloq_method == '3xblank' else "Blank Mean + 10×SD"

    # ── Summary Sheet ─────────────────────────────────────────────────
    ws = wb.create_sheet("Summary")
    # Row 1: LLOQ method metadata
    ws.cell(row=1, column=1, value="LLOQ Method:").font = SECTION_FONT
    ws.cell(row=1, column=2, value=lloq_method_label)
    headers = ["Plate", "Spot", "Group", "Min (a)", "Hill Slope (b)", "EC50 (c)", "Max (d)",
               "LLOQ Signal", "LLOQ Conc", "Acc. LLOQ", "Acc. ULOQ", "Cal Pass",
               "R²", "Flags", "Status"]
    _header_row(ws, 2, headers)

    for ri, res in enumerate(results, 3):
        _grp = res.get('group', '')
        vals = [res['plate'], res['spot'], _grp if _grp else None]
        if res['params'] is not None:
            a, b, c, d = res['params']
            vals += [
                _xv(round(float(a), 2) if np.isfinite(a) else a),
                _xv(round(float(b), 4) if np.isfinite(b) else b),
                _xv(round(float(c), 4) if np.isfinite(c) else c),
                _xv(round(float(d), 2) if np.isfinite(d) else d),
            ]
        else:
            vals += ["N/A"] * 4

        # Calculate LLOQ
        lloq_sig_val = "N/A"
        lloq_conc_val = "N/A"
        lloq_sig = res.get('lloq_sig')
        if lloq_sig is not None and np.isfinite(lloq_sig):
            lloq_sig_val = round(float(lloq_sig), 1)
            if res['params'] is not None:
                try:
                    lloq_conc = inverse_4pl(lloq_sig, *res['params'])
                    if np.isfinite(lloq_conc) and lloq_conc > 0:
                        lloq_conc_val = round(float(lloq_conc), 4)
                except (ValueError, ZeroDivisionError, OverflowError):
                    pass
        vals.append(lloq_sig_val)
        vals.append(lloq_conc_val)

        # Accuracy-based quantifiable range: reported alongside the blank-derived
        # LLOQ above, never replacing it — the two answer different questions.
        acc = res.get('accuracy')
        if acc and acc['lloq'] is not None:
            # Cal Pass counts the levels inside the quantifiable range; levels
            # below the LLOQ or above the ULOQ are outside it, not failures.
            cp = f"{acc['n_pass']}/{acc['n_in_range']}"
            if acc['n_outside']:
                cp += f" (+{acc['n_outside']} outside)"
            vals += [round(acc['lloq'], 4), round(acc['uloq'], 4), cp]
        elif acc:
            vals += ["None", "None", f"0/{len(acc['levels'])}"]
        else:
            vals += ["N/A", "N/A", "N/A"]

        flag_text = ", ".join(res.get('flags') or []) or None
        if res['params'] is not None:
            r2_raw = res['r2']
            vals += [round(float(r2_raw), 6) if (r2_raw is not None and np.isfinite(r2_raw)) else "N/A"]
            vals.append(flag_text)
            if r2_raw is None or not np.isfinite(r2_raw):
                vals.append("Poor")
            else:
                vals.append("Good" if r2_raw >= R2_GOOD else ("Acceptable" if r2_raw >= R2_ACCEPTABLE else ("Negative R²" if r2_raw < 0 else "Poor")))
        else:
            vals += ["N/A", flag_text, "Failed"]

        for ci, v in enumerate(vals, 1):
            # Never write bare empty strings — they produce invalid inlineStr cells
            if v == '':
                v = None
            ws.cell(row=ri, column=ci, value=v)
        status = ws.cell(row=ri, column=len(headers))
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
            ws.cell(row=next_row, column=1, value=_safe_str(qr['sample_name']))
            ws.cell(row=next_row, column=2, value=_safe_str(str(qr['level'])))
            ws.cell(row=next_row, column=3, value=_safe_str(str(qr['plate'])))
            ws.cell(row=next_row, column=4, value=_safe_str(str(qr['group'])) if qr['group'] else None)
            sig_cell = ws.cell(row=next_row, column=5,
                               value=round(float(qr['avg_signal']), 1) if np.isfinite(qr['avg_signal']) else "N/A")
            sig_cell.number_format = '#,##0'
            corr_cell = ws.cell(row=next_row, column=6,
                                value=round(float(qr['corrected_conc']), 4) if np.isfinite(qr['corrected_conc']) else "N/A")
            corr_cell.number_format = '#,##0.0000'
            exp_conc_val = (qc_expected_concentrations or {}).get(qr['group']) if isinstance(qc_expected_concentrations, dict) else qc_expected_concentrations
            exp_cell = ws.cell(row=next_row, column=7, value=_xv(exp_conc_val) if exp_conc_val else None)
            if exp_conc_val:
                exp_cell.number_format = '#,##0.0###'
            rec_cell = ws.cell(row=next_row, column=8)
            if np.isfinite(qr['recovery']):
                rec_cell.value = round(float(qr['recovery']), 1)
                rec_cell.number_format = '0.0'
                rec_cell.font = PASS_FONT if QC_RECOVERY_LOW <= qr['recovery'] <= QC_RECOVERY_HIGH else FAIL_FONT
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
        # Guard against duplicate sheet names after truncation
        existing = {ws.title for ws in wb.worksheets}
        if sname in existing:
            base = sname[:28]
            n = 2
            while f"{base}_{n}" in existing:
                n += 1
            sname = f"{base}_{n}"
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
                ws.cell(row=row, column=2, value=_xv(round(float(val), 6) if (val is not None and np.isfinite(val)) else val))
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
        _header_row(ws, row, ["Well(s)", "Concentration", "Mean Signal", "Fitted Signal",
                              "% Recovery", "Back-Calc Conc", "%RE"])
        row += 1

        _acc = res.get('accuracy')
        acc_by_conc = {lv['conc']: lv for lv in _acc['levels']} if _acc else {}

        std_groups = {}
        for s in res.get('standards', []):
            key = s['conc']
            if key not in std_groups:
                std_groups[key] = {'wells': [], 'signals': [], 'conc': key}
            std_groups[key]['wells'].append(s['well'])
            std_groups[key]['signals'].append(s['signal'])

        for sg in sorted(std_groups.values(), key=lambda x: x['conc'], reverse=True):
            # Filter NaN signals (OFL/Error flags) before computing mean so we
            # never pass numpy.nan into an openpyxl cell (produces corrupt XML).
            finite_sigs = [s for s in sg['signals'] if np.isfinite(s)]
            mean_sig = np.mean(finite_sigs) if finite_sigs else np.nan
            fitted = four_pl(sg['conc'], *res['params']) if res['params'] is not None else None
            mean_finite = np.isfinite(mean_sig)
            recovery = (mean_sig / fitted * 100) if (mean_finite and fitted and fitted != 0) else None

            ws.cell(row=row, column=1, value=_safe_str(', '.join(sg['wells'])))
            ws.cell(row=row, column=2, value=_xv(sg['conc']))
            ws.cell(row=row, column=2).number_format = '#,##0.00'
            ws.cell(row=row, column=3, value=round(float(mean_sig), 1) if mean_finite else "N/A")
            ws.cell(row=row, column=3).number_format = '#,##0.0'
            ws.cell(row=row, column=4, value=round(float(fitted), 1) if (fitted is not None and np.isfinite(fitted)) else "N/A")
            ws.cell(row=row, column=4).number_format = '#,##0.0'
            if recovery is not None and np.isfinite(recovery):
                ws.cell(row=row, column=5, value=round(float(recovery), 1))
                ws.cell(row=row, column=5).number_format = '0.0'

            # Back-calculated concentration and its relative error — the
            # criterion the accuracy-based range on the Summary sheet uses.
            # % Recovery above compares signals; this compares concentrations.
            lvl = acc_by_conc.get(float(sg['conc']))
            if lvl and lvl['quantifiable']:
                ws.cell(row=row, column=6, value=round(lvl['back_calc'], 4))
                ws.cell(row=row, column=6).number_format = '#,##0.0000'
                re_cell = ws.cell(row=row, column=7, value=round(lvl['re_pct'], 1))
                re_cell.number_format = '0.0'
                # Outside the quantifiable range is not a failure, so it is not
                # coloured as one.
                re_cell.font = (PASS_FONT if lvl['status'] == 'pass'
                                else WARN_FONT if lvl['status'] == 'outside'
                                else FAIL_FONT)
            elif lvl:
                # Signal sits on an asymptote — the inverse is not meaningful.
                ws.cell(row=row, column=6, value="Not quantifiable")
                ws.cell(row=row, column=7, value="N/A").font = WARN_FONT
            _style_row(ws, row, 7, fill=STD_FILL)
            row += 1

        # Individual standard points data (kept in columns I-K for reference;
        # column H is left as a narrow spacer before this side table)
        ind_start = row + 1
        ws.cell(row=ind_start, column=9,  value="Conc").font = BOLD_FONT
        ws.cell(row=ind_start, column=10, value="Signal").font = BOLD_FONT
        ws.cell(row=ind_start, column=11, value="Fitted").font = BOLD_FONT
        irow = ind_start + 1
        for s in sorted(res.get('standards', []), key=lambda x: x['conc']):
            if s['conc'] > 0 and s['signal'] > 0:
                ws.cell(row=irow, column=9,  value=s['conc'])
                ws.cell(row=irow, column=10, value=s['signal'])
                if res['params'] is not None:
                    fitted_val = four_pl(s['conc'], *res['params'])
                    if np.isfinite(fitted_val) and fitted_val > 0:
                        ws.cell(row=irow, column=11, value=round(float(fitted_val), 1))
                irow += 1

        # Blanks
        if res.get('blanks'):
            row += 2
            _section_title(ws, row, "Blanks / Background", 3)
            row += 1
            _header_row(ws, row, ["Well", "Sample Name", "Signal"])
            row += 1
            for bl in res['blanks']:
                ws.cell(row=row, column=1, value=_safe_str(bl['well']))
                ws.cell(row=row, column=2, value=_safe_str(bl.get('sample_name', '')))
                ws.cell(row=row, column=3, value=_xv(bl['signal']))
                ws.cell(row=row, column=3).number_format = '#,##0'
                _style_row(ws, row, 3, fill=BLANK_FILL)
                row += 1

        # Unknowns
        row += 2
        _section_title(ws, row, "Interpolated Unknowns")
        row += 1
        _header_row(ws, row, ["Well", "Sample Name", "Replicate", "Signal", interp_header, "Flag"])
        row += 1

        std_concs = [s['conc'] for s in res.get('standards', []) if s['conc'] > 0]
        uloq = max(std_concs) if std_concs else None
        lloq = min(std_concs) if std_concs else None

        # Use pre-computed LLOQ signal for this spot
        lloq_sig = res.get('lloq_sig')

        # Technical-replicate numbering: wells sharing the exact same Sample Name
        # string (within this plate/spot/group) are the technical replicates that
        # _aggregate_unknowns later averages into one All Unknowns row. Any
        # replicate suffix already embedded in the sample name (e.g. _P1/-1) is
        # a biological-replicate marker and is treated as part of the sample
        # identity here, not as the technical-replicate index. Numbered 1, 2, 3…
        # in natural well order (A1, A2, ..., B1, ...) so numbering is
        # deterministic regardless of raw-file parse order.
        _unk_by_sample = defaultdict(list)
        for unk in res.get('unknowns', []):
            _unk_by_sample[unk.get('sample_name', '')].append(unk['well'])
        _replicate_num = {}   # (sample_name, well) -> replicate number
        for _sname, _wells in _unk_by_sample.items():
            for _i, _w in enumerate(sorted(_wells, key=_natural_well_key), 1):
                _replicate_num[(_sname, _w)] = _i

        for unk in res.get('unknowns', []):
            ws.cell(row=row, column=1, value=_safe_str(unk['well']))
            ws.cell(row=row, column=2, value=_safe_str(unk.get('sample_name', '')))
            ws.cell(row=row, column=3, value=_replicate_num.get(
                (unk.get('sample_name', ''), unk['well']), 1))
            ws.cell(row=row, column=4, value=_xv(unk['signal']))
            ws.cell(row=row, column=4).number_format = '#,##0'
            c_val = unk['interp_conc']
            if c_val is not None and np.isfinite(c_val):
                ws.cell(row=row, column=5, value=round(float(c_val), 4))
                ws.cell(row=row, column=5).number_format = '#,##0.0000'
                # Check signal against LLOQ signal threshold first
                if lloq_sig is not None and unk['signal'] < lloq_sig:
                    ws.cell(row=row, column=6, value="< LLOQ")
                    ws.cell(row=row, column=6).font = WARN_FONT
                elif uloq and c_val > uloq:
                    ws.cell(row=row, column=6, value="> ULOQ")
                    ws.cell(row=row, column=6).font = WARN_FONT
                elif lloq and c_val < lloq:
                    ws.cell(row=row, column=6, value="< LLOQ")
                    ws.cell(row=row, column=6).font = WARN_FONT
                else:
                    ws.cell(row=row, column=6, value="In Range")
                    ws.cell(row=row, column=6).font = PASS_FONT
            else:
                ws.cell(row=row, column=5, value="N/A")
                ws.cell(row=row, column=6, value="Out of Range")
                ws.cell(row=row, column=6).font = FAIL_FONT
            _style_row(ws, row, 6, fill=UNK_FILL)
            row += 1

        # Chart — matplotlib image (pre-generated in parallel)
        chart_path = chart_map.get((res['plate'], res['spot'], res.get('group', '')))
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
                    ws.cell(row=row + ri - 1, column=ci, value=_safe_str(part.strip()))
            # Adjust row counter
            row += len(block_lines)
            added_plates.add(plate)

        for ci, w in enumerate([14, 18, 14, 16, 14, 16, 10, 2, 14, 14, 14], 1):
            ws.column_dimensions[get_column_letter(ci)].width = w

    # ── All Unknowns Combined ─────────────────────────────────────────
    ws_all = wb.create_sheet("All Unknowns")
    all_h = ["Sample Name", "Animal", "Tissue", "Study Group", "Plate", "Spot", "Group", "Wells",
             "Replicate Signals", "Replicate " + interp_header,
             "Avg Signal", avg_interp_header,
             "%CV", "Flag", "Dilution Factor", corrected_header, "Total Protein",
             "Normalized Protein Concentration"]
    _header_row(ws_all, 1, all_h)
    arow = 2
    # Track how many TP values have been consumed per (animal, tissue) key
    tp_index = defaultdict(int)

    # Aggregate unknowns and QC in one pass (shared helper avoids re-iteration
    # in generate_html_report which calls the same function independently).
    _unk_data, _qc_data_xl = _aggregate_unknowns(results)

    for (sample_name, curve_group, plate, spot_key) in sorted(_unk_data.keys()):
        grp_data = _unk_data[(sample_name, curve_group, plate, spot_key)]
        signals = grp_data['signals']   # already finite-filtered by helper
        concs   = grp_data['concs']     # already finite-filtered by helper
        avg_signal = np.mean(signals) if signals else np.nan
        avg_conc   = np.mean(concs)   if concs   else np.nan
        # Natural well sort: A1, A2, A9, A10, B1, … instead of A1, A10, A2
        wells = ', '.join(sorted(grp_data['wells'], key=_natural_well_key))
        # Per-replicate signal / interp. conc., same natural well order as `wells`
        # above, so position i in each list corresponds to well i in Wells —
        # this is what avg_signal/avg_conc/%CV below were computed from.
        _reps_sorted = sorted(grp_data['reps'], key=lambda r: _natural_well_key(r['well']))
        rep_signals_str = ', '.join(
            f"{r['signal']:,.1f}" if np.isfinite(r['signal']) else 'N/A' for r in _reps_sorted)
        rep_concs_str = ', '.join(
            f"{r['conc']:.4f}" if (r['conc'] is not None and np.isfinite(r['conc'])) else 'N/A'
            for r in _reps_sorted)
        uloq_conc    = grp_data['uloq_conc']
        lloq_conc    = grp_data['lloq_conc']
        all_lloq_sig = grp_data['lloq_sig']

        # Determine dilution factor: QC > group > plate
        factor = _resolve_dilution_factor(
            sample_name, curve_group, plate,
            qc_dilution_factors, group_dilution_factors, plate_dilution_factors)
        is_qc_factor = bool(curve_group and group_dilution_factors and
                            (curve_group if curve_group != '_default' else '') in
                            group_dilution_factors) or (plate in plate_dilution_factors)

        corrected_conc = avg_conc * factor if np.isfinite(avg_conc) else np.nan

        flag = ""
        if np.isfinite(avg_signal) and all_lloq_sig is not None and avg_signal < all_lloq_sig:
            flag = "< LLOQ"
        elif np.isfinite(avg_conc):
            if uloq_conc is not None and avg_conc > uloq_conc:
                flag = "> ULOQ"
            elif lloq_conc is not None and avg_conc < lloq_conc:
                flag = "< LLOQ"
            else:
                flag = "In Range"
        else:
            flag = "Out of Range"

        cv = np.nan
        if len(concs) > 1 and np.isfinite(avg_conc) and avg_conc != 0:
            cv = np.std(concs, ddof=1) / avg_conc * 100

        animal, tissue = _extract_animal_tissue(sample_name)
        # Enrich tissue from TP CSV's Tissue Type column when sample name has none
        if animal and tissue is None and animal_tissue_map:
            tissue = animal_tissue_map.get(animal)
        # In-vivo Group Number from the ELISA/TP CSV (keyed by animal)
        study_group = animal_group_map.get(animal) if (animal and animal_group_map) else None
        ws_all.cell(row=arow, column=1,  value=_safe_str(sample_name))
        ws_all.cell(row=arow, column=2,  value=_safe_str(animal) if animal else None)
        ws_all.cell(row=arow, column=3,  value=_safe_str(tissue) if tissue else None)
        ws_all.cell(row=arow, column=4,  value=_safe_str(study_group) if study_group else None)
        ws_all.cell(row=arow, column=5,  value=_safe_str(str(plate)))
        ws_all.cell(row=arow, column=6,  value=_safe_str(str(spot_key)))
        ws_all.cell(row=arow, column=7,  value=_safe_str(curve_group) if curve_group else None)
        ws_all.cell(row=arow, column=8,  value=_safe_str(wells))
        ws_all.cell(row=arow, column=9,  value=_safe_str(rep_signals_str))
        ws_all.cell(row=arow, column=10, value=_safe_str(rep_concs_str))
        ws_all.cell(row=arow, column=11, value=round(float(avg_signal), 1) if np.isfinite(avg_signal) else "N/A")
        ws_all.cell(row=arow, column=11).number_format = '#,##0'
        ws_all.cell(row=arow, column=12, value=round(float(avg_conc), 4) if np.isfinite(avg_conc) else "N/A")
        ws_all.cell(row=arow, column=12).number_format = '#,##0.0000'
        # %CV (col 13)
        cv_cell = ws_all.cell(row=arow, column=13)
        cv_cell.value = round(float(cv), 1) if np.isfinite(cv) else "N/A"
        cv_cell.number_format = '0.0'
        if np.isfinite(cv) and cv_threshold is not None:
            cv_cell.fill = CV_BAD_FILL if cv > cv_threshold else CV_GOOD_FILL
        # Flag (col 14)
        ws_all.cell(row=arow, column=14, value=flag)
        cell_flag = ws_all.cell(row=arow, column=14)
        cell_flag.font = PASS_FONT if flag == "In Range" else (WARN_FONT if flag in ["> ULOQ", "< LLOQ"] else FAIL_FONT)
        # Dilution Factor (col 15)
        df_cell = ws_all.cell(row=arow, column=15)
        has_factor = is_qc_factor or (plate in plate_dilution_factors)
        df_cell.value = _xv(factor) if has_factor else None
        if has_factor:
            df_cell.number_format = '0.###'
        # Corrected Avg Interp. Conc. (col 16)
        corrected_cell = ws_all.cell(row=arow, column=16)
        corrected_cell.value = round(float(corrected_conc), 4) if np.isfinite(corrected_conc) else "N/A"
        corrected_cell.number_format = '#,##0.0000'
        # Total Protein (col 17)
        # tp_map structure: {(animal, tissue): {sample_num_int: float}}
        # tissue may be enriched from the TP CSV's Tissue Type column (see above).
        # _P1/_R1 / -1/-2 suffix → direct sample_num lookup.
        # No suffix + tissue present → sequential counter (one TP value per replicate row).
        # tissue still None after enrichment → animal-only fallback lookup.
        tp_val = None
        tp_cell = ws_all.cell(row=arow, column=17)
        if total_protein_map and animal:
            tp_key = (animal, tissue)
            tp_dict = total_protein_map.get(tp_key)
            if tp_dict is None and tissue is None:
                # Last-resort: animal-only fallback (no tissue in name or CSV)
                tp_dict = next(
                    (v for (a, _t), v in total_protein_map.items() if a == animal),
                    None)
            if tp_dict:
                rep_idx = _extract_replicate_index(sample_name)
                if rep_idx is not None:
                    # Explicit replicate suffix (_P1 / -1) → direct sample number lookup
                    tp_val = tp_dict.get(rep_idx + 1)
                elif len(tp_dict) == 1:
                    # Single TP value for this animal/tissue — use it for all rows
                    # (covers Rn2541 technical duplicates and single-sample animals)
                    tp_val = next(iter(tp_dict.values()))
                else:
                    # Multiple TP values without explicit suffix → consume sequentially
                    sorted_keys = sorted(tp_dict.keys())
                    idx = tp_index[tp_key]
                    if idx < len(sorted_keys):
                        tp_val = tp_dict[sorted_keys[idx]]
                        tp_index[tp_key] += 1
            if tp_val is not None:
                tp_cell.value = _xv(tp_val)
                tp_cell.number_format = '0.0000'
        # Normalized Protein Concentration (col 18)
        norm_cell = ws_all.cell(row=arow, column=18)
        if tp_val is not None and np.isfinite(corrected_conc) and float(tp_val) != 0:
            norm_cell.value = round(float(corrected_conc) / float(tp_val), 6)
            norm_cell.number_format = '0.000000'
        _style_row(ws_all, arow, len(all_h))
        arow += 1

    for ci in range(1, len(all_h) + 1):
        ws_all.column_dimensions[get_column_letter(ci)].width = 20

    # ── MSD Data Sheet — one row per line, tab-split into columns ────────────
    ws_msd = wb.create_sheet("MSD Data")
    with open(msd_path, 'r', encoding='utf-8', errors='replace') as f:
        msd_lines = f.readlines()
    max_cols = 0
    for r_idx, line in enumerate(msd_lines, start=1):
        fields = line.rstrip('\n').split('\t')
        max_cols = max(max_cols, len(fields))
        for c_idx, val in enumerate(fields, start=1):
            cleaned = _safe_str(val)
            if cleaned is not None:   # skip empty fields — never write "" to a cell
                ws_msd.cell(row=r_idx, column=c_idx, value=cleaned)
    # Auto-size first column (usually widest — contains row labels)
    ws_msd.column_dimensions['A'].width = 40
    for col_letter in [chr(ord('A') + i) for i in range(1, min(max_cols, 25))]:
        ws_msd.column_dimensions[col_letter].width = 18

    # ── Final sanitisation: fix any cells openpyxl mis-classified as formulas ──
    # openpyxl treats any string starting with '=' as an Excel formula and writes
    # <f>...<v /> — an empty cached-value element that Excel flags as corrupt.
    # This app never writes real formulas; force every such cell back to a plain
    # string so it serialises as a valid <is><t>…</t></is> inlineStr cell.
    for _ws in wb.worksheets:
        for _row in _ws.iter_rows():
            for _cell in _row:
                if _cell.data_type == 'f':
                    _cell.data_type = 's'

    wb.save(output_path)
    print(f"Saved: {output_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def _resource_path(*parts):
    """Locate a file shipped with the app, frozen or running from source."""
    base = getattr(sys, '_MEIPASS', None) or os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, *parts)


def _plotly_bundle_js():
    """Return the plotly.js source to write beside each HTML report.

    The report only ever draws scatter, bar and heatmap traces, so it ships the
    cartesian build (~1.4 MB) instead of the full bundle plotly's Python package
    carries (~4.8 MB) — the same library, minus the 3-D, map and specialty trace
    types this report has no way to produce. That is ~3.4 MB less in every study
    folder and a visibly faster first paint.

    Falls back to the full bundle if the vendored file is missing (a source
    checkout without it, or a build that did not include it), so the report is
    never left without Plotly.
    """
    vendored = _resource_path('vendor', 'plotly-cartesian.min.js')
    try:
        with open(vendored, encoding='utf-8') as f:
            js = f.read()
        if 'heatmap' in js and 'scatter' in js:
            return js
        print("  Note: vendored plotly bundle looks incomplete — using the full bundle")
    except OSError:
        pass
    import plotly.offline as _poff
    return _poff.get_plotlyjs()


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

def generate_html_report(results, html_path, msd_path, units=None,
                          qc_dilution_factors=None, qc_expected_concentrations=None,
                          plate_dilution_factors=None, lloq_method='current',
                          total_protein_map=None, excel_path=None,
                          group_dilution_factors=None, cv_threshold=25,
                          animal_tissue_map=None, animal_group_map=None):
    """Generate a self-contained interactive HTML report alongside the Excel output."""
    try:
        import plotly.graph_objects as go
        import plotly.offline as poff
    except ImportError:
        print("Note: plotly not installed — HTML report skipped. Install with: pip install plotly")
        return

    plate_dilution_factors = plate_dilution_factors or {}
    unit_suffix = f" ({units})" if units else ""
    lloq_method_label = "3× Blank Mean" if lloq_method == '3xblank' else "Blank Mean + 10×SD"

    # ── QC summary rows (shared helper avoids duplication with create_output) ──
    qc_summary_rows, qc_overlay_points = _compute_qc_summary(
        results, qc_dilution_factors, qc_expected_concentrations)

    # ── Per-spot standard curve figures (built in parallel) ──────────────────
    curve_raw_data = {}   # curve_key → JS-facing dict (raw calibrator points + fit metadata)

    def _build_curve_div(res):
        plate, spot, group = res['plate'], res['spot'], res.get('group', '')
        label = f"Plate {plate}, Spot {spot}" + (f", Group {group}" if group else "")
        curve_key = f"p{plate}_s{spot}_{group or 'default'}"
        div_id = f"curve_p{plate}_s{spot}_{group or 'default'}"
        fig = go.Figure()

        all_concs_pos, all_sigs_pos = [], []

        if res.get('standards'):
            std_groups_local = {}
            for s in res['standards']:
                key = s['conc']
                if key not in std_groups_local:
                    std_groups_local[key] = {'conc': key, 'signals': []}
                std_groups_local[key]['signals'].append(s['signal'])
            std_concs = sorted(std_groups_local.keys())
            std_means = [np.mean(std_groups_local[c]['signals']) for c in std_concs]
            all_concs_pos = [c for c in std_concs if c > 0]
            all_sigs_pos  = [s for s in std_means if s > 0]

            # Individual replicate points (smaller, semi-transparent)
            rep_concs, rep_sigs = [], []
            for c in std_concs:
                for sig in std_groups_local[c]['signals']:
                    rep_concs.append(c)
                    rep_sigs.append(sig)
            fig.add_trace(go.Scatter(
                x=rep_concs, y=rep_sigs,
                mode='markers', name='Replicates',
                marker=dict(color='#2F5496', size=6, symbol='circle', opacity=0.4),
                hovertemplate='Conc: %{x:.4g}<br>Signal: %{y:,.0f}<extra>Replicate</extra>'
            ))

            # Means (larger, opaque)
            fig.add_trace(go.Scatter(
                x=std_concs, y=std_means,
                mode='markers', name='Std Means',
                marker=dict(color='#2F5496', size=9, symbol='circle'),
                hovertemplate='Conc: %{x:.4g}<br>Signal: %{y:,.0f}<extra>Std Mean</extra>'
            ))

        lloq_sig = res.get('lloq_sig')
        fit_trace_idx = None
        if res['params'] is not None:
            concs_for_fit = [s['conc'] for s in res.get('standards', []) if s['conc'] > 0]
            if concs_for_fit:
                c_min, c_max = min(concs_for_fit), max(concs_for_fit)
                x_fit = np.logspace(np.log10(c_min * 0.5), np.log10(c_max * 2), 80)
                y_fit = four_pl(x_fit, *res['params'])
                all_sigs_pos += [v for v in y_fit if v > 0]
                fit_trace_idx = len(fig.data)
                fig.add_trace(go.Scatter(
                    x=list(x_fit), y=list(y_fit),
                    mode='lines', name='4PL Fit',
                    line=dict(color='#E06C4A', width=2),
                    hovertemplate='Conc: %{x:.4g}<br>Signal: %{y:,.0f}<extra>4PL Fit</extra>'
                ))

            if lloq_sig is not None and lloq_sig > 0:
                fig.add_hline(y=lloq_sig, line=dict(color='#F4A522', dash='dash', width=1.5),
                              annotation_text=f'LLOQ signal: {lloq_sig:,.0f}',
                              annotation_position='bottom right')
                all_sigs_pos.append(lloq_sig)
                # Vertical line at the interpolated LLOQ concentration
                try:
                    lloq_conc = inverse_4pl(lloq_sig, *res['params'])
                    if np.isfinite(lloq_conc) and lloq_conc > 0:
                        fig.add_vline(x=lloq_conc,
                                      line=dict(color='#F4A522', dash='dot', width=1.5),
                                      annotation_text=f'LLOQ: {lloq_conc:.4g}',
                                      annotation_position='top right')
                        all_concs_pos.append(lloq_conc)
                except Exception:
                    pass

        # ── Unknown sample scatter points ─────────────────────────────────────
        _factor = _resolve_dilution_factor(
            '', group, plate,
            None, group_dilution_factors, plate_dilution_factors)
        _unk_xs, _unk_ys, _unk_names = [], [], []
        for u in res.get('unknowns', []):
            sname = u['sample_name']
            if _identify_qc_level(sname):
                continue
            sig = u['signal']
            conc = u.get('interp_conc', np.nan)
            if np.isfinite(sig) and sig > 0 and np.isfinite(conc) and conc > 0:
                _unk_xs.append(conc * _factor)
                _unk_ys.append(sig)
                _unk_names.append(sname)
        sample_trace_idx = len(fig.data)
        has_samples = bool(_unk_xs)
        if has_samples:
            fig.add_trace(go.Scatter(
                x=_unk_xs, y=_unk_ys,
                mode='markers', name='Samples',
                marker=dict(color='#27AE60', size=8, symbol='circle-open',
                            line=dict(width=2, color='#27AE60')),
                text=_unk_names,
                hovertemplate='%{text}<br>Conc: %{x:.4g}<br>Signal: %{y:,.0f}<extra>Sample</extra>'
            ))

        x_range = ([np.log10(min(all_concs_pos)) - 0.25, np.log10(max(all_concs_pos)) + 0.25]
                   if all_concs_pos else None)
        y_range = ([np.log10(min(all_sigs_pos)) - 0.15, np.log10(max(all_sigs_pos)) + 0.15]
                   if all_sigs_pos else None)

        r2_str = f"R² = {res['r2']:.6f}" if res.get('r2') is not None else "Fit Failed"
        fig.update_layout(
            title=dict(text=f"{label}<br><sup>{r2_str}</sup>", x=0.5, font=dict(size=13)),
            xaxis=dict(title=f'Concentration{unit_suffix}', type='log',
                       showgrid=True, gridcolor='#ddd',
                       exponentformat='power', showexponent='all',
                       range=x_range),
            yaxis=dict(title='Signal', type='log',
                       showgrid=True, gridcolor='#ddd',
                       exponentformat='power', showexponent='all',
                       range=y_range),
            plot_bgcolor='white', paper_bgcolor='white',
            legend=dict(orientation='v', x=1.02, y=1),
            margin=dict(l=70, r=130, t=75, b=55),
            autosize=True, height=400
        )
        chart_html = fig.to_html(full_html=False, include_plotlyjs=False,
                                  div_id=div_id, config={'responsive': True})
        if has_samples:
            btn = (
                f'<button class="curve-toggle-btn" data-active="1" '
                f'onclick="msdToggleCurveSamples(this,\'{div_id}\',{sample_trace_idx})">'
                f'\U0001f441 Samples</button>'
            )
            chart_html = btn + chart_html

        # ── Calibrator drop table (client-side re-fit) ─────────────────────
        cal_html = ''
        raw_entry = None
        if res.get('standards') and fit_trace_idx is not None:
            std_sorted = sorted(res['standards'], key=lambda s: (s['conc'], s['well']))
            cal_rows = ''.join(
                f"<tr><td>{s['well']}</td><td class='num'>{s['conc']:g}</td>"
                f"<td class='num'>{s['signal']:,.0f}</td>"
                f"<td><input type='checkbox' class='msd-cal-cb' checked "
                f"data-conc='{s['conc']}' data-signal='{s['signal']}' "
                f"onchange=\"msdRecomputeCurve('{curve_key}')\"></td></tr>"
                for s in std_sorted
            )
            cal_html = f"""
            <div class="msd-cal-wrap">
              <div class="msd-live-row">
                <span class="msd-live-r2"></span>
                <span class="msd-live-status"></span>
                <button class="msd-reset-btn" onclick="msdResetCurve('{curve_key}')">↺ Reset Calibrators</button>
              </div>
              <table class="msd-cal-table">
                <thead><tr><th>Well</th><th class='num'>Conc</th><th class='num'>Signal</th><th>Include</th></tr></thead>
                <tbody>{cal_rows}</tbody>
              </table>
            </div>"""

            a, b, c, d = res['params'] if res['params'] is not None else (None, None, None, None)
            raw_entry = {
                'divId': div_id,
                'fitTraceIdx': fit_trace_idx,
                'label': label,
                'lloqSig': (float(lloq_sig) if lloq_sig is not None and np.isfinite(lloq_sig) else None),
                'blanks': [{'signal': float(bl['signal'])} for bl in res.get('blanks', [])
                           if np.isfinite(bl['signal'])],
                'orig': ({'a': float(a), 'b': float(b), 'c': float(c), 'd': float(d),
                          'r2': float(res['r2']) if res.get('r2') is not None else None}
                         if a is not None else None),
            }

        # A card should answer "is this curve usable?" on its own, instead of
        # sending the reader back to the Summary table to cross-reference.
        _r2v = res.get('r2')
        if res['params'] is None:
            _st_label, _st_cls = 'Failed', 'status-fail'
        elif _r2v is None or not np.isfinite(_r2v):
            _st_label, _st_cls = 'Poor', 'status-fail'
        elif _r2v >= R2_GOOD:
            _st_label, _st_cls = 'Good', 'status-good'
        elif _r2v >= R2_ACCEPTABLE:
            _st_label, _st_cls = 'Acceptable', 'status-warn'
        elif _r2v < 0:
            _st_label, _st_cls = 'Negative R²', 'status-fail'
        else:
            _st_label, _st_cls = 'Poor', 'status-fail'
        _meta = [f'<span class="{_st_cls}">{_st_label}</span>']
        if _r2v is not None and np.isfinite(_r2v):
            _meta.append(f'<span class="mono">R² {_r2v:.6f}</span>')
        _accm = res.get('accuracy')
        if _accm and _accm['lloq'] is not None:
            _meta.append(f'<span class="mono">{_accm["n_pass"]}/{_accm["n_in_range"]} cal in tol</span>')
            _meta.append(f'<span class="mono">range {_accm["lloq"]:.4g}\u2013{_accm["uloq"]:.4g}</span>')
            if _accm['n_outside']:
                _meta.append(f'<span class="mono">{_accm["n_outside"]} outside range</span>')
        elif _accm:
            _meta.append('<span class="status-fail">No calibrator in tolerance</span>')
        for _fl in (res.get('flags') or []):
            _meta.append(f'<span class="status-warn">{_fl}</span>')
        meta_html = '<div class="curve-card-meta">' + ''.join(_meta) + '</div>'

        return (curve_key, label, meta_html + chart_html + cal_html, raw_entry)

    with ThreadPoolExecutor() as _pool:
        curve_divs = list(_pool.map(_build_curve_div, results))

    # Populate curve_raw_data here, in results order, rather than from inside the
    # worker: threads finish in arbitrary order, which made the emitted JSON key
    # order — and so the report file itself — differ between identical runs.
    for _ck, _lbl, _html, _raw_entry in curve_divs:
        if _raw_entry is not None:
            curve_raw_data[_ck] = _raw_entry

    # ── Overlay figure ────────────────────────────────────────────────────────
    import json as _json
    overlay_fig = go.Figure()
    # Fixed-order categorical palette (worst adjacent-pair CVD ΔE 9.1 light /
    # 8.4 dark, OKLab x100). Assigned in order and never cycled: a 9th group
    # takes a neutral and relies on its legend label rather than on a hue
    # recycled from group 1, which would read as the same series.
    colors = ['#2a78d6', '#eb6834', '#1baf7a', '#eda100',
              '#e87ba4', '#008300', '#4a3aa7', '#e34948']
    OTHER_COLOR = '#82868f'
    _group_trace_indices = defaultdict(list)        # group → [trace indices] for toggle buttons
    _overlay_sample_indices_by_group = defaultdict(list)  # group → sample trace indices only

    # Build a stable group→color map (one color per unique group, first-seen order)
    # so that curve traces and expected-concentration bands share the same color.
    _group_color_map = {}
    _col_idx = 0
    for res in results:
        if res['params'] is None:
            continue
        g = res.get('group', '') or ''
        if g not in _group_color_map:
            _group_color_map[g] = (colors[_col_idx] if _col_idx < len(colors)
                                   else OTHER_COLOR)
            _col_idx += 1

    _overlay_x_vals = []   # accumulated during first pass; avoids a second iteration
    for i, res in enumerate(results):
        if res['params'] is None:
            continue
        concs_for_fit = [s['conc'] for s in res.get('standards', []) if s['conc'] > 0]
        if not concs_for_fit:
            continue
        c_min, c_max = min(concs_for_fit), max(concs_for_fit)
        _overlay_x_vals.extend([c_min * 0.5, c_max * 2.0])
        x_fit = np.logspace(np.log10(c_min * 0.5), np.log10(c_max * 2), 80)
        y_fit = four_pl(x_fit, *res['params'])
        x_fit = list(x_fit)
        y_fit = list(y_fit)
        plate, spot, group = res['plate'], res['spot'], res.get('group', '')
        trace_label = f"P{plate} S{spot}" + (f" {group}" if group else "")
        color = _group_color_map.get(group,
                                     colors[i] if i < len(colors) else OTHER_COLOR)
        _group_trace_indices[group or ''].append(len(overlay_fig.data))
        _overlay_curve_key = f"p{plate}_s{spot}_{group or 'default'}"
        if _overlay_curve_key in curve_raw_data:
            curve_raw_data[_overlay_curve_key]['overlayFitTraceIdx'] = len(overlay_fig.data)
        overlay_fig.add_trace(go.Scatter(
            x=x_fit, y=y_fit,
            mode='lines', name=trace_label,
            legendgroup=trace_label,
            line=dict(color=color, width=1.5),
            hovertemplate=f'%{{x:.4g}} → %{{y:,.0f}}<extra>{trace_label}</extra>'
        ))

        # Standard replicate points and means for this curve
        if res.get('standards'):
            _std_grps = {}
            for s in res['standards']:
                _std_grps.setdefault(s['conc'], []).append(s['signal'])
            _rep_xs = [c for c, sigs in _std_grps.items() for _ in sigs]
            _rep_ys = [sig for sigs in _std_grps.values() for sig in sigs]
            _mean_xs = list(_std_grps.keys())
            _mean_ys = [float(np.mean(v)) for v in _std_grps.values()]
            # Individual replicates (small, semi-transparent)
            _group_trace_indices[group or ''].append(len(overlay_fig.data))
            overlay_fig.add_trace(go.Scatter(
                x=_rep_xs, y=_rep_ys,
                mode='markers', name=f'{trace_label} replicates',
                legendgroup=trace_label, showlegend=False,
                marker=dict(color=color, size=5, symbol='circle', opacity=0.35),
                hovertemplate=f'Conc: %{{x:.4g}}<br>Signal: %{{y:,.0f}}<extra>{trace_label} replicate</extra>'
            ))
            # Means (larger, opaque)
            _group_trace_indices[group or ''].append(len(overlay_fig.data))
            overlay_fig.add_trace(go.Scatter(
                x=_mean_xs, y=_mean_ys,
                mode='markers', name=f'{trace_label} means',
                legendgroup=trace_label, showlegend=False,
                marker=dict(color=color, size=8, symbol='circle'),
                hovertemplate=f'Conc: %{{x:.4g}}<br>Mean signal: %{{y:,.0f}}<extra>{trace_label} mean</extra>'
            ))

        # Sample (unknown) scatter points for this curve, color-matched
        _grp_key = group if group and group != '_default' else ''
        if _grp_key and group_dilution_factors and _grp_key in group_dilution_factors:
            _factor = group_dilution_factors[_grp_key]
        else:
            _factor = (plate_dilution_factors or {}).get(plate, 1.0)
        _unk_xs, _unk_ys, _unk_names = [], [], []
        for u in res.get('unknowns', []):
            sname = u['sample_name']
            if _identify_qc_level(sname):
                continue
            sig = u['signal']
            conc = u.get('interp_conc', np.nan)
            if np.isfinite(sig) and sig > 0 and np.isfinite(conc) and conc > 0:
                _unk_xs.append(conc * _factor)
                _unk_ys.append(sig)
                _unk_names.append(sname)
        if _unk_xs:
            _overlay_x_vals.extend(_unk_xs)
            _sample_tidx = len(overlay_fig.data)
            _group_trace_indices[group or ''].append(_sample_tidx)
            _overlay_sample_indices_by_group[group or ''].append(_sample_tidx)
            overlay_fig.add_trace(go.Scatter(
                x=_unk_xs, y=_unk_ys,
                mode='markers', name=f'{trace_label} samples',
                legendgroup=trace_label,
                showlegend=False,
                marker=dict(color=color, size=7, symbol='circle-open',
                            line=dict(width=1.5, color=color)),
                text=_unk_names,
                hovertemplate='%{text}<br>Conc: %{x:.4g}<br>Signal: %{y:,.0f}<extra>' + trace_label + '</extra>'
            ))

    if qc_overlay_points:
        qc_level_colors = {'ULOQ': '#e41a1c', 'HQC': '#ff7f00', 'MQC': '#4daf4a',
                           'LQC': '#377eb8', 'LLOQ': '#984ea3'}
        # Group by (group, level) so each group's QC stars can be toggled independently
        qc_by_grp_level = defaultdict(list)
        for qp in qc_overlay_points:
            _qgrp = qp.get('group', '') or ''
            qc_by_grp_level[(_qgrp, qp['level'])].append(qp)
        for (_qgrp, level), pts in sorted(qc_by_grp_level.items()):
            xs = [p['corrected_conc'] for p in pts]
            ys = [p['signal'] for p in pts]
            names = [f"{p['sample_name']} (P{p['plate']})" for p in pts]
            _qgrp_label = f'{_qgrp} ' if _qgrp and _qgrp != '_default' else ''
            _group_trace_indices[_qgrp].append(len(overlay_fig.data))
            overlay_fig.add_trace(go.Scatter(
                x=xs, y=ys,
                mode='markers', name=f'QC {_qgrp_label}{level}',
                marker=dict(color=qc_level_colors.get(level, 'black'), size=12, symbol='star'),
                customdata=names,
                hovertemplate='Conc: %{x:.4g}<br>Signal: %{y:,.0f}<br>%{customdata}<extra>QC ' + _qgrp_label + level + '</extra>'
            ))

    # One LLOQ line per group label — averaged across all plates/spots sharing that label
    _group_shape_indices = defaultdict(list)   # group key → [layout.shapes indices]
    _lloq_group_palette = ['#E07B00', '#C0392B', '#1A7ABF', '#27AE60', '#8E44AD',
                           '#2C3E50', '#D35400', '#16A085', '#7F8C8D', '#F39C12']
    _lloq_by_group = defaultdict(lambda: {'sigs': [], 'concs': []})
    for res in results:
        if res.get('lloq_sig') is None or res['lloq_sig'] <= 0:
            continue
        g = res.get('group') or '_ungrouped'
        _lloq_by_group[g]['sigs'].append(res['lloq_sig'])
        if res['params'] is not None:
            try:
                lc = inverse_4pl(res['lloq_sig'], *res['params'])
                if np.isfinite(lc) and lc > 0:
                    _lloq_by_group[g]['concs'].append(
                        lc * (plate_dilution_factors or {}).get(res['plate'], 1.0))
            except Exception:
                pass

    _overlay_all_sigs = []
    for gi, (g_label, d) in enumerate(sorted(_lloq_by_group.items(),
                                              key=lambda kv: -np.mean(kv[1]['sigs']) if kv[1]['sigs'] else 0)):
        if not d['sigs']:
            continue
        avg_sig = float(np.mean(d['sigs']))
        _overlay_all_sigs.append(avg_sig)
        clr = _lloq_group_palette[gi % len(_lloq_group_palette)]
        display_name = g_label if g_label != '_ungrouped' else ''
        prefix = f'LLOQ ({display_name})' if display_name else 'LLOQ'
        if d['concs']:
            avg_conc = float(np.mean(d['concs']))
            conc_str = f'{avg_conc:.4g}' + (f' {units}' if units else '')
            ann = f'{prefix}: {avg_sig:,.0f} (signal) | {conc_str} (conc)'
        else:
            ann = f'{prefix}: {avg_sig:,.0f} (signal)'
        # Dashed horizontal line — track layout shape index for toggle
        _curve_grp_key = '' if g_label == '_ungrouped' else g_label
        _lloq_shape_idx = len(overlay_fig.layout.shapes)
        overlay_fig.add_hline(
            y=avg_sig,
            line=dict(color=clr, dash='dash', width=2),
        )
        _group_shape_indices[_curve_grp_key].append(_lloq_shape_idx)
        # Dummy trace in legend2 — positioned near the LLOQ lines (bottom of chart)
        _group_trace_indices[_curve_grp_key].append(len(overlay_fig.data))
        overlay_fig.add_trace(go.Scatter(
            x=[None], y=[None],
            mode='lines',
            name=ann,
            line=dict(color=clr, dash='dash', width=2),
            showlegend=True,
            legend='legend2',
        ))

    # Per-group ±30% expected concentration bands, color-matched to each group's curve
    if isinstance(qc_expected_concentrations, dict):
        for _grp, _exp_conc in qc_expected_concentrations.items():
            if not _exp_conc or not np.isfinite(float(_exp_conc)) or float(_exp_conc) <= 0:
                continue
            _exp_conc = float(_exp_conc)
            _band_color = _group_color_map.get(_grp, 'steelblue')
            _lo, _hi = _exp_conc * 0.7, _exp_conc * 1.3
            _grp_label = f'{_grp} ' if _grp and _grp != '_default' else ''
            _vrect_shape_idx = len(overlay_fig.layout.shapes)
            overlay_fig.add_vrect(
                x0=_lo, x1=_hi,
                fillcolor=_band_color, opacity=0.15,
                layer='below', line_width=0,
                annotation_text=f'{_grp_label}±30% ({_lo:.4g}–{_hi:.4g})',
                annotation_position='top right',
            )
            _group_shape_indices[_grp].append(_vrect_shape_idx)
    elif qc_expected_concentrations and float(qc_expected_concentrations) > 0:
        # Legacy single-value fallback
        _exp_conc = float(qc_expected_concentrations)
        _lo, _hi = _exp_conc * 0.7, _exp_conc * 1.3
        overlay_fig.add_vrect(
            x0=_lo, x1=_hi,
            fillcolor='steelblue', opacity=0.15,
            layer='below', line_width=0,
            annotation_text=f'±30% ({_lo:,.1f}–{_hi:,.1f})',
            annotation_position='top right'
        )

    # Extend x-range accumulator with QC points (the curve/sample values were
    # already collected during the first trace-building loop above).
    if qc_overlay_points:
        for _qp in qc_overlay_points:
            if np.isfinite(_qp['corrected_conc']) and _qp['corrected_conc'] > 0:
                _overlay_x_vals.append(_qp['corrected_conc'])
    overlay_x_range = None
    if _overlay_x_vals:
        overlay_x_range = [np.log10(min(_overlay_x_vals)) - 0.2,
                           np.log10(max(_overlay_x_vals)) + 0.2]

    overlay_fig.update_layout(
        title=dict(text='Standard Curve Overlay', x=0.5),
        xaxis=dict(title=f'Concentration{unit_suffix}', type='log',
                   showgrid=True, gridcolor='#eee',
                   exponentformat='power', showexponent='all',
                   range=overlay_x_range),
        yaxis=dict(title='Signal', type='log',
                   showgrid=True, gridcolor='#eee',
                   exponentformat='power', showexponent='all'),
        plot_bgcolor='white', paper_bgcolor='white',
        # Main legend: horizontal, wraps into multiple rows above the plot
        legend=dict(
            orientation='h',
            x=0, xanchor='left',
            y=1.0, yanchor='bottom',
            itemclick='toggle', itemdoubleclick='toggleothers',
            font=dict(size=11),
            tracegroupgap=4,
        ),
        # LLOQ legend: pinned to bottom-right of the chart area
        legend2=dict(
            orientation='v',
            x=1.02, xanchor='left',
            y=0, yanchor='bottom',
            bgcolor='rgba(0,0,0,0)',
            borderwidth=0,
            font=dict(size=10),
        ),
        margin=dict(l=60, r=220, t=110, b=60),
        height=644
    )
    overlay_div = overlay_fig.to_html(full_html=False, include_plotlyjs=False,
                                       div_id='overlay_chart', config={'responsive': True})

    # ── Group toggle button bar (shown above the overlay chart) ──────────────
    _all_grp_indices = [idx for idxs in _group_trace_indices.values() for idx in idxs]
    _all_shape_indices = [idx for idxs in _group_shape_indices.values() for idx in idxs]
    _overlay_btns = ''
    if len(_group_trace_indices) > 1:
        _bs = ("padding:5px 14px;border:none;border-radius:4px;cursor:pointer;"
               "font-size:12px;font-weight:500;transition:opacity 0.15s;")
        _btn_parts = [
            '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px;align-items:center;">',
            '<span style="font-size:12px;color:#555;font-weight:600;margin-right:4px;">Groups:</span>',
            f'<button style="{_bs}background:#3a506b;color:white;" '
            f'onclick="msdOverlayAll(true)">Show All</button>',
            f'<button style="{_bs}background:#888;color:white;" '
            f'onclick="msdOverlayAll(false)">Hide All</button>',
        ]
        for _grp in sorted(_group_trace_indices.keys()):
            _tidxs = _group_trace_indices[_grp]
            _sidxs = _group_shape_indices.get(_grp, [])
            _display = _grp if _grp and _grp != '_default' else 'Default'
            _clr = _group_color_map.get(_grp, '#3a506b')
            _btn_parts.append(
                f'<button data-active="1" '
                f'style="{_bs}background:{_clr};color:white;" '
                f'onclick="msdToggleGrp(this,{_json.dumps(_tidxs)},{_json.dumps(_sidxs)})">'
                f'{_display}</button>'
            )
        _btn_parts.append('</div>')
        # Per-group Samples toggle row (only shown when at least one group has samples)
        if _overlay_sample_indices_by_group:
            _btn_parts.append(
                '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px;align-items:center;">'
                '<span style="font-size:12px;color:#555;font-weight:600;margin-right:4px;">Samples:</span>'
            )
            for _grp in sorted(_overlay_sample_indices_by_group.keys()):
                _sidxs = _overlay_sample_indices_by_group[_grp]
                _clr = _group_color_map.get(_grp, '#27AE60')
                _display = _grp if _grp and _grp != '_default' else 'Default'
                _btn_parts.append(
                    f'<button data-active="1" '
                    f'style="{_bs}background:{_clr};color:white;opacity:0.75;" '
                    f'onclick="msdToggleGrp(this,{_json.dumps(_sidxs)},[])">'
                    f'\U0001f441️ {_display}</button>'
                )
            _btn_parts.append('</div>')
        _overlay_btns = ''.join(_btn_parts)

    # ── Summary table rows ────────────────────────────────────────────────────
    summary_rows_html = []
    for res in results:
        plate, spot, group = res['plate'], res['spot'], res.get('group', '')
        a = b = c = d = r2 = lloq_sig_disp = lloq_conc_disp = status = flags = 'N/A'
        if res['params'] is not None:
            a, b, c, d = [f"{v:.4g}" for v in res['params']]
            r2_val = res['r2']
            r2 = f"{r2_val:.6f}" if (r2_val is not None and np.isfinite(r2_val)) else 'N/A'
            if r2_val is None or not np.isfinite(r2_val):
                status = 'Poor'
            else:
                status = ('Good' if r2_val >= R2_GOOD
                          else 'Acceptable' if r2_val >= R2_ACCEPTABLE
                          else 'Negative R²' if r2_val < 0
                          else 'Poor')
        else:
            status = 'Failed'
        flags = ', '.join(res.get('flags') or [])
        lloq_sig = res.get('lloq_sig')
        if lloq_sig is not None:
            lloq_sig_disp = f"{lloq_sig:,.1f}"
            if res['params'] is not None:
                try:
                    lconc = inverse_4pl(lloq_sig, *res['params'])
                    if np.isfinite(lconc) and lconc > 0:
                        lloq_conc_disp = f"{lconc:.4g}"
                except Exception:
                    pass
        status_class = {'Good': 'status-good', 'Acceptable': 'status-warn',
                        'Poor': 'status-fail', 'Negative R²': 'status-fail',
                        'Failed': 'status-fail'}.get(status, '')

        # Accuracy-based quantifiable range, shown beside the blank-derived LLOQ
        acc = res.get('accuracy')
        if acc and acc['lloq'] is not None:
            acc_lloq = f"{acc['lloq']:.4g}"
            acc_uloq = f"{acc['uloq']:.4g}"
        elif acc:
            acc_lloq = acc_uloq = 'None'
        else:
            acc_lloq = acc_uloq = 'N/A'
        if acc and acc['lloq'] is not None:
            cal_pass = f"{acc['n_pass']}/{acc['n_in_range']}"
            if acc['n_outside']:
                cal_pass += f" +{acc['n_outside']}"
            cal_class = ('status-good' if acc['n_pass'] == acc['n_in_range']
                         else 'status-warn')
        elif acc:
            cal_pass, cal_class = f"0/{len(acc['levels'])}", 'status-fail'
        else:
            cal_pass, cal_class = 'N/A', ''

        curve_key = f"p{plate}_s{spot}_{group or 'default'}"
        summary_rows_html.append(
            f"<tr id='sumrow_{curve_key}'>"
            f"<td class='num'>{plate}</td><td class='num'>{spot}</td><td>{group}</td>"
            f"<td class='num'>{a}</td><td class='num'>{b}</td>"
            f"<td class='num'>{c}</td><td class='num'>{d}</td>"
            f"<td class='num'>{lloq_sig_disp}</td><td class='num'>{lloq_conc_disp}</td>"
            f"<td class='num'>{acc_lloq}</td><td class='num'>{acc_uloq}</td>"
            f"<td class='num'><span class='{cal_class}'>{cal_pass}</span></td>"
            f"<td class='num'>{r2}</td>"
            f"<td class='flag-cell'>{flags}</td>"
            f"<td><span class='{status_class}'>{status}</span></td></tr>"
        )

    # ── Run summary tiles ─────────────────────────────────────────────────────
    # A run is judged by a handful of numbers; surfacing them means not having to
    # read a 12-row table to find out whether the plate set is usable.
    _n_curves = len(results)
    _r2s = [r['r2'] for r in results
            if r.get('r2') is not None and np.isfinite(r['r2'])]
    _n_good = sum(1 for r in _r2s if r >= R2_GOOD)
    _n_accept = sum(1 for r in _r2s if R2_ACCEPTABLE <= r < R2_GOOD)
    _n_poor = _n_curves - _n_good - _n_accept
    _flagged = sum(1 for r in results if r.get('flags'))
    _accs = [r['accuracy'] for r in results if r.get('accuracy')]
    # Counted over levels inside each curve's quantifiable range; levels outside
    # it were never in scope for the tolerance test.
    _cal_total = sum(a['n_in_range'] for a in _accs)
    _cal_pass = sum(a['n_pass'] for a in _accs)
    _cal_outside = sum(a['n_outside'] for a in _accs)

    def _tile(label, value, sub='', cls=''):
        cls_attr = f" {cls}" if cls else ''
        sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ''
        return (f'<div class="kpi"><div class="kpi-label">{label}</div>'
                f'<div class="kpi-value{cls_attr}">{value}</div>{sub_html}</div>')

    _tiles = [_tile('Curves', _n_curves,
                    f'{len(set(r["plate"] for r in results))} plate(s)')]
    if _r2s:
        _mean_r2 = float(np.mean(_r2s))
        _tiles.append(_tile('Mean R²', f'{_mean_r2:.4f}', 'across fitted curves',
                            'is-good' if _mean_r2 >= R2_GOOD
                            else 'is-warn' if _mean_r2 >= R2_ACCEPTABLE else 'is-bad'))
    _tiles.append(_tile('Curve status', f'{_n_good}/{_n_curves}',
                        f'good · {_n_accept} acceptable · {_n_poor} poor',
                        'is-good' if _n_good == _n_curves
                        else 'is-bad' if _n_poor else 'is-warn'))
    if _cal_total:
        _pct = _cal_pass / _cal_total * 100.0
        _sub = f'{_pct:.0f}% within \u00b120% (\u00b125% at range ends)'
        if _cal_outside:
            _sub += f' \u00b7 {_cal_outside} outside range'
        _tiles.append(_tile('Calibrators in tolerance', f'{_cal_pass}/{_cal_total}', _sub,
                            'is-good' if _pct == 100 else 'is-warn' if _pct >= 80 else 'is-bad'))
    _tiles.append(_tile('Curves flagged', _flagged,
                        'see Flags column' if _flagged else 'none',
                        'is-good' if _flagged == 0 else 'is-warn'))
    if qc_summary_rows:
        _recs = [q['recovery'] for q in qc_summary_rows if np.isfinite(q['recovery'])]
        if _recs:
            _in_range = sum(1 for r in _recs
                            if QC_RECOVERY_LOW <= r <= QC_RECOVERY_HIGH)
            _tiles.append(_tile('QC recovery', f'{_in_range}/{len(_recs)}',
                                f'within {QC_RECOVERY_LOW:.0f}\u2013{QC_RECOVERY_HIGH:.0f}%',
                                'is-good' if _in_range == len(_recs) else 'is-bad'))
    kpi_row_html = '<div class="kpi-row">' + ''.join(_tiles) + '</div>'

    # ── QC Recovery table HTML ────────────────────────────────────────────────
    qc_table_html = ''
    if qc_summary_rows:
        qc_hdr = f"Corrected Avg Interp. Conc.{unit_suffix}"
        qc_rows_html = []
        for qr in qc_summary_rows:
            rec = qr['recovery']
            if np.isfinite(rec):
                rec_class = 'status-good' if 70 <= rec <= 130 else 'status-fail'
                rec_str = f"{rec:.1f}%"
            else:
                rec_class = ''
                rec_str = 'N/A'
            avg_sig_str = f"{qr['avg_signal']:,.1f}" if np.isfinite(qr['avg_signal']) else 'N/A'
            corr_str = f"{qr['corrected_conc']:.4g}" if np.isfinite(qr['corrected_conc']) else 'N/A'
            exp_str_val = (qc_expected_concentrations or {}).get(qr['group']) if isinstance(qc_expected_concentrations, dict) else qc_expected_concentrations
            exp_str = f"{exp_str_val:.4g}" if exp_str_val else ''
            qc_rows_html.append(
                f"<tr><td>{qr['sample_name']}</td><td>{qr['level']}</td>"
                f"<td>{qr['plate']}</td><td>{qr['group'] or ''}</td>"
                f"<td>{avg_sig_str}</td><td>{corr_str}</td><td>{exp_str}</td>"
                f"<td class='{rec_class}'>{rec_str}</td></tr>"
            )
        qc_table_html = f"""
    <h2>QC Recovery</h2>
    <div class="filter-row">
      <input class="filter-input" type="search" placeholder="🔍  Filter QC…"
             oninput="filterTable(this.value,'qcTable')">
    </div>
    <div class="table-wrap">
    <table id="qcTable" class="data-table sortable">
      <thead><tr>
        <th onclick="sortTable(this)">Sample Name</th>
        <th onclick="sortTable(this)">Level</th>
        <th class="num" onclick="sortTable(this)">Plate</th>
        <th onclick="sortTable(this)">Group</th>
        <th onclick="sortTable(this)">Avg Signal</th>
        <th onclick="sortTable(this)">{qc_hdr}</th>
        <th onclick="sortTable(this)">Expected Conc.</th>
        <th onclick="sortTable(this)">% Recovery</th>
      </tr></thead>
      <tbody>{''.join(qc_rows_html)}</tbody>
    </table>
    </div>"""

    # ── All Unknowns + QC — single aggregation pass (shared with create_output) ──
    all_unk_groups, all_qc_groups_pre = _aggregate_unknowns(results)

    _sp_entries = []
    tp_index = defaultdict(int)
    unk_rows_html = []
    has_group = bool(animal_group_map)   # show Study Group column only when ELISA group #s loaded
    for (sname, group, plate, spot_key), data in sorted(all_unk_groups.items()):
        spot         = data['spot']
        lloq_sig     = data['lloq_sig']
        uloq_conc    = data['uloq_conc']
        lloq_conc    = data['lloq_conc']

        avg_sig  = np.mean(data['signals']) if data['signals'] else np.nan
        avg_conc = np.mean(data['concs'])   if data['concs']   else np.nan
        cv = np.nan
        if len(data['concs']) > 1 and np.isfinite(avg_conc) and avg_conc != 0:
            cv = np.std(data['concs'], ddof=1) / avg_conc * 100

        flag = ''
        if np.isfinite(avg_sig) and lloq_sig is not None and avg_sig < lloq_sig:
            flag = '< LLOQ'
        elif np.isfinite(avg_conc):
            if uloq_conc and avg_conc > uloq_conc:
                flag = '> ULOQ'
            elif lloq_conc and avg_conc < lloq_conc:
                flag = '< LLOQ'
            else:
                flag = 'In Range'
        else:
            flag = 'Out of Range'

        # Dilution factor & corrected conc (QC > group > plate)
        factor = _resolve_dilution_factor(
            sname, group, plate,
            qc_dilution_factors, group_dilution_factors, plate_dilution_factors)
        corrected = avg_conc * factor if np.isfinite(avg_conc) else np.nan
        # Total protein & normalized — same logic as create_output
        animal, tissue = _extract_animal_tissue(sname)
        # Enrich tissue from TP CSV's Tissue Type column when not in sample name
        if animal and tissue is None and animal_tissue_map:
            tissue = animal_tissue_map.get(animal)
        # In-vivo Group Number from ELISA/TP CSV (keyed by animal)
        study_group = animal_group_map.get(animal) if (animal and animal_group_map) else None
        tp_val = None
        if total_protein_map and animal:
            tp_key = (animal, tissue)
            tp_dict = total_protein_map.get(tp_key)
            if tp_dict is None and tissue is None:
                # Last-resort: animal-only fallback (no tissue in name or CSV)
                tp_dict = next(
                    (v for (a, _t), v in total_protein_map.items() if a == animal),
                    None)
            if tp_dict:
                rep_idx = _extract_replicate_index(sname)
                if rep_idx is not None:
                    tp_val = tp_dict.get(rep_idx + 1)
                elif len(tp_dict) == 1:
                    # Single TP value → shared across all rows for this animal/tissue
                    tp_val = next(iter(tp_dict.values()))
                else:
                    sorted_keys = sorted(tp_dict.keys())
                    idx = tp_index[tp_key]
                    if idx < len(sorted_keys):
                        tp_val = tp_dict[sorted_keys[idx]]
                        tp_index[tp_key] += 1
        norm_val = (corrected / tp_val
                    if tp_val is not None and np.isfinite(corrected) and tp_val != 0
                    else None)

        if np.isfinite(corrected):
            _sp_entries.append({'analyte': group or 'Default', 'sample': sname,
                                'conc': float(corrected),
                                'norm': float(norm_val) if norm_val is not None else None,
                                'flag': flag, 'plate': plate,
                                'tissue': tissue, 'groupNum': study_group})

        flag_class = ('status-good' if flag == 'In Range'
                      else 'status-warn' if flag in ('> ULOQ', '< LLOQ') else '')
        cv_class = 'cv-bad' if (cv_threshold is not None and np.isfinite(cv) and cv > cv_threshold) else ''
        avg_sig_str  = f"{avg_sig:,.1f}" if np.isfinite(avg_sig) else 'N/A'
        avg_conc_str = f"{avg_conc:.4g}" if np.isfinite(avg_conc) else 'N/A'
        cv_str       = f"{cv:.1f}" if np.isfinite(cv) else 'N/A'
        corr_str     = f"{corrected:.4g}" if np.isfinite(corrected) else ''
        factor_str   = str(factor) if factor and factor != 1.0 else ''
        tp_str       = f"{tp_val:.4g}" if tp_val is not None else ''
        norm_str     = f"{norm_val:.6g}" if norm_val is not None else ''
        animal_str   = animal or ''
        tissue_str   = tissue or ''
        group_td     = f"<td>{study_group or ''}</td>" if has_group else ''
        # Escape for safe embedding inside a double-quoted HTML attribute
        # (sample names come from the user's own plate map and can contain
        # characters like " or & that would otherwise break the attribute).
        sname_attr = (sname.replace('&', '&amp;').replace('"', '&quot;')
                           .replace('<', '&lt;').replace('>', '&gt;'))
        excl_td = (f'<td class="msd-excl-cell"><input type="checkbox" class="msd-excl-cb" '
                   f'data-sample="{sname_attr}" onchange="msdToggleExcludeRow(this)" '
                   f'title="Remove {sname_attr} from the Sample Plots charts"></td>')
        unk_rows_html.append(
            f"<tr>{excl_td}<td>{sname}</td><td>{animal_str}</td><td>{tissue_str}</td>"
            f"{group_td}"
            f"<td>{plate}</td><td>{spot}</td><td>{group}</td>"
            f"<td>{', '.join(data['wells'])}</td><td>{avg_sig_str}</td>"
            f"<td>{avg_conc_str}</td><td class='{cv_class}'>{cv_str}</td>"
            f"<td class='{flag_class}'>{flag}</td>"
            f"<td>{factor_str}</td><td>{corr_str}</td>"
            f"<td>{tp_str}</td><td>{norm_str}</td></tr>"
        )

    # ── Sample plot JSON ──────────────────────────────────────────────────────
    # Group by (analyte, sample_name, plate) so each plate's measurement is a
    # separate bar.  The JS layer applies plate-checkbox filtering at render time.
    _sp_by_analyte = defaultdict(lambda: defaultdict(list))
    for e in _sp_entries:
        _sp_by_analyte[e['analyte']][(e['sample'], e['plate'])].append(
            {'conc': e['conc'], 'norm': e.get('norm'), 'flag': e['flag'],
             'tissue': e.get('tissue'), 'groupNum': e.get('groupNum')})

    _sp_has_norm = any(e.get('norm') is not None for e in _sp_entries)
    _sp_has_group = any(e.get('groupNum') for e in _sp_entries)
    _sp_has_tissue = any(e.get('tissue') for e in _sp_entries)
    _sp_all_plates = sorted({e['plate'] for e in _sp_entries})
    _sp_data = {
        'analytes': [], 'units': units or '',
        'hasNorm': _sp_has_norm,
        'hasGroup': _sp_has_group,    # any in-vivo Group Number available
        'hasTissue': _sp_has_tissue,  # any tissue available
        'plates': _sp_all_plates,   # all plate numbers, for checkbox generation
        'samples': {},
    }
    for _sp_analyte in sorted(_sp_by_analyte.keys()):
        _sp_data['analytes'].append(_sp_analyte)
        _sp_data['samples'][_sp_analyte] = []
        for (_sp_sname, _sp_plate) in sorted(_sp_by_analyte[_sp_analyte].keys()):
            _sp_elist = _sp_by_analyte[_sp_analyte][(_sp_sname, _sp_plate)]
            _sp_concs = [_e['conc'] for _e in _sp_elist]
            _sp_norms = [_e['norm'] for _e in _sp_elist if _e.get('norm') is not None]
            _sp_flags = [_e['flag'] for _e in _sp_elist]
            _sp_mean = float(np.mean(_sp_concs))
            _sp_sd = float(np.std(_sp_concs, ddof=1)) if len(_sp_concs) > 1 else 0.0
            _sp_norm_mean = float(np.mean(_sp_norms)) if _sp_norms else None
            _sp_norm_sd = float(np.std(_sp_norms, ddof=1)) if len(_sp_norms) > 1 else (0.0 if _sp_norms else None)
            _sp_any_flagged = any(f != 'In Range' for f in _sp_flags)
            # tissue / groupNum are constant per sample name — take first non-empty
            _sp_tissue = next((_e.get('tissue') for _e in _sp_elist if _e.get('tissue')), None)
            _sp_groupnum = next((_e.get('groupNum') for _e in _sp_elist if _e.get('groupNum')), None)
            _sp_data['samples'][_sp_analyte].append({
                'name': _sp_sname, 'plate': _sp_plate,
                'mean': _sp_mean, 'sd': _sp_sd, 'values': _sp_concs,
                'normMean': _sp_norm_mean, 'normSd': _sp_norm_sd, 'normValues': _sp_norms,
                'flags': _sp_flags, 'anyFlagged': _sp_any_flagged,
                'tissue': _sp_tissue, 'groupNum': _sp_groupnum
            })
    _sp_json = _json.dumps(_sp_data)

    # ── QC plot JSON (uses qc_data from the same _aggregate_unknowns call) ──────
    _qp_entries = []
    for (sname, group, plate, _sp), data in sorted(all_qc_groups_pre.items()):
        avg_conc = np.mean(data['concs']) if data['concs'] else np.nan
        if not np.isfinite(avg_conc):
            continue
        level = _identify_qc_level(sname)
        qc_factor = 1.0
        if qc_dilution_factors:
            _hgrp = group if group and group != '_default' else ''
            _grp_qc = (qc_dilution_factors or {}).get(_hgrp, {})
            if level and level in _grp_qc:
                qc_factor = _grp_qc[level]
        corrected = avg_conc * qc_factor
        exp_conc = None
        if qc_expected_concentrations:
            _hgrp = group if group and group != '_default' else ''
            exp_conc = (qc_expected_concentrations.get(_hgrp)
                        if isinstance(qc_expected_concentrations, dict)
                        else qc_expected_concentrations)
        recovery = (corrected / exp_conc * 100) if (exp_conc and exp_conc != 0) else None
        _qp_entries.append({
            'analyte': group or 'Default', 'sample': sname, 'level': level or '',
            'plate': plate, 'conc': float(corrected),
            'expected': float(exp_conc) if exp_conc is not None else None,
            'recovery': float(recovery) if recovery is not None else None,
            'values': [float(c * qc_factor) for c in data['concs']],
        })

    _qp_by_analyte = defaultdict(lambda: defaultdict(list))
    for e in _qp_entries:
        _qp_by_analyte[e['analyte']][e['sample']].append(e)

    _qp_data = {'analytes': [], 'units': units or '', 'levels': list(QC_LEVELS), 'samples': {}, 'expected': {}}
    for _qp_analyte in sorted(_qp_by_analyte.keys()):
        _qp_data['analytes'].append(_qp_analyte)
        _qp_data['samples'][_qp_analyte] = []
        for _qp_sname in sorted(_qp_by_analyte[_qp_analyte].keys()):
            _qp_elist = _qp_by_analyte[_qp_analyte][_qp_sname]
            _qp_concs = [e['conc'] for e in _qp_elist]
            _qp_vals = [v for e in _qp_elist for v in e.get('values', [])]
            _qp_mean = float(np.mean(_qp_concs))
            _qp_sd = float(np.std(_qp_concs, ddof=1)) if len(_qp_concs) > 1 else 0.0
            _qp_level = _qp_elist[0]['level']
            _qp_exp = _qp_elist[0].get('expected')
            if _qp_exp is not None:
                _qp_data['expected'][_qp_analyte] = _qp_exp
            _qp_data['samples'][_qp_analyte].append({
                'name': _qp_sname, 'level': _qp_level,
                'mean': _qp_mean, 'sd': _qp_sd, 'values': _qp_vals,
            })
    _qp_json = _json.dumps(_qp_data)

    # ── Assemble curve cards HTML ─────────────────────────────────────────────
    curves_section_html = '\n'.join(
        f'<div class="curve-card" data-curvekey="{curve_key}"><h3>{label}</h3>{div_html}</div>'
        for curve_key, label, div_html, _ in curve_divs
    )
    _curve_json = _json.dumps(curve_raw_data)

    # ── Plate Heatmap data (raw signal per well, independent of plate map) ────
    _raw_plates_for_heat = parse_msd_file(msd_path)
    _well_overlay = {}   # (plate,spot) → {well: {type,name,group,conc}}
    for res in results:
        _hp, _hs, _hg = res['plate'], res['spot'], res.get('group', '')
        _hkey = (_hp, _hs)
        _ov = _well_overlay.setdefault(_hkey, {})
        for s in res.get('standards', []):
            w = normalize_well(s['well'])
            _ov.setdefault(w, {'type': 'Standard', 'name': f"STD ({s['conc']:g})",
                                'group': _hg, 'conc': float(s['conc'])})
        for bl in res.get('blanks', []):
            w = normalize_well(bl['well'])
            _ov.setdefault(w, {'type': 'Blank', 'name': bl.get('sample_name') or 'Blank',
                                'group': _hg, 'conc': 0})
        for u in res.get('unknowns', []):
            w = normalize_well(u['well'])
            _ic = u.get('interp_conc')
            _ov.setdefault(w, {'type': 'Unknown', 'name': u['sample_name'], 'group': _hg,
                                'conc': (float(_ic) if _ic is not None and np.isfinite(_ic) else None)})

    _heatmap_data = {'plates': {}}
    for _pdata in _raw_plates_for_heat:
        _pnum = _pdata['plate_num']
        _well_signals = _pdata['data']
        if not _well_signals:
            continue
        _rows = sorted(set(re.match(r'([A-P])(\d+)', w).group(1) for w in _well_signals))
        _cols = sorted(set(int(re.match(r'([A-P])(\d+)', w).group(2)) for w in _well_signals))
        _plate_entry = {'rows': _rows, 'cols': _cols, 'spots': {}}
        for _spot_idx in range(_pdata['spots_per_well']):
            _spot_num = _spot_idx + 1
            _overlay = _well_overlay.get((_pnum, _spot_num), {})
            _wells_grid = {}
            for w, _sig_list in _well_signals.items():
                if _spot_idx >= len(_sig_list):
                    continue
                _sig = _sig_list[_spot_idx]
                _ov = _overlay.get(w, {})
                _wells_grid[w] = {
                    'signal': (float(_sig) if np.isfinite(_sig) else None),
                    'type': _ov.get('type', 'Unassigned'),
                    'name': _ov.get('name', ''),
                    'group': _ov.get('group', ''),
                    'conc': _ov.get('conc'),
                }
            _plate_entry['spots'][_spot_num] = _wells_grid
        _heatmap_data['plates'][_pnum] = _plate_entry
    _heatmap_json = _json.dumps(_heatmap_data)

    # ── Plotly JS bundle — write once alongside HTML, reference by relative path ─
    # This avoids embedding the JS in every report. Both files live in the same
    # directory so a relative src= works in any browser, including file://.
    _html_dir = os.path.dirname(os.path.abspath(html_path))
    _plotly_js_path = os.path.join(_html_dir, 'plotly.min.js')
    if not os.path.exists(_plotly_js_path):
        _bundle = _plotly_bundle_js()
        with open(_plotly_js_path, 'w', encoding='utf-8') as _pf:
            _pf.write(_bundle)
        print(f"  plotly bundle: {len(_bundle) / 1048576:.1f} MB")

    msd_basename = os.path.basename(msd_path)
    excel_basename = os.path.basename(excel_path) if excel_path else None
    # download attribute makes the browser download (and therefore open) the file
    # rather than trying to navigate to it — works for same-directory local files.
    excel_btn_html = (
        f'<a class="excel-btn" href="{excel_basename}" download="{excel_basename}">⬇ Open Excel</a>'
        if excel_basename else ''
    )
    has_tp = bool(total_protein_map)
    tp_headers = (
        "<th onclick=\"sortTable(this)\">Total Protein</th>"
        "<th onclick=\"sortTable(this)\">Normalized Conc.</th>"
    ) if has_tp else ""
    group_header = "<th onclick=\"sortTable(this)\">Study Group</th>" if has_group else ""
    unk_hdr_row = (
        "<tr>"
        "<th title=\"Remove this sample from the Sample Plots charts\">Exclude</th>"
        "<th onclick=\"sortTable(this)\">Sample Name</th>"
        "<th onclick=\"sortTable(this)\">Animal</th>"
        "<th onclick=\"sortTable(this)\">Tissue</th>"
        + group_header +
        "<th onclick=\"sortTable(this)\">Plate</th>"
        "<th onclick=\"sortTable(this)\">Spot</th>"
        "<th onclick=\"sortTable(this)\">Group</th>"
        "<th onclick=\"sortTable(this)\">Wells</th>"
        "<th onclick=\"sortTable(this)\">Avg Signal</th>"
        f"<th onclick=\"sortTable(this)\">Avg Interp. Conc.{unit_suffix}</th>"
        "<th onclick=\"sortTable(this)\">%CV</th>"
        "<th onclick=\"sortTable(this)\">Flag</th>"
        "<th onclick=\"sortTable(this)\">Dilution Factor</th>"
        f"<th onclick=\"sortTable(this)\">Corrected Avg Conc.{unit_suffix}</th>"
        + tp_headers +
        "</tr>"
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MSD 4PL Analysis Report</title>
<script src="plotly.min.js"></script>
<style>
  /* ── Design tokens ──────────────────────────────────────────────────────────
     Every colour in this report resolves to one of these. Dark mode restates
     the same roles for the dark surface rather than inverting the light values,
     and the data colours below are a validated palette (adjacent-pair CVD
     ΔE ≥ 8.4 in both modes) rather than plotly's defaults. */
  :root {{
    color-scheme: light;
    --plane:        #f4f5f7;   /* page behind the cards */
    --surface:      #ffffff;   /* card / table surface */
    --surface-2:    #f7f8fa;   /* zebra stripe, table head on cards */
    --brand:        #3a506b;   /* established report navy */
    --brand-dark:   #2e3f52;
    --brand-accent: #7ba7bc;
    --ink:          #14161a;
    --ink-2:        #52565e;
    --ink-muted:    #82868f;
    --rule:         #e3e5ea;   /* hairline */
    --rule-strong:  #ccd0d8;
    --focus:        rgba(58,80,107,0.35);
    --shadow:       0 1px 2px rgba(16,20,30,0.06), 0 2px 8px rgba(16,20,30,0.05);
    --shadow-sticky:0 2px 10px rgba(16,20,30,0.12);
    /* status — fixed roles, never reused as a series colour */
    --good:         #0f7a30;
    --good-bg:      #e7f4ea;
    --warn:         #8a5a00;
    --warn-bg:      #fdf2dc;
    --bad:          #b3261e;
    --bad-bg:       #fbe9e7;
    --modified-bg:  #fff8e1;
    /* categorical data colours (fixed order, never cycled) */
    --series-1: #2a78d6; --series-2: #eb6834; --series-3: #1baf7a; --series-4: #eda100;
    --series-5: #e87ba4; --series-6: #008300; --series-7: #4a3aa7; --series-8: #e34948;
    --series-other: #82868f;
    /* sequential ramp — one hue, light → dark */
    --seq-0: #cde2fb; --seq-1: #9ec5f4; --seq-2: #5598e7; --seq-3: #2a78d6;
    --seq-4: #256abf; --seq-5: #184f95; --seq-6: #0d366b;
    --grid:         #e1e0d9;
  }}
  /* Dark values are declared twice on purpose: the media query follows the OS
     setting, the data-theme scope follows the in-page toggle, and the toggle
     must win in both directions. */
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme="light"]) {{
      color-scheme: dark;
      --plane:        #0d0d0f;
      --surface:      #17171a;
      --surface-2:    #1f1f23;
      --brand:        #1f2b3a;
      --brand-dark:   #161f2b;
      --brand-accent: #7ba7bc;
      --ink:          #f2f3f5;
      --ink-2:        #b8bcc4;
      --ink-muted:    #898d96;
      --rule:         #2c2c31;
      --rule-strong:  #3a3a41;
      --focus:        rgba(123,167,188,0.45);
      --shadow:       0 1px 2px rgba(0,0,0,0.5), 0 2px 8px rgba(0,0,0,0.35);
      --shadow-sticky:0 2px 12px rgba(0,0,0,0.6);
      --good:         #3ecf6a;  --good-bg: #10301a;
      --warn:         #f0b429;  --warn-bg: #332708;
      --bad:          #ff7b72;  --bad-bg:  #35150f;
      --modified-bg:  #2e2612;
      --series-1: #3987e5; --series-2: #d95926; --series-3: #199e70; --series-4: #c98500;
      --series-5: #d55181; --series-6: #008300; --series-7: #9085e9; --series-8: #e66767;
      --series-other: #898d96;
      --seq-0: #0d366b; --seq-1: #184f95; --seq-2: #256abf; --seq-3: #2a78d6;
      --seq-4: #5598e7; --seq-5: #9ec5f4; --seq-6: #cde2fb;
      --grid:         #2c2c2a;
    }}
  }}
  :root[data-theme="dark"] {{
  color-scheme: dark;
  --plane:        #0d0d0f;
  --surface:      #17171a;
  --surface-2:    #1f1f23;
  --brand:        #1f2b3a;
  --brand-dark:   #161f2b;
  --brand-accent: #7ba7bc;
  --ink:          #f2f3f5;
  --ink-2:        #b8bcc4;
  --ink-muted:    #898d96;
  --rule:         #2c2c31;
  --rule-strong:  #3a3a41;
  --focus:        rgba(123,167,188,0.45);
  --shadow:       0 1px 2px rgba(0,0,0,0.5), 0 2px 8px rgba(0,0,0,0.35);
  --shadow-sticky:0 2px 12px rgba(0,0,0,0.6);
  --good:         #3ecf6a;  --good-bg: #10301a;
  --warn:         #f0b429;  --warn-bg: #332708;
  --bad:          #ff7b72;  --bad-bg:  #35150f;
  --modified-bg:  #2e2612;
  --series-1: #3987e5; --series-2: #d95926; --series-3: #199e70; --series-4: #c98500;
  --series-5: #d55181; --series-6: #008300; --series-7: #9085e9; --series-8: #e66767;
  --series-other: #898d96;
  --seq-0: #0d366b; --seq-1: #184f95; --seq-2: #256abf; --seq-3: #2a78d6;
  --seq-4: #5598e7; --seq-5: #9ec5f4; --seq-6: #cde2fb;
  --grid:         #2c2c2a;
  }}

  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
          font-size: 13px; background: var(--plane); color: var(--ink);
          -webkit-font-smoothing: antialiased; }}

  /* ── Header + tab bar: both stick, so navigation survives a long scroll ── */
  .topbar {{ position: sticky; top: 0; z-index: 40; box-shadow: var(--shadow-sticky); }}
  .header {{ background: var(--brand); color: #fff; padding: 14px 28px 12px; }}
  .header h1 {{ font-size: 19px; font-weight: 600; letter-spacing: 0.2px; }}
  .header p {{ font-size: 12px; opacity: 0.78; margin-top: 3px; }}
  .header .accent {{ height: 2px; background: var(--brand-accent); margin-top: 9px;
                     border-radius: 2px; opacity: 0.55; }}
  .tabs {{ display: flex; background: var(--brand-dark); padding: 0 20px;
           overflow-x: auto; scrollbar-width: none; }}
  .tabs::-webkit-scrollbar {{ display: none; }}
  .tab-btn {{ padding: 11px 20px; cursor: pointer; color: #c5d5e8; border: none;
              background: none; font-size: 13px; font-weight: 500; white-space: nowrap;
              border-bottom: 3px solid transparent; font-family: inherit; }}
  .tab-btn:hover {{ color: #fff; }}
  .tab-btn.active {{ color: #fff; border-bottom-color: var(--brand-accent); }}
  .tab-btn:focus-visible, .export-btn:focus-visible, .excel-btn:focus-visible,
  .sp-btn:focus-visible, .filter-input:focus-visible {{
    outline: 2px solid var(--brand-accent); outline-offset: 2px; }}

  .content {{ padding: 22px 28px 40px; max-width: 1560px; margin: 0 auto; }}
  .tab-pane {{ display: none; }}
  .tab-pane.active {{ display: block; }}

  /* ── Run summary tiles ── */
  .kpi-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
              gap: 12px; margin-bottom: 20px; }}
  .kpi {{ background: var(--surface); border: 1px solid var(--rule); border-radius: 8px;
          padding: 12px 14px; box-shadow: var(--shadow); }}
  .kpi-label {{ font-size: 11px; color: var(--ink-muted); text-transform: uppercase;
                letter-spacing: 0.6px; font-weight: 600; }}
  .kpi-value {{ font-size: 23px; font-weight: 600; margin-top: 4px; line-height: 1.15;
                color: var(--ink); }}
  .kpi-sub {{ font-size: 11px; color: var(--ink-2); margin-top: 3px; }}
  .kpi-value.is-good {{ color: var(--good); }}
  .kpi-value.is-warn {{ color: var(--warn); }}
  .kpi-value.is-bad  {{ color: var(--bad); }}

  .filter-row {{ margin-bottom: 10px; }}
  .filter-input {{ padding: 7px 12px; border: 1px solid var(--rule-strong); border-radius: 7px;
                   font-size: 13px; width: 300px; outline: none; background: var(--surface);
                   color: var(--ink); font-family: inherit; }}
  .filter-input:focus {{ border-color: var(--brand-accent);
                         box-shadow: 0 0 0 3px var(--focus); }}

  /* ── Tables ── */
  .table-wrap {{ overflow-x: auto; margin-bottom: 24px; border-radius: 8px;
                 border: 1px solid var(--rule); box-shadow: var(--shadow); }}
  .data-table {{ border-collapse: separate; border-spacing: 0; width: 100%;
                 background: var(--surface); font-variant-numeric: tabular-nums; }}
  .data-table th {{ background: var(--brand); color: #fff; padding: 9px 12px;
                    text-align: left; cursor: pointer; white-space: nowrap; user-select: none;
                    font-weight: 600; font-size: 12px; }}
  /* Only a wrapper that scrolls vertically can pin its own header row. */
  .table-wrap.tall {{ max-height: calc(100vh - var(--topbar-h, 0px) - 150px); overflow-y: auto; }}
  .table-wrap.tall .data-table th {{ position: sticky; top: 0; z-index: 2; }}
  .data-table th:hover {{ background: var(--brand-dark); }}
  .data-table th.sort-asc::after {{ content: ' ▲'; font-size: 9px; }}
  .data-table th.sort-desc::after {{ content: ' ▼'; font-size: 9px; }}
  .data-table td {{ padding: 7px 12px; border-bottom: 1px solid var(--rule);
                    vertical-align: middle; color: var(--ink); }}
  .data-table tbody tr:nth-child(even) td {{ background: var(--surface-2); }}
  .data-table tbody tr:hover td {{ background: color-mix(in srgb, var(--brand-accent) 14%, var(--surface)); }}
  .data-table tr:last-child td {{ border-bottom: none; }}
  /* Numeric columns are right-aligned so magnitudes line up digit-for-digit. */
  .data-table td.num, .data-table th.num {{ text-align: right; }}

  /* Status reads as a pill, not bare coloured text, and always carries its label. */
  .status-good, .status-warn, .status-fail {{
    display: inline-block; padding: 2px 9px; border-radius: 11px;
    font-size: 11px; font-weight: 600; white-space: nowrap; }}
  .status-good {{ color: var(--good); background: var(--good-bg); }}
  .status-warn {{ color: var(--warn); background: var(--warn-bg); }}
  .status-fail {{ color: var(--bad);  background: var(--bad-bg);  }}
  td.flag-cell {{ color: var(--ink-2); font-size: 11.5px; max-width: 260px;
                  white-space: normal; line-height: 1.4; }}
  .cv-bad {{ background: var(--bad-bg) !important; color: var(--bad); font-weight: 600; }}
  .msd-excl-cell {{ text-align: center; }}
  .msd-excl-cb {{ cursor: pointer; width: 15px; height: 15px; accent-color: var(--brand); }}
  tr.msd-row-excluded td {{ opacity: 0.4; text-decoration: line-through; }}
  tr.msd-row-excluded .msd-excl-cell {{ opacity: 1; text-decoration: none; }}
  tr.msd-row-modified td {{ background: var(--modified-bg) !important; }}
  tr.msd-row-modified td:last-child::after {{ content: ' *'; color: var(--warn); font-weight: 700; }}

  /* ── Cards & sections ── */
  .curves-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(520px, 1fr)); gap: 18px; }}
  .curve-card {{ background: var(--surface); border: 1px solid var(--rule); border-radius: 10px;
                 box-shadow: var(--shadow); padding: 14px 16px; }}
  .curve-card h3 {{ font-size: 13px; color: var(--ink); margin-bottom: 4px; font-weight: 600; }}
  .curve-card-meta {{ display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
                      margin-bottom: 10px; font-size: 11.5px; color: var(--ink-2); }}
  .curve-card-meta .mono {{ font-variant-numeric: tabular-nums; }}
  .section {{ background: var(--surface); border: 1px solid var(--rule); border-radius: 10px;
              box-shadow: var(--shadow); padding: 18px; margin-bottom: 24px; }}
  h2 {{ font-size: 15px; color: var(--ink); margin: 22px 0 12px; font-weight: 600;
        letter-spacing: 0.1px; }}
  .tab-pane > h2:first-child {{ margin-top: 4px; }}
  .hint {{ font-size: 12px; color: var(--ink-2); margin: -6px 0 12px; }}

  .export-bar {{ display: flex; justify-content: flex-end; margin-bottom: 10px; }}
  .export-btn, .excel-btn {{ padding: 7px 15px; border: none; border-radius: 7px;
                 font-size: 12.5px; cursor: pointer; font-weight: 600; font-family: inherit;
                 text-decoration: none; display: inline-block; color: #fff; }}
  .export-btn {{ background: rgba(255,255,255,0.14); }}
  .export-btn:hover {{ background: rgba(255,255,255,0.24); }}
  .excel-btn {{ background: #1e6b3c; }}
  .excel-btn:hover {{ background: #175530; }}
  .theme-btn {{ background: rgba(255,255,255,0.14); color: #fff; border: none;
                border-radius: 7px; padding: 7px 11px; cursor: pointer; font-size: 13px;
                line-height: 1; font-family: inherit; }}
  .theme-btn:hover {{ background: rgba(255,255,255,0.24); }}

  .curve-toggle-btn {{ padding: 4px 12px; border: none; border-radius: 6px; cursor: pointer;
                       font-size: 11.5px; font-weight: 600; background: var(--series-3);
                       color: #fff; margin-bottom: 6px; display: inline-block;
                       font-family: inherit; }}
  .curve-toggle-btn:hover {{ filter: brightness(0.92); }}
  .msd-cal-wrap {{ margin-top: 10px; border-top: 1px solid var(--rule); padding-top: 8px; }}
  .msd-live-row {{ display: flex; align-items: center; gap: 10px; margin-bottom: 6px; min-height: 20px; }}
  .msd-live-r2 {{ font-size: 12px; font-weight: 600; color: var(--ink); font-variant-numeric: tabular-nums; }}
  .msd-live-status {{ font-size: 11px; font-weight: 600; padding: 2px 9px; border-radius: 11px; }}
  .msd-reset-btn {{ margin-left: auto; font-size: 11px; padding: 3px 10px;
                     border: 1px solid var(--rule-strong); border-radius: 6px;
                     background: var(--surface); color: var(--ink-2); cursor: pointer;
                     font-family: inherit; }}
  .msd-reset-btn:hover {{ background: var(--surface-2); color: var(--ink); }}
  .msd-cal-table {{ width: 100%; border-collapse: collapse; font-size: 11px; max-height: 160px;
                     display: block; overflow-y: auto; font-variant-numeric: tabular-nums; }}
  .msd-cal-table thead, .msd-cal-table tbody {{ display: table; width: 100%; table-layout: fixed; }}
  .msd-cal-table th {{ position: sticky; top: 0; background: var(--surface-2); text-align: left;
                        padding: 4px 6px; font-weight: 600; color: var(--ink-2); }}
  .msd-cal-table td {{ padding: 3px 6px; border-top: 1px solid var(--rule); color: var(--ink); }}
  .msd-cal-table td.num, .msd-cal-table th.num {{ text-align: right; }}
  .msd-cal-table input[type=checkbox] {{ accent-color: var(--brand); }}
  .msd-cal-table tr.msd-cal-excluded td {{ opacity: 0.4; text-decoration: line-through; }}

  /* ── Heatmaps ── */
  .hm-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 14px; }}
  .hm-cell {{ background: var(--surface); border: 1px solid var(--rule); border-radius: 10px;
              box-shadow: var(--shadow); padding: 8px; }}
  .hm-scalebar {{ display: flex; align-items: center; gap: 10px; margin-bottom: 14px;
                  flex-wrap: wrap; font-size: 12px; color: var(--ink-2); }}
  .hm-ramp {{ height: 12px; width: 220px; border-radius: 6px; border: 1px solid var(--rule);
              background: linear-gradient(to right, var(--seq-0), var(--seq-2), var(--seq-4), var(--seq-6)); }}
  .hm-ramp-end {{ font-variant-numeric: tabular-nums; color: var(--ink); font-weight: 600; }}

  /* ── Sample plots ── */
  .sp-panel {{ background: var(--surface); border: 1px solid var(--rule); border-radius: 10px;
               box-shadow: var(--shadow); padding: 14px; }}
  .sp-drop-zone {{ min-height:80px; border:2px dashed var(--rule-strong); border-radius:8px;
                   padding:6px; display:flex; flex-wrap:wrap; gap:4px; align-content:flex-start;
                   transition:background 0.15s; }}
  .sp-drop-zone.drag-over {{ background: color-mix(in srgb, var(--brand-accent) 20%, var(--surface));
                             border-color: var(--brand); }}
  .sp-chip {{ display:inline-flex; align-items:center; gap:4px; padding:3px 8px; border-radius:12px;
              font-size:11px; cursor:grab; user-select:none; border:1px solid var(--rule-strong);
              background: var(--surface); color: var(--ink); }}
  .sp-chip.flagged {{ border-color: var(--bad); color: var(--bad); }}
  .sp-chip.sp-chip-excluded {{ opacity:0.5; background: var(--surface-2); }}
  .sp-chip.sp-chip-excluded span {{ text-decoration:line-through; }}
  .sp-chip input[type=checkbox] {{ cursor:pointer; margin:0; accent-color: var(--brand); }}
  .sp-chip-exclude-btn {{ border:none; background:transparent; cursor:pointer; font-size:11px;
                          line-height:1; padding:0 1px; color: var(--ink-muted); flex-shrink:0; }}
  .sp-chip-exclude-btn:hover {{ color: var(--bad); }}
  .sp-chip-excluded .sp-chip-exclude-btn {{ color: var(--brand); }}
  .sp-chip-excluded .sp-chip-exclude-btn:hover {{ color: var(--brand-dark); }}
  .sp-group-block {{ margin-bottom:10px; border-radius:8px; overflow:hidden; border:1px solid var(--rule); }}
  .sp-group-header {{ display:flex; align-items:center; gap:6px; padding:6px 10px; font-size:12px;
                      font-weight:600; color:#fff; }}
  .sp-group-drop {{ min-height:36px; padding:6px; display:flex; flex-wrap:wrap; gap:4px;
                    align-content:flex-start; }}
  .sp-btn {{ padding:5px 12px; border:1px solid var(--rule-strong); border-radius:6px; cursor:pointer;
             font-size:12px; background: var(--surface); color: var(--ink); font-family: inherit; }}
  .sp-btn:hover {{ background: var(--surface-2); }}
  .sp-btn-primary {{ background: var(--brand); color:#fff; border-color: var(--brand); }}
  .sp-btn-primary:hover {{ background: var(--brand-dark); }}
  .sp-btn-icon {{ padding:3px 7px; font-size:11px; border-radius:5px; }}
  .sp-sort-btn {{ background: var(--surface); }}
  .active-sort {{ background: var(--brand) !important; color:#fff !important;
                  border-color: var(--brand) !important; }}
  .sp-analyte-btn {{ padding:6px 16px; border:1px solid var(--rule-strong); border-radius:7px;
                     cursor:pointer; font-size:12.5px; font-weight:500; background: var(--surface);
                     color: var(--ink); font-family: inherit; }}
  .sp-analyte-btn.active {{ background: var(--brand); color:#fff; border-color: var(--brand); }}
  .sp-subtab-btn {{ padding:8px 18px; border:none; border-bottom:2px solid transparent;
                    background:transparent; cursor:pointer; font-size:13px; font-weight:500;
                    color: var(--ink-2); margin-bottom:-2px; font-family: inherit; }}
  .sp-subtab-btn:hover {{ color: var(--ink); }}
  .sp-subtab-active {{ color: var(--brand-accent) !important;
                       border-bottom-color: var(--brand-accent) !important; font-weight:600 !important; }}
  .sp-autogroup-bar {{ display:flex; gap:8px; align-items:center; margin-bottom:14px; flex-wrap:wrap;
                       background: var(--surface-2); border:1px solid var(--rule); border-radius:8px;
                       padding:8px 12px; }}
  .sp-autogroup-sel {{ font-size:12px; padding:4px 8px; border:1px solid var(--rule-strong);
                       border-radius:6px; background: var(--surface); color: var(--ink);
                       cursor:pointer; font-family: inherit; }}
  .sp-autogroup-sel:disabled {{ opacity:0.45; cursor:not-allowed; }}
  .sp-autogroup-sel option:disabled {{ color: var(--ink-muted); }}

  @media (max-width: 720px) {{
    .content {{ padding: 16px; }}
    .curves-grid {{ grid-template-columns: 1fr; }}
    .filter-input {{ width: 100%; }}
  }}

  @media print {{
    .tabs, .export-bar, .theme-btn, .filter-row {{ display: none !important; }}
    .tab-pane {{ display: block !important; page-break-inside: avoid; }}
    .tab-pane + .tab-pane {{ page-break-before: always; }}
    body {{ background: #fff; font-size: 11px; }}
    .header {{ position: static; }}
    .content {{ max-width: 100%; padding: 10px; }}
    .data-table th {{ background: #3a506b !important; color: #fff !important;
                      position: static; -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
    .table-wrap.tall {{ max-height: none; overflow: visible; }}
    .topbar {{ position: static; box-shadow: none; }}
    .curve-card, .section, .hm-cell, .kpi {{ box-shadow: none; border: 1px solid #ccc; }}
    .header {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
  }}
</style>
</head>
<body>

<div class="topbar">
<div class="header">
  <h1>MSD 4PL Analysis Report</h1>
  <p>{msd_basename}</p>
  <div class="accent"></div>
</div>

<div class="tabs">
  <button class="tab-btn active" onclick="showTab('summary', this)">Summary</button>
  <button class="tab-btn" onclick="showTab('curves', this)">Standard Curves</button>
  <button class="tab-btn" onclick="showTab('heatmap', this)">Plate Heatmap</button>
  <button class="tab-btn" onclick="showTab('unknowns', this)">All Unknowns</button>
  <button class="tab-btn" onclick="showTab('sampleplots', this)">Sample Plots</button>
  <button class="tab-btn" onclick="showTab('qcplots', this)">QC Plots</button>
  <div style="margin-left:auto;display:flex;align-items:center;gap:8px;padding-right:12px;">
    {excel_btn_html}
    <button class="export-btn" onclick="window.print()">⬇ Export PDF</button>
    <button class="theme-btn" id="theme-btn" onclick="msdToggleTheme()"
            title="Toggle light / dark" aria-label="Toggle light or dark theme">◐</button>
  </div>
</div>
</div>

<div class="content">

  <div id="tab-summary" class="tab-pane active">
    <h2>Run Summary</h2>
    {kpi_row_html}
    <h2>Curve Fit Summary</h2>
    <p class="hint"><strong>LLOQ Method:</strong> {lloq_method_label} &nbsp;·&nbsp;
       <em>LLOQ Conc</em> is that blank-derived signal read back through the curve;
       <em>Acc. LLOQ/ULOQ</em> is the range the calibrators themselves reproduce.</p>
    <div class="filter-row">
      <input class="filter-input" type="search" placeholder="🔍  Filter summary…"
             oninput="filterTable(this.value,'summaryTable')">
    </div>
    <div class="table-wrap">
    <table id="summaryTable" class="data-table">
      <thead><tr>
        <th onclick="sortTable(this)">Plate</th>
        <th class="num" onclick="sortTable(this)">Spot</th>
        <th onclick="sortTable(this)">Group</th>
        <th class="num" onclick="sortTable(this)">Min (a)</th>
        <th class="num" onclick="sortTable(this)">Hill Slope (b)</th>
        <th class="num" onclick="sortTable(this)">EC50 (c)</th>
        <th class="num" onclick="sortTable(this)">Max (d)</th>
        <th class="num" onclick="sortTable(this)" title="Lowest signal distinguishable from blanks, and that signal read back through the curve">LLOQ Signal</th>
        <th class="num" onclick="sortTable(this)">LLOQ Conc</th>
        <th class="num" onclick="sortTable(this)" title="Lowest calibrator that back-calculates within tolerance">Acc. LLOQ</th>
        <th class="num" onclick="sortTable(this)" title="Highest calibrator that back-calculates within tolerance">Acc. ULOQ</th>
        <th class="num" onclick="sortTable(this)" title="Calibrator levels within &plusmn;20% (&plusmn;25% at the range ends)">Cal Pass</th>
        <th class="num" onclick="sortTable(this)">R²</th>
        <th onclick="sortTable(this)">Flags</th>
        <th onclick="sortTable(this)">Status</th>
      </tr></thead>
      <tbody>{''.join(summary_rows_html)}</tbody>
    </table>
    </div>
    {qc_table_html}
    <h2>Standard Curve Overlay</h2>
    <div class="section">
      {_overlay_btns}
      {overlay_div}
    </div>
  </div>

  <div id="tab-curves" class="tab-pane">
    <h2>Standard Curves</h2>
    <p class="hint">Uncheck calibrator points below a curve to drop them and re-fit live — R² and Status update here and in the Summary table above.</p>
    <div class="curves-grid">
      {curves_section_html}
    </div>
  </div>

  <div id="tab-heatmap" class="tab-pane">
    <h2>Plate Heatmaps</h2>
    <p class="hint">Raw signal and interpolated concentration per well, independent of curve fitting. All plates share one colour scale so the same colour means the same value on every plate.</p>
    <div style="display:flex;gap:6px;margin-bottom:10px;">
      <button id="hm-metric-signal" class="sp-subtab-btn sp-subtab-active" onclick="hmSetMetric('signal',this)">Signal</button>
      <button id="hm-metric-conc" class="sp-subtab-btn" onclick="hmSetMetric('conc',this)">Interp. Concentration</button>
    </div>
    <div class="hm-scalebar">
      <span id="hm-scale-toggle" style="display:flex;gap:6px;">
        <button class="sp-btn sp-btn-icon active-sort" onclick="hmSetShared(true,this)">Shared scale</button>
        <button class="sp-btn sp-btn-icon" onclick="hmSetShared(false,this)">Per-plate scale</button>
      </span>
      <span id="hm-log-toggle" style="display:flex;gap:6px;">
        <button class="sp-btn sp-btn-icon active-sort" onclick="hmSetLog(true,this)">Log</button>
        <button class="sp-btn sp-btn-icon" onclick="hmSetLog(false,this)">Linear</button>
      </span>
      <span id="hm-scale-legend" style="display:flex;align-items:center;gap:8px;"></span>
    </div>
    <div id="hm-grid" class="hm-grid"></div>
  </div>

  <div id="tab-unknowns" class="tab-pane">
    <h2>All Unknowns</h2>
    <div class="filter-row">
      <input class="filter-input" type="search" placeholder="🔍  Filter unknowns…"
             oninput="filterTable(this.value,'unkTable')">
    </div>
    <div class="table-wrap tall">
    <table id="unkTable" class="data-table">
      <thead>{unk_hdr_row}</thead>
      <tbody>{''.join(unk_rows_html)}</tbody>
    </table>
    </div>
  </div>

  <div id="tab-sampleplots" class="tab-pane">
    <h2>Sample Plots</h2>
    <!-- Sub-tab bar -->
    <div style="display:flex;gap:0;margin-bottom:16px;border-bottom:2px solid #2F5496;">
      <button id="sp-subtab-single" class="sp-subtab-btn sp-subtab-active" onclick="spSetSubtab('single')">Per Group</button>
      <button id="sp-subtab-collated" class="sp-subtab-btn" onclick="spSetSubtab('collated')">Collated</button>
    </div>

    <!-- ── Per-Group panel (existing) ── -->
    <div id="sp-single-panel">
      <div id="sp-analyte-bar" style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px;"></div>
      <div class="sp-autogroup-bar">
        <span style="font-size:12px;font-weight:600;color:#555;">Auto-group by:</span>
        <select id="sp-autogroup-1" class="sp-autogroup-sel" onchange="spAutoGroup()">
          <option value="">None (manual)</option>
          <option value="group">Group Number</option>
          <option value="tissue">Tissue Type</option>
        </select>
        <span style="font-size:12px;color:#888;">then</span>
        <select id="sp-autogroup-2" class="sp-autogroup-sel" onchange="spAutoGroup()">
          <option value="">&mdash; none &mdash;</option>
          <option value="group">Group Number</option>
          <option value="tissue">Tissue Type</option>
        </select>
      </div>
      <div style="display:grid;grid-template-columns:280px 1fr;gap:16px;margin-bottom:16px;">
        <div style="display:flex;flex-direction:column;gap:10px;">
          <div class="sp-panel">
            <div style="font-weight:600;font-size:13px;color:#3a506b;margin-bottom:8px;display:flex;align-items:center;justify-content:space-between;">
              Unassigned Samples
              <span style="display:flex;gap:8px;align-items:center;">
                <button class="sp-btn sp-btn-icon" id="sp-select-all-btn" onclick="spSelectAllUnassigned(this)" style="font-size:11px;">Select All</button>
                <label style="font-weight:400;font-size:12px;cursor:pointer;">
                  <input type="checkbox" id="sp-show-unassigned" checked onchange="spRenderChart()"> Show
                </label>
              </span>
            </div>
            <div id="sp-unassigned-pool" class="sp-drop-zone" ondragover="spDragOver(event)" ondrop="spDrop(event,'__unassigned__')"></div>
            <div style="margin-top:8px;display:flex;gap:6px;align-items:center;flex-wrap:wrap;">
              <button class="sp-btn sp-btn-primary" onclick="spAssignChecked()">Assign to Group &#x2192;</button>
              <button class="sp-btn" onclick="spCreateGroup()">&#xFF0B; New Group</button>
            </div>
          </div>
          <div class="sp-panel" style="flex:1;">
            <div style="font-weight:600;font-size:13px;color:#3a506b;margin-bottom:8px;">Groups</div>
            <div id="sp-groups-container"></div>
          </div>
        </div>
        <div>
          <div id="sp-value-toggle" style="display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap;">
            <span style="font-size:12px;font-weight:600;color:#555;">Values:</span>
            <button class="sp-btn sp-sort-btn active-sort" id="sp-val-corrected" onclick="spSetValueMode('corrected',this)">Corrected Conc.</button>
            <button class="sp-btn sp-sort-btn" id="sp-val-norm" onclick="spSetValueMode('normalized',this)" title="Requires total protein data">Normalized Protein</button>
          </div>
          <div style="display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap;">
            <span style="font-size:12px;font-weight:600;color:#555;">Sort:</span>
            <button class="sp-btn sp-sort-btn active-sort" id="sp-sort-group" onclick="spSetSort('group',this)">By Group</button>
            <button class="sp-btn sp-sort-btn" id="sp-sort-asc" onclick="spSetSort('asc',this)">Value &#x2191;</button>
            <button class="sp-btn sp-sort-btn" id="sp-sort-desc" onclick="spSetSort('desc',this)">Value &#x2193;</button>
          </div>
          <div id="sp-plate-filter" style="display:flex;gap:6px;align-items:center;margin-bottom:10px;flex-wrap:wrap;"></div>
          <div id="sp-chart" style="width:100%;"></div>
        </div>
      </div>
    </div>

    <!-- ── Collated panel (new) ── -->
    <div id="sp-collated-panel" style="display:none;">
      <!-- Analyte/group checkboxes -->
      <div id="sp-collated-group-toggles" style="display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap;"></div>
      <div class="sp-autogroup-bar">
        <span style="font-size:12px;font-weight:600;color:#555;">Auto-group by:</span>
        <select id="sp-coll-autogroup-1" class="sp-autogroup-sel" onchange="spCollAutoGroup()">
          <option value="">None (manual)</option>
          <option value="group">Group Number</option>
          <option value="tissue">Tissue Type</option>
        </select>
        <span style="font-size:12px;color:#888;">then</span>
        <select id="sp-coll-autogroup-2" class="sp-autogroup-sel" onchange="spCollAutoGroup()">
          <option value="">&mdash; none &mdash;</option>
          <option value="group">Group Number</option>
          <option value="tissue">Tissue Type</option>
        </select>
      </div>
      <!-- 2-column layout matching Per Group -->
      <div style="display:grid;grid-template-columns:280px 1fr;gap:16px;margin-bottom:16px;">
        <div style="display:flex;flex-direction:column;gap:10px;">
          <div class="sp-panel">
            <div style="font-weight:600;font-size:13px;color:#3a506b;margin-bottom:8px;display:flex;align-items:center;justify-content:space-between;">
              Unassigned Samples
              <span style="display:flex;gap:8px;align-items:center;">
                <button class="sp-btn sp-btn-icon" id="sp-coll-select-all-btn" onclick="spCollSelectAllUnassigned(this)" style="font-size:11px;">Select All</button>
                <label style="font-weight:400;font-size:12px;cursor:pointer;">
                  <input type="checkbox" id="sp-coll-show-unassigned" checked onchange="spRenderCollatedChart()"> Show
                </label>
              </span>
            </div>
            <div id="sp-coll-unassigned-pool" class="sp-drop-zone" ondragover="spCollDragOver(event)" ondrop="spCollDrop(event,'__unassigned__')"></div>
            <div style="margin-top:8px;display:flex;gap:6px;align-items:center;flex-wrap:wrap;">
              <button class="sp-btn sp-btn-primary" onclick="spCollAssignChecked()">Assign to Group &#x2192;</button>
              <button class="sp-btn" onclick="spCollCreateGroup()">&#xFF0B; New Group</button>
            </div>
          </div>
          <div class="sp-panel" style="flex:1;">
            <div style="font-weight:600;font-size:13px;color:#3a506b;margin-bottom:8px;">Groups</div>
            <div id="sp-coll-groups-container"></div>
          </div>
        </div>
        <div>
          <div style="display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap;">
            <span style="font-size:12px;font-weight:600;color:#555;">Values:</span>
            <button class="sp-btn sp-sort-btn sp-collated-val-btn active-sort" id="sp-coll-val-corrected" onclick="spCollSetValueMode('corrected',this)">Corrected Conc.</button>
            <button class="sp-btn sp-sort-btn sp-collated-val-btn" id="sp-coll-val-norm" onclick="spCollSetValueMode('normalized',this)" title="Requires total protein data">Normalized Protein</button>
          </div>
          <div style="display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap;">
            <span style="font-size:12px;font-weight:600;color:#555;">Sort:</span>
            <button class="sp-btn sp-sort-btn sp-collated-sort-btn active-sort" id="sp-coll-sort-group" onclick="spCollSetSort('group',this)">By Group</button>
            <button class="sp-btn sp-sort-btn sp-collated-sort-btn" id="sp-coll-sort-asc" onclick="spCollSetSort('asc',this)">Value &#x2191;</button>
            <button class="sp-btn sp-sort-btn sp-collated-sort-btn" id="sp-coll-sort-desc" onclick="spCollSetSort('desc',this)">Value &#x2193;</button>
          </div>
          <div id="sp-collated-chart" style="width:100%;"></div>
        </div>
      </div>
    </div>
  </div>

  <div id="tab-qcplots" class="tab-pane">
    <h2>QC Plots</h2>
    <div id="qp-analyte-bar" style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px;"></div>
    <div style="display:grid;grid-template-columns:280px 1fr;gap:16px;margin-bottom:16px;">
      <div style="display:flex;flex-direction:column;gap:10px;">
        <div class="sp-panel">
          <div style="font-weight:600;font-size:13px;color:#3a506b;margin-bottom:8px;display:flex;align-items:center;justify-content:space-between;">
            Unassigned Samples
            <span style="display:flex;gap:8px;align-items:center;">
              <button class="sp-btn sp-btn-icon" id="qp-select-all-btn" onclick="qpSelectAllUnassigned(this)" style="font-size:11px;">Select All</button>
              <label style="font-weight:400;font-size:12px;cursor:pointer;">
                <input type="checkbox" id="qp-show-unassigned" checked onchange="qpRenderChart()"> Show
              </label>
            </span>
          </div>
          <div id="qp-unassigned-pool" class="sp-drop-zone" ondragover="qpDragOver(event)" ondrop="qpDrop(event,'__unassigned__')"></div>
          <div style="margin-top:8px;display:flex;gap:6px;align-items:center;flex-wrap:wrap;">
            <button class="sp-btn sp-btn-primary" onclick="qpAssignChecked()">Assign to Group &#x2192;</button>
            <button class="sp-btn" onclick="qpCreateGroup()">&#xFF0B; New Group</button>
          </div>
        </div>
        <div class="sp-panel" style="flex:1;">
          <div style="font-weight:600;font-size:13px;color:#3a506b;margin-bottom:8px;">Groups</div>
          <div id="qp-groups-container"></div>
        </div>
      </div>
      <div>
        <div style="display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap;">
          <span style="font-size:12px;font-weight:600;color:#555;">Sort:</span>
          <button class="sp-btn sp-sort-btn active-sort" id="qp-sort-group" onclick="qpSetSort('group',this)">By Group</button>
          <button class="sp-btn sp-sort-btn" id="qp-sort-asc" onclick="qpSetSort('asc',this)">Value &#x2191;</button>
          <button class="sp-btn sp-sort-btn" id="qp-sort-desc" onclick="qpSetSort('desc',this)">Value &#x2193;</button>
        </div>
        <div id="qp-chart" style="width:100%;"></div>
      </div>
    </div>
  </div>

</div>

<script>
// Sticky table headers must clear the sticky topbar, whose height depends on how
// the source filename wraps — so it is measured rather than assumed.
function msdSyncTopbarHeight() {{
  var bar = document.querySelector('.topbar');
  if (!bar) return;
  document.documentElement.style.setProperty(
    '--topbar-h', Math.round(bar.getBoundingClientRect().height) + 'px');
}}
window.addEventListener('load', msdSyncTopbarHeight);
window.addEventListener('resize', msdSyncTopbarHeight);

// ── Theme ─────────────────────────────────────────────────────────────────────
// Plotly figures are given explicit colours at build time, so a CSS-only theme
// switch would leave every chart on a white card in a dark page. Each toggle
// therefore restyles the live figures from the same tokens the CSS uses.
function msdThemeTokens() {{
  var cs = getComputedStyle(document.documentElement);
  var get = function(n) {{ return cs.getPropertyValue(n).trim(); }};
  return {{ surface: get('--surface'), ink: get('--ink'), ink2: get('--ink-2'),
           muted: get('--ink-muted'), grid: get('--grid') }};
}}

function msdIsDark() {{
  var stamp = document.documentElement.getAttribute('data-theme');
  if (stamp) return stamp === 'dark';
  return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
}}

function msdApplyChartTheme() {{
  if (typeof Plotly === 'undefined') return;
  var t = msdThemeTokens();
  document.querySelectorAll('.js-plotly-plot').forEach(function(gd) {{
    try {{
      Plotly.relayout(gd, {{
        paper_bgcolor: t.surface, plot_bgcolor: t.surface,
        'font.color': t.ink2,
        'title.font.color': t.ink,
        'xaxis.gridcolor': t.grid, 'yaxis.gridcolor': t.grid,
        'xaxis.linecolor': t.grid, 'yaxis.linecolor': t.grid,
        'xaxis.zerolinecolor': t.grid, 'yaxis.zerolinecolor': t.grid,
        'xaxis.tickfont.color': t.muted, 'yaxis.tickfont.color': t.muted,
        'xaxis.title.font.color': t.ink2, 'yaxis.title.font.color': t.ink2,
        'legend.font.color': t.ink2
      }});
    }} catch (e) {{ /* a figure without these axes is fine to skip */ }}
  }});
  if (typeof hmChartEntries !== 'undefined' && hmChartEntries.length) hmRenderAll();
}}

function msdToggleTheme() {{
  var next = msdIsDark() ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  try {{ localStorage.setItem('msdTheme', next); }} catch (e) {{ /* private window */ }}
  msdApplyChartTheme();
}}

(function msdInitTheme() {{
  var saved = null;
  try {{ saved = localStorage.getItem('msdTheme'); }} catch (e) {{ /* private window */ }}
  if (saved === 'dark' || saved === 'light') {{
    document.documentElement.setAttribute('data-theme', saved);
  }}
  if (msdIsDark()) {{
    // Charts are built with light colours; restyle once after Plotly draws them.
    window.addEventListener('load', msdApplyChartTheme);
  }}
}})();

function showTab(name, btn) {{
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  const pane = document.getElementById('tab-' + name);
  pane.classList.add('active');
  btn.classList.add('active');
  // Resize all Plotly charts now that their containers are visible
  pane.querySelectorAll('.js-plotly-plot').forEach(el => Plotly.Plots.resize(el));
  if (name === 'sampleplots') spInit();
  if (name === 'qcplots') qpInit();
  if (name === 'heatmap') hmInit();
  // After the lazy builders, not before — these tabs create their figures on
  // first open, so theming ahead of them would leave white charts on a dark page.
  if (msdIsDark()) msdApplyChartTheme();
}}

function sortTable(th) {{
  const table = th.closest('table');
  const tbody = table.querySelector('tbody');
  const col = Array.from(th.parentNode.children).indexOf(th);
  const asc = th.classList.contains('sort-asc');
  table.querySelectorAll('th').forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
  th.classList.add(asc ? 'sort-desc' : 'sort-asc');
  const dir = asc ? -1 : 1;
  const rows = Array.from(tbody.querySelectorAll('tr'));
  rows.sort((a, b) => {{
    const av = a.cells[col]?.textContent.trim() ?? '';
    const bv = b.cells[col]?.textContent.trim() ?? '';
    const an = parseFloat(av.replace(/[,%]/g, ''));
    const bn = parseFloat(bv.replace(/[,%]/g, ''));
    if (!isNaN(an) && !isNaN(bn)) return (an - bn) * dir;
    return av.localeCompare(bv) * dir;
  }});
  rows.forEach(r => tbody.appendChild(r));
}}

function filterTable(query, tableId) {{
  const q = query.toLowerCase().trim();
  document.getElementById(tableId).querySelectorAll('tbody tr').forEach(row => {{
    const match = !q || row.textContent.toLowerCase().includes(q);
    row.style.display = match ? 'table-row' : 'none';
  }});
}}

function msdToggleGrp(btn, traceIndices, shapeIndices) {{
  var gd = document.getElementById('overlay_chart');
  var active = btn.getAttribute('data-active') === '1';
  if (traceIndices && traceIndices.length) {{
    Plotly.restyle(gd, {{visible: active ? false : true}}, traceIndices);
  }}
  if (shapeIndices && shapeIndices.length) {{
    var shapeUpd = {{}};
    shapeIndices.forEach(function(i) {{ shapeUpd['shapes[' + i + '].visible'] = !active; }});
    Plotly.relayout(gd, shapeUpd);
  }}
  btn.setAttribute('data-active', active ? '0' : '1');
  btn.style.opacity = active ? '0.4' : '1.0';
}}
function msdOverlayAll(show) {{
  var gd = document.getElementById('overlay_chart');
  var allTraceIdx = {_json.dumps(_all_grp_indices)};
  var allShapeIdx = {_json.dumps(_all_shape_indices)};
  if (allTraceIdx.length) {{
    Plotly.restyle(gd, {{visible: show ? true : false}}, allTraceIdx);
  }}
  if (allShapeIdx.length) {{
    var shapeUpd = {{}};
    allShapeIdx.forEach(function(i) {{ shapeUpd['shapes[' + i + '].visible'] = show; }});
    Plotly.relayout(gd, shapeUpd);
  }}
  document.querySelectorAll('[data-active]').forEach(function(b) {{
    b.setAttribute('data-active', show ? '1' : '0');
    b.style.opacity = show ? '1.0' : '0.4';
  }});
}}
function msdToggleCurveSamples(btn, divId, traceIdx) {{
  var gd = document.getElementById(divId);
  var active = btn.getAttribute('data-active') === '1';
  Plotly.restyle(gd, {{visible: active ? false : true}}, [traceIdx]);
  btn.setAttribute('data-active', active ? '0' : '1');
  btn.style.opacity = active ? '0.4' : '1.0';
}}

// ── Sample Plots Tab ─────────────────────────────────────────────────────────
var HEATMAP_DATA = {_heatmap_json};
var CURVE_DATA = {_curve_json};
var SP_DATA = {_sp_json};
var spInitialized = false;
var spCurrentAnalyte = null;
// Sample names removed from the chart entirely (shared across Per Group and
// Collated — a sample excluded in one view stays excluded in the other).
// Purely a display filter: excluded samples are still listed (greyed out,
// struck through) in the Unassigned/Group panels so they can be restored.
var spExcludedSamples = new Set();
var spGroups = [];          // [{{id, name, color, visible, samples:[]}}]
var spUnassigned = [];      // [sampleName, ...]
var spSortMode = 'group';
var spGroupIdCounter = 0;
var spDragPayload = null;   // {{name, fromGroup}}
var spLastCheckedIdx = -1;  // for shift+click range selection in unassigned pool
var spValueMode = 'corrected';  // 'corrected' | 'normalized'
var SP_PALETTE = ['#1f77b4','#ff7f0e','#2ca02c','#9467bd','#8c564b','#e377c2','#17becf','#bcbd22'];
var spActivePlates = new Set();   // plates currently shown; empty = all shown

var QP_DATA = {_qp_json};
var qpInitialized = false;
var qpCurrentAnalyte = null;
var qpGroups = [];
var qpUnassigned = [];
var qpSortMode = 'group';
var qpGroupIdCounter = 0;
var qpDragPayload = null;
var qpLastCheckedIdx = -1;
var QP_PALETTE = ['#e41a1c','#ff7f00','#4daf4a','#377eb8','#984ea3','#a65628','#f781bf','#999999'];

function spNextColor() {{
  return SP_PALETTE[spGroups.length % SP_PALETTE.length];
}}

function spBuildPlateFilter() {{
  var bar = document.getElementById('sp-plate-filter');
  if (!bar) return;
  var plates = SP_DATA.plates || [];
  if (plates.length <= 1) {{ bar.style.display = 'none'; return; }}
  bar.innerHTML = '<span style="font-size:12px;font-weight:600;color:#555;margin-right:2px;">Plates:</span>';
  plates.forEach(function(p) {{
    var lbl = document.createElement('label');
    lbl.style.cssText = 'display:flex;align-items:center;gap:3px;font-size:12px;cursor:pointer;';
    var cb = document.createElement('input');
    cb.type = 'checkbox'; cb.checked = true; cb.value = p;
    cb.onchange = function() {{
      if (cb.checked) {{ spActivePlates.add(p); }} else {{ spActivePlates.delete(p); }}
      spRenderChart();
    }};
    lbl.appendChild(cb);
    lbl.appendChild(document.createTextNode('Plate ' + p));
    bar.appendChild(lbl);
  }});
  // Initialise spActivePlates to all plates
  spActivePlates = new Set(plates);
}}

function spInit() {{
  if (!spInitialized) {{
    spInitialized = true;
    spGroups = [];
    spGroupIdCounter = 0;
    spSortMode = 'group';
    spValueMode = 'corrected';
    var analytes = SP_DATA.analytes || [];
    spCurrentAnalyte = analytes.length > 0 ? analytes[0] : null;
    // Unique sample names for group assignment (plate-agnostic)
    var allNames = [];
    if (spCurrentAnalyte && SP_DATA.samples[spCurrentAnalyte]) {{
      var seen = {{}};
      SP_DATA.samples[spCurrentAnalyte].forEach(function(s) {{
        if (!seen[s.name]) {{ seen[s.name] = true; allNames.push(s.name); }}
      }});
    }}
    spUnassigned = allNames;
    spBuildAnalyteBar();
    spBuildPlateFilter();
    // Initialise spActivePlates to all plates
    spActivePlates = new Set(SP_DATA.plates || []);
    // Show value-mode toggle only when normalized data is available
    var normBtn = document.getElementById('sp-val-norm');
    if (normBtn && !SP_DATA.hasNorm) {{
      normBtn.disabled = true;
      normBtn.style.opacity = '0.4';
      normBtn.style.cursor = 'not-allowed';
      normBtn.title = 'No total protein data loaded';
    }}
    spSetupAutogroupSelects('sp-autogroup-1', 'sp-autogroup-2');
  }}
  spRenderGroupPanel();
  spRenderChart();
}}

function spBuildAnalyteBar() {{
  var bar = document.getElementById('sp-analyte-bar');
  if (!bar) return;
  bar.innerHTML = '';
  (SP_DATA.analytes || []).forEach(function(a) {{
    var btn = document.createElement('button');
    btn.className = 'sp-analyte-btn' + (a === spCurrentAnalyte ? ' active' : '');
    btn.textContent = a;
    btn.onclick = function() {{ spSelectAnalyte(a); }};
    bar.appendChild(btn);
  }});
}}

function spSelectAnalyte(name) {{
  spCurrentAnalyte = name;
  // Update unassigned: unique sample names in current analyte not already in a group
  var seen = {{}};
  var allSamples = [];
  (SP_DATA.samples[name] || []).forEach(function(s) {{
    if (!seen[s.name]) {{ seen[s.name] = true; allSamples.push(s.name); }}
  }});
  var inGroups = {{}};
  spGroups.forEach(function(g) {{ g.samples.forEach(function(s) {{ inGroups[s] = true; }}); }});
  spUnassigned = allSamples.filter(function(s) {{ return !inGroups[s]; }});
  document.querySelectorAll('.sp-analyte-btn').forEach(function(b) {{
    b.classList.toggle('active', b.textContent === name);
  }});
  // If an auto-group mode is active, recompute groups for the new analyte's samples
  var _ag1 = document.getElementById('sp-autogroup-1');
  var _ag2 = document.getElementById('sp-autogroup-2');
  if ((_ag1 && _ag1.value) || (_ag2 && _ag2.value)) {{ spAutoGroup(); return; }}
  spRenderGroupPanel();
  spRenderChart();
}}

function spGetSampleData(sname) {{
  if (!spCurrentAnalyte || !SP_DATA.samples[spCurrentAnalyte]) return null;
  var arr = SP_DATA.samples[spCurrentAnalyte];
  for (var i = 0; i < arr.length; i++) {{
    if (arr[i].name === sname) return arr[i];
  }}
  return null;
}}

function spRenderGroupPanel() {{
  spLastCheckedIdx = -1;  // reset range anchor whenever the pool is rebuilt
  // Unassigned pool
  var pool = document.getElementById('sp-unassigned-pool');
  if (pool) {{
    pool.innerHTML = '';
    spUnassigned.forEach(function(sname) {{
      pool.appendChild(spMakeChip(sname, '__unassigned__'));
    }});
  }}
  // Groups container
  var gc = document.getElementById('sp-groups-container');
  if (!gc) return;
  gc.innerHTML = '';
  spGroups.forEach(function(g, gi) {{
    var block = document.createElement('div');
    block.className = 'sp-group-block';
    // Header
    var hdr = document.createElement('div');
    hdr.className = 'sp-group-header';
    hdr.style.background = g.color;
    var isFirst = (gi === 0);
    var isLast  = (gi === spGroups.length - 1);
    var btnStyle = 'background:rgba(255,255,255,0.25);color:white;border-color:rgba(255,255,255,0.4);';
    var btnDisabled = 'background:rgba(255,255,255,0.08);color:rgba(255,255,255,0.3);border-color:rgba(255,255,255,0.15);cursor:default;';
    hdr.innerHTML =
      '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + g.name + '">' + g.name + '</span>' +
      '<button class="sp-btn sp-btn-icon" style="' + (isFirst ? btnDisabled : btnStyle) + '" ' +
        (isFirst ? 'disabled ' : 'onclick="spMoveGroup(' + g.id + ',-1)" ') + 'title="Move up">&#x25B4;</button>' +
      '<button class="sp-btn sp-btn-icon" style="' + (isLast  ? btnDisabled : btnStyle) + '" ' +
        (isLast  ? 'disabled ' : 'onclick="spMoveGroup(' + g.id + ',1)" ')  + 'title="Move down">&#x25BE;</button>' +
      '<button class="sp-btn sp-btn-icon" style="' + btnStyle + '" ' +
        'onclick="spRenameGroup(' + g.id + ')" title="Rename group">&#x270F;</button>' +
      '<button class="sp-btn sp-btn-icon" style="' + btnStyle + '" ' +
        'onclick="spToggleGroup(' + g.id + ')" title="Show/hide in chart">' + (g.visible ? '&#128065;' : '&#128564;') + '</button>' +
      '<button class="sp-btn sp-btn-icon" style="' + btnStyle + '" ' +
        'onclick="spDeleteGroup(' + g.id + ')" title="Delete group">&#x2715;</button>';
    block.appendChild(hdr);
    // Drop zone
    var dz = document.createElement('div');
    dz.className = 'sp-group-drop sp-drop-zone';
    dz.setAttribute('ondragover', 'spDragOver(event)');
    dz.setAttribute('ondrop', "spDrop(event,'" + g.id + "')");
    g.samples.forEach(function(sname) {{
      dz.appendChild(spMakeChip(sname, g.id));
    }});
    block.appendChild(dz);
    gc.appendChild(block);
  }});
}}

function spMakeChip(sname, groupId) {{
  var d = spGetSampleData(sname);
  var flagged = d && d.anyFlagged;
  var excluded = spExcludedSamples.has(sname);
  var span = document.createElement('span');
  span.className = 'sp-chip' + (flagged ? ' flagged' : '') + (excluded ? ' sp-chip-excluded' : '');
  span.draggable = true;
  span.title = excluded ? sname + ' (excluded from chart)' : sname;
  var label = (flagged ? '⚠ ' : '') + sname;
  if (groupId === '__unassigned__') {{
    // Add checkbox with shift+click range selection
    var cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.setAttribute('data-sample', sname);
    cb.onclick = function(e) {{
      e.stopPropagation();
      var idx = spUnassigned.indexOf(sname);
      var allCbs = Array.from(document.querySelectorAll('#sp-unassigned-pool input[type=checkbox]'));
      if (e.shiftKey && spLastCheckedIdx >= 0 && idx !== spLastCheckedIdx) {{
        var lo = Math.min(spLastCheckedIdx, idx);
        var hi = Math.max(spLastCheckedIdx, idx);
        var newState = allCbs[idx].checked;
        for (var i = lo; i <= hi; i++) {{
          if (allCbs[i]) allCbs[i].checked = newState;
        }}
      }}
      spLastCheckedIdx = idx;
    }};
    span.appendChild(cb);
  }}
  var excludeBtn = document.createElement('button');
  excludeBtn.type = 'button';
  excludeBtn.className = 'sp-chip-exclude-btn';
  excludeBtn.title = excluded ? 'Restore to chart' : 'Remove from chart';
  excludeBtn.textContent = excluded ? '↺' : '✕';
  excludeBtn.onclick = function(e) {{
    e.stopPropagation();
    spToggleExclude(sname);
  }};
  span.appendChild(excludeBtn);
  var txt = document.createElement('span');
  txt.textContent = label;
  txt.style.maxWidth = '120px';
  txt.style.overflow = 'hidden';
  txt.style.textOverflow = 'ellipsis';
  txt.style.whiteSpace = 'nowrap';
  span.appendChild(txt);
  span.addEventListener('dragstart', function(e) {{ spDragStart(e, sname, groupId); }});
  span.addEventListener('dragend', function(e) {{ spDragEnd(e); }});
  return span;
}}

// Shared between Per Group, Collated, and the All Unknowns "Exclude" checkbox
// column — toggling exclusion from any of the three keeps all of them in
// sync, and re-renders whichever tab(s) are initialized.
function spToggleExclude(sname) {{
  if (spExcludedSamples.has(sname)) {{ spExcludedSamples.delete(sname); }}
  else {{ spExcludedSamples.add(sname); }}
  if (spInitialized) {{ spRenderGroupPanel(); spRenderChart(); }}
  var collToggles = document.getElementById('sp-collated-group-toggles');
  if (collToggles && collToggles.dataset.built) {{ spCollRenderGroupPanel(); spRenderCollatedChart(); }}
  msdSyncExcludeCheckboxes();
}}

// Called when the "Exclude" checkbox on an All Unknowns row is toggled.
function msdToggleExcludeRow(cb) {{
  spToggleExclude(cb.dataset.sample);
}}

// Keeps every "Exclude" checkbox in the All Unknowns table (a sample name can
// appear on more than one row, e.g. across plates) — and that row's styling —
// in sync with spExcludedSamples, regardless of whether the change came from
// this table or from a chip's ✕ button in Sample Plots.
function msdSyncExcludeCheckboxes() {{
  document.querySelectorAll('.msd-excl-cb').forEach(function(cb) {{
    var excluded = spExcludedSamples.has(cb.dataset.sample);
    cb.checked = excluded;
    var row = cb.closest('tr');
    if (row) {{ row.classList.toggle('msd-row-excluded', excluded); }}
  }});
}}

function spDragStart(e, name, fromGroup) {{
  spDragPayload = {{name: name, fromGroup: fromGroup}};
  e.dataTransfer.effectAllowed = 'move';
  e.currentTarget.style.opacity = '0.4';
}}

function spDragEnd(e) {{
  e.currentTarget.style.opacity = '1';
  document.querySelectorAll('.sp-drop-zone').forEach(function(z) {{
    z.classList.remove('drag-over');
  }});
}}

function spDragOver(e) {{
  e.preventDefault();
  e.dataTransfer.dropEffect = 'move';
  var zone = e.currentTarget.closest('.sp-drop-zone');
  if (zone) zone.classList.add('drag-over');
}}

function spDrop(e, toGroupId) {{
  e.preventDefault();
  document.querySelectorAll('.sp-drop-zone').forEach(function(z) {{ z.classList.remove('drag-over'); }});
  if (!spDragPayload) return;
  var name = spDragPayload.name;
  var fromGroup = spDragPayload.fromGroup;
  spDragPayload = null;
  if (fromGroup === toGroupId) return;
  // Remove from source
  if (fromGroup === '__unassigned__') {{
    spUnassigned = spUnassigned.filter(function(s) {{ return s !== name; }});
  }} else {{
    var sg = spGroups.find(function(g) {{ return g.id == fromGroup; }});
    if (sg) sg.samples = sg.samples.filter(function(s) {{ return s !== name; }});
  }}
  // Add to destination
  if (toGroupId === '__unassigned__') {{
    if (spUnassigned.indexOf(name) === -1) spUnassigned.push(name);
  }} else {{
    var tg = spGroups.find(function(g) {{ return g.id == toGroupId; }});
    if (tg && tg.samples.indexOf(name) === -1) tg.samples.push(name);
  }}
  spRenderGroupPanel();
  spRenderChart();
}}

function spCreateGroup() {{
  var name = prompt('Group name:');
  if (!name || !name.trim()) return;
  name = name.trim();
  spGroups.push({{
    id: ++spGroupIdCounter,
    name: name,
    color: spNextColor(),
    visible: true,
    samples: []
  }});
  spRenderGroupPanel();
  spRenderChart();
}}

function spAssignChecked() {{
  var checked = [];
  document.querySelectorAll('#sp-unassigned-pool input[type=checkbox]:checked').forEach(function(cb) {{
    checked.push(cb.getAttribute('data-sample'));
  }});
  if (!checked.length) {{ alert('Check at least one sample first.'); return; }}
  var name = prompt('Assign to group name:');
  if (!name || !name.trim()) return;
  name = name.trim();
  var g = spGroups.find(function(x) {{ return x.name === name; }});
  if (!g) {{
    g = {{ id: ++spGroupIdCounter, name: name, color: spNextColor(), visible: true, samples: [] }};
    spGroups.push(g);
  }}
  checked.forEach(function(s) {{
    spUnassigned = spUnassigned.filter(function(u) {{ return u !== s; }});
    if (g.samples.indexOf(s) === -1) g.samples.push(s);
  }});
  spRenderGroupPanel();
  spRenderChart();
}}

function spMoveGroup(id, dir) {{
  var idx = spGroups.findIndex(function(x) {{ return x.id == id; }});
  var newIdx = idx + dir;
  if (newIdx < 0 || newIdx >= spGroups.length) return;
  var tmp = spGroups[idx];
  spGroups[idx] = spGroups[newIdx];
  spGroups[newIdx] = tmp;
  spRenderGroupPanel();
  spRenderChart();
}}
function spRenameGroup(id) {{
  var g = spGroups.find(function(x) {{ return x.id == id; }});
  if (!g) return;
  var newName = prompt('Rename group:', g.name);
  if (newName === null || newName.trim() === '') return;
  g.name = newName.trim();
  spRenderGroupPanel();
  spRenderChart();
}}
function spDeleteGroup(id) {{
  var g = spGroups.find(function(x) {{ return x.id == id; }});
  if (!g) return;
  // Return samples to unassigned
  g.samples.forEach(function(s) {{
    if (spUnassigned.indexOf(s) === -1) spUnassigned.push(s);
  }});
  spGroups = spGroups.filter(function(x) {{ return x.id != id; }});
  spRenderGroupPanel();
  spRenderChart();
}}

function spToggleGroup(id) {{
  var g = spGroups.find(function(x) {{ return x.id == id; }});
  if (!g) return;
  g.visible = !g.visible;
  spRenderGroupPanel();
  spRenderChart();
}}

function spSetSort(mode, btn) {{
  spSortMode = mode;
  document.querySelectorAll('.sp-sort-btn').forEach(function(b) {{ b.classList.remove('active-sort'); }});
  btn.classList.add('active-sort');
  spRenderChart();
}}

function spSelectAllUnassigned(btn) {{
  var cbs = document.querySelectorAll('#sp-unassigned-pool input[type=checkbox]');
  var allChecked = Array.from(cbs).every(function(cb) {{ return cb.checked; }});
  cbs.forEach(function(cb) {{ cb.checked = !allChecked; }});
  btn.textContent = allChecked ? 'Select All' : 'Deselect All';
  spLastCheckedIdx = -1;  // reset range anchor after bulk action
}}
function spSetValueMode(mode, btn) {{
  spValueMode = mode;
  ['sp-val-corrected','sp-val-norm'].forEach(function(id) {{
    var el = document.getElementById(id);
    if (el) el.classList.remove('active-sort');
  }});
  if (btn) btn.classList.add('active-sort');
  spRenderChart();
}}

// ── Auto-grouping by Group Number / Tissue Type (shared across Per Group & Collated) ──
function spAutoKeyValue(d, mode) {{
  if (!d) return null;
  if (mode === 'group')  return (d.groupNum !== null && d.groupNum !== undefined && d.groupNum !== '') ? String(d.groupNum) : null;
  if (mode === 'tissue') return (d.tissue) ? String(d.tissue) : null;
  return null;
}}
function spAutoKeyLabel(mode, val) {{
  return mode === 'group' ? ('Group ' + val) : val;
}}
// Natural compare so 'Group 2' sorts before 'Group 10'
function spNaturalCmp(a, b) {{
  var ax = String(a).match(/(\\d+|\\D+)/g) || [];
  var bx = String(b).match(/(\\d+|\\D+)/g) || [];
  for (var i = 0; i < Math.min(ax.length, bx.length); i++) {{
    var an = parseInt(ax[i], 10), bn = parseInt(bx[i], 10);
    if (!isNaN(an) && !isNaN(bn)) {{ if (an !== bn) return an - bn; }}
    else if (ax[i] !== bx[i]) return ax[i] < bx[i] ? -1 : 1;
  }}
  return ax.length - bx.length;
}}
// Bucket `names` by the selected mode(s). A name must have ALL selected keys
// present, otherwise it falls to unassigned. getMeta(name) → datum with groupNum/tissue.
function spBuildAutoGroups(names, modes, getMeta) {{
  var buckets = {{}}, order = [], unassigned = [];
  names.forEach(function(sname) {{
    var d = getMeta(sname);
    var parts = [], ok = true;
    modes.forEach(function(m) {{
      var v = spAutoKeyValue(d, m);
      if (v === null) {{ ok = false; }} else parts.push(spAutoKeyLabel(m, v));
    }});
    if (!ok || !parts.length) {{ unassigned.push(sname); return; }}
    var label = parts.join(' \\u00B7 ');
    if (!buckets[label]) {{ buckets[label] = []; order.push(label); }}
    buckets[label].push(sname);
  }});
  order.sort(spNaturalCmp);
  return {{ groups: order.map(function(l) {{ return {{name: l, samples: buckets[l]}}; }}), unassigned: unassigned }};
}}
// Disable Group/Tissue options when that metadata isn't available, and reset to manual.
function spSetupAutogroupSelects(id1, id2) {{
  [id1, id2].forEach(function(id) {{
    var sel = document.getElementById(id);
    if (!sel) return;
    Array.from(sel.options).forEach(function(opt) {{
      if (opt.value === 'group'  && !SP_DATA.hasGroup)  opt.disabled = true;
      if (opt.value === 'tissue' && !SP_DATA.hasTissue) opt.disabled = true;
    }});
    sel.value = '';
  }});
}}
function spReadAutoModes(id1, id2) {{
  var m1 = document.getElementById(id1), m2 = document.getElementById(id2);
  var modes = [];
  if (m1 && m1.value) modes.push(m1.value);
  if (m2 && m2.value && (!m1 || m2.value !== m1.value)) modes.push(m2.value);
  return modes;
}}

function spAutoGroup() {{
  var modes = spReadAutoModes('sp-autogroup-1', 'sp-autogroup-2');
  // Universe = all unique sample names for the current analyte
  var names = [], seen = {{}};
  (SP_DATA.samples[spCurrentAnalyte] || []).forEach(function(s) {{
    if (!seen[s.name]) {{ seen[s.name] = true; names.push(s.name); }}
  }});
  if (!modes.length) {{
    spGroups = [];
    spUnassigned = names;
    spRenderGroupPanel();
    spRenderChart();
    return;
  }}
  var res = spBuildAutoGroups(names, modes, spGetSampleData);
  spGroups = res.groups.map(function(g, i) {{
    return {{ id: ++spGroupIdCounter, name: g.name, color: SP_PALETTE[i % SP_PALETTE.length], visible: true, samples: g.samples }};
  }});
  spUnassigned = res.unassigned;
  spRenderGroupPanel();
  spRenderChart();
}}

// Helper: pick mean/sd/values from a data entry based on current value mode
function spGetVals(d) {{
  if (spValueMode === 'normalized' && d.normMean !== null && d.normMean !== undefined) {{
    return {{ mean: d.normMean, sd: d.normSd || 0, values: d.normValues || [] }};
  }}
  return {{ mean: d.mean, sd: d.sd || 0, values: d.values || [] }};
}}

function spRenderChart() {{
  if (!spCurrentAnalyte || !SP_DATA.samples[spCurrentAnalyte]) {{
    Plotly.purge('sp-chart');
    return;
  }}
  var allData = SP_DATA.samples[spCurrentAnalyte];
  var showUnassigned = document.getElementById('sp-show-unassigned') ? document.getElementById('sp-show-unassigned').checked : true;
  var units = SP_DATA.units || '';
  var yTitle = spValueMode === 'normalized'
    ? spCurrentAnalyte + ' Normalized Concentration'
    : spCurrentAnalyte + ' Concentration' + (units ? ' (' + units + ')' : '');

  // ── Plate filtering ────────────────────────────────────────────────────────
  var filtered = (spActivePlates && spActivePlates.size > 0)
    ? allData.filter(function(d) {{ return spActivePlates.has(d.plate); }})
    : allData;

  // ── Excluded-sample filtering (user-removed via the ✕ chip button) ─────────
  filtered = filtered.filter(function(d) {{ return !spExcludedSamples.has(d.name); }});

  // Disambiguate display labels: add [Px] only when same sample runs on multiple
  // active plates so the x-axis clearly identifies each bar.
  var _nameCounts = {{}};
  filtered.forEach(function(d) {{ _nameCounts[d.name] = (_nameCounts[d.name] || 0) + 1; }});
  var _labelled = filtered.map(function(d) {{
    return Object.assign({{}}, d, {{
      displayLabel: _nameCounts[d.name] > 1 ? d.name + ' [P' + d.plate + ']' : d.name
    }});
  }});

  // raw name → array of displayLabels (for expanding group/unassigned assignments)
  var _nameToLabels = {{}};
  _labelled.forEach(function(d) {{
    if (!_nameToLabels[d.name]) _nameToLabels[d.name] = [];
    _nameToLabels[d.name].push(d.displayLabel);
  }});

  // Build ordered list of {{sname, color, groupName}} segments
  var segments = [];  // [{{groupName, color, items:[displayLabel]}}]

  // Pre-build a displayLabel→datum Map so sort comparators are O(1) not O(n)
  var spDataMap = new Map(_labelled.map(function(d) {{ return [d.displayLabel, d]; }}));

  spGroups.forEach(function(g) {{
    if (!g.visible) return;
    // Expand raw sample names → per-plate displayLabels that are currently visible
    var items = [];
    g.samples.forEach(function(rawName) {{
      var labels = _nameToLabels[rawName] || [];
      labels.forEach(function(lbl) {{ if (spDataMap.has(lbl)) items.push(lbl); }});
    }});
    if (spSortMode === 'asc') {{
      items.sort(function(a, b) {{
        var da = spDataMap.get(a), db = spDataMap.get(b);
        return (da ? spGetVals(da).mean : 0) - (db ? spGetVals(db).mean : 0);
      }});
    }} else if (spSortMode === 'desc') {{
      items.sort(function(a, b) {{
        var da = spDataMap.get(a), db = spDataMap.get(b);
        return (db ? spGetVals(db).mean : 0) - (da ? spGetVals(da).mean : 0);
      }});
    }}
    if (items.length) segments.push({{groupName: g.name, color: g.color, items: items, collapsed: true}});
  }});

  var unassignedItems = [];
  if (showUnassigned) {{
    spUnassigned.forEach(function(rawName) {{
      var labels = _nameToLabels[rawName] || [];
      labels.forEach(function(lbl) {{ if (spDataMap.has(lbl)) unassignedItems.push(lbl); }});
    }});
  }}
  if (spSortMode === 'asc') {{
    unassignedItems.sort(function(a, b) {{
      var da = spDataMap.get(a), db = spDataMap.get(b);
      return (da ? spGetVals(da).mean : 0) - (db ? spGetVals(db).mean : 0);
    }});
  }} else if (spSortMode === 'desc') {{
    unassignedItems.sort(function(a, b) {{
      var da = spDataMap.get(a), db = spDataMap.get(b);
      return (db ? spGetVals(db).mean : 0) - (da ? spGetVals(da).mean : 0);
    }});
  }}
  if (unassignedItems.length) segments.push({{groupName: 'Unassigned', color: 'rgba(150,150,150,0.7)', items: unassignedItems, collapsed: false}});

  // Flatten to ordered x-axis labels (collapsed groups → group name; unassigned → sample names)
  var orderedNames = [];
  segments.forEach(function(seg) {{
    if (seg.collapsed) {{
      orderedNames.push(seg.groupName);
    }} else {{
      seg.items.forEach(function(s) {{ orderedNames.push(s); }});
    }}
  }});

  if (!orderedNames.length) {{
    Plotly.purge('sp-chart');
    return;
  }}

  // Build traces per group-segment
  var traces = [];
  var shapes = [];
  var xCursor = 0;

  segments.forEach(function(seg, si) {{
    var xVals = [];
    var yMeans = [];
    var ySDs = [];
    var barColors = [];
    var scatterX = [];
    var scatterY = [];
    var scatterColors = [];
    var scatterText = [];

    if (seg.collapsed) {{
      // One bar for the entire group; individual points are all sample values
      var allVals = [], anyFlagged = false, sampleMeans = [];
      seg.items.forEach(function(sname) {{
        var d = spDataMap.get(sname);
        if (!d) return;
        if (d.anyFlagged) anyFlagged = true;
        var vals = spGetVals(d);
        sampleMeans.push(vals.mean);
        vals.values.forEach(function(v) {{
          allVals.push({{v: v, sname: sname, flagged: d.anyFlagged || false}});
        }});
      }});
      var grpMean = sampleMeans.length ? sampleMeans.reduce(function(a,b){{return a+b;}},0)/sampleMeans.length : 0;
      var grpSD = 0;
      if (allVals.length > 1) {{
        var vm = allVals.reduce(function(a,b){{return a+b.v;}},0)/allVals.length;
        grpSD = Math.sqrt(allVals.reduce(function(a,b){{return a+Math.pow(b.v-vm,2);}},0)/(allVals.length-1));
      }}
      xVals = [seg.groupName];
      yMeans = [grpMean];
      ySDs = [grpSD];
      barColors = [anyFlagged ? 'rgba(200,50,50,0.8)' : seg.color];
      allVals.forEach(function(pt) {{
        scatterX.push(seg.groupName);
        scatterY.push(pt.v);
        scatterColors.push(pt.flagged ? 'rgba(180,20,20,0.9)' : seg.color);
        scatterText.push(pt.sname);
      }});
    }} else {{
      // Individual bar per sample (unassigned pool)
      seg.items.forEach(function(sname) {{
        var d = spDataMap.get(sname);
        if (!d) return;
        var flagged = d.anyFlagged;
        var vals = spGetVals(d);
        xVals.push(sname);
        yMeans.push(vals.mean);
        ySDs.push(vals.sd);
        barColors.push(flagged ? 'rgba(200,50,50,0.8)' : seg.color);
        vals.values.forEach(function(v) {{
          scatterX.push(sname);
          scatterY.push(v);
          scatterColors.push(flagged ? 'rgba(180,20,20,0.9)' : seg.color);
          scatterText.push(sname);
        }});
      }});
    }}

    // Separator shape before this segment (except the first)
    if (si > 0 && xCursor > 0) {{
      shapes.push({{
        type: 'line',
        xref: 'x', yref: 'paper',
        x0: xCursor - 0.5, x1: xCursor - 0.5,
        y0: 0, y1: 1,
        line: {{ color: '#aaa', width: 1, dash: 'dot' }}
      }});
    }}

    // Bar trace
    traces.push({{
      type: 'bar',
      name: seg.groupName,
      x: xVals,
      y: yMeans,
      error_y: {{
        type: 'data',
        array: ySDs,
        visible: true,
        color: '#444',
        thickness: 1.5,
        width: 4
      }},
      marker: {{ color: barColors }},
      showlegend: true,
      legendgroup: seg.groupName,
      hovertemplate: '<b>%{{x}}</b><br>Mean: %{{y:.4g}}<extra>' + seg.groupName + '</extra>'
    }});

    // Scatter trace for individual points
    if (scatterX.length) {{
      traces.push({{
        type: 'scatter',
        mode: 'markers',
        name: seg.groupName + ' pts',
        x: scatterX,
        y: scatterY,
        marker: {{
          color: scatterColors,
          size: 6,
          symbol: 'circle',
          line: {{ color: 'rgba(0,0,0,0.4)', width: 1 }}
        }},
        text: scatterText,
        showlegend: false,
        legendgroup: seg.groupName,
        hovertemplate: '<b>%{{text}}</b><br>Value: %{{y:.4g}}<extra></extra>'
      }});
    }}

    xCursor += seg.collapsed ? 1 : seg.items.length;
  }});

  var layout = {{
    barmode: 'group',
    height: 460,
    margin: {{ l: 100, r: 40, t: 40, b: 160 }},
    xaxis: {{
      tickangle: -40,
      automargin: true,
      categoryorder: 'array',
      categoryarray: orderedNames
    }},
    yaxis: {{
      title: {{ text: yTitle, standoff: 12 }},
      automargin: false,
      rangemode: 'tozero'
    }},
    shapes: shapes,
    showlegend: false,  // x-axis category labels already identify each bar
    paper_bgcolor: msdThemeTokens().surface,
    plot_bgcolor: msdThemeTokens().surface
  }};

  Plotly.react('sp-chart', traces, layout, {{responsive: true}});
}}
// ── QC Plots Tab ─────────────────────────────────────────────────────────────
function qpNextColor() {{
  return QP_PALETTE[qpGroups.length % QP_PALETTE.length];
}}

function qpApplyDefaultGrouping() {{
  // Auto-group unassigned samples by QC level
  var levelSamples = {{}};
  (QP_DATA.levels || []).forEach(function(level) {{
    levelSamples[level] = [];
  }});
  var remaining = [];
  qpUnassigned.forEach(function(sname) {{
    var allSamples = QP_DATA.samples[qpCurrentAnalyte] || [];
    var entry = allSamples.find(function(s) {{ return s.name === sname; }});
    if (entry && entry.level && levelSamples.hasOwnProperty(entry.level)) {{
      levelSamples[entry.level].push(sname);
    }} else {{
      remaining.push(sname);
    }}
  }});
  qpUnassigned = remaining;
  (QP_DATA.levels || []).forEach(function(level) {{
    if (levelSamples[level].length > 0) {{
      qpGroups.push({{
        id: ++qpGroupIdCounter,
        name: level,
        color: QP_PALETTE[qpGroups.length % QP_PALETTE.length],
        visible: true,
        samples: levelSamples[level]
      }});
    }}
  }});
}}

function qpInit() {{
  if (!qpInitialized) {{
    qpInitialized = true;
    qpGroups = [];
    qpGroupIdCounter = 0;
    qpSortMode = 'group';
    var analytes = QP_DATA.analytes || [];
    qpCurrentAnalyte = analytes.length > 0 ? analytes[0] : null;
    qpUnassigned = qpCurrentAnalyte && QP_DATA.samples[qpCurrentAnalyte]
      ? QP_DATA.samples[qpCurrentAnalyte].map(function(s) {{ return s.name; }})
      : [];
    qpApplyDefaultGrouping();
    qpBuildAnalyteBar();
  }}
  qpRenderGroupPanel();
  qpRenderChart();
}}

function qpBuildAnalyteBar() {{
  var bar = document.getElementById('qp-analyte-bar');
  if (!bar) return;
  bar.innerHTML = '';
  (QP_DATA.analytes || []).forEach(function(a) {{
    var btn = document.createElement('button');
    btn.className = 'sp-analyte-btn' + (a === qpCurrentAnalyte ? ' active' : '');
    btn.textContent = a;
    btn.onclick = function() {{ qpSelectAnalyte(a); }};
    bar.appendChild(btn);
  }});
}}

function qpSelectAnalyte(name) {{
  qpCurrentAnalyte = name;
  qpGroups = [];
  qpGroupIdCounter = 0;
  var allSamples = QP_DATA.samples[name] ? QP_DATA.samples[name].map(function(s) {{ return s.name; }}) : [];
  qpUnassigned = allSamples.slice();
  qpApplyDefaultGrouping();
  document.querySelectorAll('#qp-analyte-bar .sp-analyte-btn').forEach(function(b) {{
    b.classList.toggle('active', b.textContent === name);
  }});
  qpRenderGroupPanel();
  qpRenderChart();
}}

function qpGetSampleData(sname) {{
  if (!qpCurrentAnalyte || !QP_DATA.samples[qpCurrentAnalyte]) return null;
  var arr = QP_DATA.samples[qpCurrentAnalyte];
  for (var i = 0; i < arr.length; i++) {{
    if (arr[i].name === sname) return arr[i];
  }}
  return null;
}}

function qpRenderGroupPanel() {{
  qpLastCheckedIdx = -1;
  var pool = document.getElementById('qp-unassigned-pool');
  if (pool) {{
    pool.innerHTML = '';
    qpUnassigned.forEach(function(sname) {{
      pool.appendChild(qpMakeChip(sname, '__unassigned__'));
    }});
  }}
  var gc = document.getElementById('qp-groups-container');
  if (!gc) return;
  gc.innerHTML = '';
  qpGroups.forEach(function(g, gi) {{
    var block = document.createElement('div');
    block.className = 'sp-group-block';
    var hdr = document.createElement('div');
    hdr.className = 'sp-group-header';
    hdr.style.background = g.color;
    var isFirst = (gi === 0);
    var isLast  = (gi === qpGroups.length - 1);
    var btnStyle = 'background:rgba(255,255,255,0.25);color:white;border-color:rgba(255,255,255,0.4);';
    var btnDisabled = 'background:rgba(255,255,255,0.08);color:rgba(255,255,255,0.3);border-color:rgba(255,255,255,0.15);cursor:default;';
    hdr.innerHTML =
      '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + g.name + '">' + g.name + '</span>' +
      '<button class="sp-btn sp-btn-icon" style="' + (isFirst ? btnDisabled : btnStyle) + '" ' +
        (isFirst ? 'disabled ' : 'onclick="qpMoveGroup(' + g.id + ',-1)" ') + 'title="Move up">&#x25B4;</button>' +
      '<button class="sp-btn sp-btn-icon" style="' + (isLast  ? btnDisabled : btnStyle) + '" ' +
        (isLast  ? 'disabled ' : 'onclick="qpMoveGroup(' + g.id + ',1)" ')  + 'title="Move down">&#x25BE;</button>' +
      '<button class="sp-btn sp-btn-icon" style="' + btnStyle + '" ' +
        'onclick="qpRenameGroup(' + g.id + ')" title="Rename group">&#x270F;</button>' +
      '<button class="sp-btn sp-btn-icon" style="' + btnStyle + '" ' +
        'onclick="qpToggleGroup(' + g.id + ')" title="Show/hide in chart">' + (g.visible ? '&#128065;' : '&#128564;') + '</button>' +
      '<button class="sp-btn sp-btn-icon" style="' + btnStyle + '" ' +
        'onclick="qpDeleteGroup(' + g.id + ')" title="Delete group">&#x2715;</button>';
    block.appendChild(hdr);
    var dz = document.createElement('div');
    dz.className = 'sp-group-drop sp-drop-zone';
    dz.setAttribute('ondragover', 'qpDragOver(event)');
    dz.setAttribute('ondrop', "qpDrop(event,'" + g.id + "')");
    g.samples.forEach(function(sname) {{
      dz.appendChild(qpMakeChip(sname, g.id));
    }});
    block.appendChild(dz);
    gc.appendChild(block);
  }});
}}

function qpMakeChip(sname, groupId) {{
  var span = document.createElement('span');
  span.className = 'sp-chip';
  span.draggable = true;
  span.title = sname;
  var label = sname;
  if (groupId === '__unassigned__') {{
    var cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.setAttribute('data-sample', sname);
    cb.onclick = function(e) {{
      e.stopPropagation();
      var idx = qpUnassigned.indexOf(sname);
      var allCbs = Array.from(document.querySelectorAll('#qp-unassigned-pool input[type=checkbox]'));
      if (e.shiftKey && qpLastCheckedIdx >= 0 && idx !== qpLastCheckedIdx) {{
        var lo = Math.min(qpLastCheckedIdx, idx);
        var hi = Math.max(qpLastCheckedIdx, idx);
        var newState = allCbs[idx].checked;
        for (var i = lo; i <= hi; i++) {{
          if (allCbs[i]) allCbs[i].checked = newState;
        }}
      }}
      qpLastCheckedIdx = idx;
    }};
    span.appendChild(cb);
  }}
  var txt = document.createElement('span');
  txt.textContent = label;
  txt.style.maxWidth = '120px';
  txt.style.overflow = 'hidden';
  txt.style.textOverflow = 'ellipsis';
  txt.style.whiteSpace = 'nowrap';
  span.appendChild(txt);
  span.addEventListener('dragstart', function(e) {{ qpDragStart(e, sname, groupId); }});
  span.addEventListener('dragend', function(e) {{ qpDragEnd(e); }});
  return span;
}}

function qpCreateGroup() {{
  var name = prompt('Group name:');
  if (!name || !name.trim()) return;
  name = name.trim();
  qpGroups.push({{
    id: ++qpGroupIdCounter,
    name: name,
    color: qpNextColor(),
    visible: true,
    samples: []
  }});
  qpRenderGroupPanel();
  qpRenderChart();
}}

function qpSelectAllUnassigned(btn) {{
  var cbs = document.querySelectorAll('#qp-unassigned-pool input[type=checkbox]');
  var allChecked = Array.from(cbs).every(function(cb) {{ return cb.checked; }});
  cbs.forEach(function(cb) {{ cb.checked = !allChecked; }});
  btn.textContent = allChecked ? 'Select All' : 'Deselect All';
  qpLastCheckedIdx = -1;
}}

function qpAssignChecked() {{
  var checked = [];
  document.querySelectorAll('#qp-unassigned-pool input[type=checkbox]:checked').forEach(function(cb) {{
    checked.push(cb.getAttribute('data-sample'));
  }});
  if (!checked.length) {{ alert('Check at least one sample first.'); return; }}
  var name = prompt('Assign to group name:');
  if (!name || !name.trim()) return;
  name = name.trim();
  var g = qpGroups.find(function(x) {{ return x.name === name; }});
  if (!g) {{
    g = {{ id: ++qpGroupIdCounter, name: name, color: qpNextColor(), visible: true, samples: [] }};
    qpGroups.push(g);
  }}
  checked.forEach(function(s) {{
    qpUnassigned = qpUnassigned.filter(function(u) {{ return u !== s; }});
    if (g.samples.indexOf(s) === -1) g.samples.push(s);
  }});
  qpRenderGroupPanel();
  qpRenderChart();
}}

function qpDeleteGroup(id) {{
  var g = qpGroups.find(function(x) {{ return x.id == id; }});
  if (!g) return;
  g.samples.forEach(function(s) {{
    if (qpUnassigned.indexOf(s) === -1) qpUnassigned.push(s);
  }});
  qpGroups = qpGroups.filter(function(x) {{ return x.id != id; }});
  qpRenderGroupPanel();
  qpRenderChart();
}}

function qpToggleGroup(id) {{
  var g = qpGroups.find(function(x) {{ return x.id == id; }});
  if (!g) return;
  g.visible = !g.visible;
  qpRenderGroupPanel();
  qpRenderChart();
}}

function qpRenameGroup(id) {{
  var g = qpGroups.find(function(x) {{ return x.id == id; }});
  if (!g) return;
  var newName = prompt('Rename group:', g.name);
  if (newName === null || newName.trim() === '') return;
  g.name = newName.trim();
  qpRenderGroupPanel();
  qpRenderChart();
}}

function qpMoveGroup(id, dir) {{
  var idx = qpGroups.findIndex(function(x) {{ return x.id == id; }});
  var newIdx = idx + dir;
  if (newIdx < 0 || newIdx >= qpGroups.length) return;
  var tmp = qpGroups[idx];
  qpGroups[idx] = qpGroups[newIdx];
  qpGroups[newIdx] = tmp;
  qpRenderGroupPanel();
  qpRenderChart();
}}

function qpSetSort(mode, btn) {{
  qpSortMode = mode;
  document.querySelectorAll('#tab-qcplots .sp-sort-btn').forEach(function(b) {{ b.classList.remove('active-sort'); }});
  btn.classList.add('active-sort');
  qpRenderChart();
}}

function qpDragStart(e, name, fromGroup) {{
  qpDragPayload = {{name: name, fromGroup: fromGroup}};
  e.dataTransfer.effectAllowed = 'move';
  e.currentTarget.style.opacity = '0.4';
}}

function qpDragEnd(e) {{
  e.currentTarget.style.opacity = '1';
  document.querySelectorAll('#tab-qcplots .sp-drop-zone').forEach(function(z) {{
    z.classList.remove('drag-over');
  }});
}}

function qpDragOver(e) {{
  e.preventDefault();
  e.dataTransfer.dropEffect = 'move';
  var zone = e.currentTarget.closest('.sp-drop-zone');
  if (zone) zone.classList.add('drag-over');
}}

function qpDrop(e, toGroupId) {{
  e.preventDefault();
  document.querySelectorAll('#tab-qcplots .sp-drop-zone').forEach(function(z) {{ z.classList.remove('drag-over'); }});
  if (!qpDragPayload) return;
  var name = qpDragPayload.name;
  var fromGroup = qpDragPayload.fromGroup;
  qpDragPayload = null;
  if (fromGroup === toGroupId) return;
  if (fromGroup === '__unassigned__') {{
    qpUnassigned = qpUnassigned.filter(function(s) {{ return s !== name; }});
  }} else {{
    var sg = qpGroups.find(function(g) {{ return g.id == fromGroup; }});
    if (sg) sg.samples = sg.samples.filter(function(s) {{ return s !== name; }});
  }}
  if (toGroupId === '__unassigned__') {{
    if (qpUnassigned.indexOf(name) === -1) qpUnassigned.push(name);
  }} else {{
    var tg = qpGroups.find(function(g) {{ return g.id == toGroupId; }});
    if (tg && tg.samples.indexOf(name) === -1) tg.samples.push(name);
  }}
  qpRenderGroupPanel();
  qpRenderChart();
}}

function qpRenderChart() {{
  if (!qpCurrentAnalyte || !QP_DATA.samples[qpCurrentAnalyte]) {{
    Plotly.purge('qp-chart');
    return;
  }}
  var allData = QP_DATA.samples[qpCurrentAnalyte];
  var showUnassigned = document.getElementById('qp-show-unassigned') ? document.getElementById('qp-show-unassigned').checked : true;
  var units = QP_DATA.units || '';
  var yTitle = qpCurrentAnalyte + ' Concentration' + (units ? ' (' + units + ')' : '');

  var segments = [];

  // P2-3: pre-build a name→datum Map so sort comparators are O(1) not O(n)
  var qpDataMap = new Map(allData.map(function(d) {{ return [d.name, d]; }}));

  qpGroups.forEach(function(g) {{
    if (!g.visible) return;
    var items = g.samples.filter(function(s) {{ return qpDataMap.has(s); }});
    if (qpSortMode === 'asc') {{
      items.sort(function(a, b) {{
        var da = qpDataMap.get(a), db = qpDataMap.get(b);
        return (da ? da.mean : 0) - (db ? db.mean : 0);
      }});
    }} else if (qpSortMode === 'desc') {{
      items.sort(function(a, b) {{
        var da = qpDataMap.get(a), db = qpDataMap.get(b);
        return (db ? db.mean : 0) - (da ? da.mean : 0);
      }});
    }}
    if (items.length) segments.push({{groupName: g.name, color: g.color, items: items, collapsed: true}});
  }});

  var unassignedItems = showUnassigned
    ? qpUnassigned.filter(function(s) {{ return qpDataMap.has(s); }})
    : [];
  if (qpSortMode === 'asc') {{
    unassignedItems.sort(function(a, b) {{
      var da = qpDataMap.get(a), db = qpDataMap.get(b);
      return (da ? da.mean : 0) - (db ? db.mean : 0);
    }});
  }} else if (qpSortMode === 'desc') {{
    unassignedItems.sort(function(a, b) {{
      var da = qpDataMap.get(a), db = qpDataMap.get(b);
      return (db ? db.mean : 0) - (da ? da.mean : 0);
    }});
  }}
  if (unassignedItems.length) segments.push({{groupName: 'Unassigned', color: 'rgba(150,150,150,0.7)', items: unassignedItems, collapsed: false}});

  var orderedNames = [];
  segments.forEach(function(seg) {{
    if (seg.collapsed) {{
      orderedNames.push(seg.groupName);
    }} else {{
      seg.items.forEach(function(s) {{ orderedNames.push(s); }});
    }}
  }});

  if (!orderedNames.length) {{
    Plotly.purge('qp-chart');
    return;
  }}

  var traces = [];
  var shapes = [];
  var xCursor = 0;

  segments.forEach(function(seg, si) {{
    var xVals = [];
    var yMeans = [];
    var ySDs = [];
    var barColors = [];
    var scatterX = [];
    var scatterY = [];
    var scatterColors = [];
    var scatterText = [];

    if (seg.collapsed) {{
      var allVals = [], sampleMeans = [];
      seg.items.forEach(function(sname) {{
        var d = allData.find(function(x) {{ return x.name === sname; }});
        if (!d) return;
        sampleMeans.push(d.mean);
        (d.values || []).forEach(function(v) {{
          allVals.push({{v: v, sname: sname}});
        }});
      }});
      var grpMean = sampleMeans.length ? sampleMeans.reduce(function(a,b){{return a+b;}},0)/sampleMeans.length : 0;
      var grpSD = 0;
      if (allVals.length > 1) {{
        var vm = allVals.reduce(function(a,b){{return a+b.v;}},0)/allVals.length;
        grpSD = Math.sqrt(allVals.reduce(function(a,b){{return a+Math.pow(b.v-vm,2);}},0)/(allVals.length-1));
      }}
      xVals = [seg.groupName];
      yMeans = [grpMean];
      ySDs = [grpSD];
      barColors = [seg.color];
      allVals.forEach(function(pt) {{
        scatterX.push(seg.groupName);
        scatterY.push(pt.v);
        scatterColors.push(seg.color);
        scatterText.push(pt.sname);
      }});
    }} else {{
      seg.items.forEach(function(sname) {{
        var d = allData.find(function(x) {{ return x.name === sname; }});
        if (!d) return;
        xVals.push(sname);
        yMeans.push(d.mean);
        ySDs.push(d.sd);
        barColors.push(seg.color);
        (d.values || []).forEach(function(v) {{
          scatterX.push(sname);
          scatterY.push(v);
          scatterColors.push(seg.color);
          scatterText.push(sname);
        }});
      }});
    }}

    if (si > 0 && xCursor > 0) {{
      shapes.push({{
        type: 'line',
        xref: 'x', yref: 'paper',
        x0: xCursor - 0.5, x1: xCursor - 0.5,
        y0: 0, y1: 1,
        line: {{ color: '#aaa', width: 1, dash: 'dot' }}
      }});
    }}

    traces.push({{
      type: 'bar',
      name: seg.groupName,
      x: xVals,
      y: yMeans,
      error_y: {{
        type: 'data',
        array: ySDs,
        visible: true,
        color: '#444',
        thickness: 1.5,
        width: 4
      }},
      marker: {{ color: barColors }},
      showlegend: true,
      legendgroup: seg.groupName,
      hovertemplate: '<b>%{{x}}</b><br>Mean: %{{y:.4g}}<extra>' + seg.groupName + '</extra>'
    }});

    if (scatterX.length) {{
      traces.push({{
        type: 'scatter',
        mode: 'markers',
        name: seg.groupName + ' pts',
        x: scatterX,
        y: scatterY,
        marker: {{
          color: scatterColors,
          size: 6,
          symbol: 'circle',
          line: {{ color: 'rgba(0,0,0,0.4)', width: 1 }}
        }},
        text: scatterText,
        showlegend: false,
        legendgroup: seg.groupName,
        hovertemplate: '<b>%{{text}}</b><br>Value: %{{y:.4g}}<extra></extra>'
      }});
    }}

    xCursor += seg.collapsed ? 1 : seg.items.length;
  }});

  // Expected concentration reference lines (±30% band)
  var expConc = QP_DATA.expected && QP_DATA.expected[qpCurrentAnalyte];
  if (expConc) {{
    shapes.push({{type:'rect', xref:'paper', yref:'y', x0:0, x1:1,
                 y0: expConc*0.7, y1: expConc*1.3,
                 fillcolor:'rgba(255,165,0,0.12)', line:{{width:0}}}});
    shapes.push({{type:'line', xref:'paper', yref:'y', x0:0, x1:1,
                 y0: expConc, y1: expConc,
                 line:{{color:'orange', width:1.5, dash:'dash'}}}});
  }}

  var layout = {{
    barmode: 'group',
    height: 460,
    margin: {{ l: 100, r: 40, t: 40, b: 160 }},
    xaxis: {{
      tickangle: -40,
      automargin: true,
      categoryorder: 'array',
      categoryarray: orderedNames
    }},
    yaxis: {{
      title: {{ text: yTitle, standoff: 12 }},
      automargin: false,
      rangemode: 'tozero'
    }},
    shapes: shapes,
    legend: {{ orientation: 'h', x: 0, y: 1.08 }},
    paper_bgcolor: msdThemeTokens().surface,
    plot_bgcolor: msdThemeTokens().surface
  }};

  Plotly.react('qp-chart', traces, layout, {{responsive: true}});
}}
// ── End QC Plots Tab ──────────────────────────────────────────────────────────

// ── Collated Sub-tab ─────────────────────────────────────────────────────────
var spSubtab = 'single';
var spCollatedActive  = new Set();
var spCollValueMode   = 'corrected';
var spCollSortMode    = 'group';
var spCollGroups      = [];
var spCollUnassigned  = [];
var spCollGroupIdCounter = 0;
var spCollDragPayload    = null;
var spCollLastCheckedIdx = -1;
var SP_COLL_PALETTE = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd',
                        '#8c564b','#e377c2','#17becf','#bcbd22','#7f7f7f'];

function spCollNextColor() {{
  return SP_COLL_PALETTE[spCollGroups.length % SP_COLL_PALETTE.length];
}}

function spSetSubtab(tab) {{
  spSubtab = tab;
  document.getElementById('sp-single-panel').style.display   = tab === 'single'   ? '' : 'none';
  document.getElementById('sp-collated-panel').style.display = tab === 'collated' ? '' : 'none';
  document.getElementById('sp-subtab-single').className   = 'sp-subtab-btn' + (tab === 'single'   ? ' sp-subtab-active' : '');
  document.getElementById('sp-subtab-collated').className = 'sp-subtab-btn' + (tab === 'collated' ? ' sp-subtab-active' : '');
  if (tab === 'collated') {{
    spInitCollated();
    spRenderCollatedChart();
  }}
}}

function spInitCollated() {{
  var toggles = document.getElementById('sp-collated-group-toggles');
  if (!toggles || toggles.dataset.built) return;
  toggles.dataset.built = '1';
  var normBtn = document.getElementById('sp-coll-val-norm');
  if (normBtn && !SP_DATA.hasNorm) {{
    normBtn.disabled = true; normBtn.style.opacity = '0.4';
    normBtn.style.cursor = 'not-allowed'; normBtn.title = 'No total protein data loaded';
  }}
  var analytes = SP_DATA.analytes || [];
  spCollatedActive = new Set(analytes);
  // Populate unassigned with all unique sample names across all analytes
  var seen = {{}};
  analytes.forEach(function(a) {{
    (SP_DATA.samples[a] || []).forEach(function(d) {{
      if (!seen[d.name]) {{ seen[d.name] = true; spCollUnassigned.push(d.name); }}
    }});
  }});
  // Build analyte toggle checkboxes
  toggles.innerHTML = '<span style="font-size:12px;font-weight:600;color:#555;margin-right:4px;">Plates/Groups:</span>';
  analytes.forEach(function(a, i) {{
    var color = SP_COLL_PALETTE[i % SP_COLL_PALETTE.length];
    var lbl = document.createElement('label');
    lbl.style.cssText = 'display:flex;align-items:center;gap:4px;font-size:12px;cursor:pointer;' +
                        'padding:3px 8px;border-radius:4px;border:1px solid ' + color + ';';
    var cb = document.createElement('input');
    cb.type = 'checkbox'; cb.checked = true; cb.value = a;
    cb.onchange = function() {{
      if (cb.checked) {{ spCollatedActive.add(a); }} else {{ spCollatedActive.delete(a); }}
      // If auto-grouping is active, recompute over the active analyte set
      var modes = spReadAutoModes('sp-coll-autogroup-1', 'sp-coll-autogroup-2');
      if (modes.length) {{ spCollAutoGroup(); }} else {{ spRenderCollatedChart(); }}
    }};
    var dot = document.createElement('span');
    dot.style.cssText = 'display:inline-block;width:10px;height:10px;border-radius:50%;background:' + color + ';flex-shrink:0;';
    lbl.appendChild(cb); lbl.appendChild(dot);
    lbl.appendChild(document.createTextNode(a));
    toggles.appendChild(lbl);
  }});
  spSetupAutogroupSelects('sp-coll-autogroup-1', 'sp-coll-autogroup-2');
  spCollRenderGroupPanel();
}}

// Metadata lookup for a sample name across active analytes (groupNum/tissue are
// constant per name, so the first occurrence wins).
function spCollGetMeta(sname) {{
  var analytes = SP_DATA.analytes || [];
  for (var i = 0; i < analytes.length; i++) {{
    var arr = SP_DATA.samples[analytes[i]] || [];
    for (var j = 0; j < arr.length; j++) {{
      if (arr[j].name === sname) return arr[j];
    }}
  }}
  return null;
}}

function spCollAutoGroup() {{
  var modes = spReadAutoModes('sp-coll-autogroup-1', 'sp-coll-autogroup-2');
  // Universe = all unique sample names across currently-active analytes
  var names = [], seen = {{}};
  (SP_DATA.analytes || []).forEach(function(a) {{
    if (!spCollatedActive.has(a)) return;
    (SP_DATA.samples[a] || []).forEach(function(s) {{
      if (!seen[s.name]) {{ seen[s.name] = true; names.push(s.name); }}
    }});
  }});
  if (!modes.length) {{
    spCollGroups = [];
    spCollUnassigned = names;
    spCollRenderGroupPanel();
    spRenderCollatedChart();
    return;
  }}
  var res = spBuildAutoGroups(names, modes, spCollGetMeta);
  spCollGroups = res.groups.map(function(g, i) {{
    return {{ id: ++spCollGroupIdCounter, name: g.name, color: SP_COLL_PALETTE[i % SP_COLL_PALETTE.length], visible: true, samples: g.samples }};
  }});
  spCollUnassigned = res.unassigned;
  spCollRenderGroupPanel();
  spRenderCollatedChart();
}}

// ── Group panel (mirrors spRenderGroupPanel) ──────────────────────────────────
function spCollRenderGroupPanel() {{
  spCollLastCheckedIdx = -1;
  var pool = document.getElementById('sp-coll-unassigned-pool');
  if (pool) {{
    pool.innerHTML = '';
    spCollUnassigned.forEach(function(sname) {{ pool.appendChild(spCollMakeChip(sname, '__unassigned__')); }});
  }}
  var gc = document.getElementById('sp-coll-groups-container');
  if (!gc) return;
  gc.innerHTML = '';
  spCollGroups.forEach(function(g, gi) {{
    var block = document.createElement('div');
    block.className = 'sp-group-block';
    var hdr = document.createElement('div');
    hdr.className = 'sp-group-header';
    hdr.style.background = g.color;
    var isFirst = (gi === 0), isLast = (gi === spCollGroups.length - 1);
    var bs = 'background:rgba(255,255,255,0.25);color:white;border-color:rgba(255,255,255,0.4);';
    var bd = 'background:rgba(255,255,255,0.08);color:rgba(255,255,255,0.3);border-color:rgba(255,255,255,0.15);cursor:default;';
    hdr.innerHTML =
      '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + g.name + '">' + g.name + '</span>' +
      '<button class="sp-btn sp-btn-icon" style="' + (isFirst ? bd : bs) + '" ' + (isFirst ? 'disabled ' : 'onclick="spCollMoveGroup(' + g.id + ',-1)" ') + 'title="Move up">&#x25B4;</button>' +
      '<button class="sp-btn sp-btn-icon" style="' + (isLast  ? bd : bs) + '" ' + (isLast  ? 'disabled ' : 'onclick="spCollMoveGroup(' + g.id + ',1)" ')  + 'title="Move down">&#x25BE;</button>' +
      '<button class="sp-btn sp-btn-icon" style="' + bs + '" onclick="spCollRenameGroup(' + g.id + ')" title="Rename">&#x270F;</button>' +
      '<button class="sp-btn sp-btn-icon" style="' + bs + '" onclick="spCollToggleGroup(' + g.id + ')" title="Show/hide">' + (g.visible ? '&#128065;' : '&#128564;') + '</button>' +
      '<button class="sp-btn sp-btn-icon" style="' + bs + '" onclick="spCollDeleteGroup(' + g.id + ')" title="Delete">&#x2715;</button>';
    block.appendChild(hdr);
    var dz = document.createElement('div');
    dz.className = 'sp-group-drop sp-drop-zone';
    dz.setAttribute('ondragover', 'spCollDragOver(event)');
    dz.setAttribute('ondrop', "spCollDrop(event,'" + g.id + "')");
    g.samples.forEach(function(sname) {{ dz.appendChild(spCollMakeChip(sname, g.id)); }});
    block.appendChild(dz);
    gc.appendChild(block);
  }});
}}

function spCollMakeChip(sname, groupId) {{
  var excluded = spExcludedSamples.has(sname);
  var span = document.createElement('span');
  span.className = 'sp-chip' + (excluded ? ' sp-chip-excluded' : '');
  span.draggable = true; span.title = excluded ? sname + ' (excluded from chart)' : sname;
  if (groupId === '__unassigned__') {{
    var cb = document.createElement('input');
    cb.type = 'checkbox'; cb.setAttribute('data-sample', sname);
    cb.onclick = function(e) {{
      e.stopPropagation();
      var idx = spCollUnassigned.indexOf(sname);
      var allCbs = Array.from(document.querySelectorAll('#sp-coll-unassigned-pool input[type=checkbox]'));
      if (e.shiftKey && spCollLastCheckedIdx >= 0 && idx !== spCollLastCheckedIdx) {{
        var lo = Math.min(spCollLastCheckedIdx, idx), hi = Math.max(spCollLastCheckedIdx, idx);
        var st = allCbs[idx].checked;
        for (var i = lo; i <= hi; i++) {{ if (allCbs[i]) allCbs[i].checked = st; }}
      }}
      spCollLastCheckedIdx = idx;
    }};
    span.appendChild(cb);
  }}
  var excludeBtn = document.createElement('button');
  excludeBtn.type = 'button';
  excludeBtn.className = 'sp-chip-exclude-btn';
  excludeBtn.title = excluded ? 'Restore to chart' : 'Remove from chart';
  excludeBtn.textContent = excluded ? '↺' : '✕';
  excludeBtn.onclick = function(e) {{ e.stopPropagation(); spToggleExclude(sname); }};
  span.appendChild(excludeBtn);
  var txt = document.createElement('span');
  txt.textContent = sname; txt.style.cssText = 'max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;';
  span.appendChild(txt);
  span.addEventListener('dragstart', function(e) {{ spCollDragStart(e, sname, groupId); }});
  span.addEventListener('dragend',   function(e) {{ spCollDragEnd(e); }});
  return span;
}}

function spCollDragStart(e, name, fromGroup) {{
  spCollDragPayload = {{name: name, fromGroup: fromGroup}};
  e.dataTransfer.effectAllowed = 'move'; e.currentTarget.style.opacity = '0.4';
}}
function spCollDragEnd(e) {{
  e.currentTarget.style.opacity = '1';
  document.querySelectorAll('.sp-drop-zone').forEach(function(z) {{ z.classList.remove('drag-over'); }});
}}
function spCollDragOver(e) {{
  e.preventDefault(); e.dataTransfer.dropEffect = 'move';
  var zone = e.currentTarget.closest('.sp-drop-zone');
  if (zone) zone.classList.add('drag-over');
}}
function spCollDrop(e, toGroupId) {{
  e.preventDefault();
  document.querySelectorAll('.sp-drop-zone').forEach(function(z) {{ z.classList.remove('drag-over'); }});
  if (!spCollDragPayload) return;
  var name = spCollDragPayload.name, from = spCollDragPayload.fromGroup;
  spCollDragPayload = null;
  if (from === toGroupId) return;
  if (from === '__unassigned__') {{ spCollUnassigned = spCollUnassigned.filter(function(s) {{ return s !== name; }}); }}
  else {{ var sg = spCollGroups.find(function(g) {{ return g.id == from; }}); if (sg) sg.samples = sg.samples.filter(function(s) {{ return s !== name; }}); }}
  if (toGroupId === '__unassigned__') {{ if (spCollUnassigned.indexOf(name) === -1) spCollUnassigned.push(name); }}
  else {{ var tg = spCollGroups.find(function(g) {{ return g.id == toGroupId; }}); if (tg && tg.samples.indexOf(name) === -1) tg.samples.push(name); }}
  spCollRenderGroupPanel(); spRenderCollatedChart();
}}

function spCollCreateGroup() {{
  var name = prompt('Group name:'); if (!name || !name.trim()) return;
  spCollGroups.push({{id: ++spCollGroupIdCounter, name: name.trim(), color: spCollNextColor(), visible: true, samples: []}});
  spCollRenderGroupPanel(); spRenderCollatedChart();
}}
function spCollAssignChecked() {{
  var checked = [];
  document.querySelectorAll('#sp-coll-unassigned-pool input[type=checkbox]:checked').forEach(function(cb) {{ checked.push(cb.getAttribute('data-sample')); }});
  if (!checked.length) {{ alert('Check at least one sample first.'); return; }}
  var name = prompt('Assign to group name:'); if (!name || !name.trim()) return;
  name = name.trim();
  var g = spCollGroups.find(function(x) {{ return x.name === name; }});
  if (!g) {{ g = {{id: ++spCollGroupIdCounter, name: name, color: spCollNextColor(), visible: true, samples: []}}; spCollGroups.push(g); }}
  checked.forEach(function(s) {{
    spCollUnassigned = spCollUnassigned.filter(function(u) {{ return u !== s; }});
    if (g.samples.indexOf(s) === -1) g.samples.push(s);
  }});
  spCollRenderGroupPanel(); spRenderCollatedChart();
}}
function spCollSelectAllUnassigned(btn) {{
  var cbs = document.querySelectorAll('#sp-coll-unassigned-pool input[type=checkbox]');
  var allChecked = Array.from(cbs).every(function(cb) {{ return cb.checked; }});
  cbs.forEach(function(cb) {{ cb.checked = !allChecked; }});
  btn.textContent = allChecked ? 'Select All' : 'Deselect All';
  spCollLastCheckedIdx = -1;
}}
function spCollMoveGroup(id, dir) {{
  var idx = spCollGroups.findIndex(function(x) {{ return x.id == id; }}); var ni = idx + dir;
  if (ni < 0 || ni >= spCollGroups.length) return;
  var tmp = spCollGroups[idx]; spCollGroups[idx] = spCollGroups[ni]; spCollGroups[ni] = tmp;
  spCollRenderGroupPanel(); spRenderCollatedChart();
}}
function spCollRenameGroup(id) {{
  var g = spCollGroups.find(function(x) {{ return x.id == id; }}); if (!g) return;
  var n = prompt('Rename group:', g.name); if (!n || !n.trim()) return;
  g.name = n.trim(); spCollRenderGroupPanel(); spRenderCollatedChart();
}}
function spCollDeleteGroup(id) {{
  var g = spCollGroups.find(function(x) {{ return x.id == id; }}); if (!g) return;
  g.samples.forEach(function(s) {{ if (spCollUnassigned.indexOf(s) === -1) spCollUnassigned.push(s); }});
  spCollGroups = spCollGroups.filter(function(x) {{ return x.id != id; }});
  spCollRenderGroupPanel(); spRenderCollatedChart();
}}
function spCollToggleGroup(id) {{
  var g = spCollGroups.find(function(x) {{ return x.id == id; }}); if (!g) return;
  g.visible = !g.visible; spCollRenderGroupPanel(); spRenderCollatedChart();
}}

// ── Value / sort controls ─────────────────────────────────────────────────────
function spCollSetValueMode(mode, btn) {{
  spCollValueMode = mode;
  document.querySelectorAll('.sp-collated-val-btn').forEach(function(b) {{ b.classList.toggle('active-sort', b === btn); }});
  spRenderCollatedChart();
}}
function spCollSetSort(mode, btn) {{
  spCollSortMode = mode;
  document.querySelectorAll('.sp-collated-sort-btn').forEach(function(b) {{ b.classList.toggle('active-sort', b === btn); }});
  spRenderCollatedChart();
}}
function spCollGetVals(d) {{
  if (spCollValueMode === 'normalized' && d.normMean !== null && d.normMean !== undefined)
    return {{ mean: d.normMean, sd: d.normSd || 0, values: d.normValues || [] }};
  return {{ mean: d.mean, sd: d.sd || 0, values: d.values || [] }};
}}

// ── Chart rendering ───────────────────────────────────────────────────────────
function spRenderCollatedChart() {{
  if (!document.getElementById('sp-collated-chart')) return;
  var analytes = (SP_DATA.analytes || []).filter(function(a) {{ return spCollatedActive.has(a); }});
  if (!analytes.length) {{ Plotly.purge('sp-collated-chart'); return; }}
  var showUnassigned = document.getElementById('sp-coll-show-unassigned') ? document.getElementById('sp-coll-show-unassigned').checked : true;

  // --- Build segments (assigned groups + unassigned), mirroring spRenderChart ---
  // For groups: one collapsed bar = mean of all samples in the group across active analytes
  // For unassigned: individual bar per (sample, analyte) colored by analyte

  // Map: name → [{{entry, analyte, analyteIdx}}] for active analytes
  // (excluded samples are filtered out here so they never enter any group's
  // averaged bar, nor appear as an unassigned bar)
  var nameEntries = {{}};
  analytes.forEach(function(a, ai) {{
    (SP_DATA.samples[a] || []).forEach(function(d) {{
      if (spExcludedSamples.has(d.name)) return;
      if (!nameEntries[d.name]) nameEntries[d.name] = [];
      nameEntries[d.name].push({{d: d, analyte: a, analyteIdx: ai}});
    }});
  }});

  // Analyte color map
  var analyteColor = {{}};
  analytes.forEach(function(a, ai) {{ analyteColor[a] = SP_COLL_PALETTE[ai % SP_COLL_PALETTE.length]; }});

  var units = SP_DATA.units || '';
  var yTitle = spCollValueMode === 'normalized'
    ? 'Normalized Concentration'
    : 'Concentration' + (units ? ' (' + units + ')' : '');

  var traces = []; var shapes = []; var xCursor = 0; var orderedNames = [];

  // Helper: add collapsed group trace
  function addGroupTrace(seg) {{
    var xVals = [seg.groupName], anyFlagged = false;
    var allVals = [], sampleMeans = [];
    seg.items.forEach(function(sname) {{
      var elist = nameEntries[sname] || [];
      elist.forEach(function(ei) {{
        if (ei.d.anyFlagged) anyFlagged = true;
        var vals = spCollGetVals(ei.d);
        sampleMeans.push(vals.mean);
        vals.values.forEach(function(v) {{ allVals.push({{v: v, sname: sname, flagged: ei.d.anyFlagged}}); }});
      }});
    }});
    var grpMean = sampleMeans.length ? sampleMeans.reduce(function(a,b){{return a+b;}},0)/sampleMeans.length : 0;
    var grpSD = 0;
    if (allVals.length > 1) {{
      var vm = allVals.reduce(function(a,b){{return a+b.v;}},0)/allVals.length;
      grpSD = Math.sqrt(allVals.reduce(function(a,b){{return a+Math.pow(b.v-vm,2);}},0)/(allVals.length-1));
    }}
    traces.push({{ type:'bar', name:seg.groupName, x:xVals, y:[grpMean],
      error_y:{{type:'data',array:[grpSD],visible:true,color:'#444',thickness:1.5,width:4}},
      marker:{{color:[anyFlagged?'rgba(200,50,50,0.8)':seg.color]}},
      showlegend:true, legendgroup:seg.groupName,
      hovertemplate:'<b>%{{x}}</b><br>Mean: %{{y:.4g}}<extra>'+seg.groupName+'</extra>'
    }});
    var scX=[],scY=[],scC=[],scT=[];
    allVals.forEach(function(pt) {{ scX.push(seg.groupName);scY.push(pt.v);scC.push(pt.flagged?'rgba(180,20,20,0.9)':seg.color);scT.push(pt.sname); }});
    if (scX.length) traces.push({{type:'scatter',mode:'markers',name:seg.groupName+' pts',x:scX,y:scY,text:scT,
      marker:{{color:scC,size:6,symbol:'circle',line:{{color:'rgba(0,0,0,0.4)',width:1}}}},
      showlegend:false,legendgroup:seg.groupName,hovertemplate:'<b>%{{text}}</b><br>Value: %{{y:.4g}}<extra></extra>'}});
    orderedNames.push(seg.groupName); xCursor += 1;
  }}

  // Helper: add unassigned individual bars (one per sample per active analyte)
  function addUnassignedTrace(items) {{
    // Expand items → (displayLabel, entry, analyte, color)
    var _nc = {{}};
    items.forEach(function(sname) {{
      (nameEntries[sname] || []).forEach(function(ei) {{
        if (!_nc[sname]) _nc[sname] = 0; _nc[sname]++;
      }});
    }});
    // Group by analyte for separate coloring
    var byAnalyte = {{}};
    analytes.forEach(function(a) {{ byAnalyte[a] = []; }});
    items.forEach(function(sname) {{
      (nameEntries[sname] || []).forEach(function(ei) {{
        var lbl = _nc[sname] > 1 ? sname+' ['+ei.analyte+']' : sname;
        byAnalyte[ei.analyte].push({{lbl:lbl, d:ei.d, sname:sname}});
        orderedNames.push(lbl);
      }});
    }});
    analytes.forEach(function(a) {{
      var aItems = byAnalyte[a]; if (!aItems.length) return;
      var color = analyteColor[a];
      var xVals=[],yMeans=[],ySDs=[],barColors=[],scX=[],scY=[],scC=[],scT=[];
      aItems.forEach(function(it) {{
        var vals = spCollGetVals(it.d);
        xVals.push(it.lbl); yMeans.push(vals.mean); ySDs.push(vals.sd);
        barColors.push(it.d.anyFlagged?'rgba(200,50,50,0.8)':color);
        vals.values.forEach(function(v) {{ scX.push(it.lbl);scY.push(v);scC.push(it.d.anyFlagged?'rgba(180,20,20,0.9)':color);scT.push(it.sname); }});
      }});
      traces.push({{type:'bar',name:a,x:xVals,y:yMeans,
        error_y:{{type:'data',array:ySDs,visible:true,color:'#444',thickness:1.5,width:4}},
        marker:{{color:barColors}},showlegend:true,legendgroup:a,
        hovertemplate:'<b>%{{x}}</b><br>Mean: %{{y:.4g}}<extra>'+a+'</extra>'}});
      if (scX.length) traces.push({{type:'scatter',mode:'markers',name:a+' pts',x:scX,y:scY,text:scT,
        marker:{{color:scC,size:6,symbol:'circle',line:{{color:'rgba(0,0,0,0.4)',width:1}}}},
        showlegend:false,legendgroup:a,hovertemplate:'<b>%{{text}}</b><br>Value: %{{y:.4g}}<extra></extra>'}});
      xCursor += aItems.length;
    }});
  }}

  // Build segments from assigned groups
  spCollGroups.forEach(function(g, si) {{
    if (!g.visible) return;
    var items = g.samples.filter(function(s) {{ return nameEntries[s] && nameEntries[s].length; }});
    if (spCollSortMode === 'asc') items.sort(function(a,b) {{
      var ma = (nameEntries[a]||[]).reduce(function(s,e){{return s+spCollGetVals(e.d).mean;}},0)/(nameEntries[a]||[{{d:{{mean:0}}}}]).length;
      var mb = (nameEntries[b]||[]).reduce(function(s,e){{return s+spCollGetVals(e.d).mean;}},0)/(nameEntries[b]||[{{d:{{mean:0}}}}]).length;
      return ma - mb;
    }});
    else if (spCollSortMode === 'desc') items.sort(function(a,b) {{
      var ma = (nameEntries[a]||[]).reduce(function(s,e){{return s+spCollGetVals(e.d).mean;}},0)/(nameEntries[a]||[{{d:{{mean:0}}}}]).length;
      var mb = (nameEntries[b]||[]).reduce(function(s,e){{return s+spCollGetVals(e.d).mean;}},0)/(nameEntries[b]||[{{d:{{mean:0}}}}]).length;
      return mb - ma;
    }});
    if (!items.length) return;
    if (si > 0 && xCursor > 0) shapes.push({{type:'line',xref:'x',yref:'paper',x0:xCursor-0.5,x1:xCursor-0.5,y0:0,y1:1,line:{{color:'#aaa',width:1,dash:'dot'}}}});
    addGroupTrace({{groupName:g.name,color:g.color,items:items}});
  }});

  // Unassigned
  if (showUnassigned) {{
    var uItems = spCollUnassigned.filter(function(s) {{ return nameEntries[s] && nameEntries[s].length; }});
    if (spCollSortMode === 'asc') uItems.sort(function(a,b) {{
      var ma=(nameEntries[a]||[]).reduce(function(s,e){{return s+spCollGetVals(e.d).mean;}},0)/Math.max(1,(nameEntries[a]||[]).length);
      var mb=(nameEntries[b]||[]).reduce(function(s,e){{return s+spCollGetVals(e.d).mean;}},0)/Math.max(1,(nameEntries[b]||[]).length);
      return ma-mb;
    }});
    else if (spCollSortMode === 'desc') uItems.sort(function(a,b) {{
      var ma=(nameEntries[a]||[]).reduce(function(s,e){{return s+spCollGetVals(e.d).mean;}},0)/Math.max(1,(nameEntries[a]||[]).length);
      var mb=(nameEntries[b]||[]).reduce(function(s,e){{return s+spCollGetVals(e.d).mean;}},0)/Math.max(1,(nameEntries[b]||[]).length);
      return mb-ma;
    }});
    if (uItems.length) {{
      if (spCollGroups.some(function(g){{return g.visible && g.samples.some(function(s){{return nameEntries[s]&&nameEntries[s].length;}});}}) && xCursor > 0)
        shapes.push({{type:'line',xref:'x',yref:'paper',x0:xCursor-0.5,x1:xCursor-0.5,y0:0,y1:1,line:{{color:'#aaa',width:1,dash:'dot'}}}});
      addUnassignedTrace(uItems);
    }}
  }}

  if (!orderedNames.length) {{ Plotly.purge('sp-collated-chart'); return; }}

  var layout = {{
    barmode:'group', height:480,
    margin:{{l:100,r:40,t:40,b:160}},
    xaxis:{{tickangle:-40,automargin:true,categoryorder:'array',categoryarray:orderedNames}},
    yaxis:{{title:{{text:yTitle,standoff:12}},automargin:false,rangemode:'tozero'}},
    shapes:shapes, showlegend:false,  // x-axis category labels already identify each bar
    paper_bgcolor: msdThemeTokens().surface, plot_bgcolor: msdThemeTokens().surface
  }};
  Plotly.react('sp-collated-chart', traces, layout, {{responsive:true}});
}}
// ── End Sample Plots Tab ─────────────────────────────────────────────────────
// ── Plate Heatmap Tab (all plates shown together for live comparison) ────────
var hmMetric = 'signal';
var hmChartEntries = [];

function hmInit() {{
  var grid = document.getElementById('hm-grid');
  if (grid.dataset.built === '1') {{ hmRenderAll(); return; }}
  var plates = Object.keys(HEATMAP_DATA.plates).map(Number).sort(function(a, b) {{ return a - b; }});
  plates.forEach(function(p) {{
    var pdata = HEATMAP_DATA.plates[p];
    var spots = Object.keys(pdata.spots).sort(function(a, b) {{ return a - b; }});
    spots.forEach(function(s) {{
      var chartId = 'hm-chart-p' + p + '-s' + s;
      hmChartEntries.push({{ plate: p, spot: s, chartId: chartId }});
      var cell = document.createElement('div');
      cell.className = 'hm-cell';
      var chartDiv = document.createElement('div');
      chartDiv.id = chartId;
      chartDiv.style.height = '320px';
      cell.appendChild(chartDiv);
      grid.appendChild(cell);
    }});
  }});
  grid.dataset.built = '1';
  hmRenderAll();
}}

function hmSetMetric(metric, btn) {{
  hmMetric = metric;
  document.querySelectorAll('#tab-heatmap .sp-subtab-btn').forEach(function(b) {{ b.classList.remove('sp-subtab-active'); }});
  btn.classList.add('sp-subtab-active');
  hmRenderAll();
}}

// One colour scale across every plate. Each plate used to autoscale to its own
// min/max, so two plates whose top wells differ by 23% rendered identically and
// the side-by-side comparison the tab exists for was misleading.
var hmShared = true;
var hmRange = null;
// Plate signal is strongly skewed — a handful of top standards sit orders of
// magnitude above the sample wells — so on a linear ramp every sample well
// lands in the lightest step and the plates stop being distinguishable. A log
// ramp spreads the low end out, which is where the samples are.
var hmLog = true;

function hmCollectRange() {{
  var lo = Infinity, hi = -Infinity, loPos = Infinity;
  hmChartEntries.forEach(function(e) {{
    var wells = HEATMAP_DATA.plates[e.plate].spots[e.spot];
    Object.keys(wells).forEach(function(w) {{
      var v = (hmMetric === 'signal') ? wells[w].signal : wells[w].conc;
      if (v != null && isFinite(v)) {{
        if (v < lo) lo = v;
        if (v > hi) hi = v;
        if (v > 0 && v < loPos) loPos = v;
      }}
    }});
  }});
  if (lo > hi) return null;
  // The log floor has to come from the whole set, not from one plate: clamping
  // with a per-plate minimum gives each plate a different zmin and quietly
  // un-shares the scale this function exists to share.
  return [lo, hi, isFinite(loPos) ? loPos : null];
}}

function hmFmt(v) {{
  if (v == null || !isFinite(v)) return 'N/A';
  var a = Math.abs(v);
  if (a >= 1000) return Math.round(v).toLocaleString();
  return (a >= 1 ? v.toFixed(1) : v.toPrecision(3));
}}

function hmSetLog(on, btn) {{
  hmLog = on;
  document.querySelectorAll('#hm-log-toggle .sp-btn').forEach(function(b) {{
    b.classList.remove('active-sort');
  }});
  if (btn) btn.classList.add('active-sort');
  hmRenderAll();
}}

function hmSetShared(on, btn) {{
  hmShared = on;
  document.querySelectorAll('#hm-scale-toggle .sp-btn').forEach(function(b) {{
    b.classList.remove('active-sort');
  }});
  if (btn) btn.classList.add('active-sort');
  hmRenderAll();
}}

function hmRenderAll() {{
  hmRange = hmShared ? hmCollectRange() : null;
  var bar = document.getElementById('hm-scale-legend');
  if (bar) {{
    bar.innerHTML = hmRange
      ? '<span class="hm-ramp-end">' + hmFmt(hmRange[0]) + '</span>' +
        '<span class="hm-ramp"></span>' +
        '<span class="hm-ramp-end">' + hmFmt(hmRange[1]) + '</span>' +
        '<span>shared across all plates' + (hmLog ? ', log scale' : '') + '</span>'
      : '<span>each plate scaled to its own range \u2014 colours are not comparable between plates</span>';
  }}
  hmChartEntries.forEach(function(entry) {{ hmRenderOne(entry.plate, entry.spot, entry.chartId); }});
}}

function hmRenderOne(plateNum, spotNum, chartId) {{
  var pdata = HEATMAP_DATA.plates[plateNum];
  var wells = pdata.spots[spotNum];
  var rows = pdata.rows;
  var cols = pdata.cols;
  var multiSpot = Object.keys(pdata.spots).length > 1;
  var z = [], text = [];
  rows.forEach(function(r) {{
    var zRow = [], textRow = [];
    cols.forEach(function(c) {{
      var w = r + c;
      var cell = wells[w];
      var val = null;
      var label = w + ': no data';
      if (cell) {{
        val = (hmMetric === 'signal') ? cell.signal : cell.conc;
        label = '<b>' + w + '</b>' + (cell.type ? ('<br>' + cell.type) : '') +
                (cell.name ? (': ' + cell.name) : '') +
                (cell.group ? ('<br>Group: ' + cell.group) : '') +
                '<br>Signal: ' + (cell.signal != null ? Number(cell.signal).toLocaleString() : 'N/A') +
                '<br>Interp. Conc: ' + (cell.conc != null ? cell.conc : 'N/A');
      }}
      zRow.push(val);
      textRow.push(label);
    }});
    z.push(zRow);
    text.push(textRow);
  }});
  // Single-hue sequential ramp, light -> dark. The previous scale ran navy ->
  // blue -> yellow, which is three hues and reads as a rainbow: it implies
  // category changes where there is only magnitude.
  var t = msdThemeTokens();
  var cs = getComputedStyle(document.documentElement);
  var seq = ['--seq-0','--seq-1','--seq-2','--seq-3','--seq-4','--seq-5','--seq-6']
    .map(function(n, i, arr) {{
      return [i / (arr.length - 1), cs.getPropertyValue(n).trim()];
    }});
  var trace = {{
    z: z, x: cols.map(String), y: rows, type: 'heatmap',
    text: text, hoverinfo: 'text',
    colorscale: seq, hoverongaps: false,
    // One shared legend above the grid replaces 12 identical colorbars, which
    // were each eating ~20% of their cell's width.
    showscale: !hmShared,
    xgap: 1, ygap: 1
  }};
  if (hmLog) {{
    // Colour by log10 while hover text keeps the real values.
    trace.z = z.map(function(rw) {{
      return rw.map(function(v) {{
        return (v == null || !isFinite(v) || v <= 0) ? null : Math.log10(v);
      }});
    }});
    if (hmShared && hmRange) {{
      var floorV = hmRange[2] || 1;                       // global smallest positive
      var lo = Math.max(hmRange[0], floorV);
      var hi = Math.max(hmRange[1], lo * 10);
      trace.zmin = Math.log10(lo);
      trace.zmax = Math.log10(hi);
    }}
  }} else if (hmShared && hmRange) {{
    trace.zmin = hmRange[0]; trace.zmax = hmRange[1];
  }}
  var layout = {{
    title: {{ text: 'Plate ' + plateNum + (multiSpot ? (', Spot ' + spotNum) : ''),
             font: {{ size: 12, color: t.ink }} }},
    xaxis: {{ side: 'top', type: 'category', tickfont: {{ size: 9, color: t.muted }} }},
    yaxis: {{ autorange: 'reversed', type: 'category', tickfont: {{ size: 9, color: t.muted }} }},
    margin: {{ l: 30, r: hmShared ? 10 : 60, t: 36, b: 10 }},
    height: 320,
    font: {{ color: t.ink2 }},
    paper_bgcolor: t.surface, plot_bgcolor: t.surface
  }};
  Plotly.react(chartId, [trace], layout, {{ responsive: true }});
}}
// ── End Plate Heatmap Tab ─────────────────────────────────────────────────────

// ── Live Calibrator Drop / Client-side 4PL Re-fit ─────────────────────────────
function four_pl_js(x, a, b, c, d) {{
  if (x <= 0) return a;
  return d + (a - d) / (1 + Math.pow(x / c, b));
}}

function msdStatusFor(r2) {{
  if (r2 === null || r2 === undefined || !isFinite(r2)) return {{ label: 'Poor', cls: 'status-fail' }};
  if (r2 < 0) return {{ label: 'Negative R²', cls: 'status-fail' }};
  if (r2 >= 0.99) return {{ label: 'Good', cls: 'status-good' }};
  if (r2 >= 0.95) return {{ label: 'Acceptable', cls: 'status-warn' }};
  return {{ label: 'Poor', cls: 'status-fail' }};
}}

// Levenberg-Marquardt fit of the 4PL model, 1/y² weighted (matches the Python
// scipy.curve_fit sigma=y weighting used when the report was generated).
function js4PLFit(concs, sigs) {{
  var n = concs.length;
  if (n < 4) return null;
  var pos = [];
  for (var i = 0; i < n; i++) if (concs[i] > 0) pos.push(concs[i]);
  if (!pos.length) return null;

  var a0 = Math.min.apply(null, sigs);
  var d0 = Math.max.apply(null, sigs);
  var logSum = 0;
  for (i = 0; i < pos.length; i++) logSum += Math.log(pos[i]);
  var c0 = Math.exp(logSum / pos.length);
  var b0 = 1.0;
  var params = [a0, b0, c0, d0];

  var w = [];
  for (i = 0; i < n; i++) {{
    var s = Math.max(Math.abs(sigs[i]), 1e-3);
    w.push(1 / (s * s));
  }}

  function clampParams(p) {{
    p[1] = Math.min(20, Math.max(0.01, p[1]));
    p[2] = Math.max(1e-9, p[2]);
    return p;
  }}

  function residuals(p) {{
    var r = new Array(n);
    for (var i = 0; i < n; i++) {{
      var pred = four_pl_js(concs[i], p[0], p[1], p[2], p[3]);
      r[i] = (sigs[i] - pred) * Math.sqrt(w[i]);
    }}
    return r;
  }}

  function cost(r) {{
    var s = 0;
    for (var i = 0; i < r.length; i++) s += r[i] * r[i];
    return s;
  }}

  function jacobian(p) {{
    var J = [];
    var eps = 1e-6;
    var r0 = residuals(p);
    for (var k = 0; k < 4; k++) {{
      var pk = p.slice();
      var h = Math.max(Math.abs(p[k]) * eps, 1e-9);
      pk[k] += h;
      var r1 = residuals(pk);
      var col = new Array(n);
      for (var i = 0; i < n; i++) col[i] = (r1[i] - r0[i]) / h;
      J.push(col);
    }}
    return J; // J[param][point]
  }}

  // Solve a 4x4 linear system via Gaussian elimination with partial pivoting.
  function solve4(A, bVec) {{
    var M = A.map(function(row) {{ return row.slice(); }});
    var b = bVec.slice();
    var nP = 4;
    for (var col = 0; col < nP; col++) {{
      var piv = col;
      for (var r = col + 1; r < nP; r++) if (Math.abs(M[r][col]) > Math.abs(M[piv][col])) piv = r;
      if (Math.abs(M[piv][col]) < 1e-14) return null;
      if (piv !== col) {{
        var tmp = M[piv]; M[piv] = M[col]; M[col] = tmp;
        var tb = b[piv]; b[piv] = b[col]; b[col] = tb;
      }}
      for (var r2 = col + 1; r2 < nP; r2++) {{
        var factor = M[r2][col] / M[col][col];
        for (var c2 = col; c2 < nP; c2++) M[r2][c2] -= factor * M[col][c2];
        b[r2] -= factor * b[col];
      }}
    }}
    var x = new Array(nP);
    for (var i2 = nP - 1; i2 >= 0; i2--) {{
      var sum = b[i2];
      for (var j2 = i2 + 1; j2 < nP; j2++) sum -= M[i2][j2] * x[j2];
      x[i2] = sum / M[i2][i2];
    }}
    return x;
  }}

  var lambda = 1e-3;
  var r = residuals(params);
  var c = cost(r);
  for (var iter = 0; iter < 150; iter++) {{
    var J = jacobian(params);
    var JTJ = [[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]];
    var JTr = [0,0,0,0];
    for (var p1 = 0; p1 < 4; p1++) {{
      for (var p2 = 0; p2 < 4; p2++) {{
        var s2 = 0;
        for (var i3 = 0; i3 < n; i3++) s2 += J[p1][i3] * J[p2][i3];
        JTJ[p1][p2] = s2;
      }}
      var s3 = 0;
      for (var i4 = 0; i4 < n; i4++) s3 += J[p1][i4] * r[i4];
      JTr[p1] = -s3;
    }}
    var improved = false;
    for (var tryCount = 0; tryCount < 10; tryCount++) {{
      var A = JTJ.map(function(row, ri) {{
        return row.map(function(v, ci) {{ return ri === ci ? v * (1 + lambda) : v; }});
      }});
      var delta = solve4(A, JTr);
      if (!delta) break;
      var newParams = clampParams([params[0] + delta[0], params[1] + delta[1],
                                    params[2] + delta[2], params[3] + delta[3]]);
      var newR = residuals(newParams);
      var newC = cost(newR);
      if (isFinite(newC) && newC < c) {{
        params = newParams; r = newR; c = newC;
        lambda = Math.max(lambda / 3, 1e-12);
        improved = true;
        break;
      }} else {{
        lambda *= 3;
      }}
    }}
    if (!improved && lambda > 1e10) break;
  }}

  var wSum = 0, wMeanNum = 0;
  for (i = 0; i < n; i++) {{ wSum += w[i]; wMeanNum += w[i] * sigs[i]; }}
  var wMean = wMeanNum / wSum;
  var ssRes = 0, ssTot = 0;
  for (i = 0; i < n; i++) {{
    var pred2 = four_pl_js(concs[i], params[0], params[1], params[2], params[3]);
    ssRes += w[i] * Math.pow(sigs[i] - pred2, 2);
    ssTot += w[i] * Math.pow(sigs[i] - wMean, 2);
  }}
  var r2 = ssTot > 0 ? 1 - ssRes / ssTot : 0;

  return {{ a: params[0], b: params[1], c: params[2], d: params[3], r2: r2 }};
}}

function msdInverse4PL(y, a, b, c, d) {{
  if (Math.abs(b) < 1e-9) return NaN;
  var denom = y - d;
  if (Math.abs(denom) < 1e-9) return NaN;
  var ratio = (a - d) / denom - 1;
  if (ratio <= 0) return NaN;
  return c * Math.pow(ratio, 1 / b);
}}

function msdBuildFitLine(fit, concs) {{
  var pos = concs.filter(function(v) {{ return v > 0; }});
  var cMin = Math.min.apply(null, pos), cMax = Math.max.apply(null, pos);
  var xs = [], ys = [];
  var nPts = 80;
  var lo = Math.log10(cMin * 0.5), hi = Math.log10(cMax * 2);
  for (var i = 0; i < nPts; i++) {{
    var t = lo + (hi - lo) * i / (nPts - 1);
    var xv = Math.pow(10, t);
    xs.push(xv);
    ys.push(four_pl_js(xv, fit.a, fit.b, fit.c, fit.d));
  }}
  return {{ xs: xs, ys: ys }};
}}

function msdRecomputeCurve(key) {{
  var cd = CURVE_DATA[key];
  if (!cd) return;
  var card = document.querySelector('[data-curvekey="' + key + '"]');
  if (!card) return;
  var concs = [], sigs = [], byConc = {{}};
  card.querySelectorAll('.msd-cal-cb').forEach(function(cb) {{
    var row = cb.closest('tr');
    if (cb.checked) {{
      var cv = parseFloat(cb.dataset.conc), sv = parseFloat(cb.dataset.signal);
      concs.push(cv);
      sigs.push(sv);
      (byConc[cv] = byConc[cv] || []).push(sv);
      row.classList.remove('msd-cal-excluded');
    }} else {{
      row.classList.add('msd-cal-excluded');
    }}
  }});
  // One entry per retained nominal level, ascending — the unit the
  // accuracy-based range is judged on.
  var levels = Object.keys(byConc).map(parseFloat).sort(function(x, y) {{ return x - y; }})
    .map(function(cv) {{
      var arr = byConc[cv], sum = 0;
      for (var i = 0; i < arr.length; i++) sum += arr[i];
      return {{ conc: cv, meanSignal: sum / arr.length }};
    }});
  (cd.blanks || []).forEach(function(bl) {{ concs.push(0); sigs.push(bl.signal); }});

  var badge = card.querySelector('.msd-live-r2');
  var statusBadge = card.querySelector('.msd-live-status');
  var gd = document.getElementById(cd.divId);

  var fit = js4PLFit(concs, sigs);
  if (!fit) {{
    badge.textContent = 'Fit failed (need ≥4 points)';
    statusBadge.textContent = 'Failed';
    statusBadge.className = 'msd-live-status status-fail';
    msdUpdateSummaryRow(key, null, cd, levels);
    return;
  }}

  var line = msdBuildFitLine(fit, concs);
  Plotly.restyle(gd, {{ x: [line.xs], y: [line.ys] }}, [cd.fitTraceIdx]);
  Plotly.relayout(gd, {{ 'title.text': cd.label + '<br><sup>R² = ' + fit.r2.toFixed(6) + ' (live)</sup>' }});

  if (cd.overlayFitTraceIdx !== undefined) {{
    var overlayGd = document.getElementById('overlay_chart');
    if (overlayGd) Plotly.restyle(overlayGd, {{ x: [line.xs], y: [line.ys] }}, [cd.overlayFitTraceIdx]);
  }}

  var st = msdStatusFor(fit.r2);
  badge.textContent = 'Live R²: ' + fit.r2.toFixed(6);
  statusBadge.textContent = st.label;
  statusBadge.className = 'msd-live-status ' + st.cls;

  msdUpdateSummaryRow(key, fit, cd, levels);
}}

// Resolve summary columns by header text once, so adding or reordering columns
// server-side cannot silently write live values into the wrong cell.
var MSD_SUMCOL = (function() {{
  var map = {{}};
  var ths = document.querySelectorAll('#summaryTable thead th');
  for (var i = 0; i < ths.length; i++) {{
    map[ths[i].textContent.trim()] = i;
  }}
  return map;
}})();

// Tolerances must match CAL_RE_TOLERANCE / CAL_RE_TOLERANCE_ANCHOR in the
// Python side, so a live re-fit judges calibrators the same way the report did.
var MSD_CAL_TOL = 20.0, MSD_CAL_TOL_ANCHOR = 25.0;
// Must match HILL_SLOPE_RANGE on the Python side.
var MSD_HILL_MIN = 0.5, MSD_HILL_MAX = 3.0;

// Back-calculate the retained calibrators through a live fit, mirroring
// compute_calibrator_accuracy(). Returns null when nothing can be judged.
function msdCalAccuracy(fit, levels) {{
  if (!fit || !levels.length) return null;
  // Mirrors compute_calibrator_accuracy(): %RE first, then the range ends are
  // the outermost levels clearing the anchor tolerance, and only levels
  // strictly between them face the tighter interior tolerance. Levels outside
  // the resulting range are outside it — not failures.
  var re = levels.map(function(l) {{
    var back = msdInverse4PL(l.meanSignal, fit.a, fit.b, fit.c, fit.d);
    return isFinite(back) ? (back - l.conc) / l.conc * 100.0 : null;
  }});
  var within = function(i, tol) {{ return re[i] !== null && Math.abs(re[i]) <= tol; }};
  var loI = -1, hiI = -1;
  for (var i = 0; i < levels.length; i++) {{ if (within(i, MSD_CAL_TOL_ANCHOR)) {{ loI = i; break; }} }}
  for (var k = levels.length - 1; k >= 0; k--) {{ if (within(k, MSD_CAL_TOL_ANCHOR)) {{ hiI = k; break; }} }}
  if (loI < 0) return {{ lloq: null, uloq: null, nPass: 0, nInRange: 0, nOutside: levels.length }};
  var nPass = 0, gap = false;
  for (var j = loI; j <= hiI; j++) {{
    var tol = (j === loI || j === hiI) ? MSD_CAL_TOL_ANCHOR : MSD_CAL_TOL;
    if (within(j, tol)) nPass++; else gap = true;
  }}
  return {{ lloq: levels[loI].conc, uloq: levels[hiI].conc,
           nPass: nPass, nInRange: hiI - loI + 1,
           nBelow: loI, nAbove: levels.length - 1 - hiI,
           nOutside: loI + (levels.length - 1 - hiI),
           gap: gap, single: loI === hiI }};
}}

// Mirror of compute_curve_flags() for a live re-fit, so the Flags column stays
// consistent with the Cal Pass and Acc. LLOQ/ULOQ values next to it rather than
// still describing the curve as it was served.
function msdFlagsFor(fit, acc) {{
  var flags = [];
  if (!fit) return ['Fit failed'];
  if (fit.b < MSD_HILL_MIN || fit.b > MSD_HILL_MAX) {{
    flags.push('Hill slope ' + fit.b.toFixed(2));
  }}
  if (acc) {{
    if (acc.lloq == null) {{
      flags.push('No calibrator within tolerance');
    }} else {{
      if (acc.nBelow) flags.push(acc.nBelow + ' std below range');
      if (acc.nAbove) flags.push(acc.nAbove + ' std above range');
      if (acc.gap)    flags.push('Non-contiguous range');
      if (acc.single) flags.push('Range rests on one calibrator');
    }}
  }}
  return flags;
}}

function msdUpdateSummaryRow(key, fit, cd, levels) {{
  var row = document.getElementById('sumrow_' + key);
  if (!row) return;
  row.classList.add('msd-row-modified');
  var cells = row.cells;
  var iStatus = MSD_SUMCOL['Status'];
  // Status and Cal Pass render as pills, so write into the pill, not the cell.
  var pill = function(idx, cls) {{
    var cell = cells[idx];
    var sp = cell.querySelector('span');
    if (!sp) {{ sp = document.createElement('span'); cell.textContent = ''; cell.appendChild(sp); }}
    if (cls !== undefined) sp.className = cls;
    return sp;
  }};
  if (!fit) {{
    pill(iStatus, 'status-fail').textContent = 'Failed';
    return;
  }}
  cells[MSD_SUMCOL['Min (a)']].textContent        = fit.a.toPrecision(4);
  cells[MSD_SUMCOL['Hill Slope (b)']].textContent = fit.b.toPrecision(4);
  cells[MSD_SUMCOL['EC50 (c)']].textContent       = fit.c.toPrecision(4);
  cells[MSD_SUMCOL['Max (d)']].textContent        = fit.d.toPrecision(4);
  cells[MSD_SUMCOL['R²']].textContent             = fit.r2.toFixed(6);
  if (cd && cd.lloqSig != null) {{
    var lc = msdInverse4PL(cd.lloqSig, fit.a, fit.b, fit.c, fit.d);
    cells[MSD_SUMCOL['LLOQ Conc']].textContent = (isFinite(lc) && lc > 0) ? lc.toPrecision(4) : 'N/A';
  }}
  // Dropping a calibrator changes what the curve reproduces, so the
  // accuracy-based range is re-derived rather than left showing stale values.
  var acc = msdCalAccuracy(fit, levels || []);
  if (acc) {{
    cells[MSD_SUMCOL['Acc. LLOQ']].textContent = acc.lloq == null ? 'None' : acc.lloq.toPrecision(4);
    cells[MSD_SUMCOL['Acc. ULOQ']].textContent = acc.uloq == null ? 'None' : acc.uloq.toPrecision(4);
    pill(MSD_SUMCOL['Cal Pass'],
         (acc.nPass === acc.nInRange) ? 'status-good' : 'status-warn')
      .textContent = acc.nPass + '/' + acc.nInRange +
                     (acc.nOutside ? ' +' + acc.nOutside : '');
  }}
  cells[MSD_SUMCOL['Flags']].textContent = msdFlagsFor(fit, acc).join(', ');
  var st = msdStatusFor(fit.r2);
  pill(iStatus, st.cls).textContent = st.label;
}}

function msdResetCurve(key) {{
  var cd = CURVE_DATA[key];
  if (!cd) return;
  var card = document.querySelector('[data-curvekey="' + key + '"]');
  if (!card) return;
  card.querySelectorAll('.msd-cal-cb').forEach(function(cb) {{
    cb.checked = true;
    cb.closest('tr').classList.remove('msd-cal-excluded');
  }});
  var badge = card.querySelector('.msd-live-r2');
  var statusBadge = card.querySelector('.msd-live-status');
  badge.textContent = '';
  statusBadge.textContent = '';
  statusBadge.className = 'msd-live-status';

  var gd = document.getElementById(cd.divId);
  if (cd.orig) {{
    var concs = [];
    card.querySelectorAll('.msd-cal-cb').forEach(function(cb) {{ concs.push(parseFloat(cb.dataset.conc)); }});
    var line = msdBuildFitLine(cd.orig, concs);
    Plotly.restyle(gd, {{ x: [line.xs], y: [line.ys] }}, [cd.fitTraceIdx]);
    Plotly.relayout(gd, {{ 'title.text': cd.label + '<br><sup>R² = ' + cd.orig.r2.toFixed(6) + '</sup>' }});

    if (cd.overlayFitTraceIdx !== undefined) {{
      var overlayGd = document.getElementById('overlay_chart');
      if (overlayGd) Plotly.restyle(overlayGd, {{ x: [line.xs], y: [line.ys] }}, [cd.overlayFitTraceIdx]);
    }}
  }}

  var row = document.getElementById('sumrow_' + key);
  if (row && row.dataset.origHtml !== undefined) {{
    row.innerHTML = row.dataset.origHtml;
    row.classList.remove('msd-row-modified');
  }}
}}

// Snapshot original summary-row HTML once, so "Reset Calibrators" can restore
// the exact server-computed values rather than re-deriving them client-side.
document.querySelectorAll('#summaryTable tbody tr').forEach(function(row) {{
  row.dataset.origHtml = row.innerHTML;
}});
// ── End Live Calibrator Drop ───────────────────────────────────────────────────

</script>
</body>
</html>"""

    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Saved HTML report: {html_path}")


def run_analysis(msd_path, platemap_path, output_path, spots_override=None, units=None, cv_threshold=25, dilution_factors=None, lloq_method='current', total_protein_path=None, qc_dilution_factors=None, qc_expected_concentrations=None, group_dilution_factors=None, infer_group_numbers=True):
    _ensure_deps()   # lazy-load numpy / scipy / matplotlib / openpyxl
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

    if n_plate_maps > 1 and len(plates) != n_plate_maps:
        msg = (f"MSD file contains {len(plates)} plate(s), but plate map contains "
               f"{n_plate_maps} plate map(s). Provide one map (applied to all plates) "
               f"or one map per plate.")
        print(f"Error: {msg}")
        raise RuntimeError(msg)

    try:
        plate_dilution_factors = parse_plate_dilution_factors(dilution_factors, len(plates))
    except ValueError as e:
        print(f"Error parsing dilution factors: {e}")
        raise

    # Build per-plate well lookups (well → list of entries; a well can serve multiple groups)
    plate_well_maps = {}
    for pm_num, entries in plate_maps.items():
        wm = {}
        for e in entries:
            wm.setdefault(normalize_well(e['well']), []).append(e)
        plate_well_maps[pm_num] = wm

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

            # Collect all well data for this spot, tagged with group.
            # A single well can produce multiple entries (multi-group & standard syntax).
            spot_wells = []
            for well_id, spot_signals in wd.items():
                nw = normalize_well(well_id)
                if spot_idx >= len(spot_signals):
                    continue
                signal = spot_signals[spot_idx]
                info_list = well_map.get(nw)
                if not info_list:
                    continue
                for info in info_list:
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
                        except (ValueError, ZeroDivisionError, OverflowError):
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

    # Back-calculate the calibrators through each fitted curve and derive the
    # advisory flags once, here, so the Excel workbook and the HTML report both
    # report the same numbers rather than recomputing them independently.
    for res in results:
        lloq_conc = None
        lloq_sig = res.get('lloq_sig')
        if lloq_sig is not None and res.get('params') is not None:
            try:
                lc = inverse_4pl(lloq_sig, *res['params'])
                if np.isfinite(lc) and lc > 0:
                    lloq_conc = float(lc)
            except (ValueError, ZeroDivisionError, OverflowError):
                pass
        res['lloq_conc'] = lloq_conc
        res['accuracy'] = compute_calibrator_accuracy(res)
        res['flags'] = compute_curve_flags(res, res['accuracy'])

    # Only raise for spots that have unknowns but no curve to interpolate them.
    # Spots with no standards AND no unknowns are silently empty — not an error.
    missing = [r for r in results if r.get('no_standards') and r.get('unknowns')]
    if missing:
        labels = []
        for r in missing:
            lbl = f"Plate {r['plate']}, Spot {r['spot']}"
            if r.get('group'):
                lbl += f", Group {r['group']}"
            labels.append(lbl)
        msg = "Standards are missing for spots that have unknowns: " + "; ".join(labels)
        print(f"Error: {msg}")
        raise RuntimeError(msg)

    # Parse total protein CSV if provided
    total_protein_map = None
    animal_tissue_map = None
    animal_group_map = None
    if total_protein_path:
        try:
            total_protein_map, animal_tissue_map, animal_group_map = parse_total_protein_csv(total_protein_path)
            print(f"\nLoaded total protein data: {len(total_protein_map)} animal/tissue entries"
                  f"{f', {len(animal_group_map)} group assignments' if animal_group_map else ''}")
        except Exception as e:
            print(f"Warning: could not load total protein CSV: {e}")

    # Infer Study Group Number from animal ID (e.g. 1001 -> Group 1) when enabled.
    # Real, explicit Group Numbers from the total protein CSV always take
    # precedence — inference is only used when the CSV denoted NO group data
    # at all (no CSV, or a CSV without a Group Number column/values). It is
    # not a per-animal gap-filler for a CSV that already assigns some groups:
    # animals the CSV leaves ungrouped are more likely intentionally excluded
    # from grouping (e.g. QC/controls) than accidentally missing.
    if infer_group_numbers and not animal_group_map:
        animal_group_map = infer_animal_group_map(results)
        if animal_group_map:
            print(f"Inferred Study Group from animal ID for {len(animal_group_map)} animal(s) "
                  f"(no Group Number data was provided via the total protein CSV)")
    elif infer_group_numbers and animal_group_map:
        print(f"Study Group inference skipped — {len(animal_group_map)} animal(s) already have "
              f"an explicit Group Number from the total protein CSV; that data takes precedence.")

    # Flag (don't block on) a non-sequential/missing-Group-1 result — usually
    # means the inference formula doesn't apply to this animal ID convention.
    group_sequence_warning = _validate_group_sequence(animal_group_map)
    if group_sequence_warning:
        print(f"\n⚠ WARNING: {group_sequence_warning}")

    print(f"\n{'=' * 60}")
    print(f"Generating Excel: {output_path}")
    create_output(results, output_path, msd_path, raw_plate_blocks, units, cv_threshold, plate_dilution_factors, lloq_method, total_protein_map, qc_dilution_factors, qc_expected_concentrations, group_dilution_factors=group_dilution_factors, animal_tissue_map=animal_tissue_map, animal_group_map=animal_group_map)
    print("Done!")

    # Generate and open interactive HTML report (co-located with the Excel file so
    # that the "Open Excel" button works via a same-directory relative href, and
    # plotly.min.js is written there once too — no cross-directory file:// issues).
    html_basename = os.path.splitext(os.path.basename(output_path))[0] + '.html'
    html_path = os.path.join(os.path.dirname(os.path.abspath(output_path)), html_basename)
    try:
        generate_html_report(results, html_path, msd_path, units,
                             qc_dilution_factors, qc_expected_concentrations,
                             plate_dilution_factors, lloq_method,
                             total_protein_map, output_path,
                             group_dilution_factors=group_dilution_factors,
                             cv_threshold=cv_threshold,
                             animal_tissue_map=animal_tissue_map,
                             animal_group_map=animal_group_map)
        if os.path.exists(html_path):
            _open_file(html_path)
    except Exception as e:
        import traceback as _tb
        print(f"Warning: HTML report could not be generated: {e}")
        _tb.print_exc()

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
        'qc_expected_concentrations': qc_expected_concentrations,
        'group_dilution_factors': group_dilution_factors,
        'infer_group_numbers': infer_group_numbers,
        'status': 'pass',
    }
    _save_run_to_history(last_args)
    return group_sequence_warning


def run_interactive():
    """Launch a single-page GUI for configuring all analysis options."""
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox

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
        # Default True for entries saved before this option existed
        infer_group_var.set(entry.get('infer_group_numbers', True))
        # Restore group dilution factors and per-group QC values if any were saved
        saved_grp = entry.get('group_dilution_factors') or {}
        saved_qc = entry.get('qc_dilution_factors') or {}
        saved_exp = entry.get('qc_expected_concentrations') or {}
        # Normalize old flat qc_dilution_factors format (skip silently)
        if saved_qc and not isinstance(next(iter(saved_qc.values()), {}), dict):
            saved_qc = {}
        if saved_exp and not isinstance(saved_exp, dict):
            saved_exp = {}
        if saved_grp:
            # Rebuild group rows for the saved groups
            for w in grp_rows_frame.winfo_children():
                w.destroy()
            group_df_vars.clear()
            grp_qc_vars.clear()
            grp_exp_vars.clear()
            # Collect all QC levels that appear in saved_qc across all groups
            all_saved_qc_cols = [lvl for lvl in QC_LEVELS
                                 if any(lvl in (saved_qc.get(g) or {}) for g in saved_grp)]
            col = 0
            ttk.Label(grp_rows_frame, text='Group', font=('TkDefaultFont', 9, 'bold')).grid(
                row=0, column=col, sticky=tk.W, padx=(0, 8), pady=(0, 2))
            col += 1
            ttk.Label(grp_rows_frame, text='Dil. Factor', font=('TkDefaultFont', 9, 'bold')).grid(
                row=0, column=col, sticky=tk.W, padx=(0, 8), pady=(0, 2))
            col += 1
            for lvl in all_saved_qc_cols:
                ttk.Label(grp_rows_frame, text=lvl, font=('TkDefaultFont', 9, 'bold')).grid(
                    row=0, column=col, sticky=tk.W, padx=(0, 8), pady=(0, 2))
                col += 1
            ttk.Label(grp_rows_frame, text='Expected Conc.', font=('TkDefaultFont', 9, 'bold')).grid(
                row=0, column=col, sticky=tk.W, padx=(0, 8), pady=(0, 2))
            for ri, (gname, gval) in enumerate(sorted(saved_grp.items()), 1):
                col = 0
                ttk.Label(grp_rows_frame, text=gname).grid(
                    row=ri, column=col, sticky=tk.W, padx=(0, 8), pady=2)
                col += 1
                df_var = tk.StringVar(value=str(gval))
                group_df_vars[gname] = df_var
                ttk.Entry(grp_rows_frame, textvariable=df_var, width=9).grid(
                    row=ri, column=col, sticky=tk.W, padx=(0, 8), pady=2)
                col += 1
                grp_qc_vars[gname] = {}
                g_qc = saved_qc.get(gname) or {}
                for lvl in all_saved_qc_cols:
                    if lvl in g_qc:
                        qc_var = tk.StringVar(value=str(g_qc[lvl]))
                        grp_qc_vars[gname][lvl] = qc_var
                        ttk.Entry(grp_rows_frame, textvariable=qc_var, width=8).grid(
                            row=ri, column=col, sticky=tk.W, padx=(0, 8), pady=2)
                    else:
                        ttk.Label(grp_rows_frame, text='—', foreground='grey').grid(
                            row=ri, column=col, sticky=tk.W, padx=(0, 8), pady=2)
                    col += 1
                exp_val = saved_exp.get(gname, '')
                exp_var = tk.StringVar(value=str(exp_val) if exp_val else '')
                grp_exp_vars[gname] = exp_var
                ttk.Entry(grp_rows_frame, textvariable=exp_var, width=10).grid(
                    row=ri, column=col, sticky=tk.W, padx=(0, 8), pady=2)
            grp_hint.config(text=f'Restored {len(saved_grp)} group(s) from history.')

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

    def _show_loading_screen():
        """Animated loading window with a 4PL sigmoid being drawn in real time."""
        import math, random, threading as _threading
        win = tk.Toplevel(root)
        win.title("MSD 4PL Analysis")
        win.resizable(False, False)
        win.configure(bg='white')
        win.protocol("WM_DELETE_WINDOW", lambda: None)   # prevent accidental close

        sw, sh = win.winfo_screenwidth(), win.winfo_screenheight()
        ww, wh = 460, 310
        win.geometry(f"{ww}x{wh}+{(sw-ww)//2}+{(sh-wh)//2}")
        win.grab_set()

        tk.Label(win, text="Running Analysis…", font=('Arial', 15, 'bold'),
                 bg='white', fg='#2c3e50').pack(pady=(20, 2))

        # ── Canvas ──────────────────────────────────────────────────────
        cw, ch = 420, 170
        canvas = tk.Canvas(win, width=cw, height=ch, bg='white', highlightthickness=0)
        canvas.pack(padx=20)

        # ── Status label & progress bar ─────────────────────────────────
        status_var = tk.StringVar(value="Parsing MSD data…")
        tk.Label(win, textvariable=status_var, font=('Arial', 10),
                 bg='white', fg='#555555').pack(pady=(6, 2))
        pb = ttk.Progressbar(win, mode='indeterminate', length=420)
        pb.pack(padx=20, pady=(0, 20))
        pb.start(12)

        # ── 4PL model for animation ──────────────────────────────────────
        def _4pl_anim(x):
            return 90000 + (200 - 90000) / (1 + (x / 10) ** 1.5)

        ml, mr, mt, mb = 48, 12, 14, 32          # margins
        pw, ph = cw - ml - mr, ch - mt - mb      # plot area
        xl, xh = math.log10(0.04), math.log10(600)
        yl, yh = math.log10(140),  math.log10(110000)

        def _px(x, y):
            px = ml + (math.log10(max(x, 1e-9)) - xl) / (xh - xl) * pw
            py = mt + ph - (math.log10(max(y, 1e-9)) - yl) / (yh - yl) * ph
            return px, py

        # Static axes
        canvas.create_line(ml, mt, ml, mt + ph, fill='#aaaaaa', width=1.5)
        canvas.create_line(ml, mt + ph, ml + pw, mt + ph, fill='#aaaaaa', width=1.5)
        canvas.create_text(ml + pw // 2, ch - 6, text='Concentration (log scale)',
                           font=('Arial', 7), fill='#888888')
        canvas.create_text(10, mt + ph // 2, text='Signal', angle=90,
                           font=('Arial', 7), fill='#888888')
        for lx in [math.log10(v) for v in [0.1, 1, 10, 100]]:
            px = ml + (lx - xl) / (xh - xl) * pw
            canvas.create_line(px, mt, px, mt + ph, fill='#eeeeee', width=1)

        # Curve smooth points
        _xs = [0.04 * (600 / 0.04) ** (i / 119) for i in range(120)]
        _curve_pts = [_px(x, _4pl_anim(x)) for x in _xs]

        # Scatter data (2 reps per conc, slight noise)
        random.seed(7)
        _concs = [0.1, 0.3, 1, 3, 10, 30, 100, 300]
        _scatter = []
        for c in _concs:
            for _ in range(2):
                _scatter.append(_px(c, _4pl_anim(c) * random.uniform(0.91, 1.09)))

        # Pre-create dot items (hidden)
        _dot_ids = []
        for px, py in _scatter:
            did = canvas.create_oval(px - 4, py - 4, px + 4, py + 4,
                                     fill='#2F5496', outline='#1a2f6e',
                                     width=1.2, state='hidden')
            _dot_ids.append(did)

        # Animation state
        _st = {'phase': 0, 'step': 0, 'after_id': None}

        def _animate():
            p, s = _st['phase'], _st['step']
            if p == 0:                          # dots pop in one by one
                if s < len(_dot_ids):
                    canvas.itemconfig(_dot_ids[s], state='normal')
                    _st['step'] += 1
                    _st['after_id'] = win.after(91, _animate)
                else:
                    _st['phase'], _st['step'] = 1, 0
                    _st['after_id'] = win.after(156, _animate)
            elif p == 1:                        # curve draws left → right
                if s < len(_curve_pts) - 2:
                    x1, y1 = _curve_pts[s]
                    x2, y2 = _curve_pts[s + 2]
                    canvas.create_line(x1, y1, x2, y2,
                                       fill='#E06C4A', width=2, tags='crv')
                    _st['step'] += 2
                    _st['after_id'] = win.after(26, _animate)
                else:
                    _st['phase'], _st['step'] = 2, 0
                    _st['after_id'] = win.after(26, _animate)
            elif p == 2:                        # hold
                _st['step'] += 1
                if _st['step'] > 35:
                    _st['phase'], _st['step'] = 3, 0
                _st['after_id'] = win.after(52, _animate)
            else:                               # reset
                canvas.delete('crv')
                for did in _dot_ids:
                    canvas.itemconfig(did, state='hidden')
                _st['phase'], _st['step'] = 0, 0
                _st['after_id'] = win.after(104, _animate)

        _animate()

        # Status message cycling
        _msgs = ["Parsing MSD data…", "Building plate maps…",
                 "Fitting 4PL curves…", "Calculating LLOQ values…",
                 "Writing Excel report…", "Generating HTML charts…"]
        _mi = [0]
        def _cycle():
            _mi[0] = (_mi[0] + 1) % len(_msgs)
            status_var.set(_msgs[_mi[0]])
            win._msg_id = win.after(2200, _cycle)
        win._msg_id = win.after(2200, _cycle)

        def _close():
            if _st['after_id']:
                win.after_cancel(_st['after_id'])
            if hasattr(win, '_msg_id'):
                win.after_cancel(win._msg_id)
            pb.stop()
            try:
                win.grab_release()
                win.destroy()
            except Exception:
                pass

        win.close_loading = _close
        return win

    def run():
        import threading as _threading
        import traceback as _traceback

        msd_path = msd_var.get().strip()
        platemap_path = platemap_var.get().strip()
        output_path = output_var.get().strip()
        spots_override = spots_var.get().strip()
        units = units_var.get().strip()
        cv_threshold = cv_threshold_var.get().strip()
        lloq_method = lloq_method_var.get()
        dilution_factors = dilution_factors_var.get().strip()
        total_protein_path = total_protein_var.get().strip()
        infer_group_numbers = infer_group_var.get()

        if not msd_path or not platemap_path or not output_path:
            messagebox.showerror("Error", "Please select MSD file, plate map, and output location.")
            return

        spots_override = int(spots_override) if spots_override and spots_override in ('1', '4', '10') else None
        units = units if units else None
        cv_threshold = float(cv_threshold) if cv_threshold else None
        dilution_factors = dilution_factors if dilution_factors else None
        total_protein_path = total_protein_path if total_protein_path else None

        # Collect group dilution factors
        group_dilution_factors = {}
        for gname, gvar in group_df_vars.items():
            val_str = gvar.get().strip()
            if val_str:
                try:
                    group_dilution_factors[gname] = float(val_str)
                except ValueError:
                    messagebox.showerror("Error", f"Invalid group dilution factor for '{gname}': '{val_str}'")
                    return
        group_dilution_factors = group_dilution_factors if group_dilution_factors else None

        # Collect per-group QC dilution factors
        qc_dilution_factors = {}
        for gname, level_vars in grp_qc_vars.items():
            for level, var in level_vars.items():
                val_str = var.get().strip()
                if val_str:
                    try:
                        qc_dilution_factors.setdefault(gname, {})[level] = float(val_str)
                    except ValueError:
                        messagebox.showerror("Error", f"Invalid QC dilution factor for {gname}/{level}: '{val_str}'")
                        return
        qc_dilution_factors = qc_dilution_factors if qc_dilution_factors else None

        # Collect per-group expected concentrations
        qc_expected_concentrations = {}
        for gname, var in grp_exp_vars.items():
            val_str = var.get().strip()
            if val_str:
                try:
                    qc_expected_concentrations[gname] = float(val_str)
                except ValueError:
                    messagebox.showerror("Error", f"Invalid expected concentration for {gname}: '{val_str}'")
                    return
        qc_expected_concentrations = qc_expected_concentrations if qc_expected_concentrations else None

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
        if group_dilution_factors:
            print(f"Group dilution factors: {group_dilution_factors}")
        print(f"LLOQ method: {lloq_method}")

        # Thread result container
        _result = {'error': None, 'done': False, 'group_warning': None}

        def _worker():
            try:
                _result['group_warning'] = run_analysis(
                             msd_path, platemap_path, output_path, spots_override,
                             units, cv_threshold, dilution_factors, lloq_method,
                             total_protein_path, qc_dilution_factors, qc_expected_concentrations,
                             group_dilution_factors=group_dilution_factors,
                             infer_group_numbers=infer_group_numbers)
            except Exception as exc:
                _result['error'] = exc
                print(f"\nAnalysis error: {exc}")
                _traceback.print_exc()
            finally:
                _result['done'] = True

        loading = _show_loading_screen()
        _threading.Thread(target=_worker, daemon=True).start()

        def _poll():
            if not _result['done']:
                root.after(200, _poll)
                return
            loading.close_loading()
            if _result['error'] is not None:
                err_msg = str(_result['error']) or type(_result['error']).__name__
                fail_entry = {
                    'msd': msd_path, 'platemap': platemap_path,
                    'output': output_path, 'spots': spots_override,
                    'units': units, 'cv_threshold': cv_threshold,
                    'dilution_factors': dilution_factors,
                    'lloq_method': lloq_method,
                    'total_protein': total_protein_path,
                    'qc_dilution_factors': qc_dilution_factors,
                    'qc_expected_concentrations': qc_expected_concentrations,
                    'group_dilution_factors': group_dilution_factors,
                    'infer_group_numbers': infer_group_numbers,
                    'status': 'fail', 'error': err_msg,
                }
                _save_run_to_history(fail_entry)
                messagebox.showerror("Analysis Error", err_msg, parent=root)
                root.deiconify()
                refresh_history()
            else:
                if _result.get('group_warning'):
                    root.deiconify()
                    messagebox.showwarning("Group Number Check", _result['group_warning'], parent=root)
                root.destroy()

        root.withdraw()
        root.after(200, _poll)

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
    infer_group_var = tk.BooleanVar(value=True)
    group_df_vars = {}   # {group: StringVar} — populated by _detect_groups
    grp_qc_vars = {}     # {group: {level: StringVar}} — populated by _detect_groups
    grp_exp_vars = {}    # {group: StringVar} — populated by _detect_groups

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
    # Version label (right-aligned in header)
    header_canvas.create_text(header_canvas.winfo_reqwidth() or 860, 42, anchor='e',
                              text=f'v{__version__}',
                              fill=SLATE_LIGHT, font=('Helvetica', 9), tags='ver_lbl')
    def _reposition_ver(*_):
        w = header_canvas.winfo_width()
        header_canvas.coords('ver_lbl', w - 12, 42)
    header_canvas.bind('<Configure>', _reposition_ver)

    # Thin accent rule below header
    tk.Canvas(root, height=2, bg='#7ba7bc', highlightthickness=0).pack(fill=tk.X)

    # ── Update-available banner (hidden until update checker fires) ──────
    _update_bar   = tk.Frame(root, bg='#e8a020')
    _update_lbl   = tk.Label(_update_bar, text='', bg='#e8a020', fg='white',
                             font=('Helvetica', 10), anchor='w', padx=10)
    _update_lbl.pack(side=tk.LEFT, fill=tk.X, expand=True)

    _upd_install_btn = tk.Label(_update_bar, text='  ⬇ Install Update  ',
                                bg='#b36800', fg='white',
                                font=('Helvetica', 10, 'bold'),
                                relief='flat', cursor='hand2', padx=6, pady=2)
    _upd_install_btn.pack(side=tk.RIGHT, padx=(0, 4), pady=3)

    _upd_browser_btn = tk.Label(_update_bar, text='  Open Release Page  ',
                                bg='#c47a00', fg='white',
                                font=('Helvetica', 10),
                                relief='flat', cursor='hand2', padx=6, pady=2)
    _upd_browser_btn.pack(side=tk.RIGHT, padx=(0, 2), pady=3)

    _upd_dismiss = tk.Label(_update_bar, text=' ✕ ', bg='#e8a020', fg='white',
                            font=('Helvetica', 11, 'bold'), cursor='hand2', padx=6)
    _upd_dismiss.pack(side=tk.RIGHT, pady=3)

    _update_release_info = {}   # filled by _show_update_banner: {tag, html_url, assets}

    def _open_release_page(_e=None):
        import webbrowser
        webbrowser.open(_update_release_info.get('html_url', _DOWNLOAD_URL))

    def _dismiss_update(_e=None):
        _update_bar.pack_forget()

    def _do_install_update(_e=None):
        """Download the platform-appropriate zip and swap executables."""
        import webbrowser
        assets   = _update_release_info.get('assets', {})
        plat_key = _platform_asset_key()
        dl_url   = assets.get(plat_key) if plat_key else None

        if not dl_url:
            # Fallback — open browser to release page
            webbrowser.open(_update_release_info.get('html_url', _DOWNLOAD_URL))
            return

        # ── Progress dialog ───────────────────────────────────────────────
        dlg = tk.Toplevel(root)
        dlg.title("Downloading Update")
        dlg.geometry("420x130")
        dlg.resizable(False, False)
        dlg.grab_set()
        tk.Label(dlg, text=f"Downloading {_update_release_info.get('tag','update')}…",
                 font=('Helvetica', 11)).pack(pady=(18, 6))
        _prog_var = tk.DoubleVar(value=0)
        prog_bar  = ttk.Progressbar(dlg, variable=_prog_var,
                                    maximum=100, length=360, mode='determinate')
        prog_bar.pack(pady=4)
        status_lbl = tk.Label(dlg, text='Connecting…', font=('Helvetica', 9),
                              fg='#555')
        status_lbl.pack()

        def _progress(done, total):
            if total > 0:
                pct = done / total * 100
                root.after(0, lambda: (_prog_var.set(pct),
                                       status_lbl.config(
                                           text=f'{done//1024} / {total//1024} KB')))

        def _worker():
            try:
                tmp_zip = os.path.join(tempfile.gettempdir(),
                                       f"_msd_update_{plat_key}.zip")
                _download_file(dl_url, tmp_zip, progress_cb=_progress)

                def _apply():
                    dlg.destroy()
                    if plat_key == 'windows':
                        _install_update_windows(tmp_zip)
                        import tkinter.messagebox as mb
                        mb.showinfo(
                            "Update Ready",
                            "The update has been downloaded.\n\n"
                            "The application will now close and restart "
                            "automatically with the new version.",
                            parent=root)
                        root.destroy()
                    elif plat_key == 'macos':
                        _install_update_macos(tmp_zip)
                        import tkinter.messagebox as mb
                        mb.showinfo(
                            "Update Ready",
                            "The update has been downloaded.\n\n"
                            "The application will now close. The new version "
                            "will launch automatically.",
                            parent=root)
                        root.destroy()
                root.after(0, _apply)

            except Exception as exc:
                def _err():
                    dlg.destroy()
                    import tkinter.messagebox as mb
                    mb.showerror("Update Failed",
                                 f"Could not install the update:\n{exc}\n\n"
                                 "Please download it manually from the release page.",
                                 parent=root)
                root.after(0, _err)

        threading.Thread(target=_worker, daemon=True).start()

    _upd_install_btn.bind('<Button-1>', _do_install_update)
    _upd_browser_btn.bind('<Button-1>', _open_release_page)
    _upd_dismiss.bind('<Button-1>',    _dismiss_update)

    def _show_update_banner(release_info):
        _update_release_info.clear()
        _update_release_info.update(release_info)
        tag = release_info.get('tag', '')
        _update_lbl.config(
            text=f'  ⬆  Version {tag} is available  ')
        plat_key = _platform_asset_key()
        has_asset = bool(release_info.get('assets', {}).get(plat_key))
        _upd_install_btn.pack(side=tk.RIGHT, padx=(0, 4), pady=3) if has_asset \
            else _upd_install_btn.pack_forget()
        _update_bar.pack(fill=tk.X, before=_bottom)

    def _run_update_check():
        info = _fetch_latest_release()
        if info and _parse_version(info['tag']) > _parse_version(__version__):
            root.after(0, lambda: _show_update_banner(info))

    threading.Thread(target=_run_update_check, daemon=True).start()

    # ── Email notifications dialog ────────────────────────────────────────
    def _show_notify_dialog():
        import webbrowser, tkinter.messagebox as mb
        dlg = tk.Toplevel(root)
        dlg.title("Get Update Notifications")
        dlg.geometry("480x310")
        dlg.resizable(False, False)
        dlg.grab_set()

        tk.Label(dlg, text='Get notified about new releases',
                 font=('Helvetica', 13, 'bold')).pack(pady=(18, 4))

        # ── Option A: GitHub Watch ──
        sep_a = ttk.LabelFrame(dlg, text='Option A — GitHub Notifications (recommended)',
                               padding=10)
        sep_a.pack(fill=tk.X, padx=18, pady=(8, 4))
        tk.Label(sep_a, text=(
            'GitHub emails you automatically whenever a new release is published.\n'
            'Requires a free GitHub account.'
        ), justify='left', wraplength=400, font=('Helvetica', 10)).pack(anchor='w')
        tk.Button(sep_a, text='  Open GitHub Watch Page  ',
                  command=lambda: webbrowser.open(
                      f"https://github.com/{_GITHUB_REPO}/subscription")
                  ).pack(anchor='w', pady=(6, 0))

        # ── Option B: Email registration ──
        sep_b = ttk.LabelFrame(dlg, text='Option B — Register your email',
                               padding=10)
        sep_b.pack(fill=tk.X, padx=18, pady=(4, 8))

        if _FORMSPREE_ENDPOINT:
            row = ttk.Frame(sep_b)
            row.pack(fill=tk.X)
            tk.Label(row, text='Email:', font=('Helvetica', 10)).pack(side=tk.LEFT)
            email_var = tk.StringVar()
            email_ent = ttk.Entry(row, textvariable=email_var, width=30)
            email_ent.pack(side=tk.LEFT, padx=(6, 8))
            status_v = tk.StringVar(value='')
            tk.Label(sep_b, textvariable=status_v,
                     font=('Helvetica', 9), fg='#555').pack(anchor='w', pady=(2, 0))

            def _submit():
                addr = email_var.get().strip()
                if '@' not in addr:
                    status_v.set('Please enter a valid email address.')
                    return
                status_v.set('Registering…')
                def _worker():
                    ok, msg = _register_email_formspree(addr)
                    root.after(0, lambda: status_v.set(
                        '✓ Registered! You\'ll receive release emails.' if ok
                        else f'Error: {msg}'))
                threading.Thread(target=_worker, daemon=True).start()

            tk.Button(sep_b, text='Register', command=_submit).pack(anchor='w', pady=(6,0))
        else:
            tk.Label(sep_b,
                     text=('Email registration is not configured for this installation.\n'
                           'Use Option A (GitHub) to receive automatic notifications.'),
                     justify='left', wraplength=400, font=('Helvetica', 10),
                     fg='#666').pack(anchor='w')

        ttk.Button(dlg, text='Close', command=dlg.destroy).pack(pady=10)

    # ── Fixed bottom action bar (always visible, packed before scroll area) ──
    _bottom = ttk.Frame(root, padding='6 4 12 8')
    _bottom.pack(side=tk.BOTTOM, fill=tk.X)
    ttk.Separator(_bottom, orient='horizontal').pack(fill=tk.X, pady=(0, 8))
    _btn_row = ttk.Frame(_bottom)
    _btn_row.pack(fill=tk.X)
    # Buttons are added to _btn_row after `run` is defined (see bottom of this function)

    # ── Scrollable content area ────────────────────────────────────────
    _sc_host = ttk.Frame(root)
    _sc_host.pack(fill=tk.BOTH, expand=True)

    _vscroll = ttk.Scrollbar(_sc_host, orient=tk.VERTICAL)
    _vscroll.pack(side=tk.RIGHT, fill=tk.Y)

    _scroll_canvas = tk.Canvas(_sc_host, highlightthickness=0,
                                yscrollcommand=_vscroll.set)
    _scroll_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    _vscroll.configure(command=_scroll_canvas.yview)

    outer = ttk.Frame(_scroll_canvas, padding='12 10 12 6')
    _cw = _scroll_canvas.create_window((0, 0), window=outer, anchor='nw')

    def _update_scrollregion(event=None):
        _scroll_canvas.configure(scrollregion=_scroll_canvas.bbox('all'))

    def _fit_canvas_width(event):
        _scroll_canvas.itemconfig(_cw, width=event.width)

    outer.bind('<Configure>', _update_scrollregion)
    _scroll_canvas.bind('<Configure>', _fit_canvas_width)

    def _on_mousewheel(event):
        if event.delta:
            _scroll_canvas.yview_scroll(int(-1 * event.delta / 120), 'units')
    _scroll_canvas.bind_all('<MouseWheel>', _on_mousewheel)

    outer.columnconfigure(0, weight=1)

    # helper: consistent row padding inside LabelFrames
    _rp = {'pady': 4}

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

    # Row 4 — Infer Study Group from Animal Number (on by default)
    ttk.Checkbutton(
        opts_lf, variable=infer_group_var,
        text='Infer Study Group from Animal Number if none is given (e.g. 1001 → Group 1, 13502 → Group 13). '
             'Ignored when the total protein CSV already provides Group Numbers.'
    ).grid(row=4, column=0, columnspan=6, sticky=tk.W, **_rp)

    # ── Group Dilution Factors ─────────────────────────────────────────
    grp_lf = ttk.LabelFrame(outer, text='Group Dilution Factors  (optional — applied per group detected in plate map)',
                             padding='10 6')
    grp_lf.pack(fill=tk.X, pady=(0, 8))

    grp_hint = ttk.Label(grp_lf,
                         text='Load a plate map, then click Detect Groups to set per-group dilution factors.',
                         foreground='grey')
    grp_hint.grid(row=0, column=0, columnspan=6, sticky=tk.W, pady=(0, 4))

    grp_btn_frame = ttk.Frame(grp_lf)
    grp_btn_frame.grid(row=1, column=0, columnspan=6, sticky=tk.W, pady=(0, 4))

    # Inner frame that holds the dynamically created group rows
    grp_rows_frame = ttk.Frame(grp_lf)
    grp_rows_frame.grid(row=2, column=0, columnspan=6, sticky=tk.EW)

    def _detect_groups():
        """Parse the platemap and create one dilution-factor row per group.

        Uses the lightweight stdlib-only _parse_groups_only() so that heavy
        deps (pandas / numpy) are never needed just for group detection.
        """
        pm_path = platemap_var.get().strip()
        if not pm_path or not os.path.exists(pm_path):
            messagebox.showwarning("No Plate Map", "Please select a valid plate map CSV first.")
            return
        try:
            found, grp_qc_levels = _parse_groups_only(pm_path)
        except Exception as exc:
            messagebox.showerror("Parse Error", f"Could not read plate map:\n{exc}")
            return

        # Destroy old rows
        for w in grp_rows_frame.winfo_children():
            w.destroy()

        if not found:
            ttk.Label(grp_rows_frame,
                      text='No named groups found in plate map (group prefix syntax: GroupName:value).',
                      foreground='grey').grid(row=0, column=0, columnspan=6, sticky=tk.W)
            group_df_vars.clear()
            return

        # All QC levels seen across all groups (for column headers)
        all_qc_cols = [lvl for lvl in QC_LEVELS if any(lvl in grp_qc_levels[g] for g in found)]

        # Preserve any existing values when re-detecting
        prev = {g: group_df_vars[g].get() for g in group_df_vars if g in found}
        prev_qc = {g: {lvl: grp_qc_vars[g][lvl].get() for lvl in grp_qc_vars[g]}
                   for g in grp_qc_vars if g in found}
        prev_exp = {g: grp_exp_vars[g].get() for g in grp_exp_vars if g in found}
        group_df_vars.clear()
        grp_qc_vars.clear()
        grp_exp_vars.clear()

        # Column headers: Group | Dil. Factor | [QC levels...] | Expected Conc.
        col = 0
        ttk.Label(grp_rows_frame, text='Group', font=('TkDefaultFont', 9, 'bold')).grid(
            row=0, column=col, sticky=tk.W, padx=(0, 8), pady=(0, 2))
        col += 1
        ttk.Label(grp_rows_frame, text='Dil. Factor', font=('TkDefaultFont', 9, 'bold')).grid(
            row=0, column=col, sticky=tk.W, padx=(0, 8), pady=(0, 2))
        col += 1
        for lvl in all_qc_cols:
            ttk.Label(grp_rows_frame, text=lvl, font=('TkDefaultFont', 9, 'bold')).grid(
                row=0, column=col, sticky=tk.W, padx=(0, 8), pady=(0, 2))
            col += 1
        ttk.Label(grp_rows_frame, text='Expected Conc.', font=('TkDefaultFont', 9, 'bold')).grid(
            row=0, column=col, sticky=tk.W, padx=(0, 8), pady=(0, 2))

        for ri, gname in enumerate(sorted(found), 1):
            col = 0
            ttk.Label(grp_rows_frame, text=gname).grid(
                row=ri, column=col, sticky=tk.W, padx=(0, 8), pady=2)
            col += 1
            df_var = tk.StringVar(value=prev.get(gname, ''))
            group_df_vars[gname] = df_var
            ttk.Entry(grp_rows_frame, textvariable=df_var, width=9).grid(
                row=ri, column=col, sticky=tk.W, padx=(0, 8), pady=2)
            col += 1
            grp_qc_vars[gname] = {}
            for lvl in all_qc_cols:
                if lvl in grp_qc_levels[gname]:
                    qc_var = tk.StringVar(value=prev_qc.get(gname, {}).get(lvl, ''))
                    grp_qc_vars[gname][lvl] = qc_var
                    ttk.Entry(grp_rows_frame, textvariable=qc_var, width=8).grid(
                        row=ri, column=col, sticky=tk.W, padx=(0, 8), pady=2)
                else:
                    ttk.Label(grp_rows_frame, text='—', foreground='grey').grid(
                        row=ri, column=col, sticky=tk.W, padx=(0, 8), pady=2)
                col += 1
            exp_var = tk.StringVar(value=prev_exp.get(gname, ''))
            grp_exp_vars[gname] = exp_var
            ttk.Entry(grp_rows_frame, textvariable=exp_var, width=10).grid(
                row=ri, column=col, sticky=tk.W, padx=(0, 8), pady=2)

        grp_hint.config(text=f'Found {len(found)} group(s). Enter dilution factors and QC values per group (leave blank = 1× / no QC).')

    ttk.Button(grp_btn_frame, text='Detect Groups from Plate Map',
               command=_detect_groups).pack(side=tk.LEFT)
    ttk.Label(grp_btn_frame, text='  Priority: group factor > plate factor',
              foreground='grey').pack(side=tk.LEFT)

    # ── Previous Runs ──────────────────────────────────────────────────
    hist_lf = ttk.LabelFrame(outer, text='Previous Runs', padding='10 6')
    hist_lf.pack(fill=tk.X, pady=(0, 8))
    hist_lf.columnconfigure(0, weight=1)

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

    def refresh_history():
        history_lb.configure(state=tk.NORMAL)
        history_lb.delete(0, tk.END)
        _hdata = _load_run_history()
        if _hdata:
            for i, entry in enumerate(_hdata):
                history_lb.insert(tk.END, f'  {_run_label(entry)}')
                status = entry.get('status', '')
                if status == 'pass':
                    history_lb.itemconfig(i, foreground='#2a7a2a')
                elif status == 'fail':
                    history_lb.itemconfig(i, foreground='#c0392b')
        else:
            history_lb.insert(tk.END, '  (no previous runs yet)')
            history_lb.configure(state=tk.DISABLED)

    refresh_history()
    history_lb.bind('<Double-Button-1>', lambda _e: load_selected_run())

    hist_btn_row = ttk.Frame(hist_lf)
    hist_btn_row.grid(row=1, column=0, sticky=tk.EW, pady=(6, 0))
    ttk.Label(hist_btn_row, text='Double-click or select then click Load →',
              foreground='grey').pack(side=tk.LEFT)
    ttk.Button(hist_btn_row, text='Load Selected Run',
               command=load_selected_run).pack(side=tk.RIGHT)

    # ── Action buttons (placed in the fixed bottom bar) ───────────────
    ttk.Button(_btn_row, text='Cancel',
               command=root.destroy).pack(side=tk.RIGHT, padx=(6, 0))
    ttk.Button(_btn_row, text='▶  Run Analysis',
               command=run, default='active').pack(side=tk.RIGHT)
    # Notification button — left side of the bar
    ttk.Button(_btn_row, text='🔔  Get Update Notifications',
               command=_show_notify_dialog).pack(side=tk.LEFT)

    # Preload heavy deps in the background so the first Run Analysis is snappier.
    # This is fire-and-forget — _ensure_deps() guards against double-loading.
    threading.Thread(target=_ensure_deps, daemon=True).start()

    root.mainloop()


if __name__ == '__main__':
    multiprocessing.freeze_support()   # required for ProcessPoolExecutor in PyInstaller apps
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
    parser.add_argument('--no-infer-group-numbers', dest='infer_group_numbers', action='store_false', default=True,
                        help='Disable inferring Study Group Number from animal ID digits (enabled by default; '
                             'see the GUI option of the same name for the rule)')
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
        args.infer_group_numbers = last_args.get('infer_group_numbers', True)
        args.gui = False
        print(f"Rerunning: {_run_label(last_args)}")
        print(f"  MSD: {args.msd}")
        print(f"  Plate map: {args.platemap}")
        print(f"  Output: {args.output}")

    # No args or --gui → open GUI (default when double-clicked as .app)
    if args.gui or (not args.msd and not args.platemap and not args.rerun):
        run_interactive()
    elif args.msd and args.platemap:
        run_analysis(args.msd, args.platemap, args.output, args.spots, args.units, args.cv_threshold, args.dilution_factors, args.lloq_method, args.total_protein, infer_group_numbers=args.infer_group_numbers)
    else:
        print("Error: provide both --msd and --platemap, or use --gui for interactive mode.")
        parser.print_help()


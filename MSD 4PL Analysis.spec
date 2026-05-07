# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all

datas = []
binaries = []
hiddenimports = ['scipy.optimize', 'scipy.special', 'scipy.linalg', 'openpyxl', 'matplotlib.backends.backend_agg', 'plotly', 'plotly.graph_objects', 'plotly.offline', 'plotly.io', 'plotly.validators', 'plotly.basedatatypes', 'plotly.colors']
tmp_ret = collect_all('matplotlib')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]


a = Analysis(
    ['msd_4pl_analysis.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='MSD 4PL Analysis',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='MSD 4PL Analysis',
)
app = BUNDLE(
    coll,
    name='MSD 4PL Analysis.app',
    icon=None,
    bundle_identifier=None,
)

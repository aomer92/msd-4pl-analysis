#!/bin/bash
# Build standalone executable for MSD 4PL Analysis Tool

echo "Building MSD 4PL Analysis Tool using PyInstaller..."
echo ""

# Check if PyInstaller is installed
if ! python3 -m pip show pyinstaller > /dev/null 2>&1; then
    echo "Installing PyInstaller..."
    python3 -m pip install pyinstaller
fi

# Remove old build artifacts
if [ -d build ]; then
    rm -rf build
fi
if [ -d dist ]; then
    rm -rf dist
fi

# Build the executable
echo "Creating executable..."
# Only pass --icon if the file exists
ICON_ARG=""
if [ -f "icon.icns" ]; then
    ICON_ARG="--icon=icon.icns"
fi

# The HTML report ships the cartesian plotly build (scatter/bar/heatmap only,
# ~1.4 MB) rather than the full ~4.8 MB bundle inside the plotly package.
if [ ! -f "vendor/plotly-cartesian.min.js" ]; then
    echo "⚠ vendor/plotly-cartesian.min.js is missing — reports will fall back"
    echo "  to the full plotly bundle (~3.4 MB larger per study folder)."
fi

python3 -m PyInstaller \
    --onefile \
    --windowed \
    --name "MSD_4PL_Analysis" \
    $ICON_ARG \
    --collect-data openpyxl \
    --add-data "vendor/plotly-cartesian.min.js:vendor" \
    msd_4pl_analysis.py

echo ""
echo "✓ Build complete!"
echo ""
echo "The executable is located in: dist/MSD_4PL_Analysis"
echo ""
echo "To share this tool:"
echo "  1. Copy the entire 'dist' folder"
echo "  2. Share with others (they can double-click to run)"
echo ""

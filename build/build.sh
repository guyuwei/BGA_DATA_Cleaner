#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP_NAME="EHR数据清洗"
PYTHON="${PYTHON:-python3}"

STAGE="$ROOT/build/stage"
DIST="$ROOT/dist"
RELEASE_ROOT="$ROOT/dist_release"
RELEASE="$RELEASE_ROOT/$APP_NAME"

EXPECTED_DIRS=(
  "指标数据_基线数据_1家医院_分组1_低血糖预测模型-正常血糖1_2024-04-06 140118_10"
  "指标数据_基线数据_1家医院_分组1_低血糖预测模型-正常血糖2_2024-02-06 183001_21"
  "指标数据_基线数据_1家医院_分组1_低血糖预测模型-正常血糖1_2024-02-05 155459_39"
  "指标数据_基线数据_1家医院_分组1_低血糖风险预测模型-低血糖_2024-04-06 140331_83"
  "指标数据_基线数据_1家医院_分组1_低血糖风险预测模型-低血糖_2024-02-06 183111_28"
)

rm -rf "$STAGE" "$DIST" "$ROOT/build/pyinstaller" "$RELEASE_ROOT"
mkdir -p "$STAGE" "$RELEASE_ROOT"

# Prepare staging files (no raw data files, only empty dirs)
cp -R "$ROOT/app" "$STAGE/"
cp -R "$ROOT/docs" "$STAGE/"
for f in "$ROOT"/步骤*_*.py; do
  cp "$f" "$STAGE/"
done

mkdir -p "$STAGE/原始数据"
for d in "${EXPECTED_DIRS[@]}"; do
  mkdir -p "$STAGE/原始数据/$d"
done

# Ensure build dependencies
"$PYTHON" -m pip install -q --upgrade pip
"$PYTHON" -m pip install -q -r "$ROOT/build/requirements.txt" pyinstaller

# Build app
"$PYTHON" -m PyInstaller --noconfirm --onedir --windowed \
  --name "$APP_NAME" \
  --distpath "$DIST" \
  --workpath "$ROOT/build/pyinstaller" \
  --specpath "$ROOT/build/pyinstaller" \
  "$ROOT/app/main.py"

# Assemble release folder
mkdir -p "$RELEASE"
if [ -d "$DIST/$APP_NAME.app" ]; then
  cp -R "$DIST/$APP_NAME.app" "$RELEASE/"
else
  cp -R "$DIST/$APP_NAME" "$RELEASE/"
fi
cp -R "$STAGE/docs" "$RELEASE/docs"
cp -R "$STAGE/原始数据" "$RELEASE/原始数据"
for f in "$STAGE"/步骤*_*.py; do
  cp "$f" "$RELEASE/"
done

# Create DMG (macOS)
if [[ "$(uname)" == "Darwin" ]]; then
  DMG="$RELEASE_ROOT/${APP_NAME}.dmg"
  rm -f "$DMG"
  hdiutil create -volname "$APP_NAME" -srcfolder "$RELEASE" -ov -format UDZO "$DMG"
  echo "DMG created: $DMG"
fi

echo "Release folder: $RELEASE"
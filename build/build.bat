@echo off
setlocal enabledelayedexpansion

for %%I in ("%~dp0..") do set "ROOT=%%~fI"
set "APP_NAME=EHR数据清洗"
set "PYTHON=python"

set "STAGE=%ROOT%\build\stage"
set "DIST=%ROOT%\dist"
set "RELEASE_ROOT=%ROOT%\dist_release"
set "RELEASE=%RELEASE_ROOT%\%APP_NAME%"

set "DIR1=指标数据_基线数据_1家医院_分组1_低血糖预测模型-正常血糖1_2024-04-06 140118_10"
set "DIR2=指标数据_基线数据_1家医院_分组1_低血糖预测模型-正常血糖2_2024-02-06 183001_21"
set "DIR3=指标数据_基线数据_1家医院_分组1_低血糖预测模型-正常血糖1_2024-02-05 155459_39"
set "DIR4=指标数据_基线数据_1家医院_分组1_低血糖风险预测模型-低血糖_2024-04-06 140331_83"
set "DIR5=指标数据_基线数据_1家医院_分组1_低血糖风险预测模型-低血糖_2024-02-06 183111_28"

if exist "%STAGE%" rmdir /s /q "%STAGE%"
if exist "%DIST%" rmdir /s /q "%DIST%"
if exist "%ROOT%\build\pyinstaller" rmdir /s /q "%ROOT%\build\pyinstaller"
if exist "%RELEASE_ROOT%" rmdir /s /q "%RELEASE_ROOT%"
mkdir "%STAGE%"
mkdir "%RELEASE_ROOT%"

xcopy "%ROOT%\app" "%STAGE%\app" /E /I /Y >nul
xcopy "%ROOT%\docs" "%STAGE%\docs" /E /I /Y >nul
for %%F in ("%ROOT%\步骤*_*.py") do copy /Y "%%F" "%STAGE%" >nul

mkdir "%STAGE%\原始数据"
mkdir "%STAGE%\原始数据\%DIR1%"
mkdir "%STAGE%\原始数据\%DIR2%"
mkdir "%STAGE%\原始数据\%DIR3%"
mkdir "%STAGE%\原始数据\%DIR4%"
mkdir "%STAGE%\原始数据\%DIR5%"

%PYTHON% -m pip install -q --upgrade pip
%PYTHON% -m pip install -q -r "%ROOT%\build\requirements.txt" pyinstaller

%PYTHON% -m PyInstaller --noconfirm --onedir --windowed ^
  --name "%APP_NAME%" ^
  --distpath "%DIST%" ^
  --workpath "%ROOT%\build\pyinstaller" ^
  --specpath "%ROOT%\build\pyinstaller" ^
  "%ROOT%\app\main.py"

mkdir "%RELEASE%"
if exist "%DIST%\%APP_NAME%" xcopy "%DIST%\%APP_NAME%" "%RELEASE%\%APP_NAME%" /E /I /Y >nul
xcopy "%STAGE%\docs" "%RELEASE%\docs" /E /I /Y >nul
xcopy "%STAGE%\原始数据" "%RELEASE%\原始数据" /E /I /Y >nul
for %%F in ("%STAGE%\步骤*_*.py") do copy /Y "%%F" "%RELEASE%" >nul

echo Release folder: %RELEASE%
endlocal
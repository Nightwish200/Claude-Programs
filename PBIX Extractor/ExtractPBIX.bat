@echo off
setlocal enabledelayedexpansion
title PBIX Metadata Extractor

:: ============================================================
::  PBIX METADATA EXTRACTOR
::  Double-click to run, or drag a .pbix file onto this .bat
:: ============================================================

set SCRIPT=C:\Users\rmyers1\Work\PGP IT Team - General\Coding\Claude\PBIX Extractor\pbix_extractor.py
set OUTPUT_DIR=C:\Users\rmyers1\Work\PGP IT Team - General\Coding\Claude\PBIX Extractor

echo.
echo  ============================================================
echo   PBIX Metadata Extractor
echo  ============================================================
echo.

:: -- Sanity checks -------------------------------------------
if not exist "%SCRIPT%" (
    echo  ERROR: Cannot find pbix_extractor.py at:
    echo  %SCRIPT%
    echo.
    pause
    exit /b 1
)

python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found. Install from https://python.org
    echo  and tick "Add Python to PATH" during setup.
    echo.
    pause
    exit /b 1
)

:: -- Get the .pbix file --------------------------------------
if not "%~1"=="" (
    :: Drag-and-drop: file passed as argument
    set PBIX_FILE=%~1
    goto :run
)

:: Double-click: use a VBScript file picker
:: VBScript has no execution policy and works on all Windows versions
set VBS=%TEMP%\pbix_picker_%RANDOM%.vbs
set TMP_RESULT=%TEMP%\pbix_result_%RANDOM%.txt

(
    echo Dim dlg
    echo Set dlg = CreateObject("UserAccounts.CommonDialog"^)
    echo dlg.Filter = "Power BI Files (*.pbix)|*.pbix|All Files (*.*)|*.*"
    echo dlg.FilterIndex = 1
    echo dlg.InitialDir = Environ("USERPROFILE"^) ^& "\Documents"
    echo dlg.Flags = 4096
    echo If dlg.ShowOpen Then
    echo     Dim fso, f
    echo     Set fso = CreateObject("Scripting.FileSystemObject"^)
    echo     Set f = fso.CreateTextFile("%TMP_RESULT%", True^)
    echo     f.Write dlg.FileName
    echo     f.Close
    echo End If
) > "%VBS%"

echo  Select your .pbix file in the dialog box...
echo.
cscript //nologo "%VBS%"
del "%VBS%" >nul 2>&1

:: UserAccounts.CommonDialog is blocked on Windows 10+
:: Fall back to a simpler approach if result file wasn't created
if not exist "%TMP_RESULT%" goto :manual_entry

set /p PBIX_FILE=<"%TMP_RESULT%"
del "%TMP_RESULT%" >nul 2>&1

if "!PBIX_FILE!"=="" goto :manual_entry
goto :run

:manual_entry
:: Most reliable fallback: just ask the user to paste the path
echo  The file picker could not open (common on Windows 10/11).
echo.
echo  Please paste or type the full path to your .pbix file below.
echo  Tip: In File Explorer, hold Shift and right-click the file,
echo       then choose "Copy as path" - then paste it here.
echo.
set /p PBIX_FILE= Path to .pbix file: 

:: Strip surrounding quotes if the user pasted a quoted path
set PBIX_FILE=!PBIX_FILE:"=!

if "!PBIX_FILE!"=="" (
    echo.
    echo  No path entered. Exiting.
    echo.
    pause
    exit /b 0
)

:run
:: -- Validate ------------------------------------------------
if not exist "!PBIX_FILE!" (
    echo.
    echo  ERROR: File not found:
    echo  !PBIX_FILE!
    echo.
    pause
    exit /b 1
)

:: -- Build output paths --------------------------------------
for %%F in ("!PBIX_FILE!") do set BASE_NAME=%%~nF
set PDF_OUT=%OUTPUT_DIR%\!BASE_NAME!_metadata.pdf
set JSON_OUT=%OUTPUT_DIR%\!BASE_NAME!_metadata.json

:: -- Run -----------------------------------------------------
echo.
echo  Input  : !PBIX_FILE!
echo  PDF    : !PDF_OUT!
echo  JSON   : !JSON_OUT!
echo.
echo  Extracting -- please wait...
echo.

python "%SCRIPT%" "!PBIX_FILE!" "!PDF_OUT!"
set EXIT_CODE=!errorlevel!

echo.
if !EXIT_CODE! neq 0 (
    echo  ============================================================
    echo   ERROR: Extraction failed. See messages above.
    echo  ============================================================
    echo.
    pause
    exit /b 1
)

:: -- Done ----------------------------------------------------
echo  ============================================================
echo   Done! Both files saved to:
echo   %OUTPUT_DIR%
echo  ============================================================
echo.
echo    [1]  Open PDF report
echo    [2]  Open JSON file
echo    [3]  Open output folder in Explorer
echo    [4]  Exit
echo.
set /p CHOICE= Enter 1, 2, 3 or 4 then press Enter: 

if "!CHOICE!"=="1" start "" "!PDF_OUT!"
if "!CHOICE!"=="2" start "" "!JSON_OUT!"
if "!CHOICE!"=="3" explorer "%OUTPUT_DIR%"

echo.
endlocal
exit /b 0

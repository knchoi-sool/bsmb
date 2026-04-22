@echo off
title BSM Dashboard Server

:: Git ???
for /f %%a in ('powershell -NoProfile -Command "git rev-parse --short HEAD"') do set GIT_HASH=%%a
for /f %%a in ('powershell -NoProfile -Command "git rev-parse --abbrev-ref HEAD"') do set GIT_BRANCH=%%a

echo.
echo  =====================================================
echo   BSM Dashboard  ^|  DB Edition
echo  =====================================================
echo   Server  : http://61.33.23.171:5000
echo   Start   : %date% %time%
echo   Version : %GIT_HASH%  [%GIT_BRANCH%]
echo  =====================================================
echo.
echo  [1/3] Checking Flask...
pip show flask >nul 2>&1 || pip install flask -q
echo  [2/3] Checking Pandas...
pip show pandas >nul 2>&1 || pip install pandas -q
echo  [3/3] Checking PyODBC...
pip show pyodbc >nul 2>&1 || pip install pyodbc -q
echo.
echo  Starting server...
echo  =====================================================
echo.
python app.py
pause

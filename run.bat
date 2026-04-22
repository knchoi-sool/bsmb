@echo off
title BSM Dashboard Server

:: Git 정보
for /f %%i in ('git rev-parse --short HEAD 2^>nul') do set GIT_HASH=%%i
for /f %%i in ('git rev-parse --abbrev-ref HEAD 2^>nul') do set GIT_BRANCH=%%i

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

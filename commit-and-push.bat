@echo off
setlocal

set /p "msg=Commit message: "
if "%msg%"=="" (
  echo Commit message is required.
  exit /b 1
)

git add -A
if errorlevel 1 exit /b %errorlevel%

git commit -m "%msg%"
if errorlevel 1 exit /b %errorlevel%

git push origin main
if errorlevel 1 exit /b %errorlevel%

echo Done.
endlocal

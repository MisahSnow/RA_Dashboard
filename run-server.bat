@echo off
REM =========================================
REM RetroAchievements Friends Dashboard
REM Server launcher
REM =========================================

cd /d %~dp0

echo Starting server...
echo.

REM If node_modules is missing, install deps
if not exist node_modules (
  echo Installing dependencies...
  npm install
)

echo.
echo Running server...
npm start

pause

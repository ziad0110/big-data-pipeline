@echo off
title Hybrid ELT Data Pipeline - Web Dashboard
echo ========================================================
echo   Starting Hybrid ELT Interactive Web Dashboard...
echo ========================================================
python -m streamlit run dashboard/app.py
pause

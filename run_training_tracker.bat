@echo off
REM Ejecutar comandos de TrainingTracker, por ejemplo:
REM   run_training_tracker.bat validate
REM   run_training_tracker.bat build_outputs

python -m training_tracker.cli %*

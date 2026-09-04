# Bachelorarbeit SQL Validierung
# Zweck: Großen Testdatensatz erzeugen und in PostgreSQL importieren

Set-Location "$env:USERPROFILE\Documents\Bachelorarbeit_SQL_Validierung\05_Code (Code)\Python_Import (Python Import)"

# Virtuelle Python-Umgebung aktivieren
.\.venv\Scripts\Activate.ps1

# Großen Testdatensatz erzeugen
python generate_test_data.py

# Große CSV-Dateien in PostgreSQL importieren
python import_payment_csv_large.py
python import_bank_csv_large.py

Write-Host ""
Write-Host "Pipeline abgeschlossen."
Write-Host "Bitte danach in pgAdmin die Datei 03_check_large_dataset_results.sql ausführen."
Write-Host ""
# PowerShell script to create sample data files for submission
# Run this from the project root directory

Write-Host "Creating sample data files for submission..." -ForegroundColor Green

# Create directories if they don't exist
New-Item -ItemType Directory -Force -Path "data\raw" | Out-Null
New-Item -ItemType Directory -Force -Path "data\external" | Out-Null

# Create sample raw data files (first 100 rows + header)
if (Test-Path "data\raw\events.csv") {
    Get-Content "data\raw\events.csv" -TotalCount 101 | Set-Content "data\raw\events_sample.csv"
    Write-Host "Created data\raw\events_sample.csv" -ForegroundColor Cyan
} else {
    Write-Host "Warning: data\raw\events.csv not found" -ForegroundColor Yellow
}

if (Test-Path "data\raw\fight_results.csv") {
    Get-Content "data\raw\fight_results.csv" -TotalCount 101 | Set-Content "data\raw\fight_results_sample.csv"
    Write-Host "Created data\raw\fight_results_sample.csv" -ForegroundColor Cyan
} else {
    Write-Host "Warning: data\raw\fight_results.csv not found" -ForegroundColor Yellow
}

if (Test-Path "data\raw\fight_stats.csv") {
    Get-Content "data\raw\fight_stats.csv" -TotalCount 101 | Set-Content "data\raw\fight_stats_sample.csv"
    Write-Host "Created data\raw\fight_stats_sample.csv" -ForegroundColor Cyan
} else {
    Write-Host "Warning: data\raw\fight_stats.csv not found" -ForegroundColor Yellow
}

# Create sample external data files
if (Test-Path "data\external\attendance.csv") {
    Get-Content "data\external\attendance.csv" -TotalCount 51 | Set-Content "data\external\attendance_sample.csv"
    Write-Host "Created data\external\attendance_sample.csv" -ForegroundColor Cyan
} elseif (Test-Path "data\external\attendance_full.csv") {
    Get-Content "data\external\attendance_full.csv" -TotalCount 51 | Set-Content "data\external\attendance_sample.csv"
    Write-Host "Created data\external\attendance_sample.csv" -ForegroundColor Cyan
} else {
    Write-Host "Warning: attendance.csv not found" -ForegroundColor Yellow
}

if (Test-Path "data\external\betting_odds.csv") {
    Get-Content "data\external\betting_odds.csv" -TotalCount 101 | Set-Content "data\external\betting_odds_sample.csv"
    Write-Host "Created data\external\betting_odds_sample.csv" -ForegroundColor Cyan
} else {
    Write-Host "Warning: data\external\betting_odds.csv not found" -ForegroundColor Yellow
}

if (Test-Path "data\external\google_trends.csv") {
    Get-Content "data\external\google_trends.csv" -TotalCount 201 | Set-Content "data\external\google_trends_sample.csv"
    Write-Host "Created data\external\google_trends_sample.csv" -ForegroundColor Cyan
} else {
    Write-Host "Warning: data\external\google_trends.csv not found" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Sample files created! Check data\raw\ and data\external\ directories." -ForegroundColor Green
Write-Host "These sample files are ready for submission." -ForegroundColor Green


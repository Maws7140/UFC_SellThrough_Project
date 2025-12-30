#!/bin/bash
# Script to create sample data files for submission
# Run this from the project root directory

echo "Creating sample data files for submission..."

# Create directories if they don't exist
mkdir -p data/raw
mkdir -p data/external

# Create sample raw data files (first 100 rows + header)
if [ -f "data/raw/events.csv" ]; then
    head -n 101 data/raw/events.csv > data/raw/events_sample.csv
    echo "Created data/raw/events_sample.csv"
else
    echo "Warning: data/raw/events.csv not found"
fi

if [ -f "data/raw/fight_results.csv" ]; then
    head -n 101 data/raw/fight_results.csv > data/raw/fight_results_sample.csv
    echo "Created data/raw/fight_results_sample.csv"
else
    echo "Warning: data/raw/fight_results.csv not found"
fi

if [ -f "data/raw/fight_stats.csv" ]; then
    head -n 101 data/raw/fight_stats.csv > data/raw/fight_stats_sample.csv
    echo "Created data/raw/fight_stats_sample.csv"
else
    echo "Warning: data/raw/fight_stats.csv not found"
fi

# Create sample external data files
if [ -f "data/external/attendance.csv" ]; then
    head -n 51 data/external/attendance.csv > data/external/attendance_sample.csv
    echo "Created data/external/attendance_sample.csv"
elif [ -f "data/external/attendance_full.csv" ]; then
    head -n 51 data/external/attendance_full.csv > data/external/attendance_sample.csv
    echo "Created data/external/attendance_sample.csv"
else
    echo "Warning: attendance.csv not found"
fi

if [ -f "data/external/betting_odds.csv" ]; then
    head -n 101 data/external/betting_odds.csv > data/external/betting_odds_sample.csv
    echo "Created data/external/betting_odds_sample.csv"
else
    echo "Warning: data/external/betting_odds.csv not found"
fi

if [ -f "data/external/google_trends.csv" ]; then
    head -n 201 data/external/google_trends.csv > data/external/google_trends_sample.csv
    echo "Created data/external/google_trends_sample.csv"
else
    echo "Warning: data/external/google_trends.csv not found"
fi

echo ""
echo "Sample files created! Check data/raw/ and data/external/ directories."
echo "These sample files are ready for submission."


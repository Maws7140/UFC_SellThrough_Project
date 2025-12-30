#!/bin/bash
# Script to collect all results from Roar run
# Run this after your pipeline completes

echo "=========================================="
echo "UFC Project - Results Summary"
echo "=========================================="
echo ""

# Data sizes
echo "=== DATA SIZES ==="
echo "Raw data:"
du -sh data/raw/*.csv 2>/dev/null | head -5
echo ""
echo "Processed data:"
du -sh data/processed/* 2>/dev/null
echo ""
echo "Features:"
du -sh data/features/* 2>/dev/null
echo ""

# Row counts (if Spark is available)
if command -v spark-submit &> /dev/null; then
    echo "=== ROW COUNTS ==="
    spark-submit --master local[*] << 'PYTHON'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("results").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    events = spark.read.parquet("data/processed/events")
    print(f"Events: {events.count():,}")
except:
    print("Events: N/A")

try:
    fights = spark.read.parquet("data/processed/fights")
    print(f"Fights: {fights.count():,}")
except:
    print("Fights: N/A")

try:
    stats = spark.read.parquet("data/processed/fight_stats")
    print(f"Fight stats: {stats.count():,}")
except:
    print("Fight stats: N/A")

try:
    features = spark.read.parquet("data/features/event_features")
    print(f"Event features: {features.count():,}")
except:
    print("Event features: N/A")

spark.stop()
PYTHON
    echo ""
fi

# Model metrics
echo "=== MODEL METRICS ==="
if [ -f "data/models/metrics.json" ]; then
    cat data/models/metrics.json
else
    echo "Metrics file not found - model may not have trained yet"
fi
echo ""

# Attendance data
echo "=== ATTENDANCE DATA ==="
if [ -f "data/external/attendance_full.csv" ]; then
    echo "Total events with attendance: $(tail -n +2 data/external/attendance_full.csv | wc -l)"
elif [ -f "data/external/attendance.csv" ]; then
    echo "Total events with attendance: $(tail -n +2 data/external/attendance.csv | wc -l)"
fi
echo ""

echo "=========================================="
echo "Copy these results to PRESENTATION_SCRIPT.md"
echo "=========================================="


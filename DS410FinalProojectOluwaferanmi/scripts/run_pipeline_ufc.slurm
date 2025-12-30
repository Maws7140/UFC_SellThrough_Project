#!/bin/bash
#SBATCH --job-name=ufc-pipe-improved
#SBATCH --account=open
#SBATCH --partition=open
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --mem=64G
#SBATCH --time=06:00:00
#SBATCH --output=logs/ufc_improved_%j.out
#SBATCH --error=logs/ufc_improved_%j.err

# UFC Sell-Through Prediction Pipeline - Improved version

echo "======================================"
echo "UFC Pipeline (IMPROVED) - Job $SLURM_JOB_ID"
echo "Date: $(date)"
echo "======================================"

# Load Anaconda and activate environment
module purge
module load anaconda3
source activate ds410

export JAVA_HOME="$CONDA_PREFIX"
export PATH="$JAVA_HOME/bin:$PATH"

echo "CONDA_PREFIX: $CONDA_PREFIX"
echo "JAVA_HOME: $JAVA_HOME"

if java -version 2>&1; then
    echo "Java found successfully!"
    # Check Java version meets minimum requirement
    JAVA_VER=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$JAVA_VER" -lt 17 ] 2>/dev/null; then
        echo "WARNING: Java $JAVA_VER detected. PySpark 3.4+ requires Java 17+."
    fi
else
    echo "ERROR: Java not found in conda environment!"
    exit 1
fi

# Quick PySpark check
if python -c "import pyspark" 2>/dev/null; then
    echo "PySpark found successfully!"
else
    echo "ERROR: PySpark not installed!"
    exit 1
fi

cd "${SLURM_SUBMIT_DIR}"
mkdir -p logs data/{raw,processed,features,external,models}

echo "Working directory: $(pwd)"
echo "Python: $(which python)"

# Step 1: Data ingestion
echo ""
echo "Step 1: Data Ingestion"
python src/etl/ingest.py --data-dir ./data

# Step 2: External data collection
echo ""
echo "Step 2: External Data Collection"

if [ ! -f "./data/external/betting_odds.csv" ]; then
    python src/etl/scrape_betting_odds.py --data-dir ./data --output-dir ./data
fi

if [ ! -f "./data/external/google_trends.csv" ]; then
    python src/etl/fetch_google_trends.py --data-dir ./data --output-dir ./data --max-fighters 500
fi

if [ ! -f "./data/external/reddit_comments.csv" ]; then
    python src/etl/scrape_reddit_sentiment.py --data-dir ./data --output-dir ./data --max-events 200
fi

if [ ! -f "./data/external/attendance.csv" ]; then
    echo "Scraping attendance data from Wikipedia..."
    python src/etl/scrape_attendance.py --output ./data/external/attendance.csv
fi

# Step 3: Spark ETL
echo ""
echo "Step 3: Spark ETL"
python src/etl/spark_etl.py --data-dir ./data --output-dir ./data

# Step 4: Graph analysis
echo ""
echo "Step 4: Graph Analysis"
spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 \
    src/graph/fighter_network.py --data-dir ./data --output-dir ./data

# Step 5: Feature engineering
echo ""
echo "Step 5: Feature Engineering"
python src/features/feature_engineering.py --data-dir ./data --output-dir ./data

# Step 6: Model training
echo ""
echo "Step 6: IMPROVED Model Training"
python src/models/train_improved.py --data-dir ./data --output-dir ./data --test-year 2024

# Step 7: Visualizations
echo ""
echo "Step 7: Creating Visualizations"
python src/visualization/create_plots.py --data-dir ./data --output-dir ./visualizations

# Step 8: Card optimizer demo
echo ""
echo "Step 8: Card Optimizer Demo"
python src/optimizer/card_optimizer.py

# Step 9: Multi-year validation
echo ""
echo "Step 8: Multi-Year Validation (Testing model stability)"
echo "Testing on years 2022, 2023, 2024..."

# Save validation results to file
RESULTS_FILE="logs/multi_year_results_${SLURM_JOB_ID}.txt"
echo "Multi-Year Validation Results" > $RESULTS_FILE
echo "======================================" >> $RESULTS_FILE
echo "" >> $RESULTS_FILE

for year in 2022 2023 2024; do
    echo ""
    echo "Testing on year: $year"
    python src/models/train_improved.py \
        --data-dir ./data \
        --output-dir ./data \
        --test-year $year \
        --no-cv

    echo "" >> $RESULTS_FILE
    echo "Year $year:" >> $RESULTS_FILE
    grep -E '"rmse"|"mae"|"r2"|"test_size"' ./data/models/metrics_improved.json >> $RESULTS_FILE
done

echo "" >> $RESULTS_FILE
echo "======================================" >> $RESULTS_FILE

# Summary
echo ""
echo "======================================"
echo "Pipeline Complete! $(date)"
echo "======================================"

echo ""
echo "Data sizes:"
du -sh ./data/raw ./data/external ./data/processed ./data/features ./data/models 2>/dev/null

echo ""
echo "Model metrics (Improved):"
cat ./data/models/metrics_improved.json 2>/dev/null || echo "Metrics not found"

echo ""
echo "Multi-year validation results saved to:"
echo "  $RESULTS_FILE"

echo ""
echo "Compare with baseline:"
echo "  Baseline (9 features):  R²=0.26, RMSE=0.38"
echo "  Improved (32 features): R²=$(grep '"r2"' ./data/models/metrics_improved.json | grep -oP '\d+\.\d+' | head -1)"

echo ""
echo "======================================"
echo "All Done!"
echo "======================================"

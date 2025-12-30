#!/bin/bash
#SBATCH --job-name=ufc-pipe
#SBATCH --account=open
#SBATCH --partition=open
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --mem=64G
#SBATCH --time=06:00:00
#SBATCH --output=logs/ufc_%j.out
#SBATCH --error=logs/ufc_%j.err

# =============================================================================
# UFC Sell-Through Prediction Pipeline
# DS/CMPSC 410 - Penn State
# =============================================================================

echo "======================================"
echo "UFC Pipeline - Job $SLURM_JOB_ID"
echo "Date: $(date)"
echo "======================================"

# Setup - load anaconda and activate environment
module purge
module load anaconda3
source activate ds410

# Set JAVA_HOME from conda environment (openjdk installed via conda)
export JAVA_HOME="$CONDA_PREFIX"
export PATH="$JAVA_HOME/bin:$PATH"

echo "CONDA_PREFIX: $CONDA_PREFIX"
echo "JAVA_HOME: $JAVA_HOME"

# Verify Java is working (PySpark 3.4+ requires Java 17+)
if java -version 2>&1; then
    echo "Java found successfully!"
    # Check Java version meets minimum requirement
    JAVA_VER=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$JAVA_VER" -lt 17 ] 2>/dev/null; then
        echo "WARNING: Java $JAVA_VER detected. PySpark 3.4+ requires Java 17+."
        echo "Please run: conda install -c conda-forge openjdk=17 -y"
    fi
else
    echo "ERROR: Java not found in conda environment!"
    echo "Please run: conda install -c conda-forge openjdk=17 pyspark -y"
    exit 1
fi

# Verify PySpark is installed
if python -c "import pyspark" 2>/dev/null; then
    echo "PySpark found successfully!"
else
    echo "ERROR: PySpark not installed!"
    echo "Please run: conda install -c conda-forge pyspark -y"
    exit 1
fi

# Navigate to project - use SLURM_SUBMIT_DIR
cd "${SLURM_SUBMIT_DIR}"
mkdir -p logs data/{raw,processed,features,external,models}

echo "Working directory: $(pwd)"
echo "Python: $(which python)"

# =============================================================================
# Step 1: Data Ingestion
# =============================================================================
echo ""
echo "Step 1: Data Ingestion"
python src/etl/ingest.py --data-dir ./data

# =============================================================================
# Step 2: External Data (skip if exists)
# =============================================================================
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

# =============================================================================
# Step 3: Spark ETL
# =============================================================================
echo ""
echo "Step 3: Spark ETL"
python src/etl/spark_etl.py --data-dir ./data --output-dir ./data

# =============================================================================
# Step 4: Graph Analysis (ENABLED for full 36 features)
# =============================================================================
echo ""
echo "Step 4: Graph Analysis"
echo "  Generating graph features for PageRank and community analysis"
spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 \
    src/graph/fighter_network.py --data-dir ./data --output-dir ./data

# =============================================================================
# Step 5: Feature Engineering
# =============================================================================
echo ""
echo "Step 5: Feature Engineering"
python src/features/feature_engineering.py --data-dir ./data --output-dir ./data

# =============================================================================
# Step 6: Model Training
# =============================================================================
echo ""
echo "Step 6: Model Training"
python src/models/train.py --data-dir ./data --output-dir ./data --test-year 2024

# =============================================================================
# Step 7: Card Optimizer
# =============================================================================
echo ""
echo "Step 7: Card Optimizer Demo"
python src/optimizer/card_optimizer.py

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "======================================"
echo "Pipeline Complete! $(date)"
echo "======================================"

echo ""
echo "Data sizes:"
du -sh ./data/raw ./data/external ./data/processed ./data/features ./data/models 2>/dev/null

echo ""
echo "Model metrics:"
cat ./data/models/metrics.json 2>/dev/null || echo "Metrics not found"

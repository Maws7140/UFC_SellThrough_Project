#!/bin/bash
#SBATCH --job-name=ufc-viz
#SBATCH --account=open
#SBATCH --partition=open
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=16G
#SBATCH --time=01:00:00
#SBATCH --output=logs/visualization_%j.out
#SBATCH --error=logs/visualization_%j.err

# =============================================================================
# UFC Visualization Script
# Creates all plots for presentation
# DS/CMPSC 410 - Penn State
# =============================================================================

echo "======================================"
echo "UFC Visualization - Job $SLURM_JOB_ID"
echo "Date: $(date)"
echo "======================================"

# Setup - load anaconda and activate environment
module purge
module load anaconda3
source activate ds410

# Navigate to project
cd "${SLURM_SUBMIT_DIR}"
mkdir -p logs visualizations

echo "Working directory: $(pwd)"
echo "Python: $(which python)"

# =============================================================================
# Create Visualizations
# =============================================================================
echo ""
echo "Creating visualizations..."
python src/visualization/create_plots.py --data-dir ./data --output-dir ./visualizations

echo ""
echo "======================================"
echo "Visualization complete!"
echo "Check ./visualizations/ for output PNG files"
echo "======================================"

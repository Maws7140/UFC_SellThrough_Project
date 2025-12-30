import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import argparse
import glob

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)


def load_event_features(data_dir):
    # Load event features parquet file using pandas/pyarrow
    features_path = os.path.join(data_dir, "features", "event_features")
    
    # Check if it's a directory (parquet files) or a single file
    if os.path.isdir(features_path):
        # Find all parquet files in the directory
        parquet_files = glob.glob(os.path.join(features_path, "*.parquet"))
        if not parquet_files:
            print(f"Warning: No parquet files found at {features_path}")
            return None
        
        # Read all parquet files and concatenate
        dfs = []
        for f in parquet_files:
            try:
                df = pd.read_parquet(f)
                dfs.append(df)
            except Exception as e:
                print(f"Warning: Could not read {f}: {e}")
        
        if not dfs:
            print(f"Warning: Could not read any parquet files from {features_path}")
            return None
        
        df = pd.concat(dfs, ignore_index=True)
        return df
    elif os.path.isfile(features_path):
        try:
            df = pd.read_parquet(features_path)
            return df
        except Exception as e:
            print(f"Warning: Could not read {features_path}: {e}")
            return None
    else:
        print(f"Warning: Features not found at {features_path}")
        return None


def load_attendance_data(data_dir):
    # Load attendance CSV
    attendance_path = os.path.join(data_dir, "external", "attendance.csv")
    
    if not os.path.exists(attendance_path):
        attendance_path = os.path.join(data_dir, "external", "attendance_full.csv")
    
    if not os.path.exists(attendance_path):
        print(f"Warning: Attendance data not found")
        return None
    
    df = pd.read_csv(attendance_path)
    return df


def load_metrics(data_dir):
    # Load model metrics JSON
    metrics_path = os.path.join(data_dir, "models", "metrics_improved.json")
    
    if not os.path.exists(metrics_path):
        print(f"Warning: Metrics not found at {metrics_path}")
        return None
    
    with open(metrics_path, 'r') as f:
        metrics = json.load(f)
    
    return metrics


def plot_sellthrough_distribution(df, output_dir):
    # Histogram of sell-through rates
    if df is None or 'sell_through' not in df.columns:
        print("  Skipping: No sell-through data")
        return
    
    sell_through = df['sell_through'].dropna()
    
    plt.figure(figsize=(10, 6))
    plt.hist(sell_through, bins=30, edgecolor='black', alpha=0.7)
    plt.xlabel('Sell-Through Rate', fontsize=12)
    plt.ylabel('Number of Events', fontsize=12)
    plt.title('Distribution of UFC Event Sell-Through Rates', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    
    mean_val = sell_through.mean()
    median_val = sell_through.median()
    plt.axvline(mean_val, color='red', linestyle='--', label=f'Mean: {mean_val:.2f}')
    plt.axvline(median_val, color='green', linestyle='--', label=f'Median: {median_val:.2f}')
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '1_sellthrough_distribution.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print("  Created: sellthrough_distribution.png")


def plot_sellthrough_over_time(df, output_dir):
    # Line chart showing sell-through trends over time
    if df is None or 'sell_through' not in df.columns or 'event_date' not in df.columns:
        print("  Skipping: Missing date or sell-through data")
        return
    
    df_clean = df[['event_date', 'sell_through']].dropna()
    
    if len(df_clean) == 0:
        print("  Skipping: No data after cleaning")
        return
    
    df_clean['event_date'] = pd.to_datetime(df_clean['event_date'])
    df_clean = df_clean.sort_values('event_date')
    df_clean['year'] = df_clean['event_date'].dt.year
    
    yearly_avg = df_clean.groupby('year')['sell_through'].mean()
    
    plt.figure(figsize=(12, 6))
    plt.scatter(df_clean['event_date'], df_clean['sell_through'], alpha=0.5, s=30, label='Individual Events')
    plt.plot(yearly_avg.index, yearly_avg.values, color='red', linewidth=2, marker='o', label='Yearly Average')
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Sell-Through Rate', fontsize=12)
    plt.title('UFC Event Sell-Through Rates Over Time', fontsize=14, fontweight='bold')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '2_sellthrough_over_time.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print("  Created: sellthrough_over_time.png")


def plot_sellthrough_by_event_type(df, output_dir):
    # Boxplot comparing sell-through by event type
    if df is None or 'sell_through' not in df.columns or 'event_type' not in df.columns:
        print("  Skipping: Missing event_type or sell-through data")
        return
    
    df_clean = df[['event_type', 'sell_through']].dropna()
    
    if len(df_clean) == 0:
        print("  Skipping: No data after cleaning")
        return
    
    plt.figure(figsize=(10, 6))
    df_clean.boxplot(column='sell_through', by='event_type', ax=plt.gca())
    plt.xlabel('Event Type', fontsize=12)
    plt.ylabel('Sell-Through Rate', fontsize=12)
    plt.title('Sell-Through Rates by Event Type', fontsize=14, fontweight='bold')
    plt.suptitle('')
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '3_sellthrough_by_event_type.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print("  Created: sellthrough_by_event_type.png")


def plot_sellthrough_by_location(df, output_dir):
    # Boxplot comparing Vegas vs other locations
    if df is None or 'sell_through' not in df.columns or 'is_vegas' not in df.columns:
        print("  Skipping: Missing location or sell-through data")
        return
    
    df_clean = df[['is_vegas', 'sell_through']].dropna()
    
    if len(df_clean) == 0:
        print("  Skipping: No data after cleaning")
        return
    
    df_clean['location'] = df_clean['is_vegas'].map({1: 'Las Vegas', 0: 'Other Locations'})
    
    plt.figure(figsize=(8, 6))
    df_clean.boxplot(column='sell_through', by='location', ax=plt.gca())
    plt.xlabel('Location', fontsize=12)
    plt.ylabel('Sell-Through Rate', fontsize=12)
    plt.title('Sell-Through Rates: Las Vegas vs Other Locations', fontsize=14, fontweight='bold')
    plt.suptitle('')
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '4_sellthrough_by_location.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print("  Created: sellthrough_by_location.png")


def plot_feature_importance(metrics, output_dir):
    # Horizontal bar chart of top feature importance
    if metrics is None or 'feature_importance' not in metrics:
        print("  Skipping: No feature importance data")
        return
    
    importance_data = metrics['feature_importance']
    
    if not importance_data:
        print("  Skipping: Empty feature importance")
        return
    
    # Convert to list of tuples if needed
    if isinstance(importance_data[0], dict):
        features = [(item['feature'], item['importance']) for item in importance_data]
    else:
        features = importance_data
    
    features.sort(key=lambda x: x[1], reverse=True)
    top_features = features[:9]
    
    feature_names = [f[0] for f in top_features]
    importances = [f[1] for f in top_features]
    
    plt.figure(figsize=(10, 8))
    plt.barh(range(len(feature_names)), importances)
    plt.yticks(range(len(feature_names)), feature_names)
    plt.xlabel('Feature Importance', fontsize=12)
    plt.title('Most Important Features for Sell-Through Prediction', fontsize=14, fontweight='bold')
    plt.gca().invert_yaxis()
    plt.grid(True, alpha=0.3, axis='x')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '5_feature_importance.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print("  Created: feature_importance.png")


def plot_actual_vs_predicted(df, data_dir, output_dir):
    # Scatter plot of actual vs predicted sell-through
    # Try to load predictions if available
    predictions_path = os.path.join(data_dir, 'models', 'predictions.csv')
    
    if os.path.exists(predictions_path):
        pred_df = pd.read_csv(predictions_path)
        if 'prediction' in pred_df.columns and 'sell_through' in pred_df.columns:
            actual = pred_df['sell_through']
            predicted = pred_df['prediction']
        else:
            print("  Skipping: Predictions file missing required columns")
            return
    else:
        print("  Skipping: No predictions file found")
        print("    (Note: Run model training first to generate predictions)")
        return
    
    plt.figure(figsize=(10, 8))
    plt.scatter(actual, predicted, alpha=0.6, s=50)
    
    # Add diagonal line
    min_val = min(actual.min(), predicted.min())
    max_val = max(actual.max(), predicted.max())
    plt.plot([min_val, max_val], [min_val, max_val], 'r--', linewidth=2, label='Perfect Prediction')
    
    plt.xlabel('Actual Sell-Through Rate', fontsize=12)
    plt.ylabel('Predicted Sell-Through Rate', fontsize=12)
    plt.title('Actual vs Predicted Sell-Through Rates', fontsize=14, fontweight='bold')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '6_actual_vs_predicted.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print("  Created: actual_vs_predicted.png")


def plot_model_comparison(data_dir, output_dir):
    # Compare baseline vs improved model performance
    baseline_path = os.path.join(data_dir, "models", "metrics.json")
    improved_path = os.path.join(data_dir, "models", "metrics_improved.json")
    
    baseline_metrics = None
    improved_metrics = None
    
    if os.path.exists(baseline_path):
        with open(baseline_path, 'r') as f:
            baseline_metrics = json.load(f)
    
    if os.path.exists(improved_path):
        with open(improved_path, 'r') as f:
            improved_metrics = json.load(f)
    
    if baseline_metrics is None and improved_metrics is None:
        print("  Skipping: No metrics files found")
        return
    
    metrics_to_plot = []
    labels = []
    
    if baseline_metrics:
        metrics_to_plot.append(baseline_metrics.get('r2', 0))
        labels.append('Baseline (9 features)')
    
    if improved_metrics:
        metrics_to_plot.append(improved_metrics.get('r2', 0))
        labels.append('Improved (36 features)')
    
    if not metrics_to_plot:
        print("  Skipping: No R² values found")
        return
    
    plt.figure(figsize=(8, 6))
    bars = plt.bar(labels, metrics_to_plot, color=['#3498db', '#2ecc71'])
    
    for i, (bar, val) in enumerate(zip(bars, metrics_to_plot)):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                f'{val:.3f}', ha='center', va='bottom', fontsize=12, fontweight='bold')
    
    plt.ylabel('R² Score', fontsize=12)
    plt.title('Model Performance Comparison', fontsize=14, fontweight='bold')
    plt.ylim(0, max(metrics_to_plot) * 1.2)
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '7_model_comparison.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print("  Created: model_comparison.png")


def plot_title_fight_impact(df, output_dir):
    # Boxplot showing impact of title fights
    if df is None or 'sell_through' not in df.columns or 'has_title' not in df.columns:
        print("  Skipping: Missing has_title or sell-through data")
        return
    
    df_clean = df[['has_title', 'sell_through']].dropna()
    
    if len(df_clean) == 0:
        print("  Skipping: No data after cleaning")
        return
    
    df_clean['title_status'] = df_clean['has_title'].map({1: 'Has Title Fight', 0: 'No Title Fight'})
    
    plt.figure(figsize=(8, 6))
    df_clean.boxplot(column='sell_through', by='title_status', ax=plt.gca())
    plt.xlabel('Event Type', fontsize=12)
    plt.ylabel('Sell-Through Rate', fontsize=12)
    plt.title('Impact of Title Fights on Sell-Through Rates', fontsize=14, fontweight='bold')
    plt.suptitle('')
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '8_title_fight_impact.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print("  Created: title_fight_impact.png")


def plot_residuals(df, data_dir, output_dir):
    # Residuals plot
    predictions_path = os.path.join(data_dir, 'models', 'predictions.csv')
    
    if not os.path.exists(predictions_path):
        print("  Skipping: No predictions file found")
        return
    
    pred_df = pd.read_csv(predictions_path)
    
    if 'prediction' not in pred_df.columns or 'sell_through' not in pred_df.columns:
        print("  Skipping: Predictions file missing required columns")
        return
    
    predicted = pred_df['prediction']
    actual = pred_df['sell_through']
    residuals = actual - predicted
    
    plt.figure(figsize=(10, 6))
    plt.scatter(predicted, residuals, alpha=0.6, s=50)
    plt.axhline(y=0, color='r', linestyle='--', linewidth=2)
    plt.xlabel('Predicted Sell-Through Rate', fontsize=12)
    plt.ylabel('Residuals (Actual - Predicted)', fontsize=12)
    plt.title('Residuals Plot', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '9_residuals.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print("  Created: residuals.png")


def plot_correlation_heatmap(df, output_dir, top_n=15):
    # Correlation heatmap of top features
    if df is None:
        print("  Skipping: No data available")
        return
    
    # Select numeric columns only
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    
    if 'sell_through' not in numeric_cols:
        print("  Skipping: sell_through not in numeric columns")
        return
    
    # Get top correlated features with sell_through
    correlations = df[numeric_cols].corr()['sell_through'].abs().sort_values(ascending=False)
    top_features = correlations.head(top_n + 1).index.tolist()
    
    if 'sell_through' in top_features:
        top_features.remove('sell_through')
    
    top_features = ['sell_through'] + top_features[:top_n]
    
    corr_matrix = df[top_features].corr()
    
    plt.figure(figsize=(12, 10))
    sns.heatmap(corr_matrix, annot=True, fmt='.2f', cmap='coolwarm', center=0,
                square=True, linewidths=0.5, cbar_kws={"shrink": 0.8})
    plt.title(f'Correlation Heatmap: Top {top_n} Features with Sell-Through', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '10_correlation_heatmap.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print("  Created: correlation_heatmap.png")


def create_all_plots(data_dir, output_dir):
    # Main function to create all visualizations
    print("Creating visualizations...")
    print("=" * 60)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Load data
    print("\nLoading data...")
    event_features = load_event_features(data_dir)
    attendance = load_attendance_data(data_dir)
    metrics = load_metrics(data_dir)
    
    # Merge event features with attendance if needed
    if event_features is not None and attendance is not None:
        if 'sell_through' not in event_features.columns:
            # Try to merge
            if 'event_name' in event_features.columns and 'event_name' in attendance.columns:
                event_features = event_features.merge(
                    attendance[['event_name', 'sell_through']],
                    on='event_name',
                    how='left'
                )
    
    # Use event_features as main dataframe if available
    main_df = event_features if event_features is not None else attendance
    
    print("\nCreating Phase 1: Essential Visualizations...")
    plot_sellthrough_distribution(main_df, output_dir)
    plot_feature_importance(metrics, output_dir)
    plot_actual_vs_predicted(main_df, data_dir, output_dir)
    plot_sellthrough_over_time(main_df, output_dir)
    
    print("\nCreating Phase 2: Important Visualizations...")
    plot_sellthrough_by_event_type(main_df, output_dir)
    plot_model_comparison(data_dir, output_dir)
    plot_title_fight_impact(main_df, output_dir)
    
    print("\nCreating Phase 3: Additional Visualizations...")
    plot_sellthrough_by_location(main_df, output_dir)
    plot_residuals(main_df, data_dir, output_dir)
    plot_correlation_heatmap(main_df, output_dir)
    
    print("\n" + "=" * 60)
    print("Visualization complete!")
    print(f"Plots saved to: {output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create visualizations for UFC sell-through project")
    parser.add_argument("--data-dir", default="./data", help="Data directory")
    parser.add_argument("--output-dir", default="./visualizations", help="Output directory for plots")
    
    args = parser.parse_args()
    
    create_all_plots(args.data_dir, args.output_dir)

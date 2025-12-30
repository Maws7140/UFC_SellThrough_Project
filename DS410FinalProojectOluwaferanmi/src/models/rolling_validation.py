#!/usr/bin/env python3

import subprocess
import json
import sys

def run_test(train_start, train_end, test_year):
    print(f"\n{'='*60}")
    print(f"Training: {train_start}-{train_end} | Testing: {test_year}")
    print('='*60)

    cmd = [
        "python", "src/models/train_improved.py",
        "--data-dir", "./data",
        "--output-dir", "./data",
        "--test-year", str(test_year),
        "--no-cv"
    ]

    try:
        subprocess.run(cmd, capture_output=True, text=True)

        with open("./data/models/metrics_improved.json", "r") as f:
            metrics = json.load(f)

        return {
            "test_year": test_year,
            "rmse": metrics["rmse"],
            "mae": metrics["mae"],
            "r2": metrics["r2"],
            "test_size": metrics["test_size"]
        }
    except Exception as e:
        print(f"Error: {e}")
        return None


def main():
    print("="*60)
    print("ROLLING WINDOW VALIDATION")
    print("="*60)

    # Test on years 2020-2024
    test_years = [2020, 2021, 2022, 2023, 2024]

    results = []
    for year in test_years:
        metrics = run_test(None, None, year)
        if metrics:
            results.append(metrics)
            print(f"\n  Year {year} Results:")
            print(f"    R²: {metrics['r2']:.4f}")
            print(f"    RMSE: {metrics['rmse']:.4f}")
            print(f"    Test Size: {metrics['test_size']}")

    print("\n" + "="*60)
    print("SUMMARY - Model Stability Across Years")
    print("="*60)
    print(f"{'Year':<10} {'R²':<10} {'RMSE':<10} {'MAE':<10} {'Test Size':<10}")
    print("-"*60)

    for r in results:
        print(f"{r['test_year']:<10} {r['r2']:<10.4f} {r['rmse']:<10.4f} {r['mae']:<10.4f} {r['test_size']:<10}")

    r2_values = [r['r2'] for r in results]
    avg_r2 = sum(r2_values) / len(r2_values)
    std_r2 = (sum((x - avg_r2)**2 for x in r2_values) / len(r2_values))**0.5

    print("-"*60)
    print(f"{'Average':<10} {avg_r2:<10.4f}")
    print(f"{'Std Dev':<10} {std_r2:<10.4f}")
    print("="*60)

    print("\nInterpretation:")
    if std_r2 < 0.1:
        print(f"  ✅ Model is STABLE (std={std_r2:.3f} < 0.1)")
    elif std_r2 < 0.2:
        print(f"  ⚠️  Model has MODERATE variance (std={std_r2:.3f})")
    else:
        print(f"  ❌ Model is UNSTABLE (std={std_r2:.3f} > 0.2)")
        print("      Consider: More features, larger training set, or different model")


if __name__ == "__main__":
    main()

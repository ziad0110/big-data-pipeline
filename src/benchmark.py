# -*- coding: utf-8 -*-
"""
benchmark.py — وحدة المقارنة المعيارية المباشرة بين محركي Python و Apache Spark
Side-by-Side Performance Benchmark Module
مقرر البيانات الضخمة (العملي) | جامعة الرازي | إشراف: م. عمر أبو سند
"""

import os
import sys
import time
import json
import uuid
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.metrics import get_memory_mb, PipelineMetrics
from src import batch_loader
from src import spark_loader


def run_benchmark(file_path: str, run_python: bool = True, run_spark: bool = True) -> dict:
    """
    تشغيل نفس ملف البيانات على كلا المحركين ومقارنة السرعة والذاكرة بدقة.
    """
    input_file = Path(file_path).resolve()
    if not input_file.exists():
        raise FileNotFoundError(f"الملف غير موجود: {input_file}")

    file_size_mb = os.path.getsize(str(input_file)) / (1024 * 1024)
    results = {
        "file_name": input_file.name,
        "file_size_mb": round(file_size_mb, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "python_batch": None,
        "pyspark": None,
    }

    # 1. اختبار محرك Python Batch
    if run_python:
        print("\n[Benchmark] Running Python Batch Engine...")
        p_run_id = f"bench-py-{str(uuid.uuid4())[:4]}"
        p_metrics = PipelineMetrics(run_id=p_run_id, file_source=str(input_file), engine_used="python_batch")
        
        p_start_mem = get_memory_mb()
        p_start_time = time.perf_counter()
        
        batch_loader.load(str(input_file), run_id=p_run_id, metrics=p_metrics)
        
        p_elapsed = time.perf_counter() - p_start_time
        p_end_mem = get_memory_mb()
        p_throughput = p_metrics.raw_count / p_elapsed if p_elapsed > 0 else 0.0

        results["python_batch"] = {
            "engine_name": "Python Batch Loader",
            "run_id": p_run_id,
            "total_records": p_metrics.raw_count,
            "valid_records": p_metrics.valid_count,
            "corrected_records": p_metrics.corrected_count,
            "quarantined_records": p_metrics.quarantine_count,
            "elapsed_seconds": round(p_elapsed, 2),
            "throughput_rec_per_sec": round(p_throughput, 2),
            "memory_start_mb": round(p_start_mem, 2),
            "memory_end_mb": round(p_end_mem, 2),
            "memory_delta_mb": round(p_end_mem - p_start_mem, 2),
            "architecture": "Single-Threaded Streaming Generator (Chunked 5,000)"
        }

    # 2. اختبار محرك Apache PySpark
    if run_spark:
        print("\n[Benchmark] Running Apache PySpark Engine...")
        s_run_id = f"bench-spk-{str(uuid.uuid4())[:4]}"
        s_metrics = PipelineMetrics(run_id=s_run_id, file_source=str(input_file), engine_used="pyspark")
        
        s_start_mem = get_memory_mb()
        s_start_time = time.perf_counter()
        
        spark_loader.load(str(input_file), run_id=s_run_id, metrics=s_metrics)
        
        s_elapsed = time.perf_counter() - s_start_time
        s_end_mem = get_memory_mb()
        s_throughput = s_metrics.raw_count / s_elapsed if s_elapsed > 0 else 0.0

        results["pyspark"] = {
            "engine_name": "Apache PySpark Distributed Loader",
            "run_id": s_run_id,
            "total_records": s_metrics.raw_count,
            "valid_records": s_metrics.valid_count,
            "corrected_records": s_metrics.corrected_count,
            "quarantined_records": s_metrics.quarantine_count,
            "elapsed_seconds": round(s_elapsed, 2),
            "throughput_rec_per_sec": round(s_throughput, 2),
            "memory_start_mb": round(s_start_mem, 2),
            "memory_end_mb": round(s_end_mem, 2),
            "memory_delta_mb": round(s_end_mem - s_start_mem, 2),
            "architecture": "Distributed In-Memory Partitioning (16 Partitions parallel)"
        }

    # حفظ التقرير المقارن
    rep_dir = PROJECT_ROOT / "reports"
    rep_dir.mkdir(parents=True, exist_ok=True)
    out_json = rep_dir / "benchmark_results.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Dual Engine Benchmark")
    parser.add_argument("--input", "-i", default=str(PROJECT_ROOT / "data" / "sample_orders.csv"), help="CSV input path")
    args = parser.parse_args()
    
    res = run_benchmark(args.input)
    print(json.dumps(res, ensure_ascii=False, indent=2))

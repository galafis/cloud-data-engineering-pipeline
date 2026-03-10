"""
Cloud Data Engineering Pipeline

Self-contained ETL pipeline engine with data quality validation,
transformation DSL, partitioning, and pipeline orchestration.

Author: Gabriel Demetrios Lafis
"""

import csv
import json
import os
import hashlib
import logging
import sqlite3
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── Data Quality ─────────────────────────────────────────────────────

class DataQualityRule:
    """Single data quality validation rule."""

    def __init__(self, name: str, check_fn: Callable[[List[Dict]], List[str]]):
        self.name = name
        self.check_fn = check_fn

    def evaluate(self, rows: List[Dict]) -> Dict:
        errors = self.check_fn(rows)
        return {
            "rule": self.name,
            "passed": len(errors) == 0,
            "error_count": len(errors),
            "errors": errors[:10],
        }


class DataQualityValidator:
    """Validate datasets against a collection of rules."""

    def __init__(self):
        self.rules: List[DataQualityRule] = []

    def add_not_null(self, column: str) -> "DataQualityValidator":
        def _check(rows: List[Dict]) -> List[str]:
            return [
                f"Row {i}: '{column}' is null"
                for i, r in enumerate(rows)
                if r.get(column) is None or str(r.get(column, "")).strip() == ""
            ]
        self.rules.append(DataQualityRule(f"not_null:{column}", _check))
        return self

    def add_unique(self, column: str) -> "DataQualityValidator":
        def _check(rows: List[Dict]) -> List[str]:
            seen: Dict[Any, int] = {}
            errs: List[str] = []
            for i, r in enumerate(rows):
                v = r.get(column)
                if v in seen:
                    errs.append(f"Row {i}: duplicate '{column}'={v} (first at {seen[v]})")
                else:
                    seen[v] = i
            return errs
        self.rules.append(DataQualityRule(f"unique:{column}", _check))
        return self

    def add_range(self, column: str, min_val: float, max_val: float) -> "DataQualityValidator":
        def _check(rows: List[Dict]) -> List[str]:
            errs: List[str] = []
            for i, r in enumerate(rows):
                v = r.get(column)
                if v is None:
                    continue
                try:
                    fv = float(v)
                except (ValueError, TypeError):
                    errs.append(f"Row {i}: '{column}'={v} not numeric")
                    continue
                if fv < min_val or fv > max_val:
                    errs.append(f"Row {i}: '{column}'={fv} out of [{min_val},{max_val}]")
            return errs
        self.rules.append(DataQualityRule(f"range:{column}[{min_val},{max_val}]", _check))
        return self

    def add_regex(self, column: str, pattern: str) -> "DataQualityValidator":
        import re
        compiled = re.compile(pattern)

        def _check(rows: List[Dict]) -> List[str]:
            return [
                f"Row {i}: '{column}'={r.get(column)} does not match {pattern}"
                for i, r in enumerate(rows)
                if r.get(column) is not None and not compiled.match(str(r.get(column)))
            ]
        self.rules.append(DataQualityRule(f"regex:{column}", _check))
        return self

    def add_custom(self, name: str, check_fn: Callable[[List[Dict]], List[str]]) -> "DataQualityValidator":
        self.rules.append(DataQualityRule(name, check_fn))
        return self

    def validate(self, rows: List[Dict]) -> Dict:
        results = [rule.evaluate(rows) for rule in self.rules]
        passed = sum(1 for r in results if r["passed"])
        return {
            "total_rules": len(results),
            "passed": passed,
            "failed": len(results) - passed,
            "pass_rate": passed / len(results) if results else 1.0,
            "details": results,
        }


# ── Transformations ──────────────────────────────────────────────────

class TransformStep:
    """A single transformation step."""

    def __init__(self, name: str, fn: Callable[[List[Dict]], List[Dict]]):
        self.name = name
        self.fn = fn

    def apply(self, rows: List[Dict]) -> List[Dict]:
        return self.fn(rows)


class TransformPipeline:
    """Composable transformation pipeline."""

    def __init__(self):
        self.steps: List[TransformStep] = []

    def add_step(self, name: str, fn: Callable[[List[Dict]], List[Dict]]) -> "TransformPipeline":
        self.steps.append(TransformStep(name, fn))
        return self

    def drop_nulls(self, columns: List[str]) -> "TransformPipeline":
        def _fn(rows: List[Dict]) -> List[Dict]:
            return [
                r for r in rows
                if all(r.get(c) is not None and str(r.get(c, "")).strip() != "" for c in columns)
            ]
        return self.add_step(f"drop_nulls({columns})", _fn)

    def fill_nulls(self, column: str, value: Any) -> "TransformPipeline":
        def _fn(rows: List[Dict]) -> List[Dict]:
            out = []
            for r in rows:
                nr = dict(r)
                if nr.get(column) is None or str(nr.get(column, "")).strip() == "":
                    nr[column] = value
                out.append(nr)
            return out
        return self.add_step(f"fill_nulls({column},{value})", _fn)

    def rename_columns(self, mapping: Dict[str, str]) -> "TransformPipeline":
        def _fn(rows: List[Dict]) -> List[Dict]:
            return [{mapping.get(k, k): v for k, v in r.items()} for r in rows]
        return self.add_step(f"rename({mapping})", _fn)

    def filter_rows(self, predicate: Callable[[Dict], bool]) -> "TransformPipeline":
        def _fn(rows: List[Dict]) -> List[Dict]:
            return [r for r in rows if predicate(r)]
        return self.add_step("filter", _fn)

    def add_column(self, name: str, fn: Callable[[Dict], Any]) -> "TransformPipeline":
        def _apply(rows: List[Dict]) -> List[Dict]:
            out = []
            for r in rows:
                nr = dict(r)
                nr[name] = fn(nr)
                out.append(nr)
            return out
        return self.add_step(f"add_column({name})", _apply)

    def cast_column(self, column: str, dtype: type) -> "TransformPipeline":
        def _fn(rows: List[Dict]) -> List[Dict]:
            out = []
            for r in rows:
                nr = dict(r)
                try:
                    nr[column] = dtype(nr[column])
                except (ValueError, TypeError):
                    pass
                out.append(nr)
            return out
        return self.add_step(f"cast({column},{dtype.__name__})", _fn)

    def sort_by(self, column: str, reverse: bool = False) -> "TransformPipeline":
        def _fn(rows: List[Dict]) -> List[Dict]:
            return sorted(rows, key=lambda r: r.get(column, ""), reverse=reverse)
        return self.add_step(f"sort({column})", _fn)

    def deduplicate(self, key_columns: List[str]) -> "TransformPipeline":
        def _fn(rows: List[Dict]) -> List[Dict]:
            seen = set()
            out = []
            for r in rows:
                key = tuple(r.get(c) for c in key_columns)
                if key not in seen:
                    seen.add(key)
                    out.append(r)
            return out
        return self.add_step(f"dedup({key_columns})", _fn)

    def execute(self, rows: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Execute all steps. Returns (result_rows, step_log)."""
        log = []
        current = rows
        for step in self.steps:
            before = len(current)
            t0 = time.time()
            current = step.apply(current)
            elapsed = time.time() - t0
            log.append({
                "step": step.name,
                "input_rows": before,
                "output_rows": len(current),
                "elapsed_seconds": round(elapsed, 4),
            })
            logger.info(f"Step '{step.name}': {before} -> {len(current)} rows ({elapsed:.4f}s)")
        return current, log


# ── File I/O ─────────────────────────────────────────────────────────

class FileConnector:
    """Read/write CSV and JSON files."""

    @staticmethod
    def read_csv(path: str, delimiter: str = ",") -> List[Dict]:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            return [dict(row) for row in reader]

    @staticmethod
    def write_csv(rows: List[Dict], path: str, delimiter: str = ",") -> int:
        if not rows:
            return 0
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        fieldnames = list(rows[0].keys())
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter)
            writer.writeheader()
            writer.writerows(rows)
        return len(rows)

    @staticmethod
    def read_json(path: str) -> List[Dict]:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return [data]

    @staticmethod
    def write_json(rows: List[Dict], path: str) -> int:
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, default=str)
        return len(rows)


# ── SQLite Connector ─────────────────────────────────────────────────

class SQLiteConnector:
    """Load/extract data from SQLite."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def load_rows(self, table: str, rows: List[Dict]) -> int:
        if not rows:
            return 0
        cols = list(rows[0].keys())
        placeholders = ", ".join("?" for _ in cols)
        col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
        self.conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')
        for row in rows:
            vals = [row.get(c) for c in cols]
            self.conn.execute(f'INSERT INTO "{table}" ({", ".join(cols)}) VALUES ({placeholders})', vals)
        self.conn.commit()
        return len(rows)

    def query(self, sql: str) -> List[Dict]:
        cur = self.conn.execute(sql)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

    def extract_table(self, table: str) -> List[Dict]:
        return self.query(f'SELECT * FROM "{table}"')

    def close(self):
        self.conn.close()


# ── Partitioning ─────────────────────────────────────────────────────

class Partitioner:
    """Partition rows by a key column (hash or value)."""

    @staticmethod
    def by_value(rows: List[Dict], column: str) -> Dict[str, List[Dict]]:
        partitions: Dict[str, List[Dict]] = {}
        for r in rows:
            key = str(r.get(column, "__null__"))
            partitions.setdefault(key, []).append(r)
        return partitions

    @staticmethod
    def by_hash(rows: List[Dict], column: str, num_partitions: int) -> Dict[int, List[Dict]]:
        partitions: Dict[int, List[Dict]] = {i: [] for i in range(num_partitions)}
        for r in rows:
            h = int(hashlib.md5(str(r.get(column, "")).encode()).hexdigest(), 16)
            bucket = h % num_partitions
            partitions[bucket].append(r)
        return partitions

    @staticmethod
    def by_range(rows: List[Dict], column: str, boundaries: List[float]) -> Dict[str, List[Dict]]:
        labels = [f"<{boundaries[0]}"] + \
                 [f"[{boundaries[i]},{boundaries[i+1]})" for i in range(len(boundaries) - 1)] + \
                 [f">={boundaries[-1]}"]
        partitions: Dict[str, List[Dict]] = {label: [] for label in labels}
        for r in rows:
            try:
                v = float(r.get(column, 0))
            except (ValueError, TypeError):
                partitions[labels[0]].append(r)
                continue
            placed = False
            for i, b in enumerate(boundaries):
                if v < b:
                    partitions[labels[i]].append(r)
                    placed = True
                    break
            if not placed:
                partitions[labels[-1]].append(r)
        return partitions


# ── ETL Pipeline Orchestrator ────────────────────────────────────────

class ETLPipeline:
    """Orchestrate Extract-Transform-Load with logging and metrics."""

    def __init__(self, name: str = "etl_pipeline"):
        self.name = name
        self.extract_fn: Optional[Callable[[], List[Dict]]] = None
        self.transform_pipeline: Optional[TransformPipeline] = None
        self.load_fn: Optional[Callable[[List[Dict]], int]] = None
        self.validator: Optional[DataQualityValidator] = None
        self.metrics: Dict = {}

    def set_extract(self, fn: Callable[[], List[Dict]]) -> "ETLPipeline":
        self.extract_fn = fn
        return self

    def set_transform(self, pipeline: TransformPipeline) -> "ETLPipeline":
        self.transform_pipeline = pipeline
        return self

    def set_load(self, fn: Callable[[List[Dict]], int]) -> "ETLPipeline":
        self.load_fn = fn
        return self

    def set_validator(self, validator: DataQualityValidator) -> "ETLPipeline":
        self.validator = validator
        return self

    def run(self) -> Dict:
        """Execute the full ETL pipeline."""
        t_start = time.time()
        logger.info(f"[{self.name}] Starting ETL pipeline")

        # Extract
        t0 = time.time()
        raw_rows = self.extract_fn() if self.extract_fn else []
        extract_time = time.time() - t0
        logger.info(f"[{self.name}] Extracted {len(raw_rows)} rows in {extract_time:.3f}s")

        # Pre-validation
        pre_validation = None
        if self.validator and raw_rows:
            pre_validation = self.validator.validate(raw_rows)
            logger.info(f"[{self.name}] Pre-validation: {pre_validation['passed']}/{pre_validation['total_rules']} passed")

        # Transform
        transform_log = []
        transformed = raw_rows
        t0 = time.time()
        if self.transform_pipeline:
            transformed, transform_log = self.transform_pipeline.execute(raw_rows)
        transform_time = time.time() - t0

        # Post-validation
        post_validation = None
        if self.validator and transformed:
            post_validation = self.validator.validate(transformed)
            logger.info(f"[{self.name}] Post-validation: {post_validation['passed']}/{post_validation['total_rules']} passed")

        # Load
        t0 = time.time()
        loaded = 0
        if self.load_fn and transformed:
            loaded = self.load_fn(transformed)
        load_time = time.time() - t0
        logger.info(f"[{self.name}] Loaded {loaded} rows in {load_time:.3f}s")

        total_time = time.time() - t_start

        self.metrics = {
            "pipeline": self.name,
            "started_at": datetime.utcnow().isoformat(),
            "input_rows": len(raw_rows),
            "output_rows": len(transformed),
            "loaded_rows": loaded,
            "extract_seconds": round(extract_time, 4),
            "transform_seconds": round(transform_time, 4),
            "load_seconds": round(load_time, 4),
            "total_seconds": round(total_time, 4),
            "rows_per_second": round(len(transformed) / total_time, 2) if total_time > 0 else 0,
            "transform_steps": transform_log,
            "pre_validation": pre_validation,
            "post_validation": post_validation,
        }

        logger.info(f"[{self.name}] Pipeline complete in {total_time:.3f}s")
        return self.metrics


# ── Utility: generate sample data ────────────────────────────────────

def generate_sample_data(n: int = 500, seed: int = 42) -> List[Dict]:
    """Generate deterministic sample rows for testing."""
    import random as _rnd
    _rnd.seed(seed)
    regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
    statuses = ["active", "inactive", "pending"]
    rows = []
    for i in range(n):
        rows.append({
            "id": i + 1,
            "name": f"item_{i+1:04d}",
            "value": round(_rnd.uniform(1, 1000), 2),
            "region": _rnd.choice(regions),
            "status": _rnd.choice(statuses),
            "created_at": f"2024-{_rnd.randint(1,12):02d}-{_rnd.randint(1,28):02d}",
        })
    return rows

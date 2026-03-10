"""
Tests for Cloud Data Engineering Pipeline.

Author: Gabriel Demetrios Lafis
"""

import pytest
import sys
import os
import json
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.pipeline import (
    DataQualityValidator,
    TransformPipeline,
    FileConnector,
    SQLiteConnector,
    Partitioner,
    ETLPipeline,
    generate_sample_data,
)


# ── Fixtures ─────────────────────────────────────────────────────────

@pytest.fixture
def sample_rows():
    return generate_sample_data(100, seed=42)


@pytest.fixture
def simple_rows():
    return [
        {"id": 1, "name": "Alice", "age": "30", "score": "85.5"},
        {"id": 2, "name": "Bob", "age": "25", "score": "92.0"},
        {"id": 3, "name": "", "age": "35", "score": "78.0"},
        {"id": 4, "name": "Diana", "age": None, "score": "88.5"},
        {"id": 5, "name": "Eve", "age": "28", "score": "95.0"},
    ]


# ── Data Quality Tests ──────────────────────────────────────────────

class TestDataQualityValidator:
    def test_not_null_pass(self, sample_rows):
        v = DataQualityValidator().add_not_null("id")
        result = v.validate(sample_rows)
        assert result["passed"] == 1
        assert result["failed"] == 0

    def test_not_null_fail(self, simple_rows):
        v = DataQualityValidator().add_not_null("name")
        result = v.validate(simple_rows)
        assert result["failed"] == 1

    def test_unique_pass(self, sample_rows):
        v = DataQualityValidator().add_unique("id")
        result = v.validate(sample_rows)
        assert result["passed"] == 1

    def test_unique_fail(self):
        rows = [{"id": 1}, {"id": 2}, {"id": 1}]
        v = DataQualityValidator().add_unique("id")
        result = v.validate(rows)
        assert result["failed"] == 1

    def test_range_pass(self, sample_rows):
        v = DataQualityValidator().add_range("value", 0, 1500)
        result = v.validate(sample_rows)
        assert result["passed"] == 1

    def test_range_fail(self):
        rows = [{"v": "10"}, {"v": "200"}, {"v": "-5"}]
        v = DataQualityValidator().add_range("v", 0, 100)
        result = v.validate(rows)
        assert result["failed"] == 1

    def test_regex_pass(self):
        rows = [{"email": "a@b.com"}, {"email": "x@y.org"}]
        v = DataQualityValidator().add_regex("email", r"^.+@.+\..+$")
        result = v.validate(rows)
        assert result["passed"] == 1

    def test_regex_fail(self):
        rows = [{"email": "invalid"}, {"email": "a@b.com"}]
        v = DataQualityValidator().add_regex("email", r"^.+@.+\..+$")
        result = v.validate(rows)
        assert result["failed"] == 1

    def test_multiple_rules(self, sample_rows):
        v = (DataQualityValidator()
             .add_not_null("id")
             .add_unique("id")
             .add_range("value", 0, 1500))
        result = v.validate(sample_rows)
        assert result["total_rules"] == 3
        assert result["pass_rate"] == 1.0

    def test_custom_rule(self):
        rows = [{"a": 1, "b": 2}, {"a": 5, "b": 3}]
        v = DataQualityValidator().add_custom(
            "a_less_than_b",
            lambda rs: [f"Row {i}" for i, r in enumerate(rs) if r["a"] >= r["b"]]
        )
        result = v.validate(rows)
        assert result["failed"] == 1

    def test_empty_rows(self):
        v = DataQualityValidator().add_not_null("x")
        result = v.validate([])
        assert result["passed"] == 1


# ── Transform Pipeline Tests ────────────────────────────────────────

class TestTransformPipeline:
    def test_drop_nulls(self, simple_rows):
        pipe = TransformPipeline().drop_nulls(["name"])
        result, log = pipe.execute(simple_rows)
        assert len(result) == 4  # row with empty name removed

    def test_fill_nulls(self, simple_rows):
        pipe = TransformPipeline().fill_nulls("age", "0")
        result, _ = pipe.execute(simple_rows)
        ages = [r["age"] for r in result]
        assert None not in ages

    def test_rename_columns(self):
        rows = [{"old_name": "val"}]
        pipe = TransformPipeline().rename_columns({"old_name": "new_name"})
        result, _ = pipe.execute(rows)
        assert "new_name" in result[0]
        assert "old_name" not in result[0]

    def test_filter_rows(self, sample_rows):
        pipe = TransformPipeline().filter_rows(lambda r: r["region"] == "us-east-1")
        result, _ = pipe.execute(sample_rows)
        assert all(r["region"] == "us-east-1" for r in result)

    def test_add_column(self, sample_rows):
        pipe = TransformPipeline().add_column("doubled", lambda r: r["value"] * 2)
        result, _ = pipe.execute(sample_rows)
        assert "doubled" in result[0]
        assert result[0]["doubled"] == result[0]["value"] * 2

    def test_cast_column(self, simple_rows):
        pipe = TransformPipeline().cast_column("age", int)
        result, _ = pipe.execute([r for r in simple_rows if r["age"] is not None])
        assert isinstance(result[0]["age"], int)

    def test_sort_by(self):
        rows = [{"x": 3}, {"x": 1}, {"x": 2}]
        pipe = TransformPipeline().sort_by("x")
        result, _ = pipe.execute(rows)
        assert [r["x"] for r in result] == [1, 2, 3]

    def test_deduplicate(self):
        rows = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 1, "v": "c"}]
        pipe = TransformPipeline().deduplicate(["id"])
        result, _ = pipe.execute(rows)
        assert len(result) == 2

    def test_chained_steps(self, sample_rows):
        pipe = (TransformPipeline()
                .filter_rows(lambda r: r["status"] == "active")
                .add_column("processed", lambda r: True)
                .sort_by("value"))
        result, log = pipe.execute(sample_rows)
        assert all(r["status"] == "active" for r in result)
        assert all(r["processed"] is True for r in result)
        assert len(log) == 3

    def test_step_log(self, sample_rows):
        pipe = TransformPipeline().filter_rows(lambda r: r["value"] > 500)
        _, log = pipe.execute(sample_rows)
        assert len(log) == 1
        assert log[0]["input_rows"] == 100
        assert "elapsed_seconds" in log[0]


# ── File Connector Tests ────────────────────────────────────────────

class TestFileConnector:
    def test_csv_roundtrip(self, sample_rows):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name
        try:
            FileConnector.write_csv(sample_rows, path)
            loaded = FileConnector.read_csv(path)
            assert len(loaded) == len(sample_rows)
            assert loaded[0]["name"] == sample_rows[0]["name"]
        finally:
            os.unlink(path)

    def test_json_roundtrip(self, sample_rows):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = f.name
        try:
            FileConnector.write_json(sample_rows, path)
            loaded = FileConnector.read_json(path)
            assert len(loaded) == len(sample_rows)
        finally:
            os.unlink(path)

    def test_write_empty(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name
        try:
            count = FileConnector.write_csv([], path)
            assert count == 0
        finally:
            os.unlink(path)


# ── SQLite Connector Tests ──────────────────────────────────────────

class TestSQLiteConnector:
    def test_load_and_query(self, sample_rows):
        db = SQLiteConnector(":memory:")
        count = db.load_rows("test_table", sample_rows[:10])
        assert count == 10
        rows = db.query("SELECT * FROM test_table")
        assert len(rows) == 10
        db.close()

    def test_extract_table(self, sample_rows):
        db = SQLiteConnector(":memory:")
        db.load_rows("data", sample_rows[:5])
        extracted = db.extract_table("data")
        assert len(extracted) == 5
        db.close()

    def test_empty_load(self):
        db = SQLiteConnector(":memory:")
        count = db.load_rows("empty", [])
        assert count == 0
        db.close()


# ── Partitioner Tests ────────────────────────────────────────────────

class TestPartitioner:
    def test_by_value(self, sample_rows):
        parts = Partitioner.by_value(sample_rows, "region")
        total = sum(len(v) for v in parts.values())
        assert total == len(sample_rows)

    def test_by_hash(self, sample_rows):
        parts = Partitioner.by_hash(sample_rows, "id", 4)
        assert len(parts) == 4
        total = sum(len(v) for v in parts.values())
        assert total == len(sample_rows)

    def test_by_range(self):
        rows = [{"v": i * 10} for i in range(11)]
        parts = Partitioner.by_range(rows, "v", [30, 70])
        total = sum(len(v) for v in parts.values())
        assert total == len(rows)


# ── ETL Pipeline Orchestrator Tests ──────────────────────────────────

class TestETLPipeline:
    def test_basic_pipeline(self, sample_rows):
        loaded_data = []
        pipe = (ETLPipeline("test")
                .set_extract(lambda: sample_rows)
                .set_transform(TransformPipeline().filter_rows(lambda r: r["status"] == "active"))
                .set_load(lambda rows: (loaded_data.extend(rows), len(rows))[1]))
        metrics = pipe.run()
        assert metrics["input_rows"] == 100
        assert metrics["output_rows"] <= 100
        assert metrics["loaded_rows"] == len(loaded_data)
        assert "total_seconds" in metrics

    def test_pipeline_with_validation(self, sample_rows):
        validator = DataQualityValidator().add_not_null("id").add_unique("id")
        pipe = (ETLPipeline("validated")
                .set_extract(lambda: sample_rows)
                .set_validator(validator)
                .set_load(lambda rows: len(rows)))
        metrics = pipe.run()
        assert metrics["pre_validation"]["pass_rate"] == 1.0
        assert metrics["post_validation"]["pass_rate"] == 1.0

    def test_pipeline_no_transform(self, sample_rows):
        pipe = (ETLPipeline("passthrough")
                .set_extract(lambda: sample_rows)
                .set_load(lambda rows: len(rows)))
        metrics = pipe.run()
        assert metrics["input_rows"] == metrics["output_rows"]

    def test_pipeline_metrics_keys(self, sample_rows):
        pipe = (ETLPipeline("metrics")
                .set_extract(lambda: sample_rows)
                .set_load(lambda rows: len(rows)))
        metrics = pipe.run()
        expected_keys = ["pipeline", "input_rows", "output_rows", "loaded_rows",
                         "extract_seconds", "transform_seconds", "load_seconds",
                         "total_seconds", "rows_per_second"]
        for k in expected_keys:
            assert k in metrics, f"Missing key: {k}"


# ── Sample Data Generator Tests ──────────────────────────────────────

class TestGenerateSampleData:
    def test_count(self):
        rows = generate_sample_data(200)
        assert len(rows) == 200

    def test_deterministic(self):
        a = generate_sample_data(50, seed=7)
        b = generate_sample_data(50, seed=7)
        assert a == b

    def test_schema(self):
        rows = generate_sample_data(10)
        for key in ["id", "name", "value", "region", "status", "created_at"]:
            assert key in rows[0], f"Missing key: {key}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

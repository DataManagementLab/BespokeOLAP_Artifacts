# BespokeOLAP Artifacts

> Generated C++ artifacts for [Bespoke OLAP](https://github.com/DataManagementLab/BespokeOLAP) — synthesized, bespoke implementations of the **TPC-H** and **CEB** analytical benchmarks.

---

## 📂 Contents

| Directory | Benchmark | Execution Model |
|---|---|---|
| [`bespoke_tpch/`](bespoke_tpch/) | TPC-H Q1–Q22 | 🔁 Single-threaded |
| [`bespoke_tpch_multithreading/`](bespoke_tpch_multithreading/) | TPC-H Q1–Q22 | ⚡ Multi-threaded *(new)* |
| [`bespoke_ceb/`](bespoke_ceb/) | CEB Q1a–Q11b | 🔁 Single-threaded |
| [`bespoke_ceb_multithreading/`](bespoke_ceb_multithreading/) | CEB Q1a–Q11b | ⚡ Multi-threaded *(new)* |

---

## ⚙️ Requirements

- [uv](https://docs.astral.sh/uv/) (Python package manager)

```bash
uv sync
```

---

## 🚀 Build & Run

The provided `run.py` script compiles and executes the generated C++ code.

> **⚠️ Note:** `run.py` currently supports the **single-threaded** implementations only (`tpch`, `ceb`). Runners for the multi-threaded TPC-H and CEB variants are currently not included but can easily be added.

```bash
python run.py [--sf <scale_factor>] [--no-optimize] {tpch,ceb}
```

### Options

| Argument | Description | Default |
|---|---|---|
| `benchmark` | Benchmark to run (`tpch` or `ceb`) | *(required)* |
| `--sf` | TPC-H scale factor | `1` |
| `--no-optimize` | Disable compiler optimizations | *(optimizations enabled)* |

### Examples

```bash
# TPC-H at scale factor 20
python run.py --sf 20 tpch

# CEB at default scale factor
python run.py ceb

# TPC-H without compiler optimizations (no -O3)
python run.py --no-optimize --sf 10 tpch
```

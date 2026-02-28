# BespokeOLAP Artifacts

Generated C++ artifacts for [Bespoke OLAP](https://github.com/DataManagementLab/BespokeOLAP): synthesized implementations of the **TPC-H** and **CEB** benchmarks.

## Setup

Requires [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

## Build & Run Generated Databases
We have provided a sample script to run the generated databases in `run.py`.
```bash
python run.py [--sf <scale_factor>] [--no-optimize] {tpch,ceb}
```

**Arguments:**

| Argument | Description | Default |
|---|---|---|
| `benchmark` | Benchmark to run: `tpch` or `ceb` | *(required)* |
| `--sf` | Scale factor | `1` |
| `--no-optimize` | Disable compiler optimization | *(optimization on)* |

**Examples:**

```bash
# Run TPC-H at scale factor 20
python run.py --sf 20 tpch

# Run CEB at scale factor 1
python run.py ceb

# Run TPC-H without optimization flags (compiler)
python run.py --no-optimize --sf 10 tpch
```

## Benchmarks

- **bespoke_tpch/** — TPC-H queries Q1–Q22, synthesized as bespoke C++ implementations
- **bespoke_ceb/** — CEB queries, Q1a-Q11b synthesized as bespoke C++ implementations

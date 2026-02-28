#!/usr/bin/env python3
"""Minimal runner for bespoke-tpch or bespoke-ceb."""

import argparse
import logging
import os
import random
from pathlib import Path

from benchmark.run import get_all_query_ids
from dataset.dataset_tables_dict import get_dataset_name
from dataset.query_gen_factory import get_query_gen
from llm_cache.logger import setup_logging
from tools.fasttest.run import RunTool
from tools.validate_tool.query_validator_class import format_args_string

BASE_PARQUET_DIR = "/mnt/labstore/bespoke_olap"

setup_logging(logging.INFO)
logger = logging.getLogger(__name__)


def get_instantiations(benchmark: str, query_ids: list[str], repeat: int = 1):

    # prepare query generator
    gen_query_fn = get_query_gen(benchmark)

    sql_list: list[str] = []
    placeholder_list: list[dict] = []
    query_list: list[str] = []

    rnd = random.Random(42)
    for _ in range(repeat):
        for query_id in query_ids:
            template, query, placeholders = gen_query_fn(
                query_name=f"Q{query_id}", rnd=rnd
            )
            query_list.append(str(query_id))
            placeholder_list.append(placeholders)
            sql_list.append(query)

    return sql_list, placeholder_list, query_list


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build and run bespoke OLAP implementation."
    )
    parser.add_argument(
        "benchmark", choices=["tpch", "ceb"], help="Which benchmark to run"
    )
    parser.add_argument("--sf", type=float, default=1, help="Scale factor (default: 1)")
    parser.add_argument(
        "--no-optimize",
        dest="optimize",
        help="Compile without optimization",
        action="store_false",
        default=True,
    )
    args = parser.parse_args()

    # assemble source directory for bespoke implementation
    ROOT = Path(__file__).parent
    bespoke_dir = ROOT / f"bespoke_{args.benchmark}"

    # get path for misc.fasttest (from import)
    import misc.fasttest

    API_PATH = Path(os.path.dirname(misc.fasttest.__file__))

    # assemble the tool for building and running the bespoke implementation
    db_engine = RunTool(
        cwd=bespoke_dir,
        dataset_name=args.benchmark,
        base_parquet_dir=f"{BASE_PARQUET_DIR}/{get_dataset_name(args.benchmark)}_parquet",
        api_path=API_PATH,
    )

    logger.info(
        "Building and running %s SF=%s optimize=%s...",
        args.benchmark,
        args.sf,
        args.optimize,
    )

    # assemble the queries to run - we only need the query-name and the placeholders, the tool will take care of the rest
    sql_list, placeholder_list, query_list = get_instantiations(
        benchmark=args.benchmark, query_ids=get_all_query_ids(args.benchmark), repeat=1
    )

    # format the arguments handed in to the tool
    args_list = format_args_string(query_list, placeholder_list)

    # run the queries - results will be written to '*.csv' files in the sourcefile directory e.g. bespoke_tpch
    result = db_engine.run_worker(
        scale_factor=args.sf,
        optimize=args.optimize,
        stdin_args_data=args_list,
    )

    logger.info(f"Finished running. Result files written to {bespoke_dir}/*.csv")


if __name__ == "__main__":
    main()

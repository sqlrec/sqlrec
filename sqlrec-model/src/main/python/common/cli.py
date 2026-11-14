"""Command-line configuration shared by model training and export scripts."""

import argparse

from common import filesystem


def load_pipeline_config_from_args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_config_path", required=True,
                        help="path to the JSON pipeline.config (hdfs:// or local)")
    args = parser.parse_args()
    return filesystem.load_pipeline_config(args.pipeline_config_path)

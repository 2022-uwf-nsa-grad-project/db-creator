#!/usr/bin/env python3
"""Aggregate chain batch manifest entries into a summary JSON file."""

import argparse
from glob import glob
from pathlib import Path
from typing import List, Optional

from CART.aggregate_chain_manifest import aggregate_manifest


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Aggregate streaming chain batch manifest into summary JSON'
    )
    parser.add_argument(
        '--manifest',
        '-m',
        default='thesis_results/chain_temp/*.manifest',
        help='Path or glob to a manifest file produced by the streaming builder'
    )
    parser.add_argument(
        '--output',
        '-o',
        default='hop_batch_summary.json',
        help='Output path for the aggregated JSON'
    )
    return parser


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    matches = glob(args.manifest)
    if not matches:
        raise SystemExit(f'No manifest files matched pattern: {args.manifest}')

    manifest_path = Path(matches[0])
    aggregate_manifest(str(manifest_path), args.output)
    print(f'Aggregated manifest {manifest_path} -> {args.output}')


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Utility to inspect multi-hop lateral movement chains directly from Neo4j.

This script reproduces the multi-hop chain export performed inside
`SubnetPivotAnalyzer.analyze_multi_hop_chains` but adds richer logging,
progress bars, and configurable limits. It is meant to help diagnose why the
current thesis artifacts only include 100 chains with identical subnet
sequences.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
from tqdm import tqdm

from CART.analyzers import SubnetPivotAnalyzer

LOGGER = logging.getLogger("inspect_multi_hop_chains")


LABEL_AWARE_QUERY = """
MATCH path = (a:IP)-[r1:CONNECTS]->(b:IP)-[r2:CONNECTS]->(c:IP)-[r3:CONNECTS]->(d:IP)
WHERE r1.is_attack = 1 AND r2.is_attack = 1 AND r3.is_attack = 1
  AND r2.timestamp > r1.timestamp
  AND r3.timestamp > r2.timestamp
  AND a <> c AND b <> d AND a <> d
RETURN 
    a.address AS hop1_ip,
    b.address AS hop2_ip,
    c.address AS hop3_ip,
    d.address AS hop4_ip,
    a.subnet AS hop1_subnet,
    b.subnet AS hop2_subnet,
    c.subnet AS hop3_subnet,
    d.subnet AS hop4_subnet,
    (r2.timestamp - r1.timestamp) / 3600.0 AS hours_to_hop2,
    (r3.timestamp - r2.timestamp) / 3600.0 AS hours_to_hop3,
    r1.tactic AS tactic1,
    r2.tactic AS tactic2,
    r3.tactic AS tactic3
{limit_clause}
"""

LABEL_AGNOSTIC_QUERY = """
MATCH path = (a:IP)-[r1:CONNECTS]->(b:IP)-[r2:CONNECTS]->(c:IP)-[r3:CONNECTS]->(d:IP)
WHERE r2.timestamp > r1.timestamp
  AND r3.timestamp > r2.timestamp
  AND a <> c AND b <> d AND a <> d
RETURN 
    a.address AS hop1_ip,
    b.address AS hop2_ip,
    c.address AS hop3_ip,
    d.address AS hop4_ip,
    a.subnet AS hop1_subnet,
    b.subnet AS hop2_subnet,
    c.subnet AS hop3_subnet,
    d.subnet AS hop4_subnet,
    (r2.timestamp - r1.timestamp) / 3600.0 AS hours_to_hop2,
    (r3.timestamp - r2.timestamp) / 3600.0 AS hours_to_hop3
{limit_clause}
"""

LABEL_AWARE_COUNT_QUERY = """
MATCH (a:IP)-[r1:CONNECTS]->(b:IP)-[r2:CONNECTS]->(c:IP)-[r3:CONNECTS]->(d:IP)
WHERE r1.is_attack = 1 AND r2.is_attack = 1 AND r3.is_attack = 1
    AND r2.timestamp > r1.timestamp
    AND r3.timestamp > r2.timestamp
    AND a <> c AND b <> d AND a <> d
RETURN count(*) AS total_chains
"""

LABEL_AGNOSTIC_COUNT_QUERY = """
MATCH (a:IP)-[r1:CONNECTS]->(b:IP)-[r2:CONNECTS]->(c:IP)-[r3:CONNECTS]->(d:IP)
WHERE r2.timestamp > r1.timestamp
    AND r3.timestamp > r2.timestamp
    AND a <> c AND b <> d AND a <> d
RETURN count(*) AS total_chains
"""


def configure_logging(verbosity: int) -> None:
    """Configure root logging based on the requested verbosity."""
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def build_query(label_aware: bool, limit: Optional[int]) -> tuple[str, dict]:
    """Return the Cypher query string and parameters."""
    limit_clause = ""
    params = {}
    if limit is not None:
        limit_clause = "LIMIT $limit"
        params["limit"] = limit

    template = LABEL_AWARE_QUERY if label_aware else LABEL_AGNOSTIC_QUERY
    return template.format(limit_clause=limit_clause), params


def stream_records(result_iter: Iterable, desc: str) -> List[dict]:
    """Iterate over a Neo4j result set with a tqdm progress bar."""
    records: List[dict] = []
    progress = tqdm(desc=desc, unit="row")
    for record in result_iter:
        # record can be a neo4j.Record or dict depending on driver config
        data = record.data() if hasattr(record, "data") else dict(record)
        records.append(data)
        progress.update(1)
    progress.close()
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=["label-aware", "label-agnostic"],
        default="label-aware",
        help="Query mode. Matches analyzer's labeled or unlabeled chain export.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional LIMIT applied to the Cypher query for quick sampling.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("thesis_results/multi_hop_debug.csv"),
        help="Path where the resulting CSV should be written.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Skip CSV export and only report the total chain count.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity (can be supplied multiple times).",
    )

    args = parser.parse_args()

    configure_logging(args.verbose)
    LOGGER.info("Running in %s mode", args.mode)

    analyzer = SubnetPivotAnalyzer()
    if not analyzer.connect():
        LOGGER.error("Failed to connect to Neo4j; aborting.")
        raise SystemExit(1)

    try:
        with analyzer.driver.session(database=analyzer.database) as session:
            query, params = build_query(label_aware=args.mode == "label-aware", limit=args.limit)
            LOGGER.debug("Executing query:\n%s", query)
            LOGGER.info("Executing Cypher query with params: %s", params or "<none>")

            if args.summary_only:
                count_query = (
                    LABEL_AWARE_COUNT_QUERY
                    if args.mode == "label-aware"
                    else LABEL_AGNOSTIC_COUNT_QUERY
                )
                LOGGER.info("Running count query to summarize total chains")
                total = session.run(count_query).single()["total_chains"]
                LOGGER.info("Total %s chains: %s", args.mode, f"{total:,}")
                print(f"Total {args.mode} chains: {total:,}")
                return

            result = session.run(query, **params)
            rows = stream_records(result, desc="Fetching chains")

        if not rows:
            LOGGER.warning("Query returned no rows. Nothing to write.")
            return

        df = pd.DataFrame(rows)
        LOGGER.info("Retrieved %d rows with columns: %s", len(df), list(df.columns))

        # Provide quick diagnostics for subnet/IP diversity.
        for col in sorted(c for c in df.columns if col.startswith("hop")):
            unique_values = df[col].nunique(dropna=True)
            LOGGER.info("Column %s unique values: %d", col, unique_values)

        args.output.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.output, index=False)
        LOGGER.info("Wrote CSV to %s", args.output.resolve())

    finally:
        analyzer.close()
        LOGGER.debug("Closed analyzer connection")


if __name__ == "__main__":  # pragma: no cover
    main()

"""Lightweight harness to exercise the SubnetPivotAnalyzer using a cached adjacency."""

import hashlib
import os
import pickle
import sys
import types
from typing import List, Optional

# Provide a lightweight dummy 'neo4j' module if it's not installed in this environment.
if 'neo4j' not in sys.modules:
    neo4j_mod = types.ModuleType('neo4j')

    class _DummyGraphDatabase:
        @staticmethod
        def driver(*args, **kwargs):
            class _Driver:
                def verify_connectivity(self):
                    return True

                def close(self):
                    return None

            return _Driver()

    neo4j_mod.GraphDatabase = _DummyGraphDatabase
    sys.modules['neo4j'] = neo4j_mod

from CART.analyzers import SubnetPivotAnalyzer


def build_arg_parser():
    import argparse

    parser = argparse.ArgumentParser(description='Run a lightweight analyzer harness on synthetic adjacency')
    parser.add_argument('--output-prefix', default='test_out', help='Output prefix for analyzer artifacts')
    parser.add_argument('--use-labels', action='store_true', help='Run in label-aware mode (default: label-agnostic)')
    parser.add_argument('--n-hops', type=int, default=3, help='Number of hops to build for harness run')
    return parser


def write_synthetic_cache(output_prefix: str, use_labels: bool) -> str:
    adj = {
        '10.0.0.1': [
            {'dst_ip': '10.0.1.1', 'timestamp': 100, 'src_subnet': '10.0.0.0/24', 'dst_subnet': '10.0.1.0/24', 'tactic': None}
        ],
        '10.0.1.1': [
            {'dst_ip': '10.0.2.1', 'timestamp': 200, 'src_subnet': '10.0.1.0/24', 'dst_subnet': '10.0.2.0/24', 'tactic': None}
        ],
        '10.0.2.1': [
            {'dst_ip': '10.0.3.1', 'timestamp': 300, 'src_subnet': '10.0.2.0/24', 'dst_subnet': '10.0.3.0/24', 'tactic': None}
        ],
    }

    cache_dir = os.path.join(os.path.dirname(output_prefix) or '.', '.chain_cache')
    os.makedirs(cache_dir, exist_ok=True)
    cache_key = hashlib.md5(f'labels={use_labels}'.encode()).hexdigest()
    edge_cache_file = os.path.join(cache_dir, f'edges_{cache_key}.pkl')

    with open(edge_cache_file, 'wb') as f:
        pickle.dump(adj, f)

    return edge_cache_file


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    cache_file = write_synthetic_cache(args.output_prefix, args.use_labels)
    print(f'Wrote adjacency cache to: {cache_file}')

    analyzer = SubnetPivotAnalyzer()
    analyzer.analyze_multi_hop_chains(
        use_labels=args.use_labels,
        output_prefix=args.output_prefix,
        n_hops=args.n_hops,
        use_cache=True
    )

    print('Harness run complete')


if __name__ == '__main__':
    main()

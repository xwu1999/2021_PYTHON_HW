import argparse

def task_parser():
    parser = argparse.ArgumentParser(description='task1 parser')
    parser.add_argument('--dir', default='./tpch_tbl', type=str, help='tbl path')
    parser.add_argument('--sf', default=0.01, type=float, help='tbl size')

    return parser.parse_args()
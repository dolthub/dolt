from doltpy.etl import get_df_table_writer, get_dolt_loader, load_to_dolthub
from doltpy.core.system_helpers import get_logger
import pandas as pd
import argparse
import os

RESULTS_TABLE_PKS = ['username', 'timestamp', 'committish', 'test_name']
RESULTS_TABLE = 'sysbench_benchmark'


logger = get_logger(__name__)


def write_results_to_dolt(results_dir: str, remote: str, branch: str):
    dfs = [pd.read_csv(os.path.join(results_dir, filename)) for filename in os.listdir(results_dir)]
    table_writer = get_df_table_writer(RESULTS_TABLE, lambda: pd.concat(dfs), RESULTS_TABLE_PKS, import_mode='update')
    loader = get_dolt_loader(table_writer, True, 'benchmark run', branch)
    load_to_dolthub(loader, clone=True, push=True, remote_name='origin', remote_url=remote)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--results-directory', )
    parser.add_argument('--remote-results-db', type=str, required=True)
    parser.add_argument('--remote-results-db-branch', type=str, required=False, default='master')
    args = parser.parse_args()
    logger.info('Writing the results of the tests')
    write_results_to_dolt(args.remote_results_db, args.remote_results_db_branch)

import argparse
import getpass
import logging
import os
import platform
import tempfile
from datetime import datetime
from subprocess import Popen, PIPE
from typing import List, Tuple

import pandas as pd
from doltpy.core import Dolt
from doltpy.etl import get_df_table_writer, get_dolt_loader, load_to_dolthub

logger = logging.getLogger(__name__)

# This is the list of benchmarks that we have validated can successfully run with Dolt
SUPPORTED_BENCHMARKS = [
    'bulk_insert'
]

TEST_TABLE = 'sbtest1'
RESULTS_TABLE_PKS = ['username', 'timestamp', 'dolt_version']
RESULTS_TABLE = 'sysbench_benchmark'
OUTPUT_MAPPING = {
    'read': 'sql_read_queries',
    'write': 'sql_write_queries',
    'other': 'sql_other_queries',
    'total': 'sql_total_queries',
    'transactions': 'sql_transactions',
    'irgnored errors': 'sql_ignored_errors',
    'reconnects': 'sql_reconnects',
    'total time': 'total_time',
    'total number of events': 'total_number_of_events',
    'min': 'latency_minimum',
    'avg': 'latency_average',
    'max': 'latency_maximum',
    '95th percentile': 'latency_percentile_95th',
    'sum': 'latency_sum'
}


class SysbenchFailureException(Exception):

    def __init__(self, test: str, stage: str, message: str):
        self.test = test
        self.stage = stage
        self.message = message

    def __str__(self):
        return '{} failed to {} with message:\n'.format(self.test, self.stage, self.message)


def setup() -> Dolt:
    # Setup a test repository and start the server
    logger.info('Setting up test repo for benchmarking')
    test_repo = init_empty_test_repo()
    logger.info('Test repo directory {}, starting Dolt SQL server'.format(test_repo.repo_dir()))
    test_repo.sql_server()
    return test_repo


def init_empty_test_repo() -> Dolt:
    temp_dir = tempfile.mkdtemp()
    repo_path, repo_data_dir = get_repo_path_tmp_path(temp_dir)
    assert not os.path.exists(repo_data_dir)
    return Dolt.init(repo_path)


def get_repo_path_tmp_path(path: str, subpath: str = None) -> Tuple[str, str]:
    if subpath:
        return os.path.join(path, subpath), os.path.join(path, subpath, '.dolt')
    else:
        return path, os.path.join(path, '.dolt')


def test_loop(test_list: List[str], test_repo: Dolt, print_results: bool) -> List[dict]:
    """
    This is the main loop for running the tests and collecting the output
    :param test_list:
    :param print_results:
    :return:
    """
    result = []
    for test in test_list:
        try:
            test_output = run_test(test_repo, test)
            if print_results:
                logger.info('Output for the benchmark {} was:\n{}'.format(test, test_output))
            cur_test_res = parse_output(test_output)
            cur_test_res['test_name'] = test
            result.append(cur_test_res)

        except SysbenchFailureException as e:
            logger.error('Test {} failed to produce output, moving in, error was:\n'.format(test, e))

        except ValueError as e:
            logger.error('Failure caused by failure to parse output:\n{}'.format(e))

    return result


def run_test(test_repo: Dolt, test: str) -> str:
    # ensure table is removed
    if TEST_TABLE in [t.name for t in test_repo.ls()]:
        test_repo.table_rm(TEST_TABLE)

    sysbench_args = [
        test,
        '--table-size=1000000',
        '--db-driver=mysql',
        '--mysql-db={}'.format(test_repo.repo_name),
        '--mysql-user=root',
        '--mysql-host=127.0.0.1',
    ]

    # Prepare the test
    prepare_exitcode, prepare_output = _execute(sysbench_args, 'prepare')
    if prepare_exitcode != 0:
        raise SysbenchFailureException(test, 'prepare', prepare_output)

    # Run the test
    run_exitcode, run_output = _execute(sysbench_args, 'run')
    if run_exitcode != 0:
        raise SysbenchFailureException(test, 'run', prepare_output)

    #
    return run_output


def _execute(args: List[str], stage: str):
    _args = ['sysbench'] + args + [stage]
    proc = Popen(args=_args, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    exitcode = proc.returncode
    return exitcode, out.decode('utf-8')


def parse_output(to_parse: str) -> dict:
    result = {}
    split = to_parse.split('\n')
    processing_lines = False
    for line in split:
        clean_line = line.strip()
        if not clean_line:
            pass
        elif clean_line.startswith('SQL statistics'):
            processing_lines = True
        elif clean_line.startswith('Threads fairness'):
            return result
        elif processing_lines and clean_line:
            line_split = clean_line.split(':')
            raw_name = line_split[0]
            if len(line_split) > 1 and line_split[1] and raw_name in OUTPUT_MAPPING:
                value_split = line_split[1].strip().split('(')
                clean_value = value_split[0].rstrip('s').rstrip()
                final_value = float(clean_value) if '.' in clean_value else int(clean_value)
                result[OUTPUT_MAPPING[raw_name]] = final_value

    raise ValueError('Could not parse the following output:\n{}'.format(to_parse))


def teardown(test_repo: Dolt):
    logger.info('Stopping SQL server')
    test_repo.sql_server_stop()


def write_results_to_dolt(test_repo: Dolt,
                          results: List[dict],
                          username: str,
                          note: str,
                          remote: str,
                          branch: str):
    metadata = {
        'database': 'dolt',
        'username': username,
        'note': note,
        'timestamp': datetime.now(),
        'dolt_version': '0.19.2',
        'system_info': get_os_detail()
    }
    to_insert = pd.DataFrame([{**metadata, **row} for row in results])
    table_writer = get_df_table_writer(RESULTS_TABLE, lambda: to_insert, RESULTS_TABLE_PKS, import_mode='update')
    loader = get_dolt_loader(table_writer, True, 'benchmark run', branch)
    load_to_dolthub(loader, clone=True, push=True, remote_name='origin', remote_url=remote)


def get_os_detail():
    return '{}-{}-{}'.format(os.name, platform.system(), platform.release())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tests', type=str, required=True)
    parser.add_argument('--print-results', action='store_true', default=False)
    parser.add_argument('--write-results-to-dolthub', action='store_true', default=False)
    parser.add_argument('--remote-results-db', type=str, required=False)
    parser.add_argument('--remote-results-db-branch', type=str, required=False)
    parser.add_argument('--username', type=str, required=False, default=getpass.getuser())
    parser.add_argument('--note', type=str, required=False, default=None)
    args = parser.parse_args()
    test_list = args.tests.split(',')
    assert all(test in SUPPORTED_BENCHMARKS for test in test_list), 'Must provide list of supported tests'
    test_db = setup()
    logger.info('Executing the following tests in sysbench: {}'.format(test_list))
    results = test_loop(test_list, test_db, args.print_results)
    if args.write_results_to_dolthub:
        logger.info('Writing the results of the tests')
        # This is a case where we really should be running a remote service
        write_results_to_dolt(test_db,
                              results,
                              args.username,
                              args.note,
                              args.remote_results_db,
                              args.remote_results_db_branch)

    teardown(test_db)


if __name__ == '__main__':
    main()
import argparse
import getpass
import logging
import os
import platform
import tempfile
from datetime import datetime
from subprocess import Popen, PIPE
from typing import List, Tuple
from doltpy.core import Dolt
import csv


logger = logging.getLogger(__name__)

# This is the list of benchmarks that we have validated can successfully run with Dolt
SUPPORTED_BENCHMARKS = [
    'bulk_insert'
]

TEST_TABLE = 'sbtest1'

RESULTS_TABLE = 'sysbench_benchmark'
OUTPUT_MAPPING = {
    'read': 'sql_read_queries',
    'write': 'sql_write_queries',
    'other': 'sql_other_queries',
    'total': 'sql_total_queries',
    'transactions': 'sql_transactions',
    'ignored errors': 'sql_ignored_errors',
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


def test_loop(test_list: List[str], test_repo) -> List[dict]:
    """
    This is the main loop for running the tests and collecting the output
    :param test_list:
    :return:
    """
    result = []
    for test in test_list:
        try:
            test_output = run_test(test_repo, test)
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
        'sysbench',
        test,
        '--table-size=1000000',
        '--db-driver=mysql',
        '--mysql-db={}'.format(test_repo.repo_name),
        '--mysql-user=root',
        '--mysql-host=127.0.0.1',
    ]

    # Prepare the test
    prepare_exitcode, prepare_output = _execute(sysbench_args + ['prepare'], os.getcwd())
    if prepare_exitcode != 0:
        logger.error(prepare_output)
        raise SysbenchFailureException(test, 'prepare', prepare_output)

    # Run the test
    run_exitcode, run_output = _execute(sysbench_args + ['run'], os.getcwd())
    if run_exitcode != 0:
        logger.error(run_output)
        raise SysbenchFailureException(test, 'run', prepare_output)

    return run_output


def _execute(args: List[str], cwd: str):
    proc = Popen(args=args, cwd=cwd, stdout=PIPE, stderr=PIPE)
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


def get_os_detail():
    return '{}-{}-{}'.format(os.name, platform.system(), platform.release())


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--committish', help='Commit used to build Dolt bianry being tested', required=True)
    parser.add_argument('--tests', help='List of benchmarks', type=str, required=True)
    parser.add_argument('--username', type=str, required=False, default=getpass.getuser())
    parser.add_argument('--note', type=str, required=False, default=None)
    return parser.parse_args()


def write_output_file(committish: str, username: str, output: List[dict]):
    if not os.path.exists('output'):
        os.mkdir('output')
    output_file = 'output/{}.csv'.format(committish)
    logger.info('Writing output file to {}'.format(output_file))
    with open(output_file, 'w', newline='') as csvfile:
        metadata = {
            'database': 'dolt',
            'username': username,
            'committish': committish,
            'timestamp': datetime.now(),
            'system_info': get_os_detail()
        }
        fieldnames = list(metadata.keys()) + ['test_name'] + list(OUTPUT_MAPPING.values())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in output:
            to_write = {**row, **metadata}
            writer.writerow(to_write)


def main():
    args = get_args()
    test_list = args.tests.split(',')
    assert all(test in SUPPORTED_BENCHMARKS for test in test_list), 'Must provide list of supported tests'
    test_db = setup()
    logger.info('Executing the following tests in sysbench: {}'.format(test_list))
    results = test_loop(test_list, test_db)
    write_output_file(args.committish, args.username, results)


if __name__ == '__main__':
    main()

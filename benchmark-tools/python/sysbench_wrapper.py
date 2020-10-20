import argparse
import getpass
import logging
import os
import platform
from datetime import datetime
from subprocess import Popen, PIPE
from typing import List, Optional
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


def main():
    args = get_args()
    test_list = args.tests.split(',')
    assert all(test in SUPPORTED_BENCHMARKS for test in test_list), 'Must provide list of supported tests'
    if args.committish:
        logger.info('Committish provided, benchmarking Dolt')
        run_dolt_benchmarks(args.db_host, args.committish, args.username, test_list)
    else:
        logger.info('No committish provided, benchmarking MySQL')
        run_mysql_benchmarks(args.db_host, args.username, test_list)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db-host', help='The host for the database we will connect to')
    parser.add_argument('--committish', help='Commit used to build Dolt bianry being tested')
    parser.add_argument('--tests', help='List of benchmarks', type=str, required=True)
    parser.add_argument('--username', type=str, required=False, default=getpass.getuser())
    parser.add_argument('--note', type=str, required=False, default=None)
    return parser.parse_args()


def run_dolt_benchmarks(test_db_host: str, committish: str, username: str, test_list: List[str]):
    logger.info('Executing the following tests in sysbench against Dolt: {}'.format(test_list))
    results = test_loop(test_db_host, test_list, 'test')
    write_output_file('dolt', committish, username, results)


def run_mysql_benchmarks(test_db_host: str, username: str, test_list: List[str]):
    logger.info('Executing the following tests in sysbench against MySQL: {}'.format(test_list))
    results = test_loop(test_db_host, test_list, 'test')
    write_output_file('mysql', None, username, results)


def test_loop(test_db_host: str, test_list: List[str], test_db: str) -> List[dict]:
    """
    This is the main loop for running the tests and collecting the output
    :param test_list:
    :return:
    """
    result = []
    for test in test_list:
        try:
            test_output = run_test(test_db_host, test_db, test)
            cur_test_res = parse_output(test_output)
            cur_test_res['test_name'] = test
            result.append(cur_test_res)

        except SysbenchFailureException as e:
            logger.error('Test {} failed to produce output, moving in, error was:\n'.format(test, e))

        except ValueError as e:
            logger.error('Failure caused by failure to parse output:\n{}'.format(e))

    return result


def run_test(test_db_host: str, test_db: str, test: str) -> str:
    sysbench_args = [
        'sysbench',
        test,
        '--table-size=1000000',
        '--db-driver=mysql',
        '--mysql-db={}'.format(test_db),
        '--mysql-user=root',
        '--mysql-host={}'.format(test_db_host),
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


def write_output_file(database_name: str, committish: Optional[str], username: str, output: List[dict]):
    if not os.path.exists('/output'):
        os.mkdir('/output')
    output_file = '/output/{}.csv'.format(committish if committish else database_name)
    logger.info('Writing output file to {}'.format(output_file))
    with open(output_file, 'w', newline='') as csvfile:
        metadata = {
            'database': database_name,
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


if __name__ == '__main__':
    main()

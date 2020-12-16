import sys
import csv

from math import fabs

def average_time(row):
    return float(row['latency_sum']) / float(row['sql_transactions'])

def read_result_data(filename, tests):
    mysql_result_data = {}
    dolt_result_data = {}
    with open(filename) as f:
        csvr = csv.DictReader(f)
        for row in csvr:
            test_name = row['test_name']
            if 'all' in tests or test_name in tests:
                if row['database'] == 'dolt':
                    dolt_result_data[test_name] = average_time(row)
                else:
                    mysql_result_data[test_name] = average_time(row)

    return mysql_result_data, dolt_result_data

initial_result_file = sys.argv[1]
updated_result_file = sys.argv[2]
test_names = sys.argv[3] if len(sys.argv) >= 4 else "all"

initial_mysql, initial_dolt = read_result_data(initial_result_file, test_names)
updated_mysql, updated_dolt = read_result_data(updated_result_file, test_names)

print("initial mysql", initial_mysql, "initial dolt", initial_dolt)
print("updated mysql", updated_mysql, "updated dolt", updated_dolt)
for name, time in initial_dolt.items():
    if name in updated_dolt:
        updated_time = updated_dolt[name]
        delta = time - updated_time
        initial_mysql_multiplier = time / initial_mysql[name]
        updated_mysql_multiplier = updated_time / updated_mysql[name]
        percent_change = 1.0 - (updated_time / time)
        faster_slower = "faster" if percent_change > 0.0 else "slower"

        print("% -24s: %.2f%% %s - mysql multiplier: %.2fx -> %.02fx" % (name, fabs(percent_change)*100, faster_slower, initial_mysql_multiplier, updated_mysql_multiplier))
    else:
        print("% -24s:  %4.4f - Test removed from updated result file" % (name, float(time)))

for name, time in updated_dolt.items():
    if name not in initial_dolt:
        print("% -24s:  %4.4f - New test addeed to updated result file" % (name, float(time)))




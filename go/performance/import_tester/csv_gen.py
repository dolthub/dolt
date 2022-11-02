import sys
import json
import uuid
import string
import random

random.seed(0)

# this is dramatically faster than generating a random string for every row. Generate a buffer
# then select random portions of the buffer.
random_string_buffer_size = 128*1024
letters = string.ascii_lowercase + string.ascii_uppercase
random_string_buffer = ''.join(random.choice(letters) for i in range(random_string_buffer_size))

def sequential_int(row_count, col):
    i = 0
    def f():
        nonlocal i
        x = i
        i += 1
        return str(x)

    return f

def shuffled_sequential_int(row_count, col):
    ids = list(range(row_count))
    random.shuffle(ids)
    i = 0
    def f():
        nonlocal i
        n = ids[i]
        i += 1
        return str(n)

    return f

def random_int(row_count, col):
    min_val = 0
    max_val = 2147483647
    def f():
        return str(random.randint(min_val, max_val))

    return f

def random_uuid(row_count, col):
    def f():
        return '"' + str(uuid.uuid4()) + '"'

    return f

def random_float(row_count, col):
    min_val = 0.0
    max_val = 1.0
    delta = max_val - min_val

    def f():
        fl = random.random()
        fl *= delta
        fl += min_val
        return str(fl)

    return f

def random_string(row_count, col):
    max_length = 512
    def f():
        length = random.randint(0, max_length)
        start = random.randint(0, random_string_buffer_size-length)
        return '"' + random_string_buffer[start:start+length] + '"'
    return f


generator_methods = {
    "int": {"random": random_int, "sequential": sequential_int, "shuffled_sequential": shuffled_sequential_int},
    "uuid": {"random": random_uuid},
    "string": {"random": random_string},
    "float": {"random": random_float},
}

def gen_col_methods(row_count, cols):
    names = []
    methods = []
    for col in cols:
        name = col['name']
        typ = col['type']
        generator = "random"
        if "generator" in col:
            generator = col['generator']

        if typ not in generator_methods:
            print("unknown column type '%s' for column '%s'", name, typ)
            sys.exit(1)

        generator_methods_for_type = generator_methods[typ]
        if generator not in generator_methods_for_type:
            print("'%s' is not a valid generator type for column '%s'", generator, name)

        names.append(col['name'])
        methods.append(generator_methods_for_type[generator](row_count, col))

    return names, methods

if len(sys.argv) != 2:
    print("""python csv_gen.py '{
    "cols": [
        {"name":"pk", "type":"int", "generator":"sequential"},
        {"name":"c1", "type":"uuid"},
        {"name":"c2", "type":"string", "length":512},
        {"name":"c3", "type":"float"},
        {"name":"c4", "type":"int"}
    ],
    "row_count": 1000000,
}'""")
    sys.exit(1)

spec_json = json.loads(sys.argv[1])
row_count = spec_json['row_count']
headers, col_methods = gen_col_methods(row_count, spec_json['cols'])
print(','.join(headers))


flush_interval = 1000
lines = []
for i in range(row_count):
    cols = []
    for m in col_methods:
        v = m()
        cols.append(v)
    lines.append(','.join(cols))

    if i % flush_interval == 0:
        print('\n'.join(lines))
        lines = []

if len(lines) != 0:
    print('\n'.join(lines))

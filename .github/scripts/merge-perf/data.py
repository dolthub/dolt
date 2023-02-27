import os
import shutil
import sys
import random

if len(sys.argv) != 5:
    print("usage: python3 data.py <output-dir> <table-num> <row-num> <add-num>")
    sys.exit(1)

table_dir = sys.argv[1]
tables = int(sys.argv[2])
rows = int(sys.argv[3])

adds = int(sys.argv[4])

if __name__=="__main__":
    if not os.path.exists(table_dir):
        shutil.rmtree(table_dir, ignore_errors=True)
        os.makedirs(table_dir)

    ys = [i for i in range(rows+adds)]
    random.shuffle(ys)

    with open(f"{table_dir}/create.sql", "+w") as f:
        for i in range(tables):
            if i == 0:
                f.write(f"create table table{i} (x int primary key, y int, z int, key y_idx(y));\n")
            else:
                f.write(f"create table table{i} (x int primary key, y int, z int, key y_idx(y), foreign key (y) references table{i-1}(y));\n")


    for j in range(tables):
        with open(f"{table_dir}/table{j}.csv", "+w") as f:
            f.write("x,y,z\n")
            for i in range(rows):
                f.write(f"{i},{ys[i]},{i}\n")

    with open(f"{table_dir}/branch.sql", "+w") as f:
        for i in range(tables):
            f.write(f"set foreign_key_checks = 0;\n")
            f.write(f"set unique_checks = 0;\n")
            f.write(f"insert into table{i} values\n")
            for j,k in enumerate(ys[rows:rows+adds]):
                if j == 0:
                    f.write(f" ")
                else:
                    f.write(f", ")
                f.write(f"({rows+j},{k},{rows+j})")
            f.write(f";\n")
    with open(f"{table_dir}/diverge_main.sql", "+w") as f:
        for i in range(tables):
            f.write(f"set foreign_key_checks = 0;\n")
            f.write(f"set unique_checks = 0;\n")
            f.write(f"insert into table{i} values\n")
            for j,k in enumerate(ys[rows:rows+adds]):
                if j == 0:
                    f.write(f" ")
                else:
                    f.write(f", ")
                f.write(f"({rows+j},{k+1},{rows+j})")
            f.write(f";\n")

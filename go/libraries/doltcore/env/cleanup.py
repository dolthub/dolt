import sys
import os
from os import path

switches = {}
args = []
for arg in sys.argv[1:]:
    if arg.startswith('--'):
        switches[arg[2:]] = True
    else:
        args.append(arg)

if len(args) == 1:
    os.chdir(args[0])

if not path.exists('manifest'):
    print("No manifest.")
    print("usage: python noms_cleanup.py [--all-clear] <dir>")
    print()
    print("\t<dir> the noms directory containing a manifest that should be cleaned.  If not provided the current directory is used.")
    print("")
    sys.exit(1)

with open('manifest') as f:
    man_contents = f.read()

tokens = man_contents.split(":")
times = []
referenced_file_count = 0
file_to_count = {}
for i in range(4, len(tokens), 2):
    curr = tokens[i]
    count = tokens[i + 1]
    file_to_count[curr] = count
    time = os.path.getmtime(curr)
    times.append(time)
    referenced_file_count += 1

times.sort()
oldest = times[0]

deleted_files = 0
left_files = 0

if 'all-clear' in switches:
    for dirpath, dnames, fnames in os.walk("./"):
        for fname in fnames:
            if len(fname) == 32:
                if fname not in file_to_count:
                    deleted_files += 1
                    print("removing " + fname)
                    os.remove(fname)
                else:
                    left_files += 1
                    print("leaving " + fname)
else:
    for dirpath, dnames, fnames in os.walk("./"):
        for fname in fnames:
            if len(fname) == 32:
                mt = os.path.getmtime(fname)
                if mt < oldest:
                    deleted_files += 1
                    print("removing " + fname)
                    os.remove(fname)
                else:
                    left_files += 1
                    print("leaving " + fname)

print("table file count: " + str(deleted_files+left_files))
print("files referenced by manifest: " + str(referenced_file_count))
print("deleted: " + str(deleted_files))
print("remaining: " + str(left_files))

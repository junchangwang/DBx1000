
from calendar import c
from multiprocessing.pool import ApplyResult
from numbers import Rational
from operator import index
import queue
from re import A, U
import shutil
import sys
import os


ROOT_PATH = os.getcwd()

ossystem = os.system

def analyser(filename):
    f = open(filename)
    ret = []
    cache_references = []
    cache_misses = []
    cycles = []
    instructions = []
    branches = []
    branch_misses = []
    page_faults = []
    cpu_migrations = []
    seconds = []
    for line in f:
        line = line.replace(',','')
        line = line.replace('<not counted>','0')
        a = line.split()
        if (len(a) < 2):
            continue
        if a[1] == 'cache-references':
            cache_references.append(float(a[0]))
        if a[1] == 'cache-misses':
            cache_misses.append(float(a[0]))
        if a[1] == 'cycles':
            cycles.append(float(a[0]))
        if a[1] == 'instructions':
            instructions.append(float(a[0]))
        if a[1] == 'branches':
            branches.append(float(a[0]))
        if a[1] == 'branch-misses':
            branch_misses.append(float(a[0]))
        if a[1] == 'page-faults':
            page_faults.append(float(a[0]))
        if a[1] == 'cpu-migrations':
            cpu_migrations.append(float(a[0]))
        if a[1] == 'seconds':
            seconds.append(float(a[0]))
    if len(cache_references) != 0:
        ret.append(sum(cache_references) / len(cache_references)) 
    else:
        ret.append(0)
    if len(cache_misses) != 0:
        ret.append(sum(cache_misses) / len(cache_misses)) 
    else:
        ret.append(0)
    if len(cycles) != 0:
        ret.append(sum(cycles) / len(cycles)) 
    else:
        ret.append(0)
    if len(instructions) != 0:
        ret.append(sum(instructions) / len(instructions)) 
    else:
        ret.append(0)
    if len(branches) != 0:
        ret.append(sum(branches) / len(branches)) 
    else:
        ret.append(0)
    if len(branch_misses) != 0:
        ret.append(sum(branch_misses) / len(branch_misses)) 
    else:
        ret.append(0)
    if len(page_faults) != 0:
        ret.append(sum(page_faults) / len(page_faults)) 
    else:
        ret.append(0)
    if len(cpu_migrations) != 0:
        ret.append(sum(cpu_migrations) / len(cpu_migrations)) 
    else:
        ret.append(0)
    if len(seconds) != 0:
        ret.append(sum(seconds) / len(seconds)) 
    else:
        ret.append(0)
    # print (ret)
    return ret







def main():
    # ret = analyser('perf.output.CUBIT.121774')
    print(str(sys.argv[1]) + " output:")
    ret = analyser(str(sys.argv[1]))
    print('cache-references: ' + str(ret[0]))
    print('cache-misses: ' + str(ret[1]))
    print('cycles: ' + str(ret[2]))
    print('instructions: ' + str(ret[3]))
    print('branches: ' + str(ret[4]))
    print('branch-misses: ' + str(ret[5]))
    print('page-faults: ' + str(ret[6]))
    print('cpu-migrations: ' + str(ret[7]))
    print('seconds time elapsed: ' + str(ret[8]))

    print('Done!\n')



if __name__ == '__main__': 
    main()

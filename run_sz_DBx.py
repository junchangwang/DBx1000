
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


###################################### cmd #########################################
def gen_raw_data(): 
    core_number = [1, 2, 4, 8, 16, 24, 32]
    # core_number = [1, 2, 4]
    for num in core_number:
        cmd = './rundb -t{} > dat_tmp_DBx/core_{}.dat'.format(num, num)
        os.system(cmd)

def throughput_analysis2(filename):
    f = open(filename)
    Svec = [] # scan     item/s
    Hvec = [] # hash
    Bvec = [] # btree
    Cvec = [] # cubit

    ret = []

    for line in f:
        a = line.split()
        if (len(a) != 3):
            continue
        elif (not str(a[-1]).isdigit() or not str(a[-2]).isdigit()):
            continue
        elif line.startswith('SCAN '):
            Svec.append(float(a[-2]) / float(a[-1]) * 1000)
        elif line.startswith('Hash '):
            Hvec.append(float(a[-2]) / float(a[-1]) * 1000)
        elif line.startswith('BTree '):
            Bvec.append(float(a[-2]) / float(a[-1]) * 1000)
        elif line.startswith('CUBIT '):
            Cvec.append(float(a[-2]) / float(a[-1]) * 1000)
        else:
            continue
    if len(Svec) != 0:
        ret.append(sum(Svec) / len(Svec)) 
    else:
        ret.append(0)
    if len(Hvec) != 0:
        ret.append(sum(Hvec) / len(Hvec)) 
    else:
        ret.append(0)
    if len(Bvec) != 0:
        ret.append(sum(Bvec) / len(Bvec)) 
    else:
        ret.append(0)
    if len(Cvec) != 0:
        ret.append(sum(Cvec) / len(Cvec)) 
    else:
        ret.append(0)
    return ret

def throughput_analysis(filename):
    f = open(filename)
    ret = []
    
    for line in f:
        a = line.split()
        if line.startswith('ThroughputScan '):
            ret.append(float(a[-1]))
        elif line.startswith('ThroughputHash '):
            ret.append(float(a[-1]))
        elif line.startswith('ThroughputBTree '):
            ret.append(float(a[-1]))
        elif line.startswith('ThroughputCUBIT '):
            ret.append(float(a[-1]))
        else:
            continue
    return ret

def run():
    gen_raw_data()
    print ('DBx1000 core')
    print ('-' * 10)
    core_number = [1, 2, 4, 8, 16, 24, 32]
    # core_number = [1, 2, 4]
    f = open('dat_DBx/core.dat','w')
    for num in core_number:
        res = throughput_analysis('dat_tmp_DBx/core_{}.dat'.format(num))
        print(res)
        print('\n')
        for tp in res:
            f.write('{} {} \n'.format(num, tp))
    f.close()


def gen_graph():
    os.chdir("gnuplot-scripts")
    #os.system("./check_dat_files.sh")
    #os.system("./prepare_normalized.sh")
    # os.system("rm -r ../graphs")
    os.system("make make_dir_DBx")
    os.system("make figure_DBx_core")
    os.chdir("../graphs_DBx")
    os.system('echo "Figures generated in \"`pwd`\""')
    #os.system('ls -l')

    #cdf
    os.chdir("../")
    os.system('python3 cdf_sz.py > graphs_DBx/cdf_output')


def main():
    os.system('make -j')
    itr = 0
    cmd = 'mv graphs_DBx graphs_DBx_{}'
    cmd2 = "mv dat_DBx graphs_DBx/eva_data"
    cmd3 = "mv dat_tmp_DBx graphs_DBx/eva_data"
    while itr < 1:
        os.chdir(ROOT_PATH)
        datdir = 'dat_DBx'
        if os.path.exists(datdir) and os.path.isdir(datdir):
            print ('Deleting existing directory ./dat')
            shutil.rmtree(datdir)
        os.mkdir(datdir)

        dat_tmp = 'dat_tmp_DBx'
        if os.path.exists(dat_tmp) and os.path.isdir(dat_tmp):
            print ('Deleting existing directory ./dat_tmp')
            shutil.rmtree(dat_tmp)
        os.mkdir(dat_tmp)

        graphs = 'graphs_DBx'
        if os.path.exists(graphs) and os.path.isdir(graphs):
            print ('Deleting existing directory ./graphs')
            shutil.rmtree(graphs)
        os.mkdir(graphs)

        #build()
        run()
        gen_graph()
        os.system(cmd2)
        os.system(cmd3)
        # os.system(cmd.format(itr))
        itr += 1
    print('Done!\n')



if __name__ == '__main__':
    main()

from os import listdir, remove, chdir
import os
from os.path import isfile, join, exists
import re
import statistics as stati
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import  gridspec

dir = os.getcwd()

plt.rcParams['font.family'] = ['Linux Libertine O']

# x
x_val = [1, 2, 4, 8, 16, 24, 32]

# y
Scan = []
Hash = []
B_plus_Tree = []
B_Tree = []
Trie = []
CUBIT = []

Bar_val = []

algorithm_lookup_1 = ['CUBIT',
                      'BTree',
                      'Trie',
                      'Hash',
                      'Scan'
                      ]
                      
algorithm_lookup_2 = ['Hash',
                      'BTree',
                      'Trie',
                      'CUBIT'
                      ]
                      

list_map = {
    'Scan'   :   Scan,
    'Hash'   :   Hash,
    'BTree'  :   B_plus_Tree,
    'CUBIT'  :   CUBIT,
    'Trie'   :   Trie,
    'B-Tree' :   B_Tree
}

label_nm_map = {
    'Scan'   :   'SCAN',
    'Hash'   :   'Hash',
    'BTree'  :   '$\mathregular{B^+}$-Tree ',
    'CUBIT'  :   'Our approach',
    'Trie'   :   'Trie',
    'B-Tree' :   'B-Tree'
}

ls_map = {
    'Scan'   :   (0, (3, 1, 1, 1, 1, 1)),
    'Hash'   :   '-.',
    'BTree'  :   'dashed', 
    'CUBIT'  :   '-',
    'B-Tree' :   '--',
    'Trie'   :   ':'
}

co_map = {
    'Scan'   :   'black',
    'Hash'   :   'g',
    'BTree'  :   'brown',
    'CUBIT'  :   'blue',
    'B-Tree' :   'c',
    'Trie'   :   'r'
}

mark_map = {
    'Scan'   :   'D',
    'Hash'   :   'v',
    'BTree'  :   '^',
    'CUBIT'  :   'o',
    'B-Tree' :   '>',
    'Trie'   :   's'
}


def get_data():
    f = open('graphs_DBx_1/eva_data/dat_DBx/core.dat')
    pos = 0  
    for line in f:
        a = line.split()
        if(pos % 6 == 0): 
            Scan.append(float(a[1]))
        elif(pos % 6 == 1): 
            Hash.append(float(a[1]))
        elif(pos % 6 == 2): 
            B_plus_Tree.append(float(a[1]))
        elif(pos % 6 == 3): 
            B_Tree.append(float(a[1]))
        elif(pos % 6 == 4): 
            Trie.append(float(a[1]))
        elif(pos % 6 == 5): 
            CUBIT.append(float(a[1]))
        pos += 1
    f.close()
    
    f = open('graphs_DBx_1/eva_data/dat_DBx/memory.dat')
    for line in f:
        a = line.split()
        Bar_val.append(float(a[1]) / 1024)
    f.close()


def draw():
    #plt.rcParams['font.size'] = 20
    plt.figure(figsize = (6, 8), tight_layout = True)

    gs = gridspec.GridSpec(2, 1, height_ratios = [1.8, 1], hspace = 0.3)
    ax1 = plt.subplot(gs[0, 0])
    ax2 = plt.subplot(gs[1, 0])
    ax1.tick_params(labelsize = 18)
    ax2.tick_params(labelsize = 18)
    
    #### core plot
    for exp in algorithm_lookup_1:
        ax1.plot(x_val, list_map[exp], marker = mark_map[exp], ms = 11, mfc = 'w', c = co_map[exp], ls = ls_map[exp], lw = 2, label = label_nm_map[exp])
        
    
    ax1.set_ylabel("Throughput (op/s)", fontsize= 20)
    ax1.set_xlabel("(a) Number of cores", fontsize= 20)
    ax1.set_xticks(x_val)
    ax1.legend(loc = 'upper left', fontsize = 16, frameon = False)
    
    #### memory bar
    ax2.set_ylim(0, 2.2)
    ax2.set_yticks([0, 0.5, 1, 1.5, 2])
    
    ax2.bar('Hash', Bar_val[0], lw = 0.8, fc = 'g', width = 0.3) 
    ax2.bar('$\mathregular{B^+}$-Tree ', Bar_val[1], lw = 0.8, fc = 'brown', width = 0.3)
    ax2.bar('Trie', Bar_val[3], lw = 0.8, fc = 'r', width = 0.3)
    ax2.bar('CUBIT', Bar_val[4], lw = 0.8, fc = 'blue', width = 0.3)
    
    ax2.text(0, Bar_val[0] + 0.05, round(Bar_val[0], 2), ha = 'center', va = "bottom", fontsize = 15)
    ax2.text(1, Bar_val[1] + 0.05, round(Bar_val[1], 2), ha = 'center', va = "bottom", fontsize = 15)
    ax2.text(3, Bar_val[4] + 0.05, round(Bar_val[4], 2), ha = 'center', va = "bottom", fontsize = 15)
    
    ax2.set_ylabel("Memory (GB)", fontsize = 20)
    ax2.set_xlabel("(b) algorithm type", fontsize = 20)
              
              
    #plt.show()
    
    plt.savefig(dir + '/graphs_DBx_1/graphs_DBx/mix.eps', format='eps', dpi= 1200)
    
    
    
    
get_data()  
draw()



About DBx1000-CUBIT
-------------------

This project aims to evaluate CUBIT in a DBMS from different angles. In particular, we want to answer the following questions regarding CUBIT.

  - How does CUBIT behave at high concurrency levels?

  - What are the pros and cons of CUBIT on different workloads (i.e., OLAP, HTAP, and OLTP)?

  - How does CUBIT (multi-version aware) cooperate with the CC algorithm of the DBMS and what are the benefits?

With these goals in mind, we choose DBx1000, a row-based DBMS focusing on the scalability of different CC algorithms in DBMS on multicore architectures. The original readme information of DBx1000 can be found in README-DBx1000.md.

We have integrated CUBIT into DBx1000 and added many new features, including:

  - a TPC-H (OLAP) benchmark implementation (in benchmarks/tpch),

  - a CH-benCHmark (HTAP) implementation (in benchmarks/chbench),

  - the state-of-the-art secondary indexes including Bw-Tree and ART (in storage/index_[bwtree/art]) based on the Open Bw-Tree project (https://github.com/wangziqi2016/index-microbench),

  - a faster sequential scan by using embarrassing parallelism on multicores, and

  - a new CC algorithm, named PTMVCC, that cooperates the multi-versioning mechanisms of CUBIT and the DBMS, providing index-only scans (in concurrency_control/row_ptmvcc).

Our experimental results shows that CUBIT-powered DBx1000 achieves 2x - 11x performance improvement for selective queries on any workload with updates.


How to compile and replicate key experiments?
---------------------------------------------

First, please use the following commands to compile CUBIT and DBx1000-CUBIT:

```
Compile CUBIT by using the command "cd ../ && ./build.sh"
Build the DBx1000-CUBIT project by using the command "make"
```

Then, reproduce the key results by using the following command:

```
./python3 run_sz_DBx.py
```

The above command writes the experimental results to a group of folders named graphs_DBx_x, where x is the ID of each trial. We can analyze the results in graphs_x and draw figures by using the following command:

```
./gen_graphs_DBx.sh graphs_DBx_x
```


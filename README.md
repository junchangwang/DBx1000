
About DBx1000-CUBIT
-------------------

This project aims to evaluate CUBIT in an OLTP DBMS from various angles. In particular, we want to answer the following questions regarding CUBIT.

  - How does CUBIT behave at high concurrency levels?

  - What are the pros and cons of CUBIT on different workloads (i.e., OLAP, HTAP, and OLTP)?

  - How does CUBIT, which is multi-version aware, cooperate with the underlying CC algorithms, and what benefits does this provide?

With these goals in mind, we chose DBx1000, a row-based DBMS that focuses on the scalability of various CC algorithms in DBMS on multicore architectures. The original readme information of DBx1000 can be found in README-DBx1000.md.

We have integrated CUBIT into DBx1000 and added many new features, including:

  - a TPC-H (OLAP) benchmark implementation (located in benchmarks/tpch),

  - a CH-benCHmark (HTAP) implementation (located in benchmarks/chbench),

  - the state-of-the-art secondary indexes, including Bw-Tree and ART (in storage/index_[bwtree/art]) based on the Open Bw-Tree project (https://github.com/wangziqi2016/index-microbench),

  - a faster sequential scan by using embarrassing parallelism on multicores, and

  - a new CC algorithm, named PTMVCC, that blends the multi-versioning mechanisms of CUBIT and the DBMS (located in concurrency_control/row_ptmvcc).


How to compile and replicate key experiments?
---------------------------------------------

We maintain this project as a submodule of the CUBIT project. Therefore, please first clone the CUBIT repository from https://github.com/junchangwang/CUBIT , which already includes the DBx1000-CUBIT submodule.


We use the following commands to compile CUBIT and DBx1000-CUBIT:

```
Compile CUBIT by using the command "cd ../ && ./build.sh"
Build the DBx1000-CUBIT project by using the command "make"
```

Next, reproduce the key results by using the following command:

```
./python3 run_DBx.py
```

The above command writes the experimental results to a set of folders named graphs_DBx_x, where x represents the ID of each trial. We can analyze the results in graphs_x and generate figures by using the following command:

```
./gen_graphs_DBx.sh graphs_DBx_x
```


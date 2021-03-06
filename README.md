General Information
-------------------
This is a prototype of a deduplication system called destor.

Features
--------
1. Container-based storage;
2. Chunk-level pipeline;
3. Fixed-sized chunking and Content-Defined Chunking (CDC);
4. A variety of fingerprint indexes, including DDFS, Extreme Binning, Sparse Index, SiLo, etc.
5. A variety of rewriting algorithms, including CFL, CBR, CAP, HAR etc.
6. A variety of restore algorithms, including LRU, optimal replacement algorithm, rolling forward assembly.

Related papers
--------------
1. The design of the fingerprint index:
    > a) Avoiding the Disk Bottleneck in the Data Domain Deduplication File System @ FAST'08.
    >
    > b) Sparse Indexing: Large Scale, Inline Deduplication Using Sampling and Locality @ FAST'09.
    >
    > c) Extreme Binning: Scalable, Parallel Deduplication for Chunk-based File Backup @ MASCOTS'09.
    >
    > d) SiLo: A Similarity-Locality based Near-Exact Deduplicatin Scheme with Low RAM Overhead and High Throughput @ USENIX ATC'11.
    >
    > e) Building a High-Performance Deduplication System @ USENIX ATC'11.
    >
    > f) Block Locality Caching for Data Deduplication @ SYSTOR'13.
    >
    > g) The design of a similarity based deduplication system @ SYSTOR'09.

2. The fragmentation:
    > a) Chunk Fragmentation Level: An Effective Indicator for Read Performance Degradation in Deduplication Storage @ HPCC'11.
    >
    > b) Assuring Demanded Read Performance of Data Deduplication Storage with Backup Datasets @ MASCOTS'12. 
    >
    > c) Reducing impact of data fragmentation caused by in-line deduplication @ SYSTOR'12.
    >
    > d) Improving Restore Speed for Backup Systems that Use Inline Chunk-Based Deduplication @ FAST'13.
    >
    > e) Accelerating Restore and Garbage Collection in Deduplication-based Backup Systems via Exploiting Historical Information @ USENIX ATC'14.

3. The restore cache
    > a) A Study of Replacement Algorithms for a Virtual-Storage Computer @ IBM Systems Journal'1966.
    >
    > b) Improving Restore Speed for Backup Systems that Use Inline Chunk-Based Deduplication @ FAST'13.
    >
    > c) Accelerating Restore and Garbage Collection in Deduplication-based Backup Systems via Exploiting Historical Information @ USENIX ATC'14.

4. Garbage collection (available in first release (Mouse), but to be continue in new version):
    > a) Building a High-Performance Deduplication System @ USENIX ATC'11.
    >
    > b) Cumulus: Filesystem Backup to the Cloud @ FAST'09.
    >
    > c) Accelerating Restore and Garbage Collection in Deduplication-based Backup Systems via Exploiting Historical Information @ USENIX ATC'14.

Recent publications using Destor
-----------------------------
1. **Min Fu**, Dan Feng, Yu Hua, Xubin He, Zuoning Chen, Wen Xia, Fangting Huang, and Qing Liu. *Accelerating Restore and Garbage Collection in Deduplication-based Backup Systems via Exploiting Historical Information* @ USENIX ATC'14.
2. Jian Liu, Yunpeng Chai, Xiao Qin, and Yuan Xiao. *PLC-Cache: Endurable SSD Cache for Deduplication-based Primary Storage* @ MSST'14.

Environment
-----------
Linux 64bit.

Dependencies
------------
1. libssl-dev is required to calculate sha-1 digest;
2. GLib 2.32 or later version 

   > libglib.so and glib.h may not be found when you first install it.

   > The header files (that originally locate in /usr/local/include/glib-2.0) are required to be moved to a searchable path, such as "/usr/local/include". 

   > Also a link named libglib.so should be created in "/usr/local/lib".

3. Makefile is automatically generated by GNU autoconf and automake.

INSTALL
-------
If all dependencies are installed,
compiling destor is straightforward:

>./configure
>
>make
>
>make install

To uninstall destor, run

>make uninstall

Running
-------
If compile and install are successful, the executable file, destor, should have been moved to /usr/local/bin by default.
You can create a config file, destor.config, in where you run destor.
A sample destor.config is in the project directory.
Run rebuild script before destor to make subdirectory and clean data.

destor can run as follows:

1. start a backup task,
   > destor /path/to/data -p"a line as in config file"

2. start a restore task,
   > destor -r<jobid> /path/to/restore -p"a line as in config file"

3. lookup statistics of system,
   > destor -s

4. help
   > destor -h

5. make a trace
   > destor -t /path/to/data

Configuration
-------------
A sample configuration is shown in destor.conf

Bugs
----
1. If the running destor is crashed artificially or unexpectedly, data consistency is not guaranted and you'd better run rebuild script.

2. Do NOT support concurrent backup/restore.

3. If working path in destor.config is modified, the rebuild script must be modified too.

4. CMA assumes the backups are deleted in FIFO order.
    > In any cases, the expired backup is deleted automatically.

Author
------
Min Fu

Email : fumin at hust.edu.cn

Blog : fumin.hustbackup.cn

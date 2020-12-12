# cs434-project
A project repository for POSTECH CSED434 Adv. Programming Fall 2020

### Author
**Jinho Ko (20180274)**, jinho.ko AT postech.ac.kr

### Project Name 
**DPsort** : A Distributed, Parallel Sorting MergeSort System

### Project Goal
**Distributed/Parallel** sorting K/V records stored on **multiple directories** and **multiple machines**.

### Execution Guide

#### Master
```
$ bash bin/master NUM_WORKERS
```

#### Worker
```
$ bash bin/worker M_IP:M_PORT -I I_DIR1 I_DIR2 I_DIR3 ...  -O O_DIR
```
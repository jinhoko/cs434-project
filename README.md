# cs434-project
A project repository for POSTECH CSED434 Adv. Programming Fall 2020

### Author
**Jinho Ko (20180274)**, dept. of Creative IT Eng. 

### Project Name 
**DPsort** : A Distributed, Parallel Sorting Mergesort System

### Project Goal
**Distributed/Parallel** sorting K/V records stored on **multiple disks** and **multiple machines**.

### Execution Guide

#### Compile and Generating Assembly
```$xslt
$ sbt compile
$ sbt assembly
```

#### Execution
```$xslt
$ bash bin/master 3
$ bash bin/worker M_IP:M_PORT -I I_DIR1 I_DIR2 I_DIR3 -O O_DIR
```
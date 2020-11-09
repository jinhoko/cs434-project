# cs434-project
A project repository for POSTECH CSED434 Adv. Programming Fall 2020

### Author
**Jinho Ko (20180274)**, dept. of Creative IT Eng. 

### Project Name 
**DPsort** : A Distributed, Parallel Sorting Library

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

### Project Schedule and Progress
| idx | Name                 | Due               | Status      | Comment |
|-----|----------------------|-------------------|-------------|---------|
| 1   | Project milestone 1  | 10월 19일 11:59pm | Finished     |         |
| 2   | Project milestone 2  | 11월 16일 11:59pm | On Progress |         |
| 3   | Project final report | 12월 14일 11:59pm | Not started |         |

*(recent update : 10 Nov )*


# README.md

### 1. Docker pull
```
sudo docker pull hseeberger/scala-sbt:8u222_1.3.5_2.13.1
```

### 2. Docker run
```
sudo docker run -itd -p 40000-40020:40000-40020 --name test-container hseeberger/scala-sbt:8u222_1.3.5_2.13.1
```

### 3. Execute
```
sudo docker exec -it test-container /bin/bash
```

### 4. git clone
```
git clone https://github.com/dgggit/cs434-project
```

### 5. generate sort data
```
> gensort -b0 -a 1000000 input-1m-00000
> gensort -b1000000 -a 1000000 input-1m-00001
> gensort -b2000000 -a 1000000 input-1m-00002
...
```

### 6. execute

### 7. validate sort
```
Example 1 - to validate the sorted order of a single sort output file:
    valsort sortoutputfile

Example 2 - to validate the sorted order of output that has been
partitioned into 4 output files: out0.dat, out1.dat, out2.dat and out3.dat:
    valsort -o out0.sum out0.dat
    valsort -o out1.sum out1.dat
    valsort -o out2.sum out2.dat
    valsort -o out3.sum out3.dat
    cat out0.sum out1.sum out2.sum out3.sum > all.sum
    valsort -s all.sum
```

### Reference
- gensort library : http://www.ordinal.com/gensort.html
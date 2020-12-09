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
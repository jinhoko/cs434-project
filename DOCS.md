# Documentation

## sbt

#### Basics
```
sbt compile
sbt run                      # runs project in root
sbt "run project master"     # runs project PNAME 
sbt "run project PNAME"
sbt test
> show dependencyClasspath   # shows dependency tree
> project PNAME              # set sub-project to PNAME(/,master,worker)
```
Seek for `/target` to check compiled binaries.  
Refer to [here](https://www.scala-sbt.org/1.x/docs/index.html) for `sbt` docs.

#### Compile dependency tree
```
> inspect tree compile
```

#### plugin : dependency-graph-visualization
```
> dependencyTree
```

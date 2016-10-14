# Sample spark Application to write data in ORC format

### build 
```
mvn clean package
```

### run using spark-submit
```
./bin/spark-submit --class SparkOrcWriter --verbose --master yarn-client --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 /tmp/ssa-1.1.0-1.6-s_2.10-SNAPSHOT.jar 
```

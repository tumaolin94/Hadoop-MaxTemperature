# Hadoop-MaxTemperature

## Using Method
1. local run
```
% mvn compile
% export HADOOP_CLASSPATH=target/classes/
% hadoop mvnHadoop.MaxTemperatureDriver -conf conf/hadoop-local.xml \
input/ncdc/micro output

OR

% hadoop mvnHadoop.MaxTemperatureDriver -fs file:/// -jt local input/ncdc/micro output

```

2. run on HDFS
```
% mvn package -DskipTests
% hadoop jar hadoop-examples.jar mvnHadoop.MaxTemperatureDriver \
-conf conf/hadoop-localhost.xml airtemperature max-temp

```

3. DEBUG
```
% hadoop jar hadoop-examples.jar LoggingDriver -conf conf/hadoop-localhost.xml \
-D mapreduce.map.log.level=DEBUG input/ncdc/sample.txt logging-out
```

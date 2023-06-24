# FENIO

## build with sbt

clean old build

```bash
sbt clean
```

build fat jar

```bash
sbt assembly
```

spark job submit

```bash 
bash /usr/local/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2 \
        --class com.linhnm.Main \
        --master yarn \
        fenio.jar
```

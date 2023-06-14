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
        --class com.linhnm.Main \
        --master yarn \
        fenio.jar
```

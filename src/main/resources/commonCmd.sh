# spark-shell
spark-shell --master local[*] --jars code.jar --packages "org.example:example:0.1"

# spark-submit
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 1G \
    --executor-memory 4G \
    --num-executors 4 \
    --executor-cores 2 \
    --conf spark.default.parallelism=200 \
    --conf spark.yarn.executor.memoryOverhead=2048 \
    --conf spark.storage.memoryFraction=0.5 \
    --conf spark.shuffle.memoryFraction=0.3 \
    --class baseOpt.SparkRDDTest

# spark		
` ./spark-submit --class sparktest.SparkWC` `--master spark://localhost:7077 --executor-memory 1g` `--total-executor-cores 2` `/Users/rikka/myjar/myspark.jar` `hdfs://localhost:9000/spark/input/words.txt` `hdfs://localhost:9000/spark/output `
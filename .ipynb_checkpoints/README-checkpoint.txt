# Project Steps:

### Zookeeper and kafka configuration:
    
    We start by changing the files in the config folder in order to prepare the kafka & zookeeper env.
    
    We add the following configuration to the kafka server properties in order to handle the replication error due to the fact that we only have one broker:
    
    * offsets.topic.replication.factor=1

### Run the programe:
    
    * In order to start the zookeeper, kafka and install the needed packages please run the commands in the commands.sh file
    * Install the net-tools packages and execute the following command:
        * netstat -nltp
        * Check if the ports used for zookeeper and kafka figure in the list of the used ports.
    * Run the kafka server in order to push the data of the json file to a kafka topic.
    * Run the data_stream.py file using the following command:
        * spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py
    * Access the Spark UI in order to check out the jobs.
    * You can check the kafka server in the consumer folder

### Question:
    
    1) How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
    
    I believe that changing "inputRowsPerSecond" & "processedRowsPerSecond" affect the throughput and latency of the data and that is based on the official documentation of spark
        * param: inputRowsPerSecond The rate at which data is arriving from this source.
        * param: processedRowsPerSecond The rate at which data from this source is being processed by Spark.
    
    2) What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?
    
        * In order to optimize the data_stream code, we need to handle the maximum load that our current machine(container) can handle so as to not waste ressources especially if we are in a distributed env.
        * For that reason the following properties are the one that we should care about:
            => spark.default.parallelism: Default number of partitions in RDDs returned by transformations like join, reduceByKey, and parallelize when not set by user.
         * Due to the following quote "Clusters will not be fully utilized unless you set the level of parallelism for each operation high enough"
        * And if we modify the parallelism we need to modify the param in relation to the partition:
            => spark.sql.shuffle.partitions: Configures the number of partitions to use when shuffling data for joins or aggregations.
    
### Sources:

    * These are the sources used while answering this project.
    --kafka:
    https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
    --json:
    https://docs.python.org/3/library/json.html
    --spark:
    https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=structfield#pyspark.sql.types.StructField
    https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html
    https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/streaming/SourceProgress.html#processedRowsPerSecond
    https://spark.apache.org/docs/latest/tuning.html
    https://spark.apache.org/docs/latest/configuration.html
    
### Pip:
    
    * Use the pip_fix folder and it's bash in order to fix pip.
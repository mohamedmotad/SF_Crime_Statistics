# Project Steps:

### Zookeeper and kafka configuration:
    
    We start by changing the files in the config folder in order to prepare the kafka & zookeeper env.
    
    We add the following configuration to the kafka server properties in order to handle the replication error due to the fact that we only have one broker:
    
    * offsets.topic.replication.factor=1

### Run the programe:
    
    * In order to start the zookeeper, kafka and install the needed packages please run the commands in the demarage.sh file
    * Install the net-tools packages and execute the following command:
        * netstat -nltp
        * Check if the ports used for zookeeper and kafka figure in the list of the used ports.
    * Run the kafka server in order to push the data of the json file to a kafka topic.
    * Run the data_stream.py file using the following command:
        * spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py
    * Access the Spark UI in order to check out the jobs.


import producer_server


def run_kafka_server():
	# TODO get the json file path
    input_file = "/home/workspace/police-department-calls-for-service.json"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="05_sf_crime_data",
        bootstrap_servers="localhost:9092",
        client_id="01_sf_crime_data"
    )
    print("Returning the producer")
    return producer


def feed():
    print("Starting the kafka server")
    producer = run_kafka_server()
    print("Generating the data")
    producer.generate_data()


if __name__ == "__main__":
    feed()

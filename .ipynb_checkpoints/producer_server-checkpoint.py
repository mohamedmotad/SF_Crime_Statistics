import json
import time
from kafka import KafkaProducer

class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #TODO we're generating a dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            # need to lad the json correctly otherwise the file will be read line by line
            json_file = json.load(f)
            for line in json_file:
                #print(line)
                #time.sleep(1)
                message = self.dict_to_binary(line)
                #print(message)
                #time.sleep(1)
                # TODO send the correct data
                self.send(self.topic, message)
                time.sleep(1)
        print("Finished sendig the file")
        print("Exiting")
        exit(0)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        #print(f"Before: {json_dict}")
        json_str = json.dumps(json_dict)
        json_bin = json_str.encode('utf-8')
        #print(f"After: {json_bin}")
        return json_bin
        
import os
import getpass
import logging
from datetime import date

# Date:
today = date.today()
current = today.strftime("%d-%m-%Y")

# General:
encoding = "utf-8"
logging_level = logging.INFO

# Kakfa
group_id = "05_kafka_consumer"
topic_name="05_sf_crime_data"
broker_url = "localhost:9092"

# Local:
logger_file_mode="a"
local_root=os.path.dirname(os.path.abspath(__file__)) + "/"
local_log_folder=f"{local_root}/logs/"
local_log_file=f"{local_log_folder}{current}-{topic_name}.log"
import time
import logging 
from x_common.bigquery.bigquery_client import get_data
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

class Statistics:
    def __init__(self, day, task):
        self.day = day
        self.task = task
        self.processing_time_all = 0
        self.start_timestamp = datetime(1, 1, 1, 0, 0, 0)
        self.end_timestamp = datetime(1, 1, 1, 0, 0, 0)
        self.processing_time_get_data = 0
        self.processing_time_write_db = 0
        self.api_calls = 0
        self.url_calls = 0
        self.number_rejected_elements = 0
        self.number_filtered_out_elements = 0
        self.number_of_written_elements = 0

    def increment_url_calls(self, step = 1):
        self.url_calls += step
    
    def increment_api_calls(self, step = 1):
        self.api_calls += step
    
    def update_processing_time_get_data(self, start_time, end_time):
        time_chunk = end_time - start_time
        self.processing_time_get_data += time_chunk

    def update_processing_time_write_db(self, start_time, end_time):
        time_chunk = end_time - start_time
        self.processing_time_write_db += time_chunk
    
    def update_number_filtered_out_elements(self, number):
        self.number_filtered_out_elements += number
    
    def update_number_rejected_elements(self, number):
        self.number_rejected_elements += number

    def update_number_of_written_elements(self, number):
        self.number_of_written_elements += number
    
    def update_processing_time_all(self, start_time, end_time):
        self.processing_time_all = end_time - start_time
    
    def update_start_time(self, time):
        self.start_timestamp = time
    
    def update_end_time(self, time):
        self.end_timestamp = time
    
    def show_all_stats(self):
        logger.info(f"Day: {self.day}")
        logger.info(f"Task: {self.task}") 
        logger.info(f"Time the whole processing took: {self.processing_time_all}") 
        logger.info(f"Processing time for writing into database: {self.processing_time_write_db}")
        logger.info(f"Processing time only for data reading processing: {self.processing_time_get_data}")
        logger.info(f"Number written elements: {self.number_of_written_elements}") 
        logger.info(f"Number of filtered out elements: {self.number_filtered_out_elements}")
        logger.info(f"Number of rejected elements: {self.number_rejected_elements}")
        logger.info(f"Number of api calls: {self.api_calls}")
        logger.info(f"Number of url calls: {self.url_calls}")

    def write_stats_to_db(self, test_mode):
        data = [{ 
            "day": str(self.day),
            "task": self.task,
            "processing_time_all": self.processing_time_all,
            "start_timestamp": str(self.start_timestamp.strftime('%Y-%m-%d %H:%M:%S')),
            "end_timestamp": str(self.end_timestamp.strftime('%Y-%m-%d %H:%M:%S')),
            "processing_time_get_data": self.processing_time_get_data,
            "processing_time_write_database": self.processing_time_write_db, 
            "api_calls": self.api_calls,
            "url_calls": self.url_calls,
            "number_filtered_out_elements": self.number_filtered_out_elements,
            "number_of_written_elements": self.number_of_written_elements,
            "number_of_rejected_elements": self.number_rejected_elements
        }]
        if test_mode:
            get_data("test_etl_program_statistics", data, dataset_id = "test")
        else:
            get_data("dev_etl_program_statistics", data)
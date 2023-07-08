import csv
import os
from helpers.worker import UnicefWorker

class UnicefMain():

    def __init__(self) -> None:

        self.headers:list = []
        self.reader:csv=None

    def load_csv(self) -> None:

        real_path = os.path.realpath(__file__)
        dir_name = os.path.dirname(real_path)
        csv_path = os.path.join(dir_name, 'data.csv')
        
        with open(csv_path, 'r') as file:
            csv_reader = csv.reader(file, delimiter=',')
            records = list(csv_reader)
        worker = UnicefWorker(records)
        worker.trigger()
            
    def clean_headers(self, headers:list) -> None:
        for col in headers:
            parts = col.split(':')
            col = parts[1]
            self.headers.append(col)
        
if __name__=="__main__":
    unicef = UnicefMain()
    unicef.load_csv()
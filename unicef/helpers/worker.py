from threading import Thread
from queue import Queue
import psycopg2
from psycopg2.errors import UniqueViolation, InFailedSqlTransaction


class UnicefWorker():
    """ A worker class that commits the retrieve records to the database """

    queue = Queue()
   
    def __init__(self, records:list) -> None:
        self.records = records
        self.conn =self.connect_db()
        self.cur = self.conn.cursor()
        self.create_table()

    def trigger(self):
        """ Should create db and wait for data """

        unicef = UnicefWorkerScheduler(self.records, self.queue)
        p_thread = Thread(target=unicef.process)
        p_thread.start()

        while True:

            country_row = self.queue.get()

            if country_row is None:
                self.close()
                break

            country = country_row[1:]
            self.save_data(*country)
        
        
        
    def connect_db(self):
        """ Connects to the database """
        conn = psycopg2.connect(
            database="unicef_db",
            user="postgres",
            password="1234",
            host="localhost"
        )
        return conn
    
  
    def create_table(self):
        """ Creates the database table """

        try:
            query = """CREATE TABLE IF NOT EXISTS unicef_data(
                    id SERIAL PRIMARY KEY,
                    geography varchar(200),
                    indicator varchar(200),
                    sex varchar(200),
                    age varchar(200),
                    residence varchar(255),
                    education varchar(200),
                    period varchar(200),
                    observation varchar(200),
                    timeSeries varchar(200),
                    unit varchar(200),
                    unitOfMeasurement varchar(200), 
                    SOWC varchar(200),
                    observationFootnote varchar(200),
                    status varchar(200),
                    dataSource varchar(200),
                    confidentiality varchar(200),
                    timeActivity varchar(255),
                    timeInterval varchar(255)
                )"""

            self.cur.execute(query)
            self.conn.commit()

            print("TABLE SUCCESSFULLY CREATED")
        except Exception as error:
            print("TABLE ALREADY EXISTS")

    def save_data(self, *args):
        try:
            query = """INSERT INTO unicef_data( 
                    geography,
                    indicator,
                    sex,
                    age,
                    residence,
                    education,
                    period,
                    observation,
                    timeSeries,
                    unit,
                    unitOfMeasurement, 
                    SOWC,
                    observationFootnote,
                    status,
                    dataSource,
                    confidentiality,
                    timeActivity,
                    timeInterval) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            
            self.cur.execute(query, (args))
            self.conn.commit()

        except (UniqueViolation, InFailedSqlTransaction) as errors:
            print("Record with the specified ID already exists")
            print(errors)

    def close(self) -> None: 
        """ Closes the database when done """

        self.cur.close()
        self.conn.close()
    
class UnicefWorkerScheduler:
    """  A scheduler class that adds tasks to queue """

    def __init__(self, records, data_queue) -> None:
        self.records = records
        self.queue = data_queue
           
    def get_country(self, record:list, country:str):
        """ Retrieves the records for the specified country """
        
        for col in record:
            _country = col[1].split(':')[-1]
            stripped_country = _country.strip()
            if stripped_country == country:
                _col= self.clean_data(col)
                self.queue.put(_col)

    def clean_data(self, col):
        """ Cleases the data by stripping it off unwanted text or values """
        _col = []
        for data in col:
            if data:
                try:
                    new = data.split(':')[-1]
                    _col.append(new.strip())
                except Exception:
                    _col.append(data)
            else:
                _col.append(data)

        return _col
        

    def split_records(self, records:list, n):
        """ Splits the data into different parts with 1000 records per set """

        for i in range(0, len(records), n): 
            yield records[i : i + n]

    def process(self):
        """ iterates through the generator object and calls the method to extract the record for a given country """
        records_generator =self.split_records(self.records, 1000)
        for record in records_generator:
            self.get_country(record, "Nigeria")
        self.queue.put(None)    
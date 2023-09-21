from multiprocessing.pool import ThreadPool
import datetime
import pandas as pd
import time
from pymongo.mongo_client import MongoClient

class ETL:

    def __init__(self):
        self.days_ahead = 7
        self.today = datetime.date(2023, 7, 2)
        

    def _download_rates(self, date: datetime) -> str:
        """Download file and returns local file path where file is downloaded. """
        time.sleep(1) # simulates API request time.
        print("sleeping at " + datetime.datetime.now().strftime("%H:%M:%S"))
        return 'storage/{}/rates_{}.csv'.format(self.today, date.strftime('%Y-%m-%d'))

    def _extract(self):
        dates = [self.today]
        for i in range(1,self.days_ahead):
            dates.append(self.today + datetime.timedelta(days=i))
        pool = ThreadPool(self.days_ahead)
        dir_list = pool.map(self._download_rates,dates)
        print(dir_list)
        return dir_list
    
    def _transform(self, dir_list: list[str]):
        pool = ThreadPool(self.days_ahead)
        df_list = pool.map(pd.read_csv, dir_list)
        df = pd.concat(df_list, ignore_index=True)
        print(df)
        return df

    def _load(self, df: pd.DataFrame.dtypes):
        
        # Create a new client and connect to the server
        uri = "mongodb+srv://Admin:gpd2WxCBUNapc3YG@hotelrates.sb3qhnu.mongodb.net/?retryWrites=true&w=majority"
        client = MongoClient(uri)

        db = client['HotelRates']
        collection = db['Rates']
        data = df.to_dict(orient='records')
        collection.insert_many(data)

etl = ETL()
dir_list = etl._extract()
df = etl._transform(dir_list)
etl._load(df)
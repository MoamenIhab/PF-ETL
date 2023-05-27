from engine.mysql import mysqlEngine
from engine.postgres import postgresEngine
import psycopg2
from time import sleep
import pandas as pd
from geopy import distance

#engine creation moved to engine/postgres.py
#sleep time moved after class

# Write the solution here
class ETL:
    def __init__(self):
        psInitEngine = postgresEngine()
        msInitEngine = mysqlEngine()
        self.psEngine = psInitEngine.create()
        self.msEngine = msInitEngine.create()


    def calcDistance(self, lat1, lon1, lat2, lon2):
        coords_1 = (lat1, lon1)
        coords_2 = (lat2, lon2)
        dist = distance.distance(coords_1, coords_2).kilometers
        return dist
    
    def extract(self):
        dbConnection = self.psEngine.connect()
        df = pd.read_sql("select * from \"devices\"", dbConnection)
        print(df)
        if df.empty == True:
            raise Exception("EMPTY DATA FRAME")
        else:
            return df


    def transform(self, df):
        #df = pd.DataFrame(data, columns=['device_id', 'time', 'temperature', 'location'])
        df['time'] = pd.to_datetime(df['time'])
        df.set_index('time', inplace=True)
        maxTemps = df.groupby(['device_id', pd.Grouper(key='time', freq='H')])['temperature'].max()
        dataPointsCount = df.groupby(['device_id', pd.Grouper(key='time', freq='H')]).size()
        df['distance'] = df.groupby('device_id').apply(lambda x: ETL.calcDistance(x['latitude'].shift(), x['longitude'].shift(), x['latitude'], x['longitude'])).fillna(0)
        total_distance = df.groupby(['device_id', df.index.hour])['distance'].sum()
        resultDF = pd.DataFrame({
            'max_temperature': maxTemps,
            'data_points_count': dataPointsCount,
            'total_distance': total_distance
        }).reset_index()
        return resultDF


    def load(self, resultDF):
        resultDF.to_sql('analytics', con=self.msEngine, if_exists='replace', index=False)

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')


etlInst = ETL()
df = etlInst.extract()
resultDF = etlInst.transform(df)
etlInst.load(resultDF)
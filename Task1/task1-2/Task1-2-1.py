import pandas as pd
import time


#1
df = pd.read_csv(r"C:\training\Hadasim\HomeTask\TMPFiles\time_series (1).csv")
def CheckValidation(df):
    #check Format of date
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True)
    # Chek Value is Number
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    # fill AVG
    Avg = df["value"].mean()
    df.fillna({"value": Avg}, inplace=True)
    #delete Similar
    df = df.drop_duplicates(subset=['timestamp'])
    print(df)
#2
def AVG_For_Hour(df):
    df['start_time'] = df['timestamp'].dt.floor('H')
    AVG_T_Hour = df.groupby('start_time', as_index='false', sort='false')['value'].mean().reset_index(name='average')
    AVG_T_Hour.columns = ['start time', 'average']
    print(AVG_T_Hour)
if __name__ == '__main__':
    start=time.time()
    CheckValidation(df)
    AVG_For_Hour(df)
    end=time.time()
    print(end-start)

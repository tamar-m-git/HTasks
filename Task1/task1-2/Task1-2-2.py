import time

import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count
def CheckValidationAndavg(file_path):
    try:
        if os.path.getsize(file_path) == 0:
            print(f"File empty {file_path}")
            return pd.DataFrame()
        start = time.time()
        pid = os.getpid()
        print(f"[PID {pid}] Start processing: {file_path}")
        print(f"Validation of: {file_path}")
        df = pd.read_csv(file_path,on_bad_lines='skip')
        #check Format of date
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', errors='coerce',utc=True)
        df.dropna(subset=['timestamp'], inplace=True)
        # Chek Value is Number
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        # fill AVG
        Avg = df["value"].mean()
        df.fillna({"value": Avg}, inplace=True)
        #fill AVG Similar
        df = df.groupby('timestamp', as_index=False, sort=False)['value'].mean()
        # make AVG For all date according to hour
        df['start_time'] = df['timestamp'].dt.floor('h')
        AVG_T_Hour = df.groupby('start_time', as_index='false', sort='false')['value'].mean().reset_index(name='average')
        AVG_T_Hour.columns = ['start time', 'average']
        end = time.time()
        print(f"[PID {pid}] Finished processing: {file_path} in {end - start:.2f} seconds")
        print("success validation")
        return AVG_T_Hour
    except Exception as e:
        print(f"Error in file : {e}")
        return pd.DataFrame()

def SplitFile(FileScv, DirOutput,):
    try:
        os.makedirs(DirOutput, exist_ok=True)
        for chunk in pd.read_csv(FileScv,on_bad_lines='skip',chunksize=10000,parse_dates=['timestamp'], ):
            chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], errors='coerce')
            chunk = chunk.dropna(subset=['timestamp'])
            for DATEstr, group in chunk.groupby(chunk['timestamp'].dt.strftime("%Y-%m-%d")):
                out_path = os.path.join(DirOutput, f"{DATEstr}.csv")
                write_header = not os.path.exists(out_path)
                group.to_csv(out_path, mode="a", header=write_header, index=False)
                print("success split")
    except Exception as e:
        print(f"Err in file {FileScv}: {e}")
def AvgAllFiles (FileScv,DirOutput):
    try:
        SplitFile(FileScv,DirOutput)
        max_processes=  max(1,cpu_count(),-1)
        csv_files = glob.glob(os.path.join(DirOutput, '*.csv'))
        with Pool(processes=max_processes) as pool:
            results = pool.map(CheckValidationAndavg, csv_files)
        final_csv = os.path.join(DirOutput, "final_AVG.csv")
        final_df = pd.concat(results, ignore_index=True)
        final_df.to_csv(final_csv, index=False)
        for file_path in csv_files:
            try:
                if os.path.basename(file_path) != "final_AVG.csv":
                    os.remove(file_path)
                    print(f"Deleted: {file_path}")
            except Exception as delete_err:
                print(f"Could not delete {file_path}: {delete_err}")
    except Exception as e:
        print("ERR in manage Process: {e} ")
if __name__ == '__main__':
    current_dir = os.path.dirname(__file__)
    FileCsv = "../Data/time_series (1).csv"
    DirOut= "../Data/OutPutFiles"

   # df = pd.read_csv(r"C:\training\Hadasim\HomeTask\TMPFiles\time_series (1).csv")
    start=time.time()
    AvgAllFiles(FileCsv,DirOut)
    end=time.time()
    print(end-start)
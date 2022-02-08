from pathlib import Path
import pandas as pd

cur_dir = Path(__file__).parent
parquet_name = "feature_repo_two/data/driver_stats.parquet"
parquet_path = str(Path(cur_dir, parquet_name))

df = pd.read_parquet(parquet_path)
# print(df.head())
df['new_column'] = df['driver_id'].apply(lambda x: f'{x}__{x}')
df['new_column_2'] = df['new_column'].apply(lambda x: f'{x}___3')
df['driver_id_two'] = df['driver_id']
print(df.columns)
df.drop(columns=['driver_id', 'conv_rate', 'acc_rate', 'avg_daily_trips', ], inplace=True)
print(df.head())
print(df.columns)
df.to_parquet(str(Path(str(Path(parquet_path).parent), 'driver_stats_new_column_3.parquet')))

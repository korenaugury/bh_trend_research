import os
import pandas as pd
import datetime

relabelling_file_path = os.path.join(os.getcwd(), 'trend_relabelling.xlsx')

df = pd.read_excel(relabelling_file_path, sheet_name='melted_trend_2021')
df = df[['Machine_id', 'Until']]
df.columns = ['machine_id', 'end']

df['start'] = df['end'].apply(lambda dt: dt - datetime.timedelta(days=45))
df['end'] = df['end'].apply(lambda dt: dt + datetime.timedelta(days=1))

df['start'] = df['start'].apply(lambda dt: dt.strftime("%Y/%m/%d/%H"))
df['end'] = df['end'].apply(lambda dt: dt.strftime("%Y/%m/%d/%H"))

df = df[['machine_id', 'start', 'end']]
df.to_csv(os.path.join(os.getcwd(), 'triggers_file.csv'), index=False)

import pandas as pd
import sqlite3

con = sqlite3.connect("weather_data.db")
df = pd.read_sql_query("SELECT * from weather", con)

df.drop_duplicates(inplace = True)
print(df.to_string())

con.close()
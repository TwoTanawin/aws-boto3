import pandas as pd

df = pd.read_csv('/Users/twomac/Documents/Two/AIT/ISE/Master 2/Cloud Computing/pyScript/2024-02-22-07-32-processed.csv')

df[['city', 'year']] = df['3'].str.split(', ', expand=True)

df.drop(columns=['3'], inplace=True)

# df[['city']] = df['3']
# new_columns = ['0', '1', '2', '3']
# df = df.rename(columns={'city': '3', 'year': '4'})
# df.columns = new_columns
# Output the DataFrame
df.columns = df.columns.str.replace("city", "3")
df.columns = df.columns.str.replace("Marathon", "City")
df.columns = df.columns.str.replace("year", "4")
df.loc[0, '3'] = 'City'
df.loc[0, '4'] = 'Year'
print(df)
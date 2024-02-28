import pandas as pd


url = 'https://www.runnersworld.com/races-places/a20823734/these-are-the-worlds-fastest-marathoners-and-marathon-courses/'

dfs = pd.read_html(url)

df = dfs[0]

df = df.to_csv('html_data.csv', index=False)

print(df)

print('Done!')
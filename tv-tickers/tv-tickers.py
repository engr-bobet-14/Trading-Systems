import pandas as pd
import os
import ast
import re

df = pd.read_csv('crypto_data.csv').dropna()

crypto_tickerlist = df.symbol.values
x = round(len(crypto_tickerlist)/2)

crypto_tickerlist1 = crypto_tickerlist[:x]
crypto_tickerlist2 = crypto_tickerlist[x:]


# Create the output folder if it doesn't exist
output_folder = 'tv-ticker'
os.makedirs(output_folder, exist_ok=True)

# Write crypto_1of2.txt
file1_path = os.path.join(output_folder, '_crypto_1of2.txt')
with open(file1_path, 'a') as f:
    for ticker in crypto_tickerlist1:
        if 'Stablecoins' not in df.loc[df.symbol == ticker, 'categories'].iloc[0]:
            exchange = df.loc[df.symbol == ticker, 'exchange'].iloc[0]
            pair = df.loc[df.symbol == ticker, 'crypto_ticker'].iloc[0]
            f.write(f"{exchange}:{pair}\n".upper())

# Write crypto_2of2.txt
file2_path = os.path.join(output_folder, '_crypto_2of2.txt')
with open(file2_path, 'a') as f:
    for ticker in crypto_tickerlist2:
        if 'Stablecoins' not in df.loc[df.symbol == ticker, 'categories'].iloc[0]:
            exchange = df.loc[df.symbol == ticker, 'exchange'].iloc[0]
            pair = df.loc[df.symbol == ticker, 'crypto_ticker'].iloc[0]
            f.write(f"{exchange}:{pair}\n".upper())

#Categories

df['categories'] = df['categories'].apply(ast.literal_eval)
df_explode=df.explode('categories')


crypto_categorylist = sorted(df_explode.categories.dropna().unique())


def safe_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "_", name)

def tv_tickerbycat(category, df, folder='output'):
    os.makedirs(folder, exist_ok=True)

    df_cat = df.loc[df['categories'] == category]
    safe_name = safe_filename(category)
    file_path = os.path.join(folder, f'{safe_name}.txt')

    with open(file_path, 'a') as f:
        for ticker in df_cat.symbol:
            exchange = df_cat.loc[df_cat.symbol == ticker, 'exchange'].iloc[0]
            pair = df_cat.loc[df_cat.symbol == ticker, 'crypto_ticker'].iloc[0]
            f.write(f"{exchange}:{pair}\n".upper())

    print(f'{category} successfully exported to {file_path}')


for sector in crypto_categorylist:
    tv_tickerbycat(sector, df_explode, output_folder)
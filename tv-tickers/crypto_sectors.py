import pandas as pd
from utils import crypto_category, crytoindex_bycatergoy

def main():
    # Load coin data
    df_coins = pd.read_csv("./data/Crypto Coins Screener.csv")

    # Extract all categories from the 'Category' column
    crypto_category_list = crypto_category(df_coins["Category"])

    # Remove stablecoin-related categories
    crypto_category_list = [
        cat for cat in crypto_category_list
        if "stablecoin" not in cat.lower()
    ]

    # Generate the category based capped market cap index
    crytoindex_bycatergoy(
        df=df_coins,
        file_name="capped-marketcap_category-index",
        cryptocat_list=crypto_category_list
    )

if __name__ == "__main__":
    main()
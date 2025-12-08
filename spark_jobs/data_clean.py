# Function definition: define data cleaning function
def data_clean(df):
    cols = [
        "total_rooms",
        "total_bedrooms",
        "population",
        "households",
        "median_income",
        "median_house_value",
    ]

    df = df.dropna(subset=cols)
    df = df.drop_duplicates()

    return df
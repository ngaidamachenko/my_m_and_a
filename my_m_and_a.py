from datetime import datetime
from io import StringIO
import csv

import pandas as pd

from my_ds_babel import csv_to_sql


all_columns = [
    "gender",
    "firstname",
    "lastname",
    "email",
    "age",
    "city",
    "country",
    "created_at",
    "referral",
]


def get_time():
    # recording time the order was created at
    return datetime.now().strftime("%m/%d/%Y, %H:%M:%S")


def clean_df(df):
    # eliminate excess data
    deletions = 'string_ boolean_ integer_ years year character_ "'.split()
    cleaned = df.astype(str)
    for delete in deletions:
        cleaned = cleaned.replace(delete, "", regex=True)
    return cleaned.replace(r"\\", "", regex=True)


def clean_column(column):
    # eliminate excess data from column
    return (
        column.str.replace("-", " ")
        .str.replace("_", " ")
        .str.replace(".", " ")
        .str.title()
    )


def restructure_df(df):
    # restructure dataframe to standard
    for col in all_columns:
        if col not in df.columns:
            df[col] = ""
    for col in df.columns:
        if col not in all_columns:
            df.drop(col, axis=1, inplace=True)
    return df


# Process data from first file
def process_df1(file):
    # read csv and save time
    df = pd.read_csv(file)
    time = get_time()
    # pre cleaning
    df = clean_df(df)
    # restructure
    df.columns = [df.columns[i].lower() for i in range(len(df.columns))]
    df = restructure_df(df)
    # add ancillary data
    df["created_at"] = time
    df.referral = file
    # standardize
    df["gender"] = (
        df["gender"]
        .replace({"M": "Male", "F": "Female"})
        .replace({"1": "Female", "0": "Male"})
    )
    for i in range(len(df)):
        df["firstname"].iloc[i] = df["firstname"].iloc[i].title()
        df["lastname"].iloc[i] = df["lastname"].iloc[i].title()
        df["email"].iloc[i] = df["email"].iloc[i].lower()
    df["city"] = clean_column(df["city"])
    df["country"] = "USA"
    return df[all_columns]


# Process data from second file
def process_df2(file):
    # read csv and save time
    df = pd.read_csv(file, header=None, sep=";")
    time = get_time()
    # pre cleaning
    df = clean_df(df)
    # restructure
    df.columns = ["age", "city", "gender", "name", "email"]
    for col in all_columns:
        if col not in df.columns:
            df[col] = ""
    for i in range(len(df)):
        firstname, lastname = df["name"][i].split(" ")
        df["firstname"][i] = firstname.title()
        df["lastname"][i] = lastname.title()
    df = restructure_df(df)
    # add ancillary data
    df["created_at"] = time
    df.referral = file
    # standardize
    df["gender"] = (
        df["gender"]
        .replace({"M": "Male", "F": "Female"})
        .replace({"1": "Female", "0": "Male"})
    )
    for i in range(len(df)):
        df["email"].iloc[i] = df["email"].iloc[i].lower()
    df["city"] = clean_column(df["city"])
    df["country"] = "USA"
    df["age"] = df["age"].replace("yo", "", regex=True)
    return df[all_columns]


# Process data from third file
def process_df3(file):
    # read csv and save time
    df = pd.read_csv(file, sep=",|\t")
    time = get_time()
    # pre cleaning
    df = clean_df(df)
    # restructure
    df.columns = [df.columns[i].lower() for i in range(len(df.columns))]
    for col in all_columns:
        if col not in df.columns:
            df[col] = ""
    for i in range(len(df)):
        firstname, lastname = df["name"][i].split(" ")
        df["firstname"][i] = firstname.title()
        df["lastname"][i] = lastname.title()
    df = restructure_df(df)
    # add ancillary data
    df["created_at"] = time
    df.referral = file
    # standardize
    df["gender"] = (
        df["gender"]
        .replace({"M": "Male", "F": "Female"})
        .replace({"0": "Female", "1": "Male"})
    )
    for i in range(len(df)):
        df["firstname"].iloc[i] = df["firstname"].iloc[i].title()
        df["lastname"].iloc[i] = df["lastname"].iloc[i].title()
        df["email"].iloc[i] = df["email"].iloc[i].lower()
    df["city"] = clean_column(df["city"])
    df["country"] = "USA"
    df["age"] = df["age"].replace("yo", "", regex=True)
    return df[all_columns]


def my_m_and_a(csv_1, csv_2, csv_3):
    # process data
    df1 = process_df1(csv_1)
    df2 = process_df2(csv_2)
    df3 = process_df3(csv_3)

    # Combine the dataframes
    df_all = pd.concat([df1, df2, df3], axis=0, ignore_index=True)

    # convert to csv and then SQL
    csv_string = df_all.to_csv(index=False)
    merged_csv = StringIO(csv_string)
    csv_to_sql(merged_csv, "plastic_free_boutique.sql", "customers")


def _main():
    # read data
    file_1 = "only_wood_customer_us_1.csv"
    file_2 = "only_wood_customer_us_2.csv"
    file_3 = "only_wood_customer_us_3.csv"

    my_m_and_a(file_1, file_2, file_3)


if __name__ == "__main__":
    _main()

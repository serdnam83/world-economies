#!/usr/bin/python

# Script for ETL operations on Country-GDP data

# Importing the requires libraries
import os
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

# Declare needed variables
url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
db_name = "World_Economies.db"
table_name = "Countries_by_GDP"
csv_path = os.path.join(os.getcwd(), "Countries_by_GDP.csv")
table_atribs = ["Country", "GDP_USD_millions", "Year"]
conn = sqlite3.connect(db_name)
log_file = "etl_project_log.txt"
query_statement = f"SELECT country FROM {table_name} WHERE GDP_USD_billions > 100"


def extract(url, table_atribs):
    """This function extracts the required information from the
    website and saves it to a dataframe. The function returns the
    dataframe for further processing (url:str, url of the page to
    scrap), (table_atribs: list, columns of the Data Frame)."""
    df = pd.DataFrame(columns=table_atribs)
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, "html.parser")
    table = soup.find("table", class_="wikitable")
    for row in table.find_all("tr"):
        col = row.find_all("td")
        if len(col) != 0:
            if col[0].find("a") is not None and "—" not in col[2]:
                # If the year has <a> contents.index change, because
                # it is necessary change the method to acces de year
                # data              
                if col[3].find("a") is not None: 
                    data_dict = {
                        "Country": col[0].a.contents[0],
                        "GDP_USD_millions": col[2].contents[0],
                        "Year": col[3].contents[1],
                    }
                    df_aux = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df_aux], ignore_index=True)
                else:
                    # If the year has'nt <a> contents.index is normally
                    data_dict = {
                        "Country": col[0].a.contents[0],
                        "GDP_USD_millions": col[2].contents[0],
                        "Year": col[3].contents[0],
                    }
                    df_aux = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df_aux], ignore_index=True)
    return df


def transform(df):
    """This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe."""
    df["GDP_USD_millions"] = df["GDP_USD_millions"].apply(
        lambda x: float("".join(x.split(",")))
    )
    df["GDP_USD_millions"] = df["GDP_USD_millions"].apply(lambda x: round(x / 1000, 2))
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})
    return df


def load_to_csv(df, csv_path):
    """This function saves the final dataframe as a `CSV` file
    in the provided path. Function returns nothing."""
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    """This function saves the final dataframe as a database table
    with the provided name. Function returns nothing."""
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement, sql_connection):
    """This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing."""
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


def log_progress(message):
    """This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing"""
    timestamp_format = "%Y-%m-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as file:
        file.write(timestamp + " : " + message + "\n")


# Log the start of ETL process
log_progress("ETL process start")

# Log the start of extract process
log_progress("Extract process start")
data = extract(url, table_atribs)
print(data)

# Log the end of extract process
log_progress("Extract process ends")

# Log the start of transform process
log_progress("Transform process start")
transformed_data = transform(data)

# Log the end of the transform process
log_progress("Transform Process ends")
print("**************************************")
print("Transformed data")
print("**************************************")
print(transformed_data)

# Log the start of the load process
log_progress("Load to csv start")
load_to_csv(transformed_data, csv_path)
log_progress("Load to csv ends")

log_progress("Load to data base start")
load_to_db(transformed_data, conn, table_name)
log_progress("Load to data base ends")

log_progress("Run query start")
run_query(query_statement, conn)
log_progress("Run query ends")

log_progress("ETL Process complete")

conn.close()

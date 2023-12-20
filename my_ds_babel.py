import sqlite3
import pandas as pd
import csv
from io import StringIO


def sql_to_csv(database, table_name):
    conn = sqlite3.connect(database)
    cmd = "SELECT * FROM " + table_name + ";"
    cursor = conn.execute(cmd)
    names = list(map(lambda x: x[0], cursor.description))
    df = pd.DataFrame(cursor, columns=names)
    csv_name = "list_" + table_name + ".csv"
    df.to_csv(csv_name, index=False)
    return df.to_csv(index=False)[:-1]  # to delete \n after the last row


def csv_to_sql(csv_content, database, table_name):
    df = pd.read_csv(csv_content)
    db = sqlite3.connect(database)
    df.to_sql(table_name, db, if_exists="replace", index=False)


def _main():
    # SQL to CSV
    db = "all_fault_line.db"
    tbl = "fault_lines"
    s = sql_to_csv(db, tbl)
    print("SQL to CSV test")
    print(s)

    # CSV to SQL
    csv_file = "list_volcano.csv"
    csv_string = ""
    with open(csv_file, "r") as file:
        csvreader = csv.reader(file)
        for row in csvreader:
            csv_string += ",".join(row) + "\n"
    csv_stringIO = StringIO(csv_string)
    # DF = pd.read_csv(csv_stringIO)
    csv_to_sql(csv_stringIO, "list_volcano.db", "volcano")


# if __name__ == "__main__":
#     _main()
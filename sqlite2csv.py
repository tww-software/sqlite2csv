"""
SQLite 2 CSV

convert a Sqlite database into Comma Seperated Values file(s)

written by Thomas W Whittam
"""


import argparse
import csv
import os
import sqlite3


class SQLITEDatabaseExport():
    """
    class to extract data from SQLITE3 databases

    Args:
        databasepath(str): the full filepath to the sqlite3 database file

    Attributes:
        connection(sqlite3.connect): connection to the SQLITE3 database
        cursor(sqlite3 cursor): object to interact with the database connection
    """

    def __init__(self, databasepath):
        self.connection = sqlite3.connect(databasepath)
        self.cursor = self.connection.cursor()
        self.cursor.execute("PRAGMA foreign_keys = ON")

    def get_table_names(self):
        """
        list all the tables in the database

        Returns:
            tablenames(list): list of the database table names
        """
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")
        fetched = self.cursor.fetchall()
        tables = [item[0] for item in fetched]
        tablenames = tables
        return tablenames

    def get_tables_column_names(self, table):
        """
        get the names of the columns for a table

        Args:
            table(str): the table name to get column names for

        Returns:
            columns(list): list of the column names for the specified table
        """
        sqlstatement = "SELECT name FROM PRAGMA_TABLE_INFO('{}')".format(table)
        self.cursor.execute(sqlstatement)
        fetched = self.cursor.fetchall()
        columns = [item[0] for item in fetched]
        return columns

    def get_table_data(self, table):
        """
        get all the records for the table

        Args:
            table(str): the table name to grab data from

        Returns:
            tabledata(list): list of tuples, each tuple is a table record
        """
        sqlstatement = "SELECT * FROM '{}'".format(table)
        self.cursor.execute(sqlstatement)
        tabledata = self.cursor.fetchall()
        return tabledata

    def dump_data(self, outputdir):
        """
        helper method to get all the tables and dump them to csv files in
        the output directory, also dumps the SQL commands into a file in the
        same directory

        Args:
            outputdir(str): directory to save output to
        """
        if not os.path.isdir(outputdir):
            os.makedirs(outputdir)
        tables = self.get_table_names()
        for table in tables:
            colheaders = self.get_tables_column_names(table)
            tabledata = self.get_table_data(table)
            output = os.path.join(outputdir, table + '.csv')
            write_table_to_csv(colheaders, tabledata, output)
        self.dump_sql(outputdir)
        self.connection.close()

    def dump_sql(self, outputdir):
        """
        dump output as SQL text into a file called dump.sql

        Args:
            outputdir(str): the directory to save dump.sql to
        """
        with open(os.path.join(outputdir, 'dump.sql'), 'w') as outputf:
            for line in self.connection.iterdump():
                outputf.write('%s\n' % line)


def write_table_to_csv(headers, table, csvoutpath):
    """
    write a table to a CSV file

    Args:
        headers(list): the headers for the top of the CSV file
        table(list): list of lists, each list is a row in the CSV
        csvoutpath(str): full path to write the CSV file to
    """
    with open(csvoutpath, 'w') as csvout:
        csvwriter = csv.writer(csvout)
        csvwriter.writerow(headers)
        csvwriter.writerows(table)


def parse_cli_args():
    """
    parse arguments off the command line

    Returns:
        args(argparse.Namespace): the CLI arguments
    """
    parser = argparse.ArgumentParser(
        'open a Sqlite3 database and dump tables to CSV files')
    parser.add_argument('inputdatabase', help='Sqlite3 database to read from')
    parser.add_argument('outputdir', help='directory to output tables')
    args = parser.parse_args()
    return args


def main():
    """
    main program code
    """
    args = parse_cli_args()
    sqlitedatabase = SQLITEDatabaseExport(args.inputdatabase)
    sqlitedatabase.dump_data(args.outputdir)


if __name__ == '__main__':
    main()

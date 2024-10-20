import glob
import os
import json
import re
import pandas as pd
from typing import Union

class CSVProcessor:
    def __init__(self, src_base_dir, tgt_base_dir, db_conn_uri, ds_name):
        """
        Initializes an instance of the CSVProcessor.

        :param src_base_dir: Source directory of the CSV files.
        :param tgt_base_dir: Destination directory for JSON files.
        :param db_conn_uri: Database connection URI.
        :param ds_name: Name of the table/dataset to be processed.
        """
        self.src_base_dir = src_base_dir
        self.tgt_base_dir = tgt_base_dir
        self.db_conn_uri = db_conn_uri
        self.ds_name = ds_name
        self.schemas = self.__load_schemas()
        self.files = glob.glob(f'{self.src_base_dir}/{self.ds_name}/part-*')

    def __load_schemas(self):
        """
        Loads the schemas from the schemas.json file.

        :return: Dictionary with the data schemas.
        """
        return json.load(open(f'{self.src_base_dir}/schemas.json'))

    def __get_column_names(self):
        """
        Gets the column names for the current dataset.

        :return: List of column names.
        """
        column_details = self.schemas[self.ds_name]
        return [col['column_name'] for col in column_details]

    def get_df_reader(self, file, chunksize=None) -> Union[pd.DataFrame, pd.io.parsers.TextFileReader]:
        """
        Reads a CSV file and returns a DataFrame or a file reader.

        :param file: Path of the CSV file to be read.
        :param chunksize: Number of rows to read in each chunk (optional).
        :return: DataFrame or TextFileReader, depending on the chunksize.
        """
        columns = self.__get_column_names()
        return pd.read_csv(file, names=columns, chunksize=chunksize)

    def to_sql(self, df):
        """
        Loads a DataFrame into the database.

        :param df: DataFrame to be loaded.
        """
        df.to_sql(
            self.ds_name,
            self.db_conn_uri,
            if_exists='replace',
            index=False
        )

    def to_json(self, df, file_name):
        """
        Converts a DataFrame to a JSON file.

        :param df: DataFrame to be converted.
        :param file_name: Name of the JSON file to be saved.
        """
        json_file_path = f'{self.tgt_base_dir}/{self.ds_name}/{file_name}'
        os.makedirs(f'{self.tgt_base_dir}/{self.ds_name}', exist_ok=True)
        df.to_json(
            json_file_path,
            orient='records',
            lines=True
        )

    def db_loader(self):
        """
        Loads all CSV files into the database.

        Raises an error if no files are found.
        """
        if len(self.files) == 0:
            raise NameError(f'No files found for {self.ds_name}')

        for file in self.files:
            df_reader = self.get_df_reader(file, chunksize=10_000)
            for df in df_reader:
                print(f'Loading chunk into database for {self.ds_name}')
                self.to_sql(df)

    def file_converter(self):
        """
        Converts all CSV files to JSON files.

        Raises an error if no files are found.
        """
        if len(self.files) == 0:
            raise NameError(f'No files found for {self.ds_name}')

        for file in self.files:
            df = self.get_df_reader(file)
            file_name = re.split('[/\\\]', file)[-1]
            print(f'Converting file to JSON for {self.ds_name} - {file_name}')
            self.to_json(df, file_name)

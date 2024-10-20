import sys
import glob
import os
import json
import re
import pandas as pd
import argparse
from typing import Union

src_base_dir = os.environ.get('SRC_BASE_DIR')
tgt_base_dir = os.environ.get('TGT_BASE_DIR')
db_host = os.environ.get('DB_HOST')
db_port = os.environ.get('DB_PORT')
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')
db_conn_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'

class CSVProcessor:
    def __init__(self, src_base_dir, tgt_base_dir, db_conn_uri):
        self.src_base_dir = src_base_dir
        self.tgt_base_dir = tgt_base_dir
        self.db_conn_uri = db_conn_uri
        self.schemas = self.load_schemas()

    def load_schemas(self):
        return json.load(open(f'{self.src_base_dir}/schemas.json'))

    def get_column_names(self, ds_name):
        column_details = self.schemas[ds_name]
        return sorted(column_details, key=lambda col: col['column_position'])

    def get_df_reader(self, file, chunksize=None) -> Union[pd.DataFrame, pd.io.parsers.TextFileReader]:
        file_path_list = re.split('[/\\\]', file)
        ds_name = file_path_list[-2]
        columns = get_column_names(self.schemas, ds_name)
        return pd.read_csv(file, names=columns, chunksize=chunksize)

    def to_sql(self, df, db_conn_uri, ds_name):
        df.to_sql(
            ds_name,
            db_conn_uri,
            if_exists='append',
            index=False
        )

    def to_json(self, df, tgt_base_dir, ds_name, file_name):
        json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
        os.makedirs(f'{tgt_base_dir}/{ds_name}', exist_ok=True)
        df.to_json(
            json_file_path,
            orient='records',
            lines=True
        )

    def to_sql(self, df, db_conn_uri, ds_name):
        df.to_sql(
            ds_name,
            db_conn_uri,
            if_exists='append',
            index=False
        )

    def db_loader(self, ds_name):
        schemas = json.load(open(f'{src_base_dir}/schemas.json'))
        files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
        if len(files) == 0:
            raise NameError(f'No files found for {ds_name}')

        for file in files:
            df_reader = read_csv(file, schemas)
            for idx, df in enumerate(df_reader):
                print(f'Populating chunk {idx} of {ds_name}')
                self.to_sql(df, db_conn_uri, ds_name)

    def file_converter(self, ds_name):
        schemas = json.load(open(f'{src_base_dir}/schemas.json'))
        files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
        if len(files) == 0:
            raise NameError(f'No files found for {ds_name}')

        for file in files:
            df = get_df_from_data(file, schemas)
            file_name = re.split('[/\\\]', file)[-1]
            to_json(df, tgt_base_dir, ds_name, file_name)

    def process_file(self, ds_name, mode):
        if mode == 'converter':
            self.file_converter(ds_name)
        elif mode == 'loader':
            self.db_loader(ds_name)

def get_column_names(schemas, ds_name, sorting_key='column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

def read_csv(file, schemas):
    file_path_list = re.split('[/\\\]', file)
    ds_name = file_path_list[-2]
    columns = get_column_names(schemas, ds_name)
    df_reader = pd.read_csv(file, names=columns, chunksize=10000)
    return df_reader

def get_df_from_data(file, schemas):
    file_path_list = re.split('[/\\\]', file)
    ds_name = file_path_list[-2]
    columns = get_column_names(schemas, ds_name)
    df = pd.read_csv(file, names=columns)
    return df

def to_sql(df, db_conn_uri, ds_name):
    df.to_sql(
        ds_name,
        db_conn_uri,
        if_exists='append',
        index=False
    )

def to_json(df, tgt_base_dir, ds_name, file_name):
    json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
    os.makedirs(f'{tgt_base_dir}/{ds_name}', exist_ok=True)
    df.to_json(
        json_file_path,
        orient='records',
        lines=True
    )

def db_loader(src_base_dir, db_conn_uri, ds_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')

    for file in files:
        df_reader = read_csv(file, schemas)
        for idx, df in enumerate(df_reader):
            print(f'Populating chunk {idx} of {ds_name}')
            to_sql(df, db_conn_uri, ds_name)

def file_converter(src_base_dir, tgt_base_dir, ds_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')

    for file in files:
        df = get_df_from_data(file, schemas)
        file_name = re.split('[/\\\]', file)[-1]
        to_json(df, tgt_base_dir, ds_name, file_name)

def process_files(mode: str, ds_name: str):
    try:
        if mode == 'converter':
            file_converter(src_base_dir, tgt_base_dir, ds_name)
        elif mode == 'loader':
            db_loader(src_base_dir, db_conn_uri, ds_name)
        else:
            raise ValueError("Mode not found")
    except NameError as ne:
        print(ne)
        pass
    except Exception as e:
        print(e)
        pass
    finally:
        print(f'Error Processing {ds_name}')

def execute(ds_names=None, mode: str = 'converter'):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_names:
        ds_names = schemas.keys()
    for ds_name in ds_names:
        process_files(mode, ds_name)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process CSV files to JSON or load to database.')
    parser.add_argument('-t', '--tablename', type=str, required=True, help='Name of the table to process.')
    parser.add_argument('-m', '--mode', type=str, choices=['converter', 'loader'], required=True, help='Mode of operation: "json" or "database".')

    args = parser.parse_args()

    execute(ds_names=[args.tablename], mode=args.mode)
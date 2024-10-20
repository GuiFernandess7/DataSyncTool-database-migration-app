from CSVProcessor import CSVProcessor
import argparse

from settings import *

def process_file(processor: CSVProcessor, mode: str):
    try:
        if mode == 'converter':
            processor.file_converter()
        elif mode == 'loader':
            processor.db_loader()
    except Exception as e:
        raise ValueError(f"Error: {e}")
    else:
        print("Files converted successfully.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process CSV files to JSON or load to database.')
    parser.add_argument('-t', '--tablename', type=str, required=True, help='Name of the table to process.')
    parser.add_argument('-m', '--mode', type=str, choices=['converter', 'loader'], required=True, help='Mode of operation: "json" or "database".')

    args = parser.parse_args()

    processor = CSVProcessor(src_base_dir, tgt_base_dir, db_conn_uri, args.tablename)
    process_file(processor, args.mode)

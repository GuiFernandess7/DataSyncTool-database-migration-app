#!/bin/bash

tables=("orders")

for table in "${tables[@]}"; do
    python main.py -t "$table" -m converter
    python main.py -t "$table" -m loader
done

#!/bin/bash

if [ -n "$1" ]; then
	echo "To process directory $1"
else
	echo "Must specify the directory containing experimental results."
	exit
fi

rm -fr eva_data
rm -fr dat_DBx
rm -fr graphs_DBx
rm -fr $1/graphs_DBx

cp -r $1/eva_data ./
mkdir -p dat_DBx
mkdir -p graphs_DBx

python3 data_analyser.py $1

rm -fr eva_data dat_DBx
mv graphs_DBx $1

echo "gen_graphs_DBx.sh Done!"

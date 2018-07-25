#!/usr/bin/env bash
echo "Test start..."
sh start.sh
sh start.sh --job population --load_from url --save_to mongodb
sh start.sh --job countMillionCities --load_from path --save_to csv
sh start.sh --job top5Cities --load_from url
sh start.sh --job ratioPopulation --save_to mongodb
sh start.sh --job top5BestDynamics --load_from url --save_to mongodb
sh start.sh --job top5WorstDynamics --load_from url --save_to mongodb --period_start 2010 --period_end 2015
echo "Test done."
#!/bin/bash
#conda init --all --dry-run --verbose
conda activate gichan
python crawl_weather.py
conda deactivate


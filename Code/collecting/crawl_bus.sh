#!/bin/bash
#conda init --all --dry-run --verbose
conda activate gichan
python crawl_bus.py
conda deactivate


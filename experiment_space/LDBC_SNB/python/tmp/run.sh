#!/bin/bash
export SF=0.1

export PREFIX_INPUT=/nvme0n1/00new_db/sf${SF}/social_network/dynamic/
export PREFIX_OUTPUT=/nvme0n1/sf${SF}/

python3 ./filter_dataset.py
python3 ./order_filtered_dataset.py
python3 ./partial_order_filtered_dataset.py
import csv
from collections import defaultdict
from tqdm import tqdm
import pandas as pd
import duckdb
import numpy as np
import matplotlib as mpl
from matplotlib import pyplot as plt


def set_mpl():
    mpl.rcParams.update(
        {
            'text.usetex': False,
            # 'font.sans-serif': 'Times New Roman',
            'mathtext.fontset': 'stix',
            'font.size': 20,
            'figure.figsize': (10.0, 10 * 0.618),
            'savefig.dpi': 10000,
            'axes.labelsize': 25,
            'axes.linewidth': 1.2,
            'xtick.labelsize': 20,
            'ytick.labelsize': 20,
            'legend.loc': 'upper right',
            'lines.linewidth': 3,
            'lines.markersize': 5,
            # 'axes.labelweight': 'bold'
        }
    )

def post_analysis(data_set_dir):
    conn = duckdb.connect()
    conn.execute("SET enable_progress_bar = true")
    
    query_1 = f"""
        CREATE TABLE post_hasCreator_person AS 
        SELECT * FROM read_csv_auto(?);
    """
    conn.execute(query_1, [data_set_dir+'/'+'post_hasCreator_person_0_0.csv'])
    
    query_2 = f"""
        CREATE TABLE forum_containerOf_post AS 
        SELECT * FROM read_csv_auto(?);
    """
    conn.execute(query_2, [data_set_dir+'/'+'forum_containerOf_post_0_0.csv'])
    
    conn.execute("""
        CREATE TABLE new_table AS
        SELECT 
            t1."Post.id",
            t1."Person.id",
            t2."Forum.id"
        FROM 
            post_hasCreator_person t1
        JOIN 
            forum_containerOf_post t2
        ON 
            t1."Post.id" = t2."Post.id"
    """).fetchall()
    
    conn.execute(f"""
        COPY new_table TO '{data_set_dir}/post_hasCreator_person_0_1.csv' (HEADER, DELIMITER '|');
    """)


if __name__ == "__main__":
    sf_number = 30
    nbr_name = 'post'

    data_set_dir = f'/nvme0n1/100new_db/sf{sf_number}/social_network/dynamic'
    post_analysis(data_set_dir)

    print('\nwork finished')

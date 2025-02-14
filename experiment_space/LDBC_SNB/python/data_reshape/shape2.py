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

def comment_analysis(data_set_dir):
    conn = duckdb.connect()
    conn.execute("SET enable_progress_bar = true")
    
    query_1 = f"""
        CREATE TABLE comment_vertex AS 
        SELECT * FROM read_csv_auto(?, all_varchar=true);
    """
    conn.execute(query_1, [data_set_dir+'/'+'comment_0_0.csv'])
    
    # 只修改特定的列名
    df = pd.read_csv(data_set_dir+'/'+'comment_replyOf_comment_0_0.csv', sep='|')
    # 修改特定列名
    df.columns.values[0] = 'CommentID1'  # 修改第一个列名
    df.columns.values[1] = 'CommentID2'  # 修改第二个列名
    df.to_csv(data_set_dir+'/'+'comment_replyOf_comment_0_1.csv', index=False, sep='|')
    
    query_2 = f"""
        CREATE TABLE comment_replyOf_comment AS 
        SELECT * FROM read_csv_auto(?, all_varchar=true);
    """
    conn.execute(query_2, [data_set_dir+'/'+'comment_replyOf_comment_0_1.csv'])
    
    conn.execute("""
        CREATE TABLE new_table AS
        SELECT 
            t2."CommentID1",
            t2."CommentID2",
            t1."creationDate",
        FROM 
            comment_vertex t1
        JOIN 
            comment_replyOf_comment t2
        ON 
            t1."id" = t2."CommentID1"
    """).fetchall()
    
    conn.execute(f"""
        COPY new_table TO '{data_set_dir}/comment_replyOf_comment_0_1.csv' (HEADER, DELIMITER '|');
    """)
    
    # 只修改特定的列名
    df = pd.read_csv(data_set_dir+'/'+'comment_replyOf_comment_0_1.csv', sep='|')
    # 修改特定列名
    df.columns.values[0] = 'Comment.id'  # 修改第一个列名
    df.columns.values[1] = 'Comment.id'  # 修改第二个列名
    df.to_csv(data_set_dir+'/'+'comment_replyOf_comment_0_1.csv', index=False, sep='|')
    


def comment_analysis2(data_set_dir):
    conn = duckdb.connect()
    conn.execute("SET enable_progress_bar = true")
    
    query_1 = f"""
        CREATE TABLE comment_vertex AS 
        SELECT * FROM read_csv_auto(?, all_varchar=true);
    """
    conn.execute(query_1, [data_set_dir+'/'+'comment_0_0.csv'])
    
    query_2 = f"""
        CREATE TABLE comment_replyOf_post AS 
        SELECT * FROM read_csv_auto(?, all_varchar=true);
    """
    conn.execute(query_2, [data_set_dir+'/'+'comment_replyOf_post_0_0.csv'])
    
    conn.execute("""
        CREATE TABLE new_table AS
        SELECT 
            t2."Comment.id",
            t2."Post.id",
            t1."creationDate",
        FROM 
            comment_vertex t1
        JOIN 
            comment_replyOf_post t2
        ON 
            t1."id" = t2."Comment.id"
    """).fetchall()
    
    conn.execute(f"""
        COPY new_table TO '{data_set_dir}/comment_replyOf_post_0_1.csv' (HEADER, DELIMITER '|');
    """)
    
if __name__ == "__main__":
    sf_number = 30

    data_set_dir = f'/nvme0n1/100new_db/sf{sf_number}/social_network/dynamic'
    comment_analysis(data_set_dir)
    comment_analysis2(data_set_dir)

    print('\nwork finished')

import csv
from collections import defaultdict
from tqdm import tqdm
import pandas as pd
import duckdb
from datetime import datetime
import os
import json
import random
import shutil

def find_creation_date_partition(conn, comment_csv, post_csv, forum_csv,percentage):
    # 读取 comment_csv 文件并创建 Comment 表，确保 creationDate 列为 TIMESTAMP 类型并重命名
    print("begin load comment data")
    conn.execute(f"""
    CREATE TABLE comment_data AS 
    SELECT CAST(creationDate AS TIMESTAMP) AS creationDate 
    FROM read_csv_auto('{comment_csv}')
    """)

    # 读取 post_csv 文件并创建 Post 表，确保 creationDate 列为 TIMESTAMP 类型并重命名
    print("begin load post data")
    conn.execute(f"""
    CREATE TABLE post_data AS 
    SELECT CAST(creationDate AS TIMESTAMP) AS creationDate 
    FROM read_csv_auto('{post_csv}')
    """)

    # 读取 forum_csv 文件并创建 Forum 表，确保 creationDate 列为 TIMESTAMP 类型并重命名
    print("begin load forum data")
    conn.execute(f"""
    CREATE TABLE forum_data AS 
    SELECT CAST(creationDate AS TIMESTAMP) AS creationDate 
    FROM read_csv_auto('{forum_csv}')
    """)

    # 将 comment_data 和 post_data 合并到一个临时表中，按日期统一排序
    print("begin combine data")
    conn.execute("""
    CREATE TABLE combined_data AS 
    SELECT creationDate FROM comment_data 
    UNION ALL 
    SELECT creationDate FROM post_data
    UNION ALL 
    SELECT creationDate FROM forum_data
    """)

    # 使用 PERCENT_RANK() 窗口函数找到给定百分位的 creationDate
    print("begin select filter creation data")
    query = """
    SELECT creationDate
    FROM (
        SELECT creationDate, 
               PERCENT_RANK() OVER (ORDER BY creationDate) AS percentile
        FROM combined_data
    ) AS ranked_data
    WHERE percentile >= ?
    ORDER BY percentile
    LIMIT 1
    """
    
    # 执行查询并获取结果
    result_list=[]
    step=(1-percentage)/5
    for i in range(0,5):
        cur_percentage=percentage+i*step
        result=conn.execute(query, [cur_percentage]).fetchone()
        result_list.append(result[0])
    # 返回找到的 creationDate 值
    return result_list

def delete_and_split_by_creationDate(conn, file_name, filteredDate):
    result_sets = []  # 用于存储 later_data 分成的五个数据集合，仅包含 id 列

    try:
        print(f"begin process {file_name}")
        # 指定 creationDate 为字符串类型
        types = {"creationDate": "STRING"}

        # 创建临时表 original_data 来存储 file_name 文件的所有记录，将 creationDate 转换为 TIMESTAMP 类型
        print(f"load {file_name}")
        conn.execute("""
            CREATE TEMP TABLE original_data AS 
            SELECT *, 
                   TRY_CAST(creationDate AS TIMESTAMP WITH TIME ZONE) AS creationDateTime
            FROM read_csv_auto(?, types := ?)
        """, [file_name, types])

        # 创建第一个表: creationDateTime 小于 filteredDate 的数据
        print("create base data")
        conn.execute("""
            CREATE TABLE base_data AS
            SELECT * 
            FROM original_data
            WHERE creationDateTime <= CAST(? AS TIMESTAMP WITH TIME ZONE)
        """, [filteredDate])

        # 创建第二个表: creationDateTime 大于 filteredDate 的数据，并按 creationDateTime 排序
        print("create later data")
        conn.execute("""
            CREATE TEMP TABLE later_data AS
            SELECT * 
            FROM original_data
            WHERE creationDateTime > CAST(? AS TIMESTAMP WITH TIME ZONE)
            ORDER BY creationDateTime
        """, [filteredDate])

        # 获取 base_data 表的所有列名，去掉 `creationDateTime`
        columns_query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'base_data'"
        columns = conn.execute(columns_query).fetchall()
        column_names = [col[0] for col in columns if col[0] != 'creationDateTime']
        columns_str = ', '.join(column_names)

        # 从 file_name 中获取文件名称，并生成第一个文件名
        base_name = os.path.basename(file_name)
        output_dir = NEW_PREFIX + f"modified_0/"
                # 检查目标目录是否存在，如果不存在则创建它
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        # 构建完整的输出文件路径
        base_file_name = os.path.join(output_dir, base_name)
        # 导出 base_data 表到第一个文件
        print(f"begin dump base data to {base_file_name}")
        conn.execute(f"""
            COPY (SELECT {columns_str} FROM base_data) 
            TO '{base_file_name}' (DELIMITER '|')
        """)
        print(f"Base data has been saved to {base_file_name}")

        # 计算每一份的大小，分成五等分，最后一份包含剩余的数据
        total_later_data_count = conn.execute("SELECT COUNT(*) FROM later_data").fetchone()[0]
        chunk_size = -(-total_later_data_count // 5)  # 向上取整，以确保最后一份包含剩余数据

        all_ids_set = set()

        # 依次生成追加数据文件
        for i in range(1, 6):
            # 当前分片的 limit 应该包含所有之前的分片数据，即累积到当前分片的大小
            limit = chunk_size * i if (chunk_size * i) <= total_later_data_count else total_later_data_count

            # 创建临时表 current_chunk 来存储从第一个到当前分片的累积数据
            conn.execute("""
                CREATE TEMP TABLE current_chunk AS
                SELECT * 
                FROM later_data
                LIMIT ?
            """, [limit])
            if i!=5:
                # 生成文件名
                # 构建目标目录路径
                output_dir = NEW_PREFIX + f"modified_{i*2}/"
                # 检查目标目录是否存在，如果不存在则创建它
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                # 构建完整的输出文件路径
                current_file_name = os.path.join(output_dir, base_name)

                # 将当前累积分片数据追加到基准数据并导出
                print(f"begin dump later data to {current_file_name}")
                conn.execute(f"""
                    COPY (SELECT {columns_str} FROM base_data
                        UNION ALL
                        SELECT {columns_str} FROM current_chunk)
                    TO '{current_file_name}' (DELIMITER '|')
                """)
                print(f"Data with {2 * i * 10}% added has been saved to {current_file_name}")

            # 获取当前分片的 id 数据
            current_chunk_ids = conn.execute("SELECT id FROM current_chunk").fetchall()
            result_set = set(row[0] for row in current_chunk_ids)  # 只存储 id

            # 将 result_set 中的 id 和之前的 id 比较，删去 result_set 中已有的 id
            result_set -= all_ids_set  # 将已经出现过的 id 从 result_set 中移除

            # 更新 all_ids_set，将当前分片的 id 加入其中
            all_ids_set.update(result_set)

            # 将更新后的 result_set 追加到 result_sets 中
            result_sets.append(result_set)

            # 删除当前分片的临时表
            conn.execute("DROP TABLE IF EXISTS current_chunk")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # 清理临时表
        conn.execute("DROP TABLE IF EXISTS original_data")
        conn.execute("DROP TABLE IF EXISTS base_data")
        conn.execute("DROP TABLE IF EXISTS later_data")

    # 返回包含五个分片的 id 集合列表
    return result_sets

import os

def delete_and_split_by_creationDateList0(conn, file_name, filterDateList):
    result_sets = []  # 用于存储每个分段的 ID 集合

    try:
        print(f"begin process {file_name}")
        # 指定 creationDate 为字符串类型
        types = {"creationDate": "STRING"}

        # 创建临时表 original_data 来存储 file_name 文件的所有记录，将 creationDate 转换为 TIMESTAMP 类型
        print(f"load {file_name}")
        conn.execute("""
            CREATE TEMP TABLE original_data AS 
            SELECT *, 
                   TRY_CAST(creationDate AS TIMESTAMP WITH TIME ZONE) AS creationDateTime
            FROM read_csv_auto(?, types := ?)
        """, [file_name, types])

        # 创建第一个表: creationDateTime 小于 filterDateList[0] 的数据
        print("create base data")
        conn.execute("""
            CREATE TABLE base_data AS
            SELECT * 
            FROM original_data
            WHERE creationDateTime <= CAST(? AS TIMESTAMP WITH TIME ZONE)
        """, [filterDateList[0]])

        # 导出 base_data 表
        base_name = os.path.basename(file_name)
        output_dir = NEW_PREFIX + "modified_0/"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        base_file_name = os.path.join(output_dir, base_name)
        print(f"begin dump base data to {base_file_name}")
        conn.execute(f"""
            COPY (SELECT * FROM base_data) 
            TO '{base_file_name}' (DELIMITER '|')
        """)
        print(f"Base data has been saved to {base_file_name}")

        all_ids_set = set()

        # 对 filterDateList 进行分段处理
        for i in range(len(filterDateList)):
            # 获取当前分段的起始日期和结束日期
            start_date = filterDateList[i]
            end_date = filterDateList[i + 1] if i + 1 < len(filterDateList) else None

            # 创建当前分段的表，并根据起始和结束日期筛选数据
            if end_date:
                conn.execute("""
                    CREATE TEMP TABLE current_segment AS
                    SELECT * 
                    FROM original_data
                    WHERE creationDateTime > CAST(? AS TIMESTAMP WITH TIME ZONE)
                      AND creationDateTime <= CAST(? AS TIMESTAMP WITH TIME ZONE)
                    ORDER BY creationDateTime
                """, [start_date, end_date])
            else:
                conn.execute("""
                    CREATE TEMP TABLE current_segment AS
                    SELECT * 
                    FROM original_data
                    WHERE creationDateTime > CAST(? AS TIMESTAMP WITH TIME ZONE)
                    ORDER BY creationDateTime
                """, [start_date])

            # 导出当前分段的表
            output_dir = NEW_PREFIX + f"modified_{i+1}/"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            current_file_name = os.path.join(output_dir, base_name)
            print(f"begin dump later data to {current_file_name}")
            conn.execute(f"""
                COPY (SELECT * FROM base_data
                      UNION ALL
                      SELECT * FROM current_segment)
                TO '{current_file_name}' (DELIMITER '|')
            """)
            print(f"Data with up to date {i+1} has been saved to {current_file_name}")

            # 获取当前分段的 ID 集合
            current_segment_ids = conn.execute("SELECT id FROM current_segment").fetchall()
            result_set = set(row[0] for row in current_segment_ids)

            # 去重处理：删除已存在的 ID
            result_set -= all_ids_set
            all_ids_set.update(result_set)
            result_sets.append(result_set)

            # 删除当前分段的临时表
            conn.execute("DROP TABLE IF EXISTS current_segment")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # 清理临时表
        conn.execute("DROP TABLE IF EXISTS original_data")
        conn.execute("DROP TABLE IF EXISTS base_data")

    # 返回包含五个分段的 ID 集合列表
    return result_sets

def delete_and_split_by_creationDateList(conn, file_name, filterDateList):
    result_sets = []  # 用于存储每个分段的 ID 集合

    try:
        print(f"begin process {file_name}")
        # 指定 creationDate 为字符串类型
        types = {"creationDate": "STRING"}

        # 创建临时表 original_data 来存储 file_name 文件的所有记录，将 creationDate 转换为 TIMESTAMP 类型
        print(f"load {file_name}")
        conn.execute("""
            CREATE TEMP TABLE original_data AS 
            SELECT *, 
                   TRY_CAST(creationDate AS TIMESTAMP WITH TIME ZONE) AS creationDateTime
            FROM read_csv_auto(?, types := ?)
        """, [file_name, types])

        # 创建第一个表: creationDateTime 小于 filterDateList[0] 的数据
        print("create base data")
        conn.execute("""
            CREATE TABLE base_data AS
            SELECT * 
            FROM original_data
            WHERE creationDateTime <= CAST(? AS TIMESTAMP WITH TIME ZONE)
            ORDER BY creationDateTime
        """, [filterDateList[0]])

        # 获取所有列名，排除 creationDateTime
        columns_query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'base_data'"
        columns = conn.execute(columns_query).fetchall()
        column_names = [col[0] for col in columns if col[0] != 'creationDateTime']
        columns_str = ', '.join(column_names)

        # 导出 base_data 表
        base_name = os.path.basename(file_name)
        output_dir = NEW_PREFIX + "modified_0/"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        base_file_name = os.path.join(output_dir, base_name)
        print(f"begin dump base data to {base_file_name}")
        conn.execute(f"""
            COPY (SELECT {columns_str} FROM base_data) 
            TO '{base_file_name}' (DELIMITER '|')
        """)
        print(f"Base data has been saved to {base_file_name}")

        all_ids_set = set()
        
        start_date = filterDateList[0]
        date_str=str(start_date)
        print(f"start date is {date_str}")
        # 对 filterDateList 进行分段处理
        for i in range(1,len(filterDateList)+1):
            # 获取当前分段的起始日期和结束日期
            end_date = filterDateList[i] if i < len(filterDateList) else None
            if end_date!=None:
                date_str=str(end_date)
                print(f"end data is {date_str}")

                # 创建当前分段的表，并根据起始和结束日期筛选数据
                conn.execute("""
                    CREATE TEMP TABLE current_segment AS
                    SELECT * 
                    FROM original_data
                    WHERE creationDateTime > CAST(? AS TIMESTAMP WITH TIME ZONE)
                    AND creationDateTime <= CAST(? AS TIMESTAMP WITH TIME ZONE)
                    ORDER BY creationDateTime
                """, [start_date, end_date])
            else:
                # 创建当前分段的表，并根据起始和结束日期筛选数据
                conn.execute("""
                    CREATE TEMP TABLE current_segment AS
                    SELECT * 
                    FROM original_data
                    WHERE creationDateTime > CAST(? AS TIMESTAMP WITH TIME ZONE)
                    ORDER BY creationDateTime
                """, [filterDateList[i-1]])

            # 导出当前分段的表
            output_dir = NEW_PREFIX + f"modified_{i*2}/"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            current_file_name = os.path.join(output_dir, base_name)
            # print(f"begin dump later data to {current_file_name}")
            conn.execute(f"""
                COPY (SELECT {columns_str} FROM base_data
                      UNION ALL
                      SELECT {columns_str} FROM current_segment)
                TO '{current_file_name}' (DELIMITER '|')
            """)
            # print(f"Data with up to date {i*2} has been saved to {current_file_name}")

            # 获取当前分段的 ID 集合
            current_segment_ids = conn.execute("SELECT id FROM current_segment").fetchall()
            result_set = set(row[0] for row in current_segment_ids)

            # 去重处理：删除已存在的 ID
            result_set -= all_ids_set
            all_ids_set.update(result_set)
            result_sets.append(result_set)

            # 删除当前分段的临时表
            conn.execute("DROP TABLE IF EXISTS current_segment")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # 清理临时表
        conn.execute("DROP TABLE IF EXISTS original_data")
        conn.execute("DROP TABLE IF EXISTS base_data")

    # 返回包含五个分段的 ID 集合列表
    for set1 in result_sets:
        print(len(set1))
    return result_sets


def delete_from_single_vid(conn, file_name, column_name, set_list):
    """
    This function reads the given CSV file into a table `original_data`, then for each set in `set_list`,
    creates a temporary table with a single column (`column_name`), deletes corresponding elements
    from the `original_data` table based on the entries in each temporary table, and exports the updated
    data to a new file after each deletion.

    :param conn: DuckDB connection object
    :param file_name: The CSV file to load into the database
    :param column_name: The column name to filter on (e.g., ID column)
    :param set_list: List of sets, where each set contains IDs to delete
    :return: None (but exports modified files after each deletion)
    """
    try:
        print(f"begin process {file_name}")
        # Load the CSV file into a table named `original_data`
        types = {column_name: "STRING"}
        print(f"begin load original data")
        conn.execute(f"""
            CREATE TEMP TABLE original_data AS 
            SELECT * 
            FROM read_csv_auto(?, types := ?)
        """, [file_name, types])

        # Create tables for each set in the `set_list`
        table_list = []
        for i, data_set in enumerate(set_list):
            # Create a temporary table for each set, containing only the column_name (ID column)
            table_name = f"set_data_{i+1}"
            ids = list(data_set)
            print(f"create temp table {i}")
            conn.execute(f"""
                CREATE TEMP TABLE {table_name} ("{column_name}" STRING)
            """)

            # Insert the set data (IDs) into the temporary table
            conn.executemany(f"""
                INSERT INTO {table_name} ("{column_name}")
                VALUES (?)
            """, [(id,) for id in ids])

            # Add table name to the table list
            table_list.append(table_name)

        # Traverse table_list in reverse order to perform deletion and file export
        for i in range(len(table_list)-1, -1, -1):
            temp_table = table_list[i]

            # Delete elements from the original data that are in the temp table
            print(f"delete data from original data by set {i}")
            conn.execute(f"""
                DELETE FROM original_data
                WHERE "{column_name}" IN (SELECT "{column_name}" FROM {temp_table})
            """)

            # Export the modified data to a new file
            base_name=base_name = os.path.basename(file_name)
            # 构建目标目录路径
            output_dir = NEW_PREFIX + f"modified_{i*2}/"
            # 检查目标目录是否存在，如果不存在则创建它
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            # 构建完整的输出文件路径
            output_file = os.path.join(output_dir, base_name)
            print(f"begin write data to {output_file}")
            conn.execute(f"""
                COPY (SELECT * FROM original_data)
                TO '{output_file}' (DELIMITER '|')
            """)
            print(f"Modified data after deleting from table {temp_table} has been saved to {output_file}")

            # Drop the current temp table
            conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Clean up the original data temporary table
        conn.execute("DROP TABLE IF EXISTS original_data")

def delete_list_from_single_vid(conn, file_list, column_name, set_list):
    """
    This function reads the given CSV file into a table `original_data`, then for each set in `set_list`,
    creates a temporary table with a single column (`column_name`), deletes corresponding elements
    from the `original_data` table based on the entries in each temporary table, and exports the updated
    data to a new file after each deletion.

    :param conn: DuckDB connection object
    :param file_name: The CSV file to load into the database
    :param column_name: The column name to filter on (e.g., ID column)
    :param set_list: List of sets, where each set contains IDs to delete
    :return: None (but exports modified files after each deletion)
    """
    try:        
        table_list = []
        for i, data_set in enumerate(set_list):
            # Create a temporary table for each set, containing only the column_name (ID column)
            table_name = f"set_data_{i+1}"
            ids = list(data_set)
            print(f"create temp table {i}")
            # conn.execute(f"""
            #     CREATE TEMP TABLE {table_name} ("{column_name}" STRING)
            # """)

            # # Insert the set data (IDs) into the temporary table
            # conn.executemany(f"""
            #     INSERT INTO {table_name} ("{column_name}")
            #     VALUES (?)
            # """, [(id,) for id in ids])

            # 将数据转换为 DataFrame
            df = pd.DataFrame(ids, columns=[column_name])
            df[column_name] = df[column_name].astype(str)
            # 将 DataFrame df 作为一个临时表传入
            conn.register("df_temp", df)  # 将 DataFrame 注册为 DuckDB 的临时表

            # 使用 SQL 从临时表插入数据到目标表
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df_temp")
            conn.unregister("df_temp")
            # Add table name to the table list
            table_list.append(table_name)

        # print(f"begin process {file_name}")
        # Load the CSV file into a table named `original_data`
        for file_name in file_list:
            file_name=os.path.join(PREFIX,file_name)
            types = {column_name: "STRING"}
            print(f"begin load original data")
            conn.execute(f"""
                CREATE TEMP TABLE original_data AS 
                SELECT * 
                FROM read_csv_auto(?, types := ?)
            """, [file_name, types])

            # Create tables for each set in the `set_list`

            # Traverse table_list in reverse order to perform deletion and file export
            for i in range(len(table_list)-1, -1, -1):
                temp_table = table_list[i]

                # Delete elements from the original data that are in the temp table
                print(f"delete data from original data by set {i}")
                conn.execute(f"""
                    DELETE FROM original_data
                    WHERE "{column_name}" IN (SELECT "{column_name}" FROM {temp_table})
                """)

                # Export the modified data to a new file
                base_name=base_name = os.path.basename(file_name)
                # 构建目标目录路径
                output_dir = NEW_PREFIX + f"modified_{i*2}/"
                # 检查目标目录是否存在，如果不存在则创建它
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                # 构建完整的输出文件路径
                output_file = os.path.join(output_dir, base_name)
                print(f"begin write data to {output_file}")
                conn.execute(f"""
                    COPY (SELECT * FROM original_data)
                    TO '{output_file}' (DELIMITER '|')
                """)
                print(f"Modified data after deleting from table {temp_table} has been saved to {output_file}")
            conn.execute("DROP TABLE IF EXISTS original_data")

        for i in range(len(table_list)-1, -1, -1):
            temp_table = table_list[i]    # Drop the current temp table
            conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Clean up the original data temporary table
        conn.execute("DROP TABLE IF EXISTS original_data")

def delete_from_double_vid(conn, file_name, column_name1, column_name2, set_list1, set_list2):
    """
    This function reads the given CSV file into a table `original_data`, then for each set in `set_list1` and `set_list2`,
    creates temporary tables with a single column (`column_name1` or `column_name2`), deletes corresponding elements
    from the `original_data` table based on the entries in each temporary table, and exports the updated data to a new file 
    after both deletions for each set.

    :param conn: DuckDB connection object
    :param file_name: The CSV file to load into the database
    :param column_name1: The first column name (e.g., Person.id for first condition)
    :param column_name2: The second column name (e.g., Post.id for second condition)
    :param set_list1: List of sets, where each set contains IDs to delete for column_name1
    :param set_list2: List of sets, where each set contains IDs to delete for column_name2
    :return: None (but exports modified files after each deletion)
    """
    try:
        # Load the CSV file into a table named `original_data`
        print(f"begin process {file_name}")
        types = {column_name1: "STRING", column_name2: "STRING"}
        conn.execute(f"""
            CREATE TEMP TABLE original_data AS 
            SELECT * 
            FROM read_csv_auto(?, types := ?)
        """, [file_name, types])

        # Create tables for each set in set_list1 (for column_name1)
        table_list1 = []
        for i, data_set in enumerate(set_list1):
            # Create a temporary table for each set, containing only the column_name1 (ID column)
            table_name = f"set_data1_{i+1}"
            ids = list(data_set)

            print(f"begin create temp table1 {i}")
            # conn.execute(f"""
            #     CREATE TEMP TABLE {table_name} ("{column_name1}" STRING)
            # """)

            # 将数据转换为 DataFrame
            df = pd.DataFrame(ids, columns=[column_name1])
            df[column_name1] = df[column_name1].astype(str)
            # 将 DataFrame df 作为一个临时表传入
            conn.register("df_temp", df)  # 将 DataFrame 注册为 DuckDB 的临时表

            # 使用 SQL 从临时表插入数据到目标表
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df_temp")

            # Add table name to the table list
            table_list1.append(table_name)

        # Create tables for each set in set_list2 (for column_name2)
        table_list2 = []
        for i, data_set in enumerate(set_list2):
            # Create a temporary table for each set, containing only the column_name2 (ID column)
            table_name = f"set_data2_{i+1}"
            ids = list(data_set)
            print(f"begin create temp table2 {i}")
            # conn.execute(f"""
            #     CREATE TEMP TABLE {table_name} ("{column_name2}" STRING)
            # """)

            # # Insert the set data (IDs) into the temporary table
            # conn.executemany(f"""
            #     INSERT INTO {table_name} ("{column_name2}")
            #     VALUES (?)
            # """, [(id,) for id in ids])

            # 将数据转换为 DataFrame
            df = pd.DataFrame(ids, columns=[column_name2])
            df[column_name2] = df[column_name2].astype(str)
            # 将 DataFrame df 作为一个临时表传入
            conn.register("df_temp", df)  # 将 DataFrame 注册为 DuckDB 的临时表

            # 使用 SQL 从临时表插入数据到目标表
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df_temp")

            # Add table name to the table list
            table_list2.append(table_name)
            conn.unregister("df_temp")

        # Combine both lists and iterate over them simultaneously
        for i, (temp_table1, temp_table2) in enumerate(zip(reversed(table_list1), reversed(table_list2))):
            # Delete elements from the original data that are in the temp table1 (for column_name1)
            print(f"begin delete form temp table1 {i}")
            conn.execute(f"""
                DELETE FROM original_data
                WHERE "{column_name1}" IN (SELECT "{column_name1}" FROM {temp_table1})
            """)
            print(f"begin delete from temp table2 {i}")
            # Delete elements from the original data that are in the temp table2 (for column_name2)
            conn.execute(f"""
                DELETE FROM original_data
                WHERE "{column_name2}" IN (SELECT "{column_name2}" FROM {temp_table2})
            """)

            # Export the modified data to a new file
            base_name=base_name = os.path.basename(file_name)
            j=4-i
            # 构建目标目录路径
            output_dir = NEW_PREFIX + f"modified_{j*2}/"
            # 检查目标目录是否存在，如果不存在则创建它
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            # 构建完整的输出文件路径
            output_file = os.path.join(output_dir, base_name)
            print(f"begin dump to {output_file}")
            conn.execute(f"""
                COPY (SELECT * FROM original_data)
                TO '{output_file}' (DELIMITER '|')
            """)
            print(f"Modified data after deleting from {temp_table1} for {column_name1} and {temp_table2} for {column_name2} has been saved to {output_file}")

            # Drop the current temp tables for both column_name1 and column_name2
            conn.execute(f"DROP TABLE IF EXISTS {temp_table1}")
            conn.execute(f"DROP TABLE IF EXISTS {temp_table2}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Clean up the original data temporary table
        conn.execute("DROP TABLE IF EXISTS original_data")

def rename_specific_column_in_csv(filename, columnname1, columnname2, occurrence=1):
    """
    修改CSV文件中指定列的名称，当文件中有多个同名列时，根据指定的occurrence来修改列名。

    :param filename: CSV文件的路径
    :param columnname1: 需要修改的列的当前列名
    :param columnname2: 修改后的新列名
    :param occurrence: 要修改的列的出现次数，默认为1，表示第一个匹配的列
    :return: 无返回，直接修改并保存文件
    """
    try:
        # 读取CSV文件
        df = pd.read_csv(filename,sep='|')
        print(df.columns)
        # 获取所有列名为columnname1的列的索引
        column_indices = [i for i, col in enumerate(df.columns) if col == columnname1]
        
        if column_indices:
            # 如果找到了该列，检查occurrence是否有效
            if occurrence <= len(column_indices):
                # 获取要修改的列的索引
                column_index_to_rename = column_indices[occurrence - 1]
                # 修改指定列的列名
                df.columns.values[column_index_to_rename] = columnname2
                
                base_name='new_'+os.path.basename(filename)
                # 将修改后的DataFrame保存回CSV文件
                df.to_csv(NEW_PREFIX+base_name, index=False,sep='|')

                print(f"列 '{columnname1}' 的第 {occurrence} 次出现已被重命名为 '{columnname2}'，并保存到文件 {base_name}。")
            else:
                print(f"在文件中只找到 {len(column_indices)} 次列 '{columnname1}'，第 {occurrence} 次不存在。")
        else:
            print(f"文件中不存在列 '{columnname1}'。")
    
    except Exception as e:
        print(f"发生错误: {e}")

def rename_back_columns_in_csv(filename,new_filename, columnname1, columnname2, new_columnname, occurrence1=1, occurrence2=1):
    """
    修改CSV文件中指定列的名称，当文件中有多个同名列时，根据指定的occurrence分别修改columnname1和columnname2的列名。

    :param filename: CSV文件的路径
    :param columnname1: 需要修改的第一个列的当前列名
    :param columnname2: 需要修改的第二个列的当前列名
    :param new_columnname: 修改后的新列名
    :param occurrence1: 修改第一个列的出现次数，默认为1，即第一个匹配的列
    :param occurrence2: 修改第二个列的出现次数，默认为1，即第一个匹配的列
    :return: 无返回，直接修改并保存文件
    """
    try:
        # 读取CSV文件
        df = pd.read_csv(filename, sep='|')
        
        # 获取所有列名为columnname1的列的索引
        column_indices1 = [i for i, col in enumerate(df.columns) if col == columnname1]
        # 获取所有列名为columnname2的列的索引
        column_indices2 = [i for i, col in enumerate(df.columns) if col == columnname2]

        # 修改第一个指定列的列名
        if column_indices1 and occurrence1 <= len(column_indices1):
            column_index_to_rename1 = column_indices1[occurrence1 - 1]
            df.columns.values[column_index_to_rename1] = new_columnname
        else:
            print(f"在文件中找不到第 {occurrence1} 次出现的列 '{columnname1}'。")

        # 修改第二个指定列的列名
        if column_indices2 and occurrence2 <= len(column_indices2):
            column_index_to_rename2 = column_indices2[occurrence2 - 1]
            df.columns.values[column_index_to_rename2] = new_columnname
        else:
            print(f"在文件中找不到第 {occurrence2} 次出现的列 '{columnname2}'。")

        # 将修改后的DataFrame保存回原CSV文件
        df.to_csv(new_filename, index=False, sep='|')
        print(f"列 '{columnname1}' 和 '{columnname2}' 已被重命名为 '{new_columnname}'，并保存到文件 {new_filename}。")

    except Exception as e:
        print(f"发生错误: {e}")

def rand_delete_edge(filename):
    # 读取原始 CSV 文件
    df = pd.read_csv(filename)
    
    # 获取文件名和扩展名，用于生成输出文件名
    basename=os.path.basename(filename)
    
    # 计算要删除的行数（1%）
    num_rows_to_delete = int(len(df) * 0.01)
    
    for i in range(1, 6):  # 进行 5 次迭代
        # 随机选择要删除的行索引
        rows_to_delete = random.sample(range(len(df)), num_rows_to_delete)
        
        # 删除选择的行
        df = df.drop(rows_to_delete).reset_index(drop=True)
        j=5-i
        # 输出新的 CSV 文件
        output_dir = NEW_PREFIX + f"modified_{j*2}/"
         # 检查目标目录是否存在，如果不存在则创建它
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        output_filename = os.path.join(output_dir, basename)
        df.to_csv(output_filename, index=False)
        print(f"生成文件：{output_filename}")

PREFIX = os.getenv('PREFIX_INPUT')
# PREFIX='/data-1/yichengzhang/data/experiment_space/LDBC_SNB-nvme/nvme/100_data/sf100/social_network/dynamic/'
comment_csv=PREFIX+'comment_0_0.csv'
post_csv=PREFIX+'post_0_0.csv'
forum_csv=PREFIX+'forum_0_0.csv'

NEW_PREFIX = os.getenv('PREFIX_OUTPUT')
# NEW_PREFIX='/data-1/yichengzhang/data/experiment_space/LDBC_SNB-nvme/nvme/breakdown/'


conn = duckdb.connect()
conn.execute("SET enable_progress_bar = true")


filtered_date_list=find_creation_date_partition(conn,comment_csv,post_csv,forum_csv,0.9)
with open(NEW_PREFIX+'filter_data.json','w') as json_file:
    result_list=[]
    for date in filtered_date_list:
        result_list.append(str(date))
    json.dump(result_list,json_file,indent=4)

file_list=['comment_0_0.csv','forum_0_0.csv','post_0_0.csv']

file_set_dict={}
output_set_dict={}
# 遍历指定目录中的所有文件
for file_name in file_list:
    # 获取完整文件路径
    file_path = os.path.join(PREFIX, file_name)
    # 检查是否为文件
    if os.path.isfile(file_path):
            print(f"Processing file: {file_path}")
            # 调用 delete_and_split_by_creationDate 函数
            result_set=delete_and_split_by_creationDateList(conn, file_path, filtered_date_list)
        # 将每个 set 转换为 list 存储
            file_set_dict[file_name] = result_set
            output_set_dict[file_name] = [list(s) if isinstance(s, set) else s for s in result_set]

# exit()

import json
# 保存到 JSON 文件
set_dir = NEW_PREFIX + f"filtered_sets/"
# 检查目标目录是否存在，如果不存在则创建它
if not os.path.exists(set_dir):
    os.makedirs(set_dir)
for filename in output_set_dict:
    name=filename.split('_')[0]
    cur_dir=set_dir+name
    if not os.path.exists(cur_dir):
        os.makedirs(cur_dir)
    for i in range(0,len(output_set_dict[filename])):
        with open(os.path.join(cur_dir,f'{name}_{i*2}_{i*2+2}'),'w') as outfile:
            for key in output_set_dict[filename][i]:
                outfile.write(f"{key}\n")  # Write each key on a new line

# exit()
filename=PREFIX+'new_comment_replyOf_comment_0_0.csv'
print("begin delete")
# delete_from_single_vid(conn,filename,'Person.id',file_set_dict['person_0_0.csv'])
# person_single_vid_list=['person_hasInterest_tag_0_0.csv','person_isLocatedIn_place_0_0.csv','person_studyAt_organisation_0_0.csv','person_workAt_organisation_0_0.csv']
forum_single_vid_list=['forum_hasTag_tag_0_0.csv','forum_hasMember_person_0_0.csv','forum_hasModerator_person_0_0.csv']
comment_single_vid_list=['comment_hasTag_tag_0_0.csv','comment_isLocatedIn_place_0_0.csv','comment_hasCreator_person_0_0.csv','person_likes_comment_0_0.csv']
post_single_vid_list=['post_hasTag_tag_0_0.csv','post_isLocatedIn_place_0_0.csv','post_hasCreator_person_0_0.csv','person_likes_post_0_0.csv']

# for filename in person_single_vid_list:
#     file_path=os.path.join(PREFIX,filename)
#     delete_from_single_vid(conn,filename,"Person.id",file_set_dict['person_0_0.csv'])

# for filename in forum_single_vid_list:
#     file_path=os.path.join(PREFIX,filename)
#     delete_from_single_vid(conn,file_path,"Forum.id",file_set_dict['forum_0_0.csv'])
# for filename in comment_single_vid_list:
#     file_path=os.path.join(PREFIX,filename)
#     delete_from_single_vid(conn,file_path,"Comment.id",file_set_dict['comment_0_0.csv'])
# for filename in post_single_vid_list:
#     file_path=os.path.join(PREFIX,filename)
#     delete_from_single_vid(conn,file_path,"Post.id",file_set_dict['post_0_0.csv'])

delete_list_from_single_vid(conn,forum_single_vid_list,"Forum.id",file_set_dict['forum_0_0.csv'])
delete_list_from_single_vid(conn,comment_single_vid_list,"Comment.id",file_set_dict['comment_0_0.csv'])
delete_list_from_single_vid(conn,post_single_vid_list,"Post.id",file_set_dict['post_0_0.csv'])

filename='comment_replyOf_post_0_0.csv'
delete_from_double_vid(conn,PREFIX+filename,'Comment.id','Post.id',file_set_dict['comment_0_0.csv'],file_set_dict['post_0_0.csv'])
filename='forum_containerOf_post_0_0.csv'
delete_from_double_vid(conn,PREFIX+filename,'Forum.id','Post.id',file_set_dict['forum_0_0.csv'],file_set_dict['post_0_0.csv'])

# old_person_filename='person_knows_person_0_0.csv'
old_comment_filename='comment_replyOf_comment_0_0.csv'

# rename_specific_column_in_csv(PREFIX+old_person_filename,"Person.id","Person.id1")
rename_specific_column_in_csv(PREFIX+old_comment_filename,"Comment.id","Comment.id1")

filename='new_comment_replyOf_comment_0_0.csv'
filename=NEW_PREFIX+filename
delete_from_double_vid(conn,filename,'Comment.id1','Comment.id.1',file_set_dict['comment_0_0.csv'],file_set_dict['comment_0_0.csv'])
# filename='new_person_knows_person_0_0.csv'
# delete_from_double_vid(conn,filename,'Person.id1','Person.id.1',file_set_dict['person_0_0.csv'],file_set_dict['person_0_0.csv'])

# 遍历NEW_PREFIX目录下的所有目录和文件
for root, dirs, files in os.walk(NEW_PREFIX):
    # 检查当前目录是否以modified_开头
    if root.startswith(NEW_PREFIX + 'modified_'):
        # 遍历当前目录下的所有文件
        for file in files:
            # 检查文件名是否为filename
            if file == 'new_comment_replyOf_comment_0_0.csv':
                print(f'modified {file}')
                # 构造完整的文件路径
                filepath = os.path.join(root, file)
                newfilepath=os.path.join(root,'comment_replyOf_comment_0_0.csv')
                # 调用func函数
                rename_back_columns_in_csv(filepath,newfilepath,'Comment.id1','Comment.id.1','Comment.id')

person_knows_person_file=PREFIX+'person_knows_person_0_0.csv'
rand_delete_edge(person_knows_person_file)

for i in range(0,5):
    cur_dir=NEW_PREFIX+f'modified_{i*2}/'
    shutil.copy(PREFIX+'person_0_0.csv',cur_dir)
    shutil.copy(PREFIX+'person_hasInterest_tag_0_0.csv',cur_dir)
    shutil.copy(PREFIX+'person_isLocatedIn_place_0_0.csv',cur_dir)
    shutil.copy(PREFIX+'person_studyAt_organisation_0_0.csv',cur_dir)
    shutil.copy(PREFIX+'person_workAt_organisation_0_0.csv',cur_dir)


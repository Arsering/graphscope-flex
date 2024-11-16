import csv
from collections import defaultdict
from tqdm import tqdm
import pandas as pd
import duckdb
from datetime import datetime

# 辅助函数：处理边关系，构建对应的出边和入边图
def process_edges(file, from_col, to_col, out_graph, in_graph,out_flag,in_flag):
    with open(file, newline='') as csvfile:
        total_lines = sum(1 for row in csvfile) - 1  # 减去表头
    with open(file, 'r') as f:
        reader = csv.reader(f, delimiter='|')
        next(reader)  # 跳过表头
        for row in tqdm(reader, total=total_lines, desc=f"Processing {file}"):
            from_id = int(row[from_col])
            to_id = int(row[to_col])
            if out_flag:
                out_graph[from_id].append(to_id)  # 出边图
            if in_flag:
                in_graph[to_id].append(from_id)   # 入边图

def person_normal_order(person_csv):
    person_list=[]
    with open(person_csv,'r') as f:
        reader=csv.reader(f,delimiter='|')
        header=next(reader)
        for row in reader:
            person_list.append(int(row[0]))
    return person_list

def get_item_date_map(item_csv):
    item_id_to_date={}
    with open(item_csv, newline='') as csvfile:
        total_lines = sum(1 for row in csvfile) - 1  # 减去表头
    with open(item_csv, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter='|')
        # 遍历每一行
        for row in tqdm(reader,total=total_lines,desc="Begin get item date map"):
            # 获取id和creationDate
            record_id = int(row['id'])
            creation_date = row['creationDate']
            # 将creationDate转换为datetime对象
            try:
                parsed_date = datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f%z')
            except ValueError:
                try:
                    # 如果没有毫秒部分
                    parsed_date = datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S%z')
                except ValueError:
                    # 捕捉异常, 打印错误信息
                    print(f"无法解析日期: {creation_date}")
                    continue
            # 将id和转换后的日期存入字典
            item_id_to_date[record_id] = parsed_date
    return item_id_to_date

def get_item_date_map_duck(conn, item_csv):
    # 创建一个临时表来存储 CSV 数据
    conn.execute(f"""
        CREATE TEMP TABLE items AS 
        SELECT id, TRY_CAST(creationDate AS TIMESTAMP WITH TIME ZONE) AS creation_date
        FROM read_csv_auto('{item_csv}', delim='|')
    """)

    # 查询解析成功的记录，并将其加载到字典中
    query = "SELECT id, creation_date FROM items WHERE creation_date IS NOT NULL"
    item_id_to_date = {}
    
    for record_id, creation_date in tqdm(conn.execute(query).fetchall(), desc="Begin get item date map"):
        item_id_to_date[record_id] = creation_date

    # 删除临时表以释放资源
    conn.execute("DROP TABLE items")

    return item_id_to_date

def order_item_by_person(conn,person_list,item_hasCreator_person_csv,item_csv,filtered_date):
    person_create_item_out = defaultdict(list)
    person_create_item_in = defaultdict(list)
    # 处理 comment_hasCreator_person （注意这里是反转的关系）
    process_edges(item_hasCreator_person_csv, 1, 0, person_create_item_out, person_create_item_in,True,False)
    date_map=get_item_date_map_duck(conn,item_csv)
    totalperson=len(person_list)
    added_items=set()
    item_list=[]
    with tqdm(total=totalperson, desc="Processing ordering") as pbar:
        for person in person_list:
            # 将当前 person 加入到 person_list
            # person_list.append(person)
            to_add_item=[]
            pbar.update(1)
            # 处理 comment 的 create
            for item in person_create_item_out.get(person, []):
                if item not in added_items and date_map[item].replace(tzinfo=None)<filtered_date:
                    to_add_item.append(item)
                    added_items.add(item)
            to_add_item.sort(key=lambda x:date_map[x],reverse=False)
            item_list.extend(to_add_item)  # 加入 comment_list
    return item_list

def reorder_and_write_csv(original_csv, new_csv, ordered_ids):
    # 读取原始 CSV 文件的数据
    with open(original_csv, 'r') as f:
        reader = csv.reader(f, delimiter='|')
        header = next(reader)  # 保存表头
        data = {}
        # 使用tqdm显示读取数据的进度
        for row in tqdm(reader, desc=f"Reading {original_csv}", unit="row"):
            data[int(row[0])] = row  # 根据第一列ID作为键的字典

    # 重新排序的列表
    ordered_data = [data[pid] for pid in tqdm(ordered_ids, desc="Sorting data", unit="id") if pid in data]
    inset = set(ordered_ids)  # 优化：直接用set避免重复查找
    # 找到未在ordered_ids中的ID，追加到列表末尾
    remaining_data = [row for pid, row in tqdm(data.items(), desc="Finding remaining data", unit="id") if pid not in inset]
    
    # 将重新排序的数据和剩余数据组合
    combined_data = ordered_data + remaining_data
    
    # 将数据转换为 DataFrame
    df = pd.DataFrame(combined_data, columns=header)

    # 使用 pandas to_csv 写入文件
    tqdm.pandas(desc=f"Writing to {new_csv}")
    df.to_csv(new_csv, index=False, sep='|')

def reorder_and_write_csv_duck(conn, original_csv, new_csv, ordered_ids_csv):
    types = {"creationDate": "VARCHAR"}
    print('read ordered id file')
    # 创建一个临时表来存储ordered_ids_csv中的顺序信息，并添加行号
    conn.execute("CREATE TABLE ordered_ids AS "+
                 "SELECT id, row_number() OVER() AS row_num "+
                 "FROM read_csv_auto(?)", [ordered_ids_csv])

    print('make original data table')
    # 创建一个临时表来存储original_csv中的所有记录
    conn.execute("CREATE TABLE original_data AS "+
                 "SELECT * "+
                 "FROM read_csv_auto(?, types = ?)", [original_csv,types])

    # 将ordered_ids_csv中的顺序信息和original_csv中的记录结合起来
    # 并写入新的CSV文件
    query = """
    CREATE TABLE sorted_data AS
    SELECT a.* FROM original_data a
    JOIN ordered_ids b ON a.id = b.id
    ORDER BY b.row_num
    """
    print('sort original table by ordered id')
    conn.execute(query)

    # 找出original_csv中有ID但ordered_ids_csv中没有ID的记录，并追加到sorted_data表中
    query = """
    INSERT INTO sorted_data
    SELECT * FROM original_data
    WHERE id NOT IN (SELECT id FROM ordered_ids)
    """
    print('append remained data in original file')
    conn.execute(query)

    # 将sorted_data表写入新的CSV文件，使用|作为分隔符
    print('dump file')
    conn.execute(f"COPY sorted_data TO '{new_csv}' (DELIMITER '|')")

    # 清理临时表
    conn.execute("DROP TABLE ordered_ids")
    conn.execute("DROP TABLE original_data")
    conn.execute("DROP TABLE sorted_data")

def reorder_and_write_csv_with_date_duck(conn, original_csv, new_csv, ordered_ids_csv):
    # 解析并排序用的 creationDate 类型为 TIMESTAMP
    types = {"creationDate": "TIMESTAMP"}
    
    print('read ordered id file')
    # 创建一个临时表来存储 ordered_ids_csv 中的顺序信息，并添加行号
    conn.execute("CREATE TABLE ordered_ids AS "+
                 "SELECT id, row_number() OVER() AS row_num "+
                 "FROM read_csv_auto(?)", [ordered_ids_csv])

    print('make original data table')
    # 创建一个临时表来存储 original_csv 中的所有记录
    conn.execute("CREATE TABLE original_data AS "+
                 "SELECT * "+
                 "FROM read_csv_auto(?, types = ?)", [original_csv, types])

    # 按 ordered_ids_csv 中的顺序排列 original_csv 中的记录并写入 sorted_data
    query = """
    CREATE TABLE sorted_data AS
    SELECT a.*, CAST(a.creationDate AS VARCHAR) AS creationDate_str
    FROM original_data a
    JOIN ordered_ids b ON a.id = b.id
    ORDER BY b.row_num
    """
    print('sort original table by ordered id')
    conn.execute(query)

    # 找出 original_csv 中有 ID 但 ordered_ids_csv 中没有 ID 的记录，按 creationDate 排序后追加到 sorted_data 表中
    query = """
    INSERT INTO sorted_data
    SELECT *, CAST(creationDate AS VARCHAR) AS creationDate_str
    FROM original_data
    WHERE id NOT IN (SELECT id FROM ordered_ids)
    ORDER BY creationDate
    """
    print('append remained data in original file sorted by creationDate')
    conn.execute(query)

    # 将 sorted_data 表写入新的 CSV 文件，使用 | 作为分隔符
    print('dump file')
    conn.execute(f"COPY (SELECT * FROM sorted_data) TO '{new_csv}' (DELIMITER '|')")

    # 清理临时表
    conn.execute("DROP TABLE ordered_ids")
    conn.execute("DROP TABLE original_data")
    conn.execute("DROP TABLE sorted_data")

def reorder_and_write_csv_with_date_duck1(conn, original_csv, new_csv, ordered_ids_csv):
    try:
        # 指定 creationDate 为 STRING 类型
        types = {"creationDate": "STRING"}

        print('Reading ordered ID file')
        # 创建一个临时表来存储 ordered_ids_csv 中的顺序信息，并添加行号
        conn.execute("""
            CREATE TEMP TABLE ordered_ids AS 
            SELECT id, row_number() OVER() AS row_num 
            FROM read_csv_auto(?)
        """, [ordered_ids_csv])

        print('Creating original data table')
        # 创建一个临时表来存储 original_csv 中的所有记录，并将 creationDate 设置为字符串类型
        conn.execute("""
            CREATE TEMP TABLE original_data AS 
            SELECT *, 
                   TRY_CAST(creationDate AS TIMESTAMP WITH TIME ZONE) AS creationDateTime  -- 转换为 TIMESTAMP
            FROM read_csv_auto(?, types := ?)
        """, [original_csv, types])

        # 按 ordered_ids_csv 中的顺序排列 original_csv 中的记录，并创建 sorted_data 表
        conn.execute("""
            CREATE TABLE sorted_data AS
            SELECT a.* 
            FROM original_data a
            JOIN ordered_ids b ON a.id = b.id
            ORDER BY b.row_num
        """)

        # 将 original_csv 中有 ID 但不在 ordered_ids_csv 中的记录按 creationDateTime 排序后追加到 sorted_data 表中
        conn.execute("""
            INSERT INTO sorted_data
            SELECT * FROM original_data
            WHERE id NOT IN (SELECT id FROM ordered_ids)
            ORDER BY creationDateTime
        """)

        # 获取 sorted_data 表的所有列名，去掉 `creationDateTime`
        columns_query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'sorted_data'"
        columns = conn.execute(columns_query).fetchall()

        # 提取列名并去掉 `creationDateTime`
        column_names = [col[0] for col in columns if col[0] != 'creationDateTime']

        # 生成 SQL 语句
        columns_str = ', '.join(column_names)
        copy_query = f"""
            COPY (SELECT {columns_str} FROM sorted_data) 
            TO '{new_csv}' (DELIMITER '|')
        """
        conn.execute(copy_query)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # 清理临时表
        conn.execute("DROP TABLE IF EXISTS ordered_ids")
        conn.execute("DROP TABLE IF EXISTS original_data")
        conn.execute("DROP TABLE IF EXISTS sorted_data")


def find_creation_date_partition(conn, csv_file, percentage):
    # 读取 CSV 文件并创建 DuckDB 表
    conn.execute(f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{csv_file}')")

    # 将 creationDate 字段转换为 TIMESTAMP 类型，确保按日期排序
    conn.execute("ALTER TABLE data ALTER creationDate TYPE TIMESTAMP")

    # 使用 PERCENT_RANK() 窗口函数找到给定百分位的 creationDate
    query = """
    SELECT creationDate
    FROM (
        SELECT creationDate, 
               PERCENT_RANK() OVER (ORDER BY creationDate) AS percentile
        FROM data
    ) AS ranked_data
    WHERE percentile >= ?
    ORDER BY percentile
    LIMIT 1
    """
    
    # 执行查询并获取结果
    result = conn.execute(query, [percentage]).fetchone()

    # 关闭连接
    # conn.close()

    # 返回找到的 creationDate 值
    return result[0] if result else None

PREFIX='/nvme0n1/Cnew_db/sf30/social_network/dynamic/'
# PREFIX='/data-1/yichengzhang/data/data_processing/scripts/IOE-Order/dataset/100_data/sf100/social_network/dynamic/'
person_csv=PREFIX+'person_0_0.csv'
comment_csv=PREFIX+'comment_0_0.csv'
post_csv=PREFIX+'post_0_0.csv'
person_likes_comment_csv=PREFIX+'person_likes_comment_0_0.csv'
person_likes_post_csv=PREFIX+'person_likes_post_0_0.csv'
person_knows_person_csv=PREFIX+'person_knows_person_0_0.csv'
comment_hasCreator_person_csv=PREFIX+'comment_hasCreator_person_0_0.csv'
comment_reply_comment_csv=PREFIX+'comment_replyOf_comment_0_0.csv'
comment_reply_post_csv=PREFIX+'comment_replyOf_post_0_0.csv'
post_hasCreator_person_csv=PREFIX+'post_hasCreator_person_0_0.csv'

# NEW_PREFIX='/data-1/yichengzhang/data/data_processing/scripts/IOE-Order/dataset/100_data/sf100/new_dynamic/'
NEW_PREFIX = PREFIX
new_person_csv=NEW_PREFIX+'new_person_0_0.csv'
new_comment_csv=NEW_PREFIX+'new_comment_0_0.csv'
new_post_csv=NEW_PREFIX+'new_post_0_0.csv'
new_create_csv=NEW_PREFIX+'new_comment_hasCreator_person_0_0.csv'
new_knows_csv=NEW_PREFIX+'new_person_knows_person_0_0.csv'

person_list_file=NEW_PREFIX+'person_file.csv'
comment_list_file=NEW_PREFIX+'comment_file.csv'
post_list_file=NEW_PREFIX+'post_file.csv'


conn = duckdb.connect()
conn.execute("SET enable_progress_bar = true")

# person_list=perosn_ordering(person_knows_person_csv)
filtered_date=find_creation_date_partition(conn, comment_csv, 1)
print(filtered_date)

person_list=person_normal_order(person_csv)
reorder_and_write_csv(person_csv,new_person_csv,person_list)

print('begin write person order')
with open(person_list_file, 'w') as ids_file:
    pd.DataFrame(person_list, columns=["id"]).to_csv(ids_file, index=False)

comment_list=order_item_by_person(conn,person_list,comment_hasCreator_person_csv,comment_csv,filtered_date)
print('begin write comment order')
with open(comment_list_file, 'w') as ids_file:
    pd.DataFrame(comment_list, columns=["id"]).to_csv(ids_file, index=False)
del comment_list

post_list=order_item_by_person(conn,person_list,post_hasCreator_person_csv,post_csv,filtered_date)
print('begin write post order')
with open(post_list_file, 'w') as ids_file:
    pd.DataFrame(post_list, columns=["id"]).to_csv(ids_file, index=False)
del post_list

# reorder_and_write_csv_from_file(comment_csv, new_comment_csv, comment_list_file)
# reorder_and_write_csv_from_file(post_csv, new_post_csv, post_list_file)


# conn.execute("CREATE FUNCTION pandas_read_csv(varchar, varchar) -> table(...) external('python-pandas')")
reorder_and_write_csv_with_date_duck1(conn,comment_csv,new_comment_csv,comment_list_file)
reorder_and_write_csv_with_date_duck1(conn,post_csv,new_post_csv,post_list_file)
# reorder_and_write_csv(comment_hasCreator_person_csv,new_create_csv,comment_list)
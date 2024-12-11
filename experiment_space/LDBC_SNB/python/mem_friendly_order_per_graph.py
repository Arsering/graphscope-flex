import csv
from collections import defaultdict, deque
from tqdm import tqdm
import pandas as pd
import duckdb
import random
from datetime import datetime, timezone
import numpy as np



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

def read_csv():
    # 初始化每种边关系的出边和入边图
    person_knows_person_out = defaultdict(list)
    person_knows_person_in = defaultdict(list)
    
    person_likes_comment_out = defaultdict(list)
    person_likes_comment_in = defaultdict(list)
    
    person_likes_post_out = defaultdict(list)
    person_likes_post_in = defaultdict(list)
    
    person_create_comment_out = defaultdict(list)
    person_create_comment_in = defaultdict(list)
    
    comment_reply_comment_out = defaultdict(list)
    comment_reply_comment_in = defaultdict(list)
    
    comment_reply_post_out = defaultdict(list)
    comment_reply_post_in = defaultdict(list)
    
    person_create_post_out = defaultdict(list)
    person_create_post_in = defaultdict(list)

    # 处理每种边关系，并构建各自的图
    # 处理 person_knows_person
    process_edges(person_knows_person_csv, 0, 1, person_knows_person_out, person_knows_person_in,True,True)

    # 处理 person_likes_comment
    process_edges(person_likes_comment_csv, 0, 1, person_likes_comment_out, person_likes_comment_in,True,False)

    # 处理 person_likes_post
    process_edges(person_likes_post_csv, 0, 1, person_likes_post_out, person_likes_post_in,True,False)

    # 处理 comment_hasCreator_person （注意这里是反转的关系）
    process_edges(comment_hasCreator_person_csv, 1, 0, person_create_comment_out, person_create_comment_in,True,False)

    # 处理 comment_reply_comment
    process_edges(comment_reply_comment_csv, 0, 1, comment_reply_comment_out, comment_reply_comment_in,False,True)

    # 处理 comment_reply_post
    process_edges(comment_reply_post_csv, 0, 1, comment_reply_post_out, comment_reply_post_in,False,True)

    # 处理 post_hasCreator_post （注意这里是反转的关系）
    process_edges(post_hasCreator_person_csv, 1, 0, person_create_post_out, person_create_post_in,True,False)

    # 返回每种边关系的出边和入边图
    return {
        "person_knows_person": (person_knows_person_out, person_knows_person_in),
        "person_likes_comment": (person_likes_comment_out, person_likes_comment_in),
        "person_likes_post": (person_likes_post_out, person_likes_post_in),
        "comment_hasCreator_person": (person_create_comment_out, person_create_comment_in),
        "comment_reply_comment": (comment_reply_comment_out, comment_reply_comment_in),
        "comment_reply_post": (comment_reply_post_out, comment_reply_post_in),
        "post_hasCreator_post": (person_create_post_out, person_create_post_in)
    }

def compute_degrees(graph_post_creator, graph_likes_post, graph_comment_creator, graph_likes_comment, graph_replyOf_comment_in, graph_replyOf_post_in):
    """
    计算每个 post 和 comment 的总度数（与 post 和 comment 相关的所有边的总数）
    graph_post_creator: post_hasCreator_person 图
    graph_likes_post: person_likes_post 图
    graph_comment_creator: comment_hasCreator_person 图
    graph_likes_comment: person_likes_comment 图
    graph_replyOf_comment: comment_replyOf_comment 图
    graph_replyOf_post: comment_replyOf_post 图
    """
    degree_map_post = defaultdict(int)
    degree_map_comment = defaultdict(int)

    # 1. 统计 post 的度数（创建边、喜欢边、回复边）
    postlen=len(graph_post_creator)+len(graph_likes_post)+len(graph_replyOf_post_in)
    with tqdm(total=postlen, desc="Processing post degree") as pbar:
        for person, posts in graph_post_creator.items():
            pbar.update(1)
            for post in posts:
                degree_map_post[post] += 1  # 创建边增加度数
                
        
        for person, liked_posts in graph_likes_post.items():
            pbar.update(1)
            for post in liked_posts:
                degree_map_post[post] += 1  # 被喜欢边增加度数
                

        for post, reply_comment_list in graph_replyOf_post_in.items():
            degree_map_post[post] += len(reply_comment_list)  # 回复 post 边增加度数
            pbar.update(1)
    
    # 2. 统计 comment 的度数（创建边、喜欢边、回复边）
    commentlen=len(graph_comment_creator)+len(graph_likes_comment)+len(graph_replyOf_comment_in)
    with tqdm(total=commentlen, desc="Processing comment degree") as pbar:
        for person, comments in graph_comment_creator.items():
            pbar.update(1)
            for comment in comments:
                degree_map_comment[comment] += 1  # 创建边增加度数
        
        for person, liked_comments in graph_likes_comment.items():
            pbar.update(1)
            for comment in liked_comments:
                degree_map_comment[comment] += 1  # 被喜欢边增加度数

        for replied_comment, reply_comment_list in graph_replyOf_comment_in.items():
            degree_map_comment[replied_comment] += len(reply_comment_list)  # 回复 comment 边增加度数
            pbar.update(1)

    return degree_map_post, degree_map_comment

# B type
def person_ordering_bfs(person_knows_csv):
    graph_knows_out = defaultdict(list)
    graph_knows_in = defaultdict(list)
    process_edges(person_knows_csv, 0, 1, graph_knows_out, graph_knows_in, True, True)
    # 计算每个 person's 总度数 (入度+出度)
    degree_map_person = defaultdict(int)
    
    for person, out_neighbors in graph_knows_out.items():
        degree_map_person[person] += len(out_neighbors)  # 出度
    for person, in_neighbors in graph_knows_in.items():
        degree_map_person[person] += len(in_neighbors)  # 入度
    
    # 初始化 BFS 相关数据结构
    person_list = []    # 存储排序后的 person
    visited = set()
    
    # BFS 队列
    Active = []
    
    totalperson = len(graph_knows_out)
    with tqdm(total = totalperson, desc="Processing ordering") as pbar:
        while len(person_list) < len(graph_knows_out):
            # 如果 Active 为空，选择未访问的点中度数最大的点开始
            if not Active:
                unvisited_persons = [p for p in graph_knows_out.keys() if p not in visited]
                if unvisited_persons:
                    max_person = max(unvisited_persons, key=lambda x: degree_map_person[x])
                    Active.append(max_person)
                    visited.add(max_person)
                    # print(visited)
            # 按 person 总度数排序
            Active.sort(key=lambda x: degree_map_person[x], reverse=True)
            NextActive = []
            for person in Active:
                # 将当前 person 加入到 person_list
                person_list.append(person)
                pbar.update(1)
                # 继续 BFS，查找 knows 的 neighbors，考虑双向边
                for neighbor in graph_knows_out.get(person, []):
                    if neighbor not in visited:
                        NextActive.append(neighbor)
                        visited.add(neighbor)
                for neighbor in graph_knows_in.get(person, []):
                    if neighbor not in visited:
                        NextActive.append(neighbor)
                        visited.add(neighbor)
            # 处理下一轮 BFS
            Active = NextActive
        
    return person_list

def convert_to_seconds(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.timestamp()
# A type
def person_ordering_creationDate(person_vertex_csv):
    df = pd.read_csv(person_vertex_csv, delimiter='|', low_memory=False)
    creationDate_list = df['creationDate']
    id_list = df['id']
    creationDate_list = [convert_to_seconds(date_str) for date_str in creationDate_list]
    sorted_indices = np.argsort(creationDate_list)
    person_list = id_list[sorted_indices]
    return person_list

# C type
def person_ordering_random(person_vertex_csv):
    df = pd.read_csv(person_vertex_csv, delimiter='|', low_memory=False)

    id_list = df['id']
    sorted_indices = random.sample(range(0, id_list.size), id_list.size)
    person_list = id_list[sorted_indices]
    return person_list

# D type
def person_ordering_degree(person_vertex_csv, person_knows_csv):
    graph_knows_out = defaultdict(list)
    graph_knows_in = defaultdict(list)
    process_edges(person_knows_csv, 0, 1, graph_knows_out, graph_knows_in, True, True)
    # 计算每个 person's 总度数 (入度+出度)
    degree_map_person = defaultdict(int)
    for person, out_neighbors in graph_knows_out.items():
        degree_map_person[person] += len(out_neighbors)  # 出度
    for person, in_neighbors in graph_knows_in.items():
        degree_map_person[person] += len(in_neighbors)  # 入度
        
    df = pd.read_csv(person_vertex_csv, delimiter='|', low_memory=False)
    id_list = df['id']
    degree_sub = np.zeros(id_list.size)
    for item_id in range(0, id_list.size):
        degree_sub[item_id] = degree_map_person[id_list[item_id]]

    rsorted_indices = np.argsort(degree_sub)
    person_list = id_list[rsorted_indices]
    return person_list

# E type
def person_ordering_ldbc(person_vertex_csv):
    df = pd.read_csv(person_vertex_csv, delimiter='|', low_memory=False)

    id_list = df['id']
    person_list = id_list
        
    return person_list

def count_forum_message():
    comment_reply_comment_out = defaultdict(list)
    comment_reply_comment_in = defaultdict(list)
    comment_reply_post_out = defaultdict(list)
    comment_reply_post_in = defaultdict(list)
    forum_contain_post_in = defaultdict(list)
    forum_contain_post_out = defaultdict(list)
    process_edges(comment_reply_comment_csv, 0, 1, comment_reply_comment_out, comment_reply_comment_in, False, True)
    process_edges(comment_reply_post_csv, 0, 1, comment_reply_post_out, comment_reply_post_in, False, True)
    process_edges(forum_containOf_post_csv, 0, 1, forum_contain_post_out, forum_contain_post_in, True, False)
    forum_dict = defaultdict(int)
    for forum in forum_contain_post_out:
        # print(forum)
        collected_comments = set()
        post_list = forum_contain_post_out[forum]
        for post in post_list:
            # 存储结果的集合
            queue = deque()
            for reply in comment_reply_post_in.get(post,[]):
                queue.append(reply)
            visited=set()
            while queue:
                current_comment=queue.popleft()
                if current_comment not in visited:
                    visited.add(current_comment)
                    collected_comments.add(current_comment)
                    for current_reply in comment_reply_comment_in.get(current_comment,[]):
                        queue.append(current_reply)
        forum_dict[forum] = len(collected_comments) + len(post_list)
    return forum_dict

def forum_ordering_degree(forum_vertex_csv):
    forum_dict = count_forum_message()
    
    df = pd.read_csv(forum_vertex_csv, delimiter='|', low_memory=False)
    id_list = df['id']
    degree_sub = np.zeros(id_list.size)
    for item_id in range(0, id_list.size):
        degree_sub[item_id] = forum_dict[id_list[item_id]]

    rsorted_indices = np.argsort(degree_sub)
    forum_list = id_list[rsorted_indices]
    
    return forum_list
def order_item_by_person(person_list, item_hasCreator_person_csv):
    person_create_item_out = defaultdict(list)
    person_create_item_in = defaultdict(list)
    # 处理 comment_hasCreator_person （注意这里是反转的关系）
    process_edges(item_hasCreator_person_csv, 1, 0, person_create_item_out, person_create_item_in, True, False)
    totalperson = len(person_list)
    added_items = set()
    item_list = []
    with tqdm(total=totalperson, desc="Processing ordering") as pbar:
        for person in person_list:
            # 将当前 person 加入到 person_list
            # person_list.append(person)
            to_add_item = []
            pbar.update(1)
            # 处理 comment 的 create
            for item in person_create_item_out.get(person, []):
                if item not in added_items:
                    to_add_item.append(item)
                    added_items.add(item)
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

def reorder_and_write_csv_from_file(original_csv, new_csv, ordered_id_file):
    # 读取原始 CSV 文件的数据
    with open(original_csv, 'r') as f:
        reader = csv.reader(f, delimiter='|')
        header = next(reader)  # 保存表头
        data = {}
        # 使用tqdm显示读取数据的进度
        for row in tqdm(reader, desc=f"Reading {original_csv}", unit="row"):
            key = int(row[0])
            # 将整行转为字符串，使用'|'连接并保存
            data[key] = '|'.join(row)  # 直接将整行存为字符串
    # 临时文件用于存储已排序和未排序的行
    # temp_file_ordered = NEW_PREFIX+"temp_ordered.csv"
    visited=set()

    # 创建新 CSV 并写入表头
    with open(original_csv, 'r') as f:
        reader = csv.reader(f, delimiter='|')
        header = next(reader)
        pd.DataFrame(columns=header).to_csv(new_csv, index=False, sep='|', mode='w')  # 初始化排序临时文件
    
    ordered_id=[]
    with open(ordered_id_file,'r') as ids_file:
        ids_reader=csv.reader(ids_file)
        next(ids_reader)
        for id in tqdm(ids_reader,desc="Processing Ordered IDs"):
            ordered_id.append(int(id[0]))
    # for id in tqdm(ordered_id,desc="ordering order"):
    #     row_data = data[id].split('|')
    #     pd.DataFrame([row_data],columns=header).to_csv(new_csv,index=False,sep='|',mode='a',header=False)
    #     visited.add(id)
    buffer=[]
    batch_size=80000
    for id in tqdm(ordered_id, desc="Ordering order"):
        row_data = data[id].split('|')
        buffer.append(row_data)  # 累积行数据
        visited.add(id)
        
        # 每到达批量大小，写入一次文件
        if len(buffer) >= batch_size:
            pd.DataFrame(buffer, columns=header).to_csv(new_csv, index=False, sep='|', mode='a', header=False)
            buffer.clear()  # 清空缓冲区

    # 将剩余数据写入文件
    if buffer:
        pd.DataFrame(buffer, columns=header).to_csv(new_csv, index=False, sep='|', mode='a', header=False)
        buffer.clear()
    # remaining_data = [row for pid, row in tqdm(data.items(), desc="Finding remaining data", unit="id") if pid not in visited]
    # pd.DataFrame(remaining_data,columns=header).to_csv(new_csv,index=False,sep='|',mode='a',header=False)
    for id in tqdm(data,desc="ordering remaing"):
        if id not in visited:
            row_data=data[int(id)].split('|')
            buffer.append(row_data)  # 累积行数据
        # 每到达批量大小，写入一次文件
        if len(buffer) >= batch_size:
            pd.DataFrame(buffer, columns=header).to_csv(new_csv, index=False, sep='|', mode='a', header=False)
            buffer.clear()  # 清空缓冲区
    # 将剩余数据写入文件
    if buffer:
        pd.DataFrame(buffer, columns=header).to_csv(new_csv, index=False, sep='|', mode='a', header=False)
        buffer.clear()

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


def reorder_and_write_csv_duck_1(conn, original_csv, new_csv, ordered_ids_csv):
    # 创建一个临时表来存储 ordered_ids_csv 中的顺序信息，并添加行号
    conn.execute("CREATE TABLE ordered_ids AS "+
                 "SELECT id, row_number() OVER() AS row_num "+
                 "FROM read_csv_auto(?)", [ordered_ids_csv])

    # 创建一个临时表来存储 original_csv 中的所有记录
    conn.execute("CREATE TABLE original_data AS "+
                 "SELECT * "+
                 "FROM read_csv_auto(?)", [original_csv])

    # 将 ordered_ids_csv 中的顺序信息和 original_csv 中的记录结合起来
    query = """
    SELECT a.* FROM original_data a
    JOIN ordered_ids b ON a.id = b.id
    ORDER BY b.row_num
    """
    result_ordered = conn.execute(query).fetchall()

    # 找出 original_csv 中有 ID 但 ordered_ids_csv 中没有 ID 的记录，并追加到 sorted_data 表中
    query = """
    SELECT * FROM original_data
    WHERE id NOT IN (SELECT id FROM ordered_ids)
    """
    result_remaining = conn.execute(query).fetchall()

    # 获取 original_data 表的列名
    result_schema = conn.execute("PRAGMA table_info(original_data)").fetchall()
    columns = [row[1] for row in result_schema]  # 假设列名在第二列

    # 清理临时表
    conn.execute("DROP TABLE ordered_ids")
    conn.execute("DROP TABLE original_data")

    # 将查询结果转换为 Pandas DataFrame
    df_ordered = pd.DataFrame(result_ordered, columns=columns)
    df_remaining = pd.DataFrame(result_remaining, columns=columns)

    # 合并数据
    df_combined = pd.concat([df_ordered, df_remaining], ignore_index=True)

    # 写入新的 CSV 文件，使用 | 作为分隔符
    df_combined.to_csv(new_csv, index=False, sep='|')

# PREFIX='/data-1/yichengzhang/data/experiment_space/LDBC_SNB-nvme/lgraph_db/sf0.1/social_network/dynamic/'
PREFIX = '/nvme0n1/100new_db/sf100/social_network/dynamic/'
person_csv = PREFIX + 'person_0_0.csv'
comment_csv = PREFIX + 'comment_0_0.csv'
post_csv = PREFIX + 'post_0_0.csv'
forum_csv = PREFIX + 'forum_0_0.csv'
person_likes_comment_csv = PREFIX + 'person_likes_comment_0_0.csv'
person_likes_post_csv = PREFIX + 'person_likes_post_0_0.csv'
person_knows_person_csv = PREFIX + 'person_knows_person_0_0.csv'
comment_hasCreator_person_csv = PREFIX + 'comment_hasCreator_person_0_0.csv'
comment_reply_comment_csv = PREFIX + 'comment_replyOf_comment_0_0.csv'
comment_reply_post_csv = PREFIX + 'comment_replyOf_post_0_0.csv'
post_hasCreator_person_csv = PREFIX + 'post_hasCreator_person_0_0.csv'
forum_containOf_post_csv = PREFIX + 'forum_hasMember_person_0_0.csv'

NEW_PREFIX = PREFIX
# NEW_PREFIX='/data-1/yichengzhang/data/data_processing/scripts/IOE-Order/dataset/100_data/sf100/temp/'
new_person_csv = NEW_PREFIX + 'new_person_0_0.csv'
new_comment_csv = NEW_PREFIX + 'new_comment_0_0.csv'
new_post_csv = NEW_PREFIX + 'new_post_0_0.csv'
new_forum_csv = NEW_PREFIX + 'new_forum_0_0.csv'
new_create_csv = NEW_PREFIX + 'new_comment_hasCreator_person_0_0.csv'
new_knows_csv = NEW_PREFIX + 'new_person_knows_person_0_0.csv'

NUM_BUCKET = 10000

person_list_file = NEW_PREFIX + 'person_file.csv'
comment_list_file = NEW_PREFIX + 'comment_file.csv'
post_list_file = NEW_PREFIX + 'post_file.csv'
forum_list_file = NEW_PREFIX + 'forum_file.csv'


# 生成person_list
# person_list = person_ordering_creationDate(person_csv)
# person_list = person_ordering_bfs(person_knows_person_csv)
# person_list = person_ordering_random(person_csv)
# person_list = person_ordering_degree(person_csv, person_knows_person_csv)
person_list = person_ordering_ldbc(person_csv)
print('begin write person order')
with open(person_list_file, 'w') as ids_file:
    pd.DataFrame(person_list, columns=["id"]).to_csv(ids_file, index=False)

# 生成comment_list
comment_list = order_item_by_person(person_list, comment_hasCreator_person_csv)
print('begin write comment order')
with open(comment_list_file, 'w') as ids_file:
    pd.DataFrame(comment_list, columns=["id"]).to_csv(ids_file, index=False)
del comment_list

# 生成post_list
post_list = order_item_by_person(person_list, post_hasCreator_person_csv)
print('begin write post order')
with open(post_list_file, 'w') as ids_file:
    pd.DataFrame(post_list, columns=["id"]).to_csv(ids_file, index=False)
del post_list

# 生成forum_list
forum_list = forum_ordering_degree(forum_csv)
print('begin write forum order')
with open(forum_list_file, 'w') as ids_file:
    pd.DataFrame(forum_list, columns=["id"]).to_csv(ids_file, index=False)
del forum_list

# reorder_and_write_csv_from_file(comment_csv, new_comment_csv, comment_list_file)
# reorder_and_write_csv_from_file(post_csv, new_post_csv, post_list_file)
conn = duckdb.connect()
conn.execute("SET enable_progress_bar = true")
# conn.execute("CREATE FUNCTION pandas_read_csv(varchar, varchar) -> table(...) external('python-pandas')")
reorder_and_write_csv(person_csv, new_person_csv, person_list)
reorder_and_write_csv_duck(conn, comment_csv, new_comment_csv, comment_list_file)
reorder_and_write_csv_duck(conn, post_csv, new_post_csv, post_list_file)
reorder_and_write_csv_duck(conn, forum_csv, new_forum_csv, forum_list_file)

# reorder_and_write_csv(comment_hasCreator_person_csv,new_create_csv,comment_list)
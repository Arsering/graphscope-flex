import csv
from collections import defaultdict
from tqdm import tqdm
import pandas as pd
from datetime import datetime
import random

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

    # 辅助函数：处理边关系，构建对应的出边和入边图
    def process_edges(file, from_col, to_col, out_graph, in_graph):
        with open(file, newline='') as csvfile:
            total_lines = sum(1 for row in csvfile) - 1  # 减去表头
        with open(file, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            next(reader)  # 跳过表头
            for row in tqdm(reader, total=total_lines, desc=f"Processing {file}"):
                from_id = int(row[from_col])
                to_id = int(row[to_col])
                out_graph[from_id].append(to_id)  # 出边图
                in_graph[to_id].append(from_id)   # 入边图

    # 处理每种边关系，并构建各自的图
    # 处理 person_knows_person
    process_edges(person_knows_person_csv, 0, 1, person_knows_person_out, person_knows_person_in)

    # 处理 person_likes_comment
    process_edges(person_likes_comment_csv, 0, 1, person_likes_comment_out, person_likes_comment_in)

    # 处理 person_likes_post
    process_edges(person_likes_post_csv, 0, 1, person_likes_post_out, person_likes_post_in)

    # 处理 comment_hasCreator_person （注意这里是反转的关系）
    process_edges(comment_hasCreator_person_csv, 1, 0, person_create_comment_out, person_create_comment_in)

    # 处理 comment_reply_comment
    process_edges(comment_reply_comment_csv, 0, 1, comment_reply_comment_out, comment_reply_comment_in)

    # 处理 comment_reply_post
    process_edges(comment_reply_post_csv, 0, 1, comment_reply_post_out, comment_reply_post_in)

    # 处理 post_hasCreator_post （注意这里是反转的关系）
    process_edges(post_hasCreator_post, 1, 0, person_create_post_out, person_create_post_in)

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

def get_date_map(comment_csv,post_csv):
    comment_id_to_date={}
    post_id_to_date={}
    with open(comment_csv, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter='|')
        id_to_creation_date = {}
        # 遍历每一行
        for row in reader:
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
            comment_id_to_date[record_id] = parsed_date
    # 打开并读取CSV文件
    with open(post_csv, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter='|')
        post_id_to_date = {}
        # 遍历每一行
        for row in reader:
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
            post_id_to_date[record_id] = parsed_date
    return comment_id_to_date,post_id_to_date

def comment_post_group(graph_knows_out, graph_post_creator, graph_comment_creator, shuffle_person):
    person_list = []
    with open(person_csv,'r') as f:
        reader = csv.reader(f,delimiter='|')
        header = next(reader)
        for row in reader:
            person_list.append(int(row[0]))
    if shuffle_person:
        random.shuffle(person_list)

    # 初始化 BFS 相关数据结构
    post_list = []      # 存储排序后的 post
    comment_list = []   # 存储排序后的 comment
    comment_date_map, post_date_map = get_date_map(comment_csv, post_csv)
    def parse_date(date_str):
        try:
            return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f%z')
        except ValueError:
            try:
                return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')
            except ValueError:
                print(f"无法解析日期: {date_str}")
                return None
    # 引入两个 set 来追踪是否已经添加到 post_list 和 comment_list
    added_posts = set()
    added_comments = set()
    
    totalperson=len(graph_knows_out)
    with tqdm(total = totalperson, desc="Processing ordering") as pbar:
        for person in person_list:
            # 将当前 person 加入到 person_list
            # person_list.append(person)
            pbar.update(1)
            
            # 对每个 pop 出的 person，进行 post 和 comment 的处理
            to_add_post = []
            to_add_comment = []
            # 处理 post 的 create
            for post in graph_post_creator.get(person, []):
                if post not in added_posts:
                    to_add_post.append(post)
                    added_posts.add(post)
            to_add_post.sort(key=lambda x: post_date_map[x], reverse=False)  # 按总度数排序
            post_list.extend(to_add_post)  # 加入 post_list
            # 处理 comment 的 create
            for comment in graph_comment_creator.get(person, []):
                if comment not in added_comments:
                    to_add_comment.append(comment)
                    added_comments.add(comment)
            to_add_comment.sort(key=lambda x: comment_date_map[x], reverse=False)  # 按总度数排序
            comment_list.extend(to_add_comment)  # 加入 comment_list
        
    return person_list, post_list, comment_list

PREFIX='/nvme0n1/Anew_db/sf30/social_network/dynamic/'
person_csv=PREFIX+'person_0_0.csv'
comment_csv=PREFIX+'comment_0_0.csv'
post_csv=PREFIX+'post_0_0.csv'
person_likes_comment_csv=PREFIX+'person_likes_comment_0_0.csv'
person_likes_post_csv=PREFIX+'person_likes_post_0_0.csv'
person_knows_person_csv=PREFIX+'person_knows_person_0_0.csv'
comment_hasCreator_person_csv=PREFIX+'comment_hasCreator_person_0_0.csv'
comment_reply_comment_csv=PREFIX+'comment_replyOf_comment_0_0.csv'
comment_reply_post_csv=PREFIX+'comment_replyOf_post_0_0.csv'
post_hasCreator_post=PREFIX+'post_hasCreator_person_0_0.csv'

NEW_PREFIX=PREFIX
new_person_csv=NEW_PREFIX+'new_person_0_0.csv'
new_comment_csv=NEW_PREFIX+'new_comment_0_0.csv'
new_post_csv=NEW_PREFIX+'new_post_0_0.csv'
new_create_csv=NEW_PREFIX+'new_comment_hasCreator_person_0_0.csv'
new_knows_csv=NEW_PREFIX+'new_person_knows_person_0_0.csv'

# 使用示例：
edge_graphs = read_csv()
person_knows_out, person_knows_in = edge_graphs["person_knows_person"]
person_likes_comment_out, person_likes_comment_in = edge_graphs["person_likes_comment"]
person_likes_post_out, person_likes_post_in = edge_graphs["person_likes_post"]
person_create_comment_out, person_create_comment_in = edge_graphs["comment_hasCreator_person"]
comment_reply_comment_out, comment_reply_comment_in = edge_graphs["comment_reply_comment"]
comment_reply_post_out, comment_reply_post_in = edge_graphs["comment_reply_post"]
person_create_post_out, person_create_post_in = edge_graphs["post_hasCreator_post"]

# 调用排序算法
person_list, post_list, comment_list = comment_post_group(
    person_knows_out, person_create_post_out,
    person_create_comment_out,shuffle_person=True
)

# person_list=random.shuffle(person_list)

# 根据person_list, comment_list, post_list重新排序文件并输出
reorder_and_write_csv(person_csv, new_person_csv, person_list)
reorder_and_write_csv(comment_csv, new_comment_csv, comment_list)
reorder_and_write_csv(post_csv, new_post_csv, post_list)
# reorder_and_write_csv(comment_hasCreator_person_csv,new_create_csv,comment_list)
# reorder_and_write_csv(person_knows_person_csv,new_knows_csv,person_list)
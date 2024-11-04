import csv
from collections import defaultdict
from tqdm import tqdm
import pandas as pd

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


def breadth_first_degree_second_ordering(
    graph_knows_out, graph_knows_in, graph_post_creator, graph_likes_post,
    graph_comment_creator, graph_likes_comment,
    graph_replyOf_comment, graph_replyOf_post):
    
    # 计算每个 person's 总度数 (入度+出度)
    degree_map_person = defaultdict(int)
    
    for person, out_neighbors in graph_knows_out.items():
        degree_map_person[person] += len(out_neighbors)  # 出度
    for person, in_neighbors in graph_knows_in.items():
        degree_map_person[person] += len(in_neighbors)  # 入度
    
    # 初始化 BFS 相关数据结构
    person_list = []    # 存储排序后的 person
    post_list = []      # 存储排序后的 post
    comment_list = []   # 存储排序后的 comment
    visited = set()

    # 引入两个 set 来追踪是否已经添加到 post_list 和 comment_list
    added_posts = set()
    added_comments = set()

    degree_map_post, degree_map_comment = compute_degrees(
        graph_post_creator, graph_likes_post,
        graph_comment_creator, graph_likes_comment,
        graph_replyOf_comment, graph_replyOf_post
    )
    
    # BFS 队列
    Active = []
    
    totalperson=len(graph_knows_out)
    with tqdm(total=totalperson, desc="Processing ordering") as pbar:
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
                
                # 对每个 pop 出的 person，进行 post 和 comment 的处理
                to_add_post = []
                to_add_comment = []
                # 处理 post 的 create
                for post in graph_post_creator.get(person, []):
                    if post not in added_posts:
                        to_add_post.append(post)
                        added_posts.add(post)
                to_add_post.sort(key=lambda x: degree_map_post[x], reverse=True)  # 按总度数排序
                post_list.extend(to_add_post)  # 加入 post_list
                # 处理 comment 的 create
                for comment in graph_comment_creator.get(person, []):
                    if comment not in added_comments:
                        to_add_comment.append(comment)
                        added_comments.add(comment)
                to_add_comment.sort(key=lambda x: degree_map_comment[x], reverse=True)  # 按总度数排序
                comment_list.extend(to_add_comment)  # 加入 comment_list
                
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
        
    return person_list, post_list, comment_list

# def reorder_and_write_csv(original_csv, new_csv, ordered_ids):
#     # 读取原始 CSV 文件的数据
#     with open(original_csv, 'r') as f:
#         reader = csv.reader(f, delimiter='|')
#         header = next(reader)  # 保存表头
#         data = {}
#         # 使用tqdm显示读取数据的进度
#         for row in tqdm(reader, desc=f"Reading {original_csv}", unit="row"):
#             data[int(row[0])] = row  # 根据第一列ID作为键的字典

#     # 重新排序的列表
#     ordered_data = [data[pid] for pid in tqdm(ordered_ids, desc="Sorting data", unit="id") if pid in data]
#     inset=set()
#     for pid in ordered_ids:
#         inset.add(pid)
#     # 找到未在ordered_ids中的ID，追加到列表末尾
#     remaining_data = [row for pid, row in tqdm(data.items(), desc="Finding remaining data", unit="id") if pid not in inset]
    
#     # 将重新排序的内容写入新的CSV文件
#     with open(new_csv, 'w', newline='') as f:
#         writer = csv.writer(f, delimiter='|')
#         writer.writerow(header)  # 写入表头
#         # 使用tqdm显示写入数据的进度
#         for row in tqdm(ordered_data + remaining_data, desc=f"Writing to {new_csv}", unit="row"):
#             writer.writerow(row)  # 写入排序后的数据和剩余数据

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

PREFIX='/nvme0n1/Bnew_db/sf30/social_network/dynamic/'
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
person_list, post_list, comment_list = breadth_first_degree_second_ordering(
    person_knows_out, person_knows_in, person_create_post_out, person_likes_post_out,
    person_create_comment_out, person_likes_comment_out, comment_reply_comment_in, comment_reply_post_in
)

# 根据person_list, comment_list, post_list重新排序文件并输出
reorder_and_write_csv(person_csv, new_person_csv, person_list)
reorder_and_write_csv(comment_csv, new_comment_csv, comment_list)
reorder_and_write_csv(post_csv, new_post_csv, post_list)
reorder_and_write_csv(comment_hasCreator_person_csv,new_create_csv,comment_list)
# reorder_and_write_csv(person_knows_person_csv,new_knows_csv,person_list)
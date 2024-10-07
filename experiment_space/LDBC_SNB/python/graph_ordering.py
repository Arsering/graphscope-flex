import csv
from collections import defaultdict
import random
from tqdm import tqdm
import pickle

class HeadNode:
    def __init__(self,v):
        self.value=v
        self.prev=None
        self.next=None
class HeadList:
    def __init__(self,n):
        self.queue=[None]*(n+1)
        self.size=0
        self.begin=None
    def insert(self,node:HeadNode):
        if self.size==0:
            self.queue[node.value]=node
            self.size+=1
            self.begin=node
        else:
            self.queue[node.value]=node
            t_prev=None
            t_node=self.begin
            while t_node is not None and t_node.value > node.value:
                t_prev=t_node
                t_node=t_node.next
            if t_prev is None:
                self.begin.prev=node
                node.next=self.begin
                node.prev=None
                self.begin=node
            else:
                if t_prev.next:
                    t_prev.next.prev=node
                node.next=t_prev.next
                node.prev=t_prev
                t_prev.next=node
            self.size+=1
    def remove(self,value)->bool:
        # print('headlist remove',value)
        if self.begin.value == value:
            # print('change begin')
            self.begin=self.begin.next
        if self.queue[value] is None:
            # print('return false')
            return False
        else:
            if self.queue[value].prev:
                self.queue[value].prev.next=self.queue[value].next
            if self.queue[value].next:
                self.queue[value].next.prev=self.queue[value].prev
            self.size-=1
            return True
    def get_list(self):
        t=self.begin
        l=[]
        while t:
            l.append(t.value)
            t=t.next
        return l

def test_HeadList():
    hlist=HeadList(10)
    # for i in range(10):
    #     hlist.insert(HeadNode(i))
    hlist.insert(HeadNode(1))
    hlist.insert(HeadNode(0))
    print(hlist.get_list())
    # assert(hlist.remove(7))
    # hlist.print_list()
    # hlist.insert(HeadNode(7))
    # hlist.print_list()
    # for i in range(10):
    #     hlist.remove(i)
    #     hlist.print_list()
    # for i in range(10):
    #     hlist.insert(HeadNode(i))
    #     hlist.print_list()


class Node:
    def __init__(self, vi):
        self.vi = vi
        self.key = 0
        self.update = 0
        self.prev = None
        self.next = None

class PriorityQueue:
    def __init__(self, n):
        self.Q = [None] * n
        self.head = [None] * (n + 1)
        self.end = [None] * (n + 1)
        self.top = None
        self.size = 0
        self.headlist=HeadList(n)
        

    def incKey(self, vi):
        # print('inc',vi)
        # self.print_list()
        node = self.Q[vi]
        # 增加更新计数
        node.update += 1
        
        # 如果 update > 0，更新节点的键值并调整位置
        if node.update == 1:  # 刚更新到 1
            # print('update',vi)
            # 计算真实键值
            true_key = node.key + node.update
            
            # 更新链表并插入
            # print(f'inc remove node {node.vi}')
            self._remove_node(node)
            
            node.key = true_key  # 增加键值
            node.update = 0  # 重置更新计数
            self._insert_in_list(node, true_key)

            # 更新 top 指针
            if self.top is None or node.key > self.top.key:
                self.top = node
        # self.print_list()

    def init_pq(self,vertex_len):
        self.size=vertex_len
        self.headlist.insert(HeadNode(0))
        # self.headlist.print_list()
        self.Q[0]=Node(0)
        for i in range(1,vertex_len):
            self.Q[i]=Node(i)
            self.Q[i].prev=self.Q[i-1]
            self.Q[i-1].next=self.Q[i]
        self.head[0]=self.Q[0]
        self.end[0]=self.Q[vertex_len-1]
        self.top=self.Q[0]

    def decKey(self, vi):
        # print('dec',vi)
        node = self.Q[vi]
        if node is not None:
            node.update -= 1  # 递减更新计数

    def pop(self):
        # print('begin pop, and now self.top is',self.top.vi)
        # self.print_list()
        while self.top is not None:
            # 获取当前顶端节点
            vt = self.top
            # 如果更新计数为0，直接返回该节点
            if vt.update == 0:
                # print(f'pop1 remove node {vt.vi}')
                self.top = vt.next
                self._remove_node(vt)
                # print(f'vt is {vt.vi},{vertex[vt.vi]}')
                # print(f'vt {vt.vi} next is {vt.next.vi}')
                return vt.vi
            
            # 处理更新计数 < 0 的情况
            self._remove_node(vt)  # 从队列中移除节点
            vt.key += vt.update  # 更新键值为真实键值
            vt.update = 0  # 重置更新计数
            if vt.key<0:
                print(f'{vt.vi}.key is {vt.key}')
                vt.key=0
            # print(f'pop2 remove node {vt.vi}')
            self._insert_in_list(vt, vt.key)  # 重新插入链表以调整位置
        # print('end pop')

        return None  # 如果没有可返回的节点
    

    def _remove_node(self, node):
        # print(f'begin remove node {node.vi}')
        # self.print_list()
        self.size-=1
        if node.prev:
            node.prev.next = node.next
        if node.next:
            node.next.prev = node.prev
        if self.head[node.key]==self.end[node.key]:
            # print('headlist remove node',node.key)
            self.headlist.remove(node.key)
            self.head[node.key]=self.end[node.key]=None
        else:
            if self.head[node.key] == node:
                self.head[node.key] = node.next
            if self.end[node.key] == node:
                self.end[node.key] = node.prev
        if self.top == node:
            self.top = node.next
        # print(f'end remove node {node.vi}')
        # self.print_list()

    def _insert_in_list(self, node:Node, key_value):
        # 插入节点到链表
        # print(f'begin insert node {node.vi}')
        # self.print_list()
        if self.head[key_value] is None:  # 新键值
            # print("self head key value is none")
            self.head[key_value] = node
            self.end[key_value] = node
            self.headlist.insert(HeadNode(key_value))
            # self.headlist.print_list()
            prev_head=self.headlist.queue[key_value].prev
            next_head=self.headlist.queue[key_value].next
            prev_node=None
            next_node=None
            # print(f'prev head is {prev_head}, and next node is {next_head.value}')
            if prev_head is not None:
                prev_node=self.end[prev_head.value]
            if next_head is not None:
                # print(f'self head[next value] is {self.head[next_head.value]}')
                next_node=self.head[next_head.value]
            if prev_node is not None:
                assert(prev_node.next==next_node)
                prev_node.next=node
                node.prev=prev_node
            else:
                node.prev=None
            if next_node is not None:
                assert(next_node.prev==prev_node)
                next_node.prev=node
                node.next=next_node
            else:
                node.next=None
            if prev_node is None:
                self.top=node
        else:
            node.prev = self.end[key_value]
            node.next=self.end[key_value].next
            if self.end[key_value].next is not None:
                self.end[key_value].next.prev=node
            self.end[key_value].next = node
            self.end[key_value] = node
        self.size+=1
        # print(f'end insert node {node.vi}')
        # self.print_list()

    def print_list(self):
        v=self.top
        list=[]
        while v != None:
            list.append(v.vi)
            # print(f'list append {v.vi}')
            v=v.next
        hl=self.headlist.get_list()
        # print('hl:',hl)
        head_hl=[]
        end_hl=[]
        for i in hl:
            # print(i)
            # if self.end[i] is None:
                # print('end is none')
            head_hl.append(self.head[i].vi)
            end_hl.append(self.end[i].vi)
        res_hl=[]
        for i in range(len(hl)):
            res_hl.append((hl[i],head_hl[i],end_hl[i]))
        print(res_hl,list)

def go_pq(G_in,G_out,vertex,r_vertex,w):
    vertex_len=len(vertex)
    P = [None] * vertex_len
    visited = set()
    pq=PriorityQueue(vertex_len)
    # for i in range(vertex_len):#这里的i是在vertex list中的index，不是在graph中对应的id，graph中对应的id应该是vertex[i]
    #     pq.incKey(i)#这里对插入点进行初始化
    pq.init_pq(vertex_len)
    P[0] = 0 #p[]中存的是图中点的id
    pq.pop()
    visited.add(0)
    # visited,P,pq中的索引都是vertex数组的索引，不是graph_id，因此后面的代码需要转换一下
    i = 1
    with tqdm(total=vertex_len, desc="Processing vertices") as pbar:
        while i < vertex_len:
            ve = vertex[P[i - 1]]

            # 处理邻接节点
            for u in G_out.get(ve, []):  # NO(ve)
                if r_vertex[u] not in visited:
                    # print(f'inc node {r_vertex[u]}')
                    pq.incKey(r_vertex[u])

            # 处理入边节点
            for u in G_in.get(ve, []):  # NI(ve)
                if r_vertex[u] not in visited:
                    pq.incKey(r_vertex[u])
                    for v in G_out.get(u, []):  # NO(u)
                        if r_vertex[v] not in visited:
                            # print(v,r_vertex[v])
                            pq.incKey(r_vertex[v])

            if i > w + 1:
                vb = vertex[P[i - w - 1]]
                for u in G_out.get(vb, []):  # NO(vb)
                    if r_vertex[u] not in visited:
                        pq.decKey(r_vertex[u])

                for u in G_in.get(vb, []):  # NI(vb)
                    if r_vertex[u] not in visited:
                        pq.decKey(r_vertex[u])
                        for v in G_out.get(u, []):  # NO(u)
                            if r_vertex[v] not in visited:
                                pq.decKey(r_vertex[v])

            
            # pq.print_list()
            vmax = pq.pop()
            # pq.print_list()
            # print(f'vmax is {vmax}')
            if vmax is not None:
                P[i] = vmax
                visited.add(vmax)
                # print(f'visited add {vmax}')
                i += 1
                pbar.update(1)
    return P

def load_processed_data(input_file):
    with open(input_file, 'rb') as f:
        graph_in, graph_out, vertex, r_vertex = pickle.load(f)
    
    return graph_in, graph_out, vertex, r_vertex

def ordering_raw():
    # 从文件读取图的数据
    output_file='/data-1/yichengzhang/data/data_processing/scripts/locality_analyse/graph01.pkl'
    edge_file_name = '/data-1/yichengzhang/data/experiment_space/LDBC_SNB-nvme/lgraph_db/sf0.1/social_network/dynamic/person_knows_person_0_0.csv'  # 假设你的文件名为edges.txt
    # edge_file_name = '/data-1/yichengzhang/data/data_processing/scripts/locality_analyse/temp.csv'
    # edge_file_name='/Users/zedyc/workspace/graph_ordering/test.csv'
    # 构建有向图结构
    with open(edge_file_name, newline='') as csvfile:
        total_lines = sum(1 for row in csvfile) - 1  # 减去表头

    with open(edge_file_name, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter='|')
        next(reader)  # 跳过表头
        graph_in={}
        graph_out={}
        vertex=[]
        r_vertex={}
        for row in tqdm(reader, total=total_lines, desc="Processing rows"):
            source = row[0]
            target = row[1]
            if target not in graph_in:
                graph_in[target] = []
            graph_in[target].append(source)  # 添加入边
            if source not in graph_out:
                graph_out[source]=[]
            graph_out[source].append(target)
            if source not in vertex:
                vertex.append(source)
                r_vertex[source]=len(vertex)-1
            if target not in vertex:
                vertex.append(target)
                r_vertex[target]=len(vertex)-1
    with open(output_file, 'wb') as f:
        pickle.dump((graph_in, graph_out, vertex, r_vertex), f)

    print('end load graph')

    # 运行 GO-PQ 算法
    w = 128  # 设定窗口大小
    p_vertex = go_pq(graph_in,graph_out,vertex,r_vertex, w)
    p_id=[]
    for i in range(len(p_vertex)):
        p_id.append(vertex[p_vertex[i]])
    # print("Resulting path:", p_id)
    print(len(p_id),len(vertex))
    return p_id

def ordering_loaded():
    input_file = '/data-1/yichengzhang/data/data_processing/scripts/locality_analyse/graph01.pkl'
    graph_in, graph_out, vertex, r_vertex = load_processed_data(input_file)
        # 运行 GO-PQ 算法
    w = 128  # 设定窗口大小
    p_vertex = go_pq(graph_in,graph_out,vertex,r_vertex, w)
    p_id=[]
    for i in range(len(p_vertex)):
        p_id.append(vertex[p_vertex[i]])
    # print("Resulting path:", p_id)
    print(len(p_id),len(vertex))
    return p_id

def reorder(new_order, input_file, output_file):
    print('begin reorder')
    # 读取 CSV 文件并保存每一行数据
    with open(input_file, mode='r', newline='') as infile:
        reader = csv.reader(infile, delimiter='|')
        header = next(reader)  # 读取表头
        data = [row for row in reader]  # 读取数据行

    # 创建一个字典，使用person id作为键，行数据作为值
    data_dict = {row[0]: row for row in data}

    # 按照新的顺序找到对应的person id，并重新排列
    reordered_data = [data_dict[str(pid)] for pid in new_order if str(pid) in data_dict]

    # 找到input file中存在但不在new order中的person id
    # remaining_data = [row for pid, row in data_dict.items() if str(pid) not in new_order]
    remaining_data=[data_dict[str(pid)] for pid in data_dict if str(pid) not in new_order]

    # 将剩余的行附加到已排序的数据末尾
    reordered_data.extend(remaining_data)

    # 将重新排列的数据写入新文件
    with open(output_file, mode='w', newline='') as outfile:
        writer = csv.writer(outfile, delimiter='|')
        writer.writerow(header)  # 写入表头
        writer.writerows(reordered_data)  # 写入重新排序的数据

new_order=ordering_raw()
input_file = '/data-1/yichengzhang/data/experiment_space/LDBC_SNB-nvme/lgraph_db/sf0.1/social_network/dynamic/person_0_0.csv'
output_file = '/data-1/yichengzhang/data/data_processing/scripts/locality_analyse/new_person_01_0.csv'
reorder(new_order, input_file, output_file)

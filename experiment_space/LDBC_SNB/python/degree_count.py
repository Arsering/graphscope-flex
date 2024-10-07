import yaml
import matplotlib as mpl
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict
import os
from matplotlib.ticker import PercentFormatter,MaxNLocator
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import json
import pickle
import random

# def plot_degree_distribution(degrees, degree_type, edge_type):
#     degree_distribution = defaultdict(int)
#     for degree in degrees.values():
#         degree_distribution[degree] += 1

#     plt.figure(figsize=(10, 6))
#     plt.bar(degree_distribution.keys(), degree_distribution.values(), color='b' if degree_type == 'in' else 'r')
#     plt.xlabel(f'{degree_type} degrees num')
#     plt.ylabel('vertex num')
#     plt.title(f'{degree_type} distribution ({edge_type})')
#     plt.yscale('log')
#     plt.xscale('log')
#     plt.grid(True)
    
#     # Create directory if it doesn't exist
#     os.makedirs(f'/data-1/yichengzhang/data/data_processing/graph_properties_result/{edge_type}',exist_ok=True)
    
#     plt.savefig(f'/data-1/yichengzhang/data/data_processing/graph_properties_result/{edge_type}/{degree_type}_degree_distribution.png')
#     plt.close()

def plot_cdf(degrees, degree_type, edge_type):
    degree_distribution = defaultdict(int)
    for degree in degrees.values():
        degree_distribution[degree] += 1

    # Prepare data for CDF plot
    sorted_degrees = sorted(degree_distribution.items())
    x, y = zip(*sorted_degrees)
    
    # Compute CDF
    cdf = np.cumsum(y) / sum(y)
    
    # plt.figure(figsize=(10, 6))
    # plt.plot(x, cdf, marker='o', linestyle='-', color='b' if degree_type == 'in' else 'r')
    plt.plot(x, cdf)
    plt.xlabel(f'{degree_type} degrees num')
    plt.ylabel('vertex num')
    plt.title(f'{degree_type} distribution ({edge_type})')
    # plt.yscale('log')
    # plt.xscale('log')
    plt.grid(True)    
    plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
    # plt.gca().yaxis.set_major_locator(MaxNLocator(integer=True, nbins=5))
    # plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True, nbins=5))
    # Create directory if it doesn't exist
    os.makedirs(f'figs/fig_DC/{edge_type}',exist_ok=True)
    with open(f'figs/fig_DC/{edge_type}/{degree_type}_result.json','w') as json_file:
        json.dump(sorted_degrees,json_file,indent=4)
    plt.savefig(f'figs/fig_DC/{edge_type}/{degree_type}_degree_distribution_cdf.png')
    plt.close()
    

def plot_degree_distribution(degrees, degree_type, edge_type):
    degree_distribution = defaultdict(int)
    for degree in degrees.values():
        degree_distribution[degree] += 1

    # Prepare data for line plot
    sorted_degrees = sorted(degree_distribution.items())
    x, y = zip(*sorted_degrees)

    
    plt.figure(figsize=(10, 6))
    plt.plot(x, y, marker='o', linestyle='-', color='b' if degree_type == 'in' else 'r')
    plt.xlabel(f'{degree_type} degrees num')
    plt.ylabel('vertex num')
    plt.title(f'{degree_type} distribution ({edge_type})')
    plt.yscale('log')
    plt.xscale('log')
    plt.grid(True)
    
    # Create directory if it doesn't exist
    os.makedirs(f'figs/fig_DC/{edge_type}',exist_ok=True)
    plt.savefig(f'figs/fig_DC/{edge_type}/{degree_type}_degree_distribution.svg')
    plt.close()

def process_edge(input_file, source_column, dest_column, edge_type, base_location, data_set_name):
    print(f'begin process {input_file}')
    full_path = os.path.join(base_location, input_file)
    df = pd.read_csv(full_path, delimiter='|')
    
    in_degrees = defaultdict(int)
    out_degrees = defaultdict(int)
    for index, row in df.iterrows():
        source = row[source_column]
        destination = row[dest_column]
        out_degrees[source] += 1
        in_degrees[destination] += 1

    # plot_degree_distribution(in_degrees, 'in', edge_type)
    # plot_degree_distribution(out_degrees, 'out', edge_type)
    # plot_cdf(in_degrees, 'in', edge_type)
    # plot_cdf(out_degrees, 'out', edge_type)
    os.makedirs(f'figs/fig_DC/sf{data_set_name}/{edge_type}',exist_ok=True)
    with open(f'figs/fig_DC/sf{data_set_name}/{edge_type}/in_degrees.pkl','wb') as f:
        pickle.dump(in_degrees, f)
    with open(f'figs/fig_DC/sf{data_set_name}/{edge_type}/out_degrees.pkl','wb') as f:
        pickle.dump(out_degrees, f)


def set_mpl():
    mpl.rcParams.update(
        {
            'text.usetex': False,
            # 'font.sans-serif': 'Times New Roman',
            'mathtext.fontset': 'stix',
            'font.size': 20,
            'figure.figsize': (10.0, 10 * 0.618),
            # 'savefig.dpi': 10000,
            'axes.labelsize': 25,
            'axes.linewidth': 1.2,
            'xtick.labelsize': 20,
            'ytick.labelsize': 20,
            'legend.loc': 'upper right',
            'lines.linewidth': 2,
            'lines.markersize': 5

        }
    )

def process_edge_wrapper(args):
    process_edge(*args)

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.int64):
            return int(obj)
        return super(CustomEncoder, self).default(obj)
    
def compute_degree(yaml_file, data_set_name):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    
    base_location = config['loading_config']['data_source']['location']
    degrees = {}
    for vertex_mapping in config['vertex_mappings']:
        degrees[vertex_mapping['type_name']] = {}
        degrees[vertex_mapping['type_name']]['in_degrees'] = defaultdict(int)
        degrees[vertex_mapping['type_name']]['out_degrees'] = defaultdict(int)
    tasks = []
    for edge_mapping in config['edge_mappings']:
        edge_name = edge_mapping['type_triplet']['edge']
        source_vertex = edge_mapping['type_triplet']['source_vertex']
        destination_vertex = edge_mapping['type_triplet']['destination_vertex']
        edge_type=f'{source_vertex}_{edge_name}_{destination_vertex}'
            
        source_column = edge_mapping['source_vertex_mappings'][0]['column']['index']
        dest_column = edge_mapping['destination_vertex_mappings'][0]['column']['index']
        for input_file in edge_mapping['inputs']:
            tasks.append((input_file, source_column, dest_column, edge_type, base_location, data_set_name))
    
    with ProcessPoolExecutor() as executor:
        result = list(executor.map(process_edge_wrapper, tasks))
    
    for edge_mapping in config['edge_mappings']:
        edge_name = edge_mapping['type_triplet']['edge']
        source_vertex = edge_mapping['type_triplet']['source_vertex']
        destination_vertex = edge_mapping['type_triplet']['destination_vertex']
        edge_type=f'{source_vertex}_{edge_name}_{destination_vertex}'
            
        for input_file in edge_mapping['inputs']:
            with open(f'figs/fig_DC/sf{data_set_name}/{edge_type}/out_degrees.pkl','rb') as f:
                out_degrees = pickle.load(f)
                for key, value in out_degrees.items():
                    degrees[source_vertex]['out_degrees'][key] += value
                    
            with open(f'figs/fig_DC/sf{data_set_name}/{edge_type}/in_degrees.pkl','rb') as f:
                in_degrees = pickle.load(f)
                for key, value in in_degrees.items():
                    degrees[destination_vertex]['in_degrees'][key] += value

    for vertex_mapping in config['vertex_mappings']:
        print(vertex_mapping['type_name'], end='\t')
        for key, value in degrees[vertex_mapping['type_name']]['out_degrees'].items():
            print(key, end='\t')
            print(degrees[vertex_mapping['type_name']]['in_degrees'][key] + value)
            break
        
    with open(f'figs/fig_DC/degrees_sf{data_set_name}.pkl', 'wb') as f:
        pickle.dump(degrees, f)
    
    with open(f'figs/fig_DC/degrees_sf{data_set_name}.pkl', 'rb') as f:
        degrees_t = pickle.load(f)
    for vertex_mapping in config['vertex_mappings']:
        print(vertex_mapping['type_name'], end='\t')
        for key, value in degrees_t[vertex_mapping['type_name']]['out_degrees'].items():
            print(key, end='\t')
            print(degrees_t[vertex_mapping['type_name']]['in_degrees'][key] + value)
            break
    
def reorder_csv(bulk_load_file, new_db_path, data_set_name):
    with open(f'figs/fig_DC/degrees_sf{data_set_name}.pkl', 'rb') as f:
        degrees = pickle.load(f)
    with open(bulk_load_file, 'r') as file:
        config = yaml.safe_load(file)
    # vertex_name = 'PERSON'

    base_location = config['loading_config']['data_source']['location']
    for vertex_mapping in config['vertex_mappings']:
        # if vertex_mapping['type_name'] != vertex_name:
        #     continue
        print(vertex_mapping['type_name'], end='\t')
        for input_file in vertex_mapping['inputs']:
            input_path = os.path.join(base_location, input_file)
            print(input_path, end='\t')
            df = pd.read_csv(input_path, delimiter='|')
            
            output_path = os.path.join(new_db_path, input_file)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            id_column = df['id']
            
            # 计算每一行的degree            
            degree_sub = np.zeros(id_column.size)
            for item_id in range(0, id_column.size):
                degree_sub[item_id] = degrees[vertex_mapping['type_name']]['out_degrees'][id_column[item_id]]
                degree_sub[item_id] += degrees[vertex_mapping['type_name']]['in_degrees'][id_column[item_id]]

            sorted_indices = np.argsort(degree_sub)
            order_map = {name: i for i, name in enumerate(id_column[sorted_indices[::-1]])}
            df['order_key'] = df['id'].map(order_map)
            sorted_df = df.sort_values(by='order_key')
            sorted_df.drop(columns=['order_key'], inplace=True)
            sorted_df.to_csv(output_path, index=False, sep='|')
            print(output_path)
            
def reorder_csv_single(bulk_load_file, new_db_path, data_set_name):
    with open(f'figs/fig_DC/sf{data_set_name}/PERSON_KNOWS_PERSON/in_degrees.pkl', 'rb') as f:
        in_degrees = pickle.load(f)
    with open(f'figs/fig_DC/sf{data_set_name}/PERSON_KNOWS_PERSON/out_degrees.pkl', 'rb') as f:
        out_degrees = pickle.load(f)
    with open(bulk_load_file, 'r') as file:
        config = yaml.safe_load(file)
        
    base_location = config['loading_config']['data_source']['location']
    vertex_name = 'PERSON'
    

    for vertex_mapping in config['vertex_mappings']:
        if vertex_mapping['type_name'] != vertex_name:
            continue
        for input_file in vertex_mapping['inputs']:
            input_path = os.path.join(base_location, input_file)
            print(input_path, end='\t')
            df = pd.read_csv(input_path, delimiter='|')
            
            output_path = os.path.join(new_db_path, input_file)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            id_column = df['id']
            
            # 计算每一行的degree            
            degree_sub = np.zeros(id_column.size)
            for item_id in range(0, id_column.size):
                degree_sub[item_id] = out_degrees[id_column[item_id]]
                degree_sub[item_id] += in_degrees[id_column[item_id]]

            # sorted_indices = np.argsort(degree_sub)
            sorted_indices = random.sample(range(0, id_column.size), id_column.size)
            order_map = {name: i for i, name in enumerate(id_column[sorted_indices[::-1]])}
            df['order_key'] = df['id'].map(order_map)
            sorted_df = df.sort_values(by='order_key')
            sorted_df.drop(columns=['order_key'], inplace=True)
            sorted_df.to_csv(output_path, index=False, sep='|')
            print(output_path)

if __name__ == "__main__":
    set_mpl()
    data_set_name = '30'
    bulk_load_file = f'/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/configurations/bulk_load_{data_set_name}.yaml'

    new_db_path = f'/nvme0n1/Bnew_db/sf{data_set_name}/social_network'
    # compute_degree(bulk_load_file, data_set_name)
    # reorder_csv(bulk_load_file, new_db_path, data_set_name)
    reorder_csv_single(bulk_load_file, new_db_path, data_set_name)

    
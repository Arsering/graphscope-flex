import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as ticker
import matplotlib as mpl

def set_mpl():
    mpl.rcParams.update(
        {
            'text.usetex': False,
            'figure.figsize': (8, 4),
            'savefig.dpi': 300,
            'axes.labelsize': 17,
            'xtick.labelsize': 16,
            'ytick.labelsize': 16,
            'xtick.direction': 'in',
            'ytick.direction': 'in',
            'legend.fontsize': 17
        }
    )
    
# 数据
cache_sizes = [30, 50]  # 缓存大小（单位：%）
results_30 = [1.1, 0.5, 1.45]  # 30%缓存大小的吞吐量数据 (单位：千QPS)
results_50 = [1.8, 0.9, 2.4]  # 50%缓存大小的吞吐量数据 (单位：千QPS)

# 创建图形和子图
set_mpl()
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))
set_mpl()

# 设置宽度
bar_width = 0.5
x = np.arange(3)

# 绘制第一个子图（30%缓存大小）
ax1.bar(x[0], results_30[0], width=bar_width, color='white', edgecolor='black', linewidth=1.5,
        hatch='x', label='MMAP', zorder=10)
ax1.bar(x[1], results_30[1], width=bar_width, color='lightgray', 
        edgecolor='black', linewidth=1.5, hatch='///', label='TriCache', zorder=10)
ax1.bar(x[2], results_30[2], width=bar_width, color='#3366cc', edgecolor='black', linewidth=1.5,
        label='GoCache', zorder=10)

# 绘制第二个子图（50%缓存大小）
ax2.bar(x[0], results_50[0], width=bar_width, color='white', edgecolor='black', linewidth=1.5,
        hatch='x', label='MMAP', zorder=10)
ax2.bar(x[1], results_50[1], width=bar_width, color='lightgray', 
        edgecolor='black', linewidth=1.5, hatch='///', label='TriCache', zorder=10)
ax2.bar(x[2], results_50[2], width=bar_width, color='#3366cc', edgecolor='black', linewidth=1.5,
        label='GoCache', zorder=10)

# 设置第一个子图的标签和标题
ax1.set_ylabel('Throughput (QPS)')
ax1.set_ylim(0, 1.5)
ax1.set_yticks([0, 0.7, 1.4])
ax1.set_xlim(-0.5, 2.5)
ax1.set_xticks([x[1]])
ax1.set_xticklabels(['30'])
ax1.grid(axis='y', linestyle='--', alpha=0.8, zorder=0)  # 只添加y轴方向的网格线

# 设置第二个子图的标签和标题
ax2.set_ylim(0, 2.5)
ax2.set_yticks([0, 1.2, 2.4])
ax2.set_xlim(-0.5, 2.5)
ax2.set_xticks([x[1]])
ax2.set_xticklabels(['50'])
ax2.grid(axis='y', linestyle='--', alpha=0.8, zorder=0)  # 只添加y轴方向的网格线

# # 创建自定义刻度格式化器
# def format_func(value, tick_number):
#     return f'{int(value)}'

# # 应用自定义刻度格式
# ax1.yaxis.set_major_formatter(ticker.FuncFormatter(format_func))
# ax2.yaxis.set_major_formatter(ticker.FuncFormatter(format_func))

# 在y轴上添加1e3标签
ax1.text(0, 1.03, '1e3', transform=ax1.transAxes, ha='center', va='center', fontsize=16)
ax2.text(0, 1.03, '1e3', transform=ax2.transAxes, ha='center', va='center', fontsize=16)

# 添加图例
handles, labels = ax1.get_legend_handles_labels()
fig.legend(handles, labels, loc='upper center', ncol=3, frameon=False)

# 调整布局
plt.tight_layout()
plt.subplots_adjust(top=0.85)

plt.savefig('fig1.pdf', bbox_inches='tight')

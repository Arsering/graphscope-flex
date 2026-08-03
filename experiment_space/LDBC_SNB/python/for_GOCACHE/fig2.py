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

# Apply matplotlib settings for inward ticks
set_mpl()

# mean latency values for each query type
mmap = [81.9647, 53.683, 1387.17, 13.1071, 4571.39, 873.144, 4.90508, 263.977, 6522.79, 224.608, 7.87602, 1523.67, 4.77618, 2759.89]
gocache_with_dsieve_batch = [18.4764, 45.2686, 208.761, 11.9861, 1022.51, 803.88, 6.1639, 305.464, 3842.08, 184.645, 9.0881, 103.661, 3.89116, 3252.05]
gocache_with_dsieve = [87.316, 61.9759, 812.477, 10.4804, 3743.35, 570.422, 4.79755, 220.825, 7774.6, 158.025, 7.21917, 962.748, 4.63994, 2487.24]

# P99 latency values for each query type
# mmap = [2667752.032, 98991.414, 2861682.684, 26236.246, 6498129.58, 1207939.596, 32703.454, 653133.6, 9009980.332, 359958.47, 15113.068, 2517643.298, 18849.26, 21898736.512]
# gocache_with_dsieve_batch = [241963.386, 70211.718, 295820.764, 26308.96, 1394911.456, 1355467.156, 39405.516, 748359.29, 4534356.31, 300599.452, 20295.5, 185082.976, 10829.286, 25038599.296]
# gocache_with_dsieve = [3064897.212, 84928.138, 2134486.496, 19211.688, 4975104.976, 817875.902, 30228.562, 599431.358, 8902361.816, 249351.964, 11849.886, 1624993.758, 16756.324, 19300365.546]

query_types = ['IC1', 'IC2', 'IC3', 'IC4', 'IC5', 'IC6', 'IC7', 'IC8', 'IC9', 'IC10', 'IC11', 'IC12', 'IC13', 'IC14']

x = np.arange(len(query_types))
width = 0.25

fig, ax = plt.subplots(figsize=(10, 6))

# Bar patterns
bars1 = ax.bar(x - width/2, np.array(mmap)/np.array(mmap), width, label='MMAP', color='white', edgecolor='black', hatch='x')
bars3 = ax.bar(x + width/2, np.array(gocache_with_dsieve_batch)/np.array(mmap), width, label='GoCache', color='#1f77b4', edgecolor='black')

# Labels and title
ax.set_xlabel('Query Types', fontsize=14)
ax.set_ylabel('Normalized Latency', fontsize=14)
ax.set_xticks(x)
ax.set_xticklabels(query_types, fontsize=14)
ax.set_ylim(0, 1.5)
ax.set_yticks([0, 0.5, 1.0, 1.5])

# Legend
ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.1), ncol=3, fontsize=14, frameon=False)

# Grid
ax.yaxis.grid(True, linestyle='--', alpha=0.5)
# ax.set_axisbelow(True)

plt.tight_layout()
plt.savefig('fig2-2.pdf', bbox_inches='tight')

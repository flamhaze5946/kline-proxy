"""Render measured hour latency and CPU comparison; read validated hour-comparison.json."""
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager

ROOT = Path(__file__).resolve().parents[1] / 'evidence/deployment-20260913'
FONT = Path('/System/Library/Fonts/Hiragino Sans GB.ttc')
if FONT.exists():
    font_manager.fontManager.addfont(str(FONT))
    plt.rcParams['font.family'] = font_manager.FontProperties(fname=str(FONT)).get_name()
plt.rcParams.update({'axes.unicode_minus': False, 'font.size': 10, 'axes.spines.top': False,
                     'axes.spines.right': False})
data = json.loads((ROOT / 'hour-comparison.json').read_text())
colors = {'old': '#9A6172', 'new': '#167D8D'}
labels = {'old': '旧版 · 17:00 UTC', 'new': '新版 · 18:00 UTC'}


def probe_values(row):
    return {p['label']: p.get('body_received_offset_ms', p.get('complete_response_corrected_offset_ms'))
            for p in row['probes']}


fig, ax = plt.subplots(figsize=(10.5, 5.1))
names = ['全部合约收盘帧已到达', '全部合约收盘数据可查询', '回环 HTTP · 718 币完整响应',
         '公网 HTTPS · 6 币完整响应', '公网 HTTPS · 718 币完整响应']
for shift, version in ((-.18, 'old'), (.18, 'new')):
    row = data[version]
    market = row['markets']['binanceFuture']['summary']
    probes = probe_values(row)
    values = [market['receive_offset_ms_max'], market['ready_offset_ms_max'], probes['loopback-all'],
              probes['public-six'], probes['public-all']]
    positions = [i + shift for i in range(len(names))]
    ax.barh(positions, values, height=.32, color=colors[version], label=labels[version])
    for y, value in zip(positions, values):
        ax.text(value + 10, y, f'{value:.0f} ms', va='center', fontsize=9)
ax.set_yticks(range(len(names)), names)
ax.invert_yaxis()
ax.set_xlim(0, max(p['complete_response_corrected_offset_ms']
                   for v in ('old', 'new') for p in data[v]['probes'] if p['label'].startswith('public')) * 1.17)
ax.set_xlabel('从整点 T 到该阶段完成（ms）')
ax.set_title('生产实测：同组币、同类探针的相邻两个整点', loc='left', fontweight='bold')
ax.grid(axis='x', alpha=.15)
ax.legend(loc='upper right', frameon=False)
fig.text(.015, .018, '718 个合约币；HTTP 探针约 T+20ms 发起。公网时间包含网络传输及校时误差。\n只有一组上线前后观察；预热、市场负载与 JFR 设置不同，不能据此承诺每小时上限。', fontsize=9, color='#555555')
fig.tight_layout(rect=(0, .09, 1, 1))
fig.savefig(ROOT / 'hour-latency.png', dpi=180)
fig.savefig(ROOT / 'hour-latency.svg')
plt.close(fig)

fig, (ax, bars) = plt.subplots(2, 1, figsize=(11, 7.7), gridspec_kw={'height_ratios': [1.35, 1]})
for version in ('old', 'new'):
    samples = data[version]['cpu_intervals']
    ax.plot([r['offset_seconds'] for r in samples], [r['jvm_cpu_cores'] / 2 * 100 for r in samples],
            color=colors[version], marker='o', markersize=3, lw=1.8, label=labels[version])
ax.axvline(0, color='#777777', ls='--', lw=.8)
ax.set_ylim(0, 105)
ax.set_ylabel('JVM 占双核总容量（%）')
ax.set_xlabel('整点前后的秒数（点为约 1 秒采样区间的终点）')
ax.set_title('整点 CPU：相同口径的一秒差分', loc='left', fontweight='bold')
ax.grid(axis='y', alpha=.2)
ax.legend(frameon=False)

categories = [('jit', 'JIT 编译', '#826BA8'), ('kline_workers', 'K 线处理', '#258D92'),
              ('http', 'HTTP', '#DFAC55'), ('websocket_receive', 'WebSocket 接收', '#77ADCF'),
              ('gc', 'GC', '#CB705B'), ('scheduled_tasks', '定时任务', '#6D8968'),
              ('rest_and_management', 'REST / 管理', '#B9AB8D'), ('profiler', 'JFR', '#7D8490'),
              ('other', '其他', '#BBBBBB')]
for i, version in enumerate(('old', 'new')):
    left = 0
    totals = data[version]['thread_cpu']['categories']
    for key, label, color in categories:
        amount = totals.get(key, {}).get('cpu_seconds', 0)
        bars.barh(i, amount, left=left, height=.45, color=color, label=label if i == 0 else None)
        if amount >= .35:
            bars.text(left + amount / 2, i, f'{amount:.2f}', ha='center', va='center', fontsize=9)
        left += amount
    bars.text(left + .04, i, f'合计 {left:.2f}s', va='center', fontsize=9)
bars.set_yticks([0, 1], [labels['old'], labels['new']])
bars.invert_yaxis()
bars.set_xlabel('线程累计 CPU 秒（约 T-5 到 T+12 秒）')
bars.set_title('CPU 花在哪里：首尾均存在的线程累计值', loc='left', fontweight='bold')
bars.set_xlim(0, max(data[v]['thread_cpu']['tracked_thread_cpu_seconds'] for v in ('old', 'new')) * 1.25)
bars.grid(axis='x', alpha=.15)
bars.legend(loc='upper center', bbox_to_anchor=(.5, -.34), ncol=5, fontsize=9, frameon=False)
fig.text(.02, .018, '上图 100% = 两个核全忙；下图统计整个观察窗口，不仅是 T+1 秒。\n旧版为重启后约 6 分钟，新版约 55 分钟；新版整点启用 JFR，开销计入 JVM。', fontsize=9, color='#555555')
fig.tight_layout(rect=(0, .1, 1, 1))
fig.savefig(ROOT / 'hour-cpu.png', dpi=180)
fig.savefig(ROOT / 'hour-cpu.svg')
plt.close(fig)

# Matplotlib emits spaces before path-data newlines; retain the separating newlines.
for name in ('hour-latency.svg', 'hour-cpu.svg'):
    path = ROOT / name
    path.write_text('\n'.join(line.rstrip() for line in path.read_text().splitlines()) + '\n')

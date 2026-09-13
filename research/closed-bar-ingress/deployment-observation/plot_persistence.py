import json
from pathlib import Path
from datetime import datetime,timezone
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager

ROOT=Path(__file__).resolve().parents[1] / 'evidence/deployment-20260913'
FONT=Path('/System/Library/Fonts/Hiragino Sans GB.ttc')
if FONT.exists():
    font_manager.fontManager.addfont(str(FONT))
    plt.rcParams['font.family']=font_manager.FontProperties(fname=str(FONT)).get_name()
plt.rcParams.update({'axes.unicode_minus':False,'font.size':10,'axes.spines.top':False})
rows=json.loads((ROOT/'persistence-cpu-input.json').read_text())
fig,axes=plt.subplots(2,1,figsize=(11,7.1),sharey=True)
for ax,minute,title in zip(axes,(10,15),('首次定时落盘（启用 JFR）','下一轮落盘（未启用 JFR）')):
    boundary=datetime(2026,9,13,17,minute,tzinfo=timezone.utc).timestamp()
    selected=[r for r in rows if 28<=datetime.fromisoformat(r['utc']).timestamp()-boundary<=58]
    x=[datetime.fromisoformat(r['utc']).timestamp()-boundary for r in selected]
    cpu=[r['jvm_cpu_percent'] for r in selected]
    scheduled=[sum(v for k,v in r['thread_cpu_seconds'].items() if k.startswith('symbols-sync'))/r['seconds']/2*100 for r in selected]
    jit=[sum(v for k,v in r['thread_cpu_seconds'].items() if 'Compiler' in k)/r['seconds']/2*100 for r in selected]
    ax.plot(x,cpu,color='#173F5F',lw=2,label='JVM 总 CPU')
    ax.plot(x,scheduled,color='#D47B15',lw=1.6,label='定时任务线程 CPU')
    ax.plot(x,jit,color='#8B5FBF',lw=1.4,label='JIT 编译线程 CPU')
    ax.set_ylim(0,105);ax.set_xlim(28,58);ax.set_ylabel('双核总容量占比（%）')
    ax.set_title(title,loc='left',fontweight='bold');ax.grid(axis='y',alpha=.2)
    ax.set_xlabel('UTC 17:'+str(minute).zfill(2)+' 后的秒数（每点约 1 秒区间）')
    ioax=ax.twinx();ioax.spines['top'].set_visible(False)
    ioax.bar(x,[r['jvm_written_MiB']/r['seconds'] for r in selected],width=.75,alpha=.13,color='#278C7B',label='进程物理写入')
    ioax.set_ylim(0,4);ioax.set_ylabel('进程写入（MiB/s）',color='#278C7B')
    peak=max(range(len(cpu)),key=cpu.__getitem__)
    ax.annotate(f'峰值 {cpu[peak]:.1f}%',(x[peak],cpu[peak]),xytext=(x[peak]+2,min(101,cpu[peak]+14)),
                arrowprops={'arrowstyle':'-','color':'#173F5F'},color='#173F5F')
    ax.legend(loc='upper right',frameon=False,ncol=3,fontsize=9)
fig.suptitle('定时持久化形成可重复的 CPU 峰值',x=.07,ha='left',fontsize=17,fontweight='bold')
fig.text(.07,.02,'生产版本 407afae · 2026-09-13 UTC · 100% = 2 个 CPU 全忙\n'
         'CPU 来自 /proc 计数；物理写入包含进程其他文件。首次周期的缓存文件逻辑写入由 JFR 单独确认：13.3 MiB。',fontsize=9,color='#555555')
fig.tight_layout(rect=(0,.08,1,.94))
for extension in ('png','svg'):
    fig.savefig(ROOT/('persistence-cpu.'+extension),dpi=170,facecolor='white')
plt.close(fig)
svg = ROOT / 'persistence-cpu.svg'
svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines()) + '\n')
print(ROOT/'persistence-cpu.png')

#!/usr/bin/env python3
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import glob 
from math import floor, ceil, sqrt
import yaml
from tqdm import tqdm
import argparse

plt.style.use('ggplot')
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42
matplotlib.rcParams['hatch.linewidth'] = 0.2
matplotlib.rcParams['xtick.labelsize'] = 10
sns.set_palette(sns.color_palette('Set2', n_colors=14, desat=0.9))
sns.set_style("ticks")   



#########
# Globals
#########
# Ingore unused data for faster loading
IGNORED_FILES = ['reads.csv', 'writes.csv']
# Used to sort variants
BASIC_VARIANT_ORDER = ['OS', 'LACHESIS', 'HAREN', 'EDGEWISE', 'RANDOM']
# Figures 
EXPORT_FOLDER='./figures'
# Discard warmup and cooldown
WARMUP_PERCENTAGE = 0.3
COOLDOWN_PERCENTAGE = 0.2

# Auto-defined
REPORT_FOLDER=''
DATA = None
VARIANT_ORDER = None
VARIANT_PARAMETERS=set()


def percentageDiff(value, reference):
    if reference == 0 or np.isnan(reference):
        return np.nan
    return 100*(value - reference) / reference

def get95CI(data):
    return (1.96*np.std(data))/np.sqrt(len(data))

def relative_variance(x):
    mean = np.mean(x)
    std = np.std(x)
    return std/mean if mean > 0 else np.nan

def sum_dropna(a, **kwargs):
    if np.isnan(a).all():
        return np.nan
    else:
        return np.sum(a, **kwargs)


def aggregate_rep(parameter, extra_params, aggfunc=np.mean):
    return get(parameter=parameter).groupby(['parameter', 'variant', 'rep', 'node'] + extra_params, )\
                                    .aggregate({'value': np.mean})\
                                    .groupby(['parameter', 'variant', 'rep'] + extra_params)\
                                    .aggregate({'value': aggfunc})\
                                    .reset_index()

def aggregate_rep_spe(parameter, extra_params, aggfunc=np.mean):
    return get(parameter=parameter).groupby(['parameter', 'variant', 'rep', 'spe', 'node'] + extra_params)\
                                    .aggregate({'value': np.mean})\
                                    .groupby(['parameter', 'variant', 'rep', 'spe'] + extra_params)\
                                    .aggregate({'value': aggfunc})\
                                    .reset_index()


def aggregate_node_rep_spe(parameter, extra_params, aggfunc, nodefunc):
    aggregated = get(parameter=parameter).groupby(['parameter', 'variant', 'spe', 'node', 'rep'] + extra_params)\
                .aggregate({'value': aggfunc}).reset_index()
    aggregated = aggregated.groupby(['parameter', 'variant', 'spe', 'rep'] + extra_params)\
                            .aggregate({'value': nodefunc}).reset_index()
    return aggregated

def aggregate_node_rep(parameter, extra_params, aggfunc, nodefunc, new_parameter=None):
    aggregated = get(parameter=parameter).groupby(['parameter', 'variant', 'node', 'rep'] + extra_params)\
                                    .aggregate({'value': aggfunc}).reset_index()
    aggregated = aggregated.groupby(['parameter', 'variant', 'rep'] + extra_params)\
                            .aggregate({'value': nodefunc}).reset_index()
    if new_parameter:
        aggregated['parameter'] = new_parameter
    return aggregated

def save_fig(fig, name, experiment, export=False):
    
    def do_save_fig(fig, path):
        print(f'Saving {path}')
        fig.savefig(path, pad_inches=.1, bbox_inches='tight',)
        
    filename = f'{experiment}_{name}.pdf'
    do_save_fig(fig, f'{REPORT_FOLDER}/{filename}')
    if export:
        do_save_fig(fig, f'{EXPORT_FOLDER}/{filename}')
        

def pivotTable(index, parameter):
    def roundPivot(row):
        nr = row.copy().astype(str)
        for i in range(len(row)):
            nr[i] = f'{row[i]:0.3f}'
        return nr.astype(float)

    def computePercentageDiffs(row, bl_index):
        nr = row.copy().astype(str)
        for i in range(len(row)):
            if i == bl_index:
                continue
            pdiff = percentageDiff(row[i], row[bl_index])
            if np.isnan(pdiff):
                nr[i] = 'missing'
                continue
            nr[i] = f'{pdiff:+0.0f}%' if abs(pdiff) < 100 else f'{row[i]/float(row[bl_index]):+0.1f}x'
        nr[bl_index] = '-'
        return nr

    pv = pd.pivot_table(get(parameter=parameter), index=index, columns=['rate'], values=['value'])
    pv.columns = pv.columns.droplevel()
    rounded = pv.apply(roundPivot)
        
    file = f'{REPORT_FOLDER}/{parameter}.xlsx'
    with pd.ExcelWriter(file) as writer:
        rounded.to_excel(writer, sheet_name='Absolute Values')
        for i, variant in enumerate(pv.index):
            if isinstance(variant, tuple):
                variant = '-'.join(str(v) for v in variant)
            relative = pv.apply(computePercentageDiffs, args=(i, ), axis=0).to_excel(writer, sheet_name=f'Relative Diffs {variant}')
        print(f'Saved {file}')

def createTables(index=['variant'], parameters=['throughput', 'latency', 'end-latency', 'average-latency', 'sink-throughput', 'total-cpu', 'cpu', 'graphite-cpu', 'scheduler-cpu', 'scheduler-memory', 'memory']):
    for parameter in parameters:
        try:
            pivotTable(index, parameter)
        except:
            print(f'Failed to create table {parameter}')
            
    
def compute_latency_percentiles(parameter, aggfuncs, names):
    if len(aggfuncs) != len(names):
        raise ValueError('len must be equal')
    dfs = []
    for aggfunc, name in zip(aggfuncs, names):
        df = get(parameter=parameter).groupby(['experiment', 'rate', 'variant']).aggregate({'value': aggfunc}).copy().reset_index()
        if len(df) == 0:
            continue
        df['parameter'] = parameter + '-' + name
        df['rep'] = 1
        df['node'] = 'default'
        df['t'] = 1
        dfs.append(df)
    return dfs



def computeAverageLatency(latency):
    global DATA
    tdata = DATA.set_index(['variant', 'rep', 't', 'rate', 'node'])
    sinkThroughputData = tdata[tdata['parameter'] == 'sink-throughput'].copy()
    latencyData = tdata[tdata['parameter'] == latency].copy().dropna()
    totalSinkThroughput = sinkThroughputData.groupby(['variant', 'rep', 't', 'rate']).sum().value
    relativeSinkThroughput = sinkThroughputData['value'] / totalSinkThroughput
    relativeSinkThroughput = relativeSinkThroughput[~relativeSinkThroughput.index.duplicated()]
    latencyData = latencyData[~latencyData.index.duplicated()]
    latencyData['value'] *= relativeSinkThroughput
    latencyData['parameter'] = f'average-{latency}'
    tdata = tdata.append(latencyData, sort=True)
    DATA = tdata.reset_index()

    
def computeTotalCpu():
    global DATA
    tdata = DATA.set_index(['variant', 'rep', 't', 'rate'])
    cpuData = tdata[tdata['parameter'] == 'cpu'].copy()
    cpuData['value'] += tdata.loc[tdata['parameter'] == 'graphite-cpu', 'value']
    cpuData['value'] += tdata.loc[tdata['parameter'] == 'scheduler-cpu', 'value']
    cpuData['parameter'] = 'total-cpu'
    tdata = tdata.append(cpuData, sort=True)
    DATA = tdata.reset_index()
    
def linePlots(parameters, extra_group=None):
    def linePerVariant(*args, **kwargs):
        data = kwargs.pop('data')
        ax = plt.gca()
        data.dropna().groupby('t').aggregate({'value': np.mean}).rolling(30, min_periods=1).mean().reset_index().plot(x='t', y='value', ax=ax, **kwargs)
    plot_data = DATA.copy()
    plot_data = plot_data[plot_data.parameter.isin(parameters)]
    if extra_group:
        plot_data[extra_group] = plot_data[extra_group].astype(str)
        plot_data['hue'] = plot_data[['variant', extra_group]].agg('-'.join, axis=1)
    else:
        plot_data['hue'] = plot_data['variant']
    g = sns.FacetGrid(plot_data, col='rate', row='parameter', row_order=parameters, sharey='row', hue='hue', height=2)
    g.map_dataframe(linePerVariant)
    g.set_titles('{row_name} | {col_name}')
    g.add_legend()
    for i, parameter in enumerate(parameters):
        if 'latency' in parameter:
            for ax in g.axes[i, :]:
                ax.set_yscale('log')
        
    save_fig(g.fig, 'time-series', experimentId(), export=False)


def variantOrderKey(variant):
    for idx, variantPart in enumerate(BASIC_VARIANT_ORDER):
        if variantPart in variant:
            return idx
    raise ValueError(f'Unknown variant: {variant}')

    
def is_ignored_file(path):
    for ignored in IGNORED_FILES:
        if ignored in path:
            return True
    return False

    
def sortedLsByTime(path):
    try:
        mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
        dirs = list(sorted(os.listdir(path), key=mtime, reverse=True))
        return [directory for directory in dirs if os.path.isdir(os.path.join(path, directory))]
    except Exception as e:
        print(e)
        return []
        

def loadData(folder):
    global DATA
    global VARIANT_ORDER
    
    def removeWarmupCooldown(df):
        tmax = df.t.max()
        warmup = floor(tmax * WARMUP_PERCENTAGE)
        cooldown = ceil(tmax - tmax * COOLDOWN_PERCENTAGE)
        #print(f'Removing [0, {warmup}) and ({cooldown}, {tmax}]')
        df.loc[(df.t < warmup) | (df.t > cooldown), 'value'] = np.nan
        return df
    
    def subtractMin(df, key):
        df[key] -= df[key].min()
        return df

    def readCsv(file):
        if not file:
            return pd.DataFrame()
        df = pd.read_csv(f'{file}', names=('rep', 'node', 't', 'value'))
        df['rep'] = df['rep'].astype(int)
        df['value'] = df['value'].astype(float)
        df['t'] = df['t'].astype(int)
        if len(df) == 0:
            return df
        df = df.groupby(['rep']).apply(subtractMin, key='t')
#         print(file)
#         display((df.groupby(['t', 'rep'])['t'].size()))
        df = df.groupby(['rep']).apply(removeWarmupCooldown)
        return df

    VARIANT_PARAMETERS.clear()
    dataFrames = []
    with open(f'{folder}/experiment.yaml') as infoYaml:
        experimentInfo = yaml.load('\n'.join(infoYaml.readlines()), Loader=yaml.CLoader)
        variantSchema = experimentInfo['dimensions']['schema'].split('.')
        VARIANT_PARAMETERS.update(variantSchema[1:])
    for experimentDir in tqdm(os.listdir(folder)):
#         print(experimentDir)
        if not os.path.isdir(folder + '/' + experimentDir):
            continue
        experimentName, experimentVariant = experimentDir.split('_')
#         print(experimentName, experimentVariant)
        for dataFile in glob.glob(folder + '/' + experimentDir + '/' + '*.csv'):
            if is_ignored_file(dataFile):
                continue
            parameter = dataFile.split('/')[-1].split('.')[0]
#             print(f'Loading {dataFile}')
            try:
                df = readCsv(dataFile)
                if len(df) == 0:
#                     print(f'Skipping {dataFile}')
                    continue
            except Exception as e:
                print(f'Failed to read {dataFile}')
                raise e
            df['parameter'] = parameter
            df['experiment'] = experimentName
            df['variant'] = experimentVariant
            df[variantSchema] = df.variant.str.split('\.', expand=True)
            df[variantSchema[1:]] = df[variantSchema[1:]].apply(pd.to_numeric)
            dataFrames.append(df)
    DATA = pd.concat(dataFrames, sort=False)
    
    def replaceParameter(data, other, preferred):
        if preferred in data.parameter.unique():
            data.loc[data.parameter == other, 'parameter'] = f'{other}-ignored'
            data.loc[data.parameter == preferred, 'parameter'] = other


    # Convert 30-sec reporting rate to 1 sec for EDGEWISE but only for throughputs from graphite
    DATA.loc[DATA['variant'].isin(['EDGEWISE']) & DATA['parameter'].isin(['throughput', 'sink-throughput']), 'value'] /= 30 

    replaceParameter(DATA, 'throughput', 'throughput-raw')
    replaceParameter(DATA, 'sink-throughput', 'sink-throughput-raw')
    replaceParameter(DATA, 'latency', 'latency-raw')
    replaceParameter(DATA, 'end-latency', 'end-latency-raw')
        
    # Preprocess
    DATA.loc[DATA['parameter'].isin(['latency', 'end-latency', 'latency-sampled', 'end-latency-sampled']), 'value'] /= 1e3 # Convert latency to seconds
    DATA.loc[(DATA['parameter'].isin(['latency',  'end-latency', 'latency-sampled', 'end-latency-sampled']) & (DATA['value'] < 0)), 'value'] = np.nan 
    
    DATA['spe'] = DATA.node.str.split('\.', expand=True)[0].replace('taskmanager', 'Flink')

    
    latency_percentiles = compute_latency_percentiles('latency-sampled', [lambda x: np.quantile(x.dropna(), .99), lambda x: np.quantile(x.dropna(), .999)], ['p99', 'p999'])
    DATA = pd.concat([DATA] + latency_percentiles, ignore_index=True, sort=False)
    end_latency_percentiles = compute_latency_percentiles('end-latency-sampled', [lambda x: np.quantile(x.dropna(), .99), lambda x: np.quantile(x.dropna(), .999)], ['p99', 'p999'])
    DATA = pd.concat([DATA] + end_latency_percentiles, ignore_index=True, sort=False)
    
#     assert set(DATA.variant.unique()) == set(VARIANT_ORDER), f'Data has different variants: {set(DATA.variant.unique())} but requested {VARIANT_ORDER}'
    VARIANT_ORDER = sorted(list(DATA.variant.unique()), key=variantOrderKey)
    print(f'Variant order = {VARIANT_ORDER}')
    DATA.variant = DATA.variant.astype("category")
    DATA.variant = DATA.variant.cat.set_categories(VARIANT_ORDER) #CHECK IF WORKS!
    
    print()
    print(f'Warmup = {int(WARMUP_PERCENTAGE*100)}% / Cooldown = {int(COOLDOWN_PERCENTAGE*100)}%')
    print('-'*100)
    header = f'{"Experiment": <20}{"Variant": <20}'
    parameters_list = list(VARIANT_PARAMETERS)
    for p in parameters_list:
        header += str(p).ljust(20)
    header += f'{"Reps": <7}{"Duration"}'
    print(header)
    print('-'*100)
    for label, group in DATA.groupby(['experiment', 'variant'] + parameters_list):
        reps = group.rep.nunique()
        duration = group.t.max() / 60 
        row = ''
        for p in label:
            row += str(p).ljust(20)
        row += f'{reps: <7}{duration:3.1f} min'
        print(row)
    
def get(**kwargs):
    if len(kwargs) == 0:
        raise ValueError('Need at least one argument!')
    queryParts = []
    for key, value in kwargs.items():
        queryParts.append(f'({key} == "{value}")')
    queryStr = ' & '.join(queryParts)
    return DATA.query(queryStr)


def experimentId():
    values = DATA.experiment.unique()
    assert len(values) == 1
    return values[0]   




'''
    PLOT FUNCTIONS
'''

def basicPerformancePlot(rates, metric, metric_data, metric_title, metric_scale='linear', ncol=3, bbox=(1,1), bottom=0, export=False, order=None):
    
    def set_axis_info(g, idx, title, xlabel, ylabel, yscale='linear'):
        g.axes.flat[idx].set_title(title)
        g.axes.flat[idx].set_xlabel(xlabel)
        g.axes.flat[idx].set_ylabel(ylabel)
        g.axes.flat[idx].set_yscale(yscale)
    
    raw_data = []
    raw_data.append(aggregate_rep('throughput', ['rate'], sum_dropna))
    raw_data.append(aggregate_rep('latency', ['rate']))
    raw_data.append(aggregate_rep('end-latency', ['rate']))
    raw_data.append(metric_data)
    plot_data = pd.concat(raw_data)
    print(f'Plotting rates', rates)
    plot_data = plot_data[(plot_data.rate >= rates[0]) & (plot_data.rate <= rates[1])]
    g = sns.relplot(data=plot_data, x='rate', y='value', hue='variant', style='variant', col='parameter', 
                    hue_order=order, style_order=order,
                    col_order=['throughput', 'latency', 'end-latency', metric],
                    height=2, aspect=1.75, col_wrap=2, kind='line',  markers=True, 
                    facet_kws={'sharey': False, 'legend_out': False})
    
    set_axis_info(g, 0, 'Throughput (t/s)', 'Input Rate (t/s)', '')
    set_axis_info(g, 1, 'Latency (s)', 'Input Rate (t/s)', '', 'log')
    set_axis_info(g, 2, 'End-to-end Latency (s)', 'Input Rate (t/s)', '', 'log')
    set_axis_info(g, 3, metric_title, 'Input Rate (t/s)', '', metric_scale)
    sns.despine()
    h,l = g.axes[0].get_legend_handles_labels()
    g.axes[0].legend_.remove()
    g.fig.legend(h,l, ncol=ncol, bbox_to_anchor=bbox, frameon=False) 
    g.fig.subplots_adjust(bottom=bottom)
    save_fig(g.fig, f'{metric}', experimentId(), export=export)


def queueSizeBoxPlots(rates, export):
    aggregated = DATA[(DATA.parameter == 'input-queue') & (DATA.rate.isin(rates))]
    aggregated = aggregated[~(aggregated.node.str.contains('system|spout'))].copy()
    aggregated = aggregated.groupby(['rate', 'variant', 'node'])\
                                             .aggregate({'value': np.mean}).reset_index()
    g = sns.catplot(data=aggregated, x='rate', y='value', color='C0',
                    col='variant', height=2.25, aspect=0.9, kind='box')
    g.set_titles('{col_name}')
    g.set_ylabels('Input Queue Sizes')
    g.set_xlabels('Input Rate (t/s)')
    for ax in g.axes.flat:
        ax.set_yscale('log')
    g.fig.autofmt_xdate(rotation=70, ha='center')
    save_fig(g.fig, 'qs-hist', experimentId(), export)


def multiPolicyPerformancePlot(rates, export=False):
    
    def set_axis_info(g, idx, title, xlabel, ylabel, yscale='linear'):
        g.axes.flat[idx].set_title(title)
        g.axes.flat[idx].set_xlabel(xlabel)
        g.axes.flat[idx].set_ylabel(ylabel)
        g.axes.flat[idx].set_yscale(yscale)
    raw_data = []
    raw_data.append(aggregate_rep('throughput', ['rate'], sum_dropna))
    raw_data.append(aggregate_rep('latency', ['rate']))
    raw_data.append(aggregate_rep('end-latency', ['rate']))
    raw_data.append(aggregate_node_rep('input-queue', ['rate'], np.mean, relative_variance))
    raw_data.append(aggregate_node_rep('latency', ['rate'], np.mean, np.max, 'max-latency'))
    raw_data.append(aggregate_rep('average-latency', ['rate'], np.mean))
    plot_data = pd.concat(raw_data)
    plot_data['Scheduler'] = plot_data['variant'].str.replace('^(?P<one>\w+)-.+$', lambda m: m.group('one'), regex=True)
    plot_data['Policy'] = plot_data['variant'].str.replace('^\w+-(\w+-)?(?P<one>\w+)$', lambda m: m.group('one'), regex=True)
    plot_data['Policy'] = plot_data['Policy'].str.replace('OS', 'default', regex=False)

    print(f'Plotting rates', rates)
    plot_data = plot_data[(plot_data.rate >= rates[0]) & (plot_data.rate <= rates[1])]
    g = sns.relplot(data=plot_data, x='rate', y='value', col='parameter', hue='Scheduler', style='Policy', 
                    hue_order=['OS', 'LACHESIS', 'HAREN'], style_order=['default', 'QS', 'FCFS', 'HR'],
                    col_order=['throughput', 'latency', 'end-latency', 'input-queue', 'max-latency', 'average-latency'],
                     height=2, aspect=1.5, col_wrap=2, kind='line', markers=True,
                    facet_kws={'sharey': False}, dashes=True, markersize=8)
    
    set_axis_info(g, 0, 'Throughput (tuples/sec)', 'Input Rate (tuples/sec)', '')
    set_axis_info(g, 1, 'Latency (sec)', 'Input Rate (tuples/sec)', '', 'log')
    set_axis_info(g, 2, 'End-to-end Latency (sec)', 'Input Rate (tuples/sec)', '', 'log')
    set_axis_info(g, 3, 'QS Goal', 'Input Rate (tuples/sec)', '', 'linear')
    set_axis_info(g, 4, 'FCFS Goal', 'Input Rate (tuples/sec)', '', 'log')
    set_axis_info(g, 5, 'HR Goal', 'Input Rate (tuples/sec)', '', 'log')
    sns.despine()
    
    save_fig(g.fig, 'multi', experimentId(), export)


def multiSpePerformancePlot(rates, export=False, ncol=2, bbox=(.8, 0), bottom=0.125):

    raw_data = []
    raw_data.append(aggregate_rep_spe('throughput', ['rate']))
    raw_data.append(aggregate_rep_spe('latency', ['rate']))
    raw_data.append(aggregate_rep_spe('end-latency', ['rate']))
    plot_data = pd.concat(raw_data)
    print(f'Plotting rates', rates)
    plot_data = plot_data[(plot_data.rate >= rates[0]) & (plot_data.rate <= rates[1])]
    g = sns.relplot(data=plot_data, x='rate', y='value', hue='variant', col='spe',
                    col_order=['Storm', 'Flink'],
                    row='parameter',
                    row_order=['throughput', 'latency', 'end-latency'],
                    height=2, aspect=1.75, kind='line', style='variant', markers=True, 
                     facet_kws={'sharey': 'row', 'legend_out': False})

    
    
    for i, name in enumerate(['Storm (VS)', 'Flink (LR)']):
        g.axes[0, i].set_title(name)
        g.axes[1, i].set_title('')
        g.axes[2, i].set_title('')
        g.axes[0, 0].set_ylabel('Throughput (t/s)')
        g.axes[1, 0].set_ylabel('Latency (sec)')
        g.axes[1, i].set_yscale('log')
        g.axes[2, 0].set_ylabel('End-to-end Latency (s)')
        g.axes[2, i].set_yscale('log')
        g.axes[2, i].set_xlabel('Input Rate (%)')
    sns.despine()
    
    
    h,l = g.axes.flat[0].get_legend_handles_labels()
    g.axes.flat[0].legend_.remove()
    g.fig.legend(h,l, ncol=ncol, bbox_to_anchor=bbox, frameon=False) 
    g.fig.subplots_adjust(bottom=bottom)
    save_fig(g.fig, 'summary', experimentId(), export)


def multiSpe3PerformancePlot(rates, export=False, ncol=2, bbox=(.55, 0), bottom=0.125):

    raw_data = []
    
    # Hack: Liebre does not place SPE name in data, but storm and flink do, so set all other to liebre
    DATA.loc[DATA.parameter.isin(['throughput', 'latency', 'end-latency']) & (DATA.spe != 'Storm') & (DATA.spe != 'Flink'), 'spe'] = 'Liebre'
    
    raw_data.append(aggregate_rep_spe('throughput', ['rate'], aggfunc=sum_dropna))
    raw_data.append(aggregate_rep_spe('latency', ['rate']))
    raw_data.append(aggregate_rep_spe('end-latency', ['rate']))
    plot_data = pd.concat(raw_data)
    print(f'Plotting rates', rates)
    plot_data = plot_data[(plot_data.rate >= rates[0]) & (plot_data.rate <= rates[1])]
    g = sns.relplot(data=plot_data, x='rate', y='value', hue='variant', col='spe',
                    col_order=['Storm', 'Flink', 'Liebre'],
                    row='parameter',
                    row_order=['throughput', 'latency', 'end-latency'],
                    height=2, aspect=1.75, kind='line', style='variant', markers=True, 
                     facet_kws={'sharey': 'none', 'legend_out': False})

    
    
    for i, name in enumerate(['Storm (VS)', 'Flink (LR)', 'Liebre (SYN)']):
        g.axes[0, i].set_title(name)
        g.axes[1, i].set_title('')
        g.axes[2, i].set_title('')
        g.axes[0, 0].set_ylabel('Throughput (t/s)')
        g.axes[1, 0].set_ylabel('Latency (sec)')
        g.axes[1, i].set_yscale('log')
        g.axes[2, 0].set_ylabel('End-to-end Latency (s)')
        g.axes[2, i].set_yscale('log')
        g.axes[2, i].set_xlabel('Input Rate (%)')
    sns.despine()
    
    
    h,l = g.axes.flat[0].get_legend_handles_labels()
    g.axes.flat[0].legend_.remove()
    g.fig.legend(h,l, ncol=ncol, bbox_to_anchor=bbox, frameon=False) 
    g.fig.subplots_adjust(bottom=bottom)
    save_fig(g.fig, 'summary', experimentId(), export)


def parallelismPerformancePlot(rates, metric, metric_data, metric_title, metric_scale='linear', export=False):
    
    def set_axis_info(g, idx, title, xlabel, ylabel, yscale='linear'):
        g.axes.flat[idx].set_title(title)
        g.axes.flat[idx].set_xlabel(xlabel)
        g.axes.flat[idx].set_ylabel(ylabel)
        g.axes.flat[idx].set_yscale(yscale)
    
    raw_data = []
    raw_data.append(aggregate_rep('throughput', ['rate', 'parallelism'], sum_dropna))
    print('throughput')
    raw_data.append(aggregate_rep('latency', ['rate', 'parallelism']))
    print('latency')
    raw_data.append(aggregate_rep('end-latency', ['rate', 'parallelism']))
    print('end-latency')
    raw_data.append(metric_data)
    plot_data = pd.concat(raw_data)
    plot_data.to_csv(f'{REPORT_FOLDER}/parallelism_summary.csv')
    print(f'Plotting rates', rates)
    plot_data = plot_data[(plot_data.rate >= rates[0]) & (plot_data.rate <= rates[1])]
    g = sns.relplot(data=plot_data, x='rate', y='value', hue='variant', style='parallelism', col='parameter', 
                    col_order=['throughput', 'latency', 'end-latency', metric],
                    height=3, aspect=1.5, col_wrap=2, kind='line',  markers=True,
                    facet_kws={'sharey': False})
    
    set_axis_info(g, 0, 'Throughput (tuples/sec)', 'Input Rate (tuples/sec)', '')
    set_axis_info(g, 1, 'Latency (sec)', 'Input Rate (tuples/sec)', '', 'log')
    set_axis_info(g, 2, 'End-to-end Latency (sec)', 'Input Rate (tuples/sec)', '', 'log')
    set_axis_info(g, 3, metric_title, 'Input Rate (tuples/sec)', '', metric_scale)
    sns.despine()
    save_fig(g.fig, f'summary_{metric}', experimentId(), export=export)


def latency_percentile_box(parameter='latency', rates=(-np.inf, np.inf), legend=True, ncol=3, bbox=(0.8, 0), bottom=0.25, export=False):
    query_names = {'StormVoipStreamKafka': 'b) VS – Storm', 'StormLinearRoadKafka': 'a) LR – Storm',
                   'FlinkVoipStreamKafka': 'd) VS – Flink', 'FlinkLinearRoadKafka': 'c) LR – Flink'
                  }
    plot_data = get(parameter=f'{parameter}-sampled')
    plot_data = plot_data[(plot_data.rate >= rates[0]) & (plot_data.rate <= rates[1])]

    width = 6
    height = 2
    aspect = width/height
    
    g = sns.catplot(data=plot_data, x='rate', y='value', kind='boxen', hue='variant', linewidth=0.5, scale='linear',
                    height=height, aspect=aspect, legend_out=False, legend=legend)
    
    g.axes.flat[0].set_yscale('log')
    g.set_ylabels('Latency (s)')
    g.set_xlabels('Rate (t/s)')
    
    sns.despine()
    if legend:
        h,l = g.axes.flat[0].get_legend_handles_labels()
        g.axes.flat[0].legend_.remove()
        g.fig.legend(h,l, ncol=ncol, bbox_to_anchor=bbox, frameon=False) 
        g.fig.subplots_adjust(bottom=bottom)
    g.fig.suptitle(query_names[plot_data.experiment.unique()[0]], fontsize=14, y=1.05)
    save_fig(g.fig, f'{parameter}-percentiles', experimentId(), export=export)


def paperIntroPlot(rates, order=['OS', 'LACHESIS'], export=False):
    
    def set_axis_info(g, idx, title, xlabel, ylabel, yscale='linear'):
        g.axes.flat[idx].set_title(title)
        g.axes.flat[idx].set_xlabel(xlabel)
        g.axes.flat[idx].set_ylabel(ylabel)
        g.axes.flat[idx].set_yscale(yscale)
    
    raw_data = []
    raw_data.append(aggregate_rep('throughput', ['rate'], sum_dropna))
    print('throughput')
    raw_data.append(aggregate_rep('latency', ['rate']))
    print('latency')
    plot_data = pd.concat(raw_data)
    plot_data = plot_data[plot_data['variant'].isin(order)]
    print(f'Plotting rates', rates)
    plot_data = plot_data[(plot_data.rate >= rates[0]) & (plot_data.rate <= rates[1])].dropna()
    g = sns.relplot(data=plot_data, x='rate', y='value', col='parameter', height=2.5, aspect=1.5,
                 hue='variant', hue_order=order, kind='line', style='variant', style_order=order, markers=True, dashes=False,
                 ci=None, facet_kws={'sharey': False, 'legend_out': False}, markersize=7)
    set_axis_info(g, 0, 'Average Throughput (tuples/sec)', 'Input Rate (tuples/sec)', '')
    g.axes.flat[0].set_ylim(top=7000)
    set_axis_info(g, 1, 'Average Latency (sec)', 'Input Rate (tuples/sec)', '', 'log')
    h,l = g.axes.flat[0].get_legend_handles_labels()
    g.axes.flat[0].legend_.remove()
    g.fig.legend(h, ['Default OS Scheduling', 'Custom Scheduling (Lachesis)'], ncol=2, bbox_to_anchor=(.87, 0), frameon=True, fontsize=12)
    g.fig.subplots_adjust(bottom=0.2)
    sns.despine()
    save_fig(g.fig, f'intro-performance', experimentId(), export=export)
    

    
PLOT_FUNCTIONS = {
    'liebre-20q-period': lambda: basicPerformancePlot(rates=(-np.inf, np.inf), metric='max-latency', metric_scale='log',
                     metric_data=aggregate_node_rep('latency', ['rate'], np.mean, np.max, 'max-latency'),
                    metric_title='FCFS Goal', export=True, ncol=3, bbox=(.8, 0), bottom=0.125, order=['HAREN', 'LACHESIS', 'HAREN-1000']),
    'liebre-20q-blocking': lambda: basicPerformancePlot(rates=(-np.inf, np.inf), metric='max-latency', metric_scale='log',
                     metric_data=aggregate_node_rep('latency', ['rate'], np.mean, np.max, 'max-latency'),
                    metric_title='FCFS Goal', export=True, ncol=3, bbox=(.8, 0), bottom=0.125),
    'qs-comparison': lambda: basicPerformancePlot(rates=(-np.inf, np.inf), metric='input-queue', 
                     metric_data=aggregate_node_rep('input-queue', ['rate'], np.mean, relative_variance),
                    metric_title='QS Goal', export=True, ncol=3, bbox=(.8, 0), bottom=0.125),
    'qs-hist':      lambda: queueSizeBoxPlots(rates=(-np.inf, np.inf), export=True),
    'multi-policy': lambda: multiPolicyPerformancePlot(rates=(-np.inf, np.inf), export=True),
    'multi-spe': lambda: multiSpePerformancePlot(rates=(-np.inf, np.inf), variants=['OS', 'LACHESIS'], export=True, bbox=(.7, 0), bottom=0.1),
    'multi-3-spe': lambda: multiSpe3PerformancePlot(rates=(-np.inf, np.inf), export=True),
    'distributed': lambda: parallelismPerformancePlot(rates=(-np.inf, np.inf), metric='sink-throughput', 
                     metric_data=aggregate_rep('sink-throughput', ['rate', 'parallelism'], sum_dropna),
                     metric_title='Sink Throughput', export=True),
    'latency-percentiles': lambda: latency_percentile_box(parameter='latency', rates=(-np.inf, np.inf), export=True, legend=False),
    'latency-percentiles-legend': lambda: latency_percentile_box(parameter='latency', rates=(-np.inf, np.inf), export=True, legend=True),
    'end-latency-percentiles': lambda: latency_percentile_box(parameter='end-latency', rates=(-np.inf, np.inf), export=True, legend=False),
    'tables': lambda: createTables(),
    'tables-percentiles': lambda: createTables(parameters=['latency-sampled-p99', 'latency-sampled-p999', 'end-latency-sampled-p99', 'end-latency-sampled-p999']),
    'intro': lambda: paperIntroPlot(rates=(-np.inf, np.inf), export=True)
}

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Plot selected graph')
    parser.add_argument('--path', type=str, required=True, help='Path to the experiment result')
    parser.add_argument('--plots', type=str, required=True, nargs='+', help=f'Space-separated list of plots to produce ({",".join(PLOT_FUNCTIONS.keys())})')
    parser.add_argument('--export', type=str, required=False, help=f'Path to export the results, in addition to experiment folder', default='./figures')
    args = parser.parse_args()

    REPORT_FOLDER = args.path
    EXPORT_FOLDER = args.export
    os.makedirs(EXPORT_FOLDER, exist_ok=True)

    for plot in args.plots:
        if not plot in PLOT_FUNCTIONS:
            print('ERROR: Unknown plot requested!')
            exit(1)
    loadData(REPORT_FOLDER)
    for plot in args.plots:
        PLOT_FUNCTIONS[plot]()

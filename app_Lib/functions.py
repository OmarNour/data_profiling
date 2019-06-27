import os
import webbrowser
import sys
sys.path.append(os.getcwd())
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pyarrow.formatting import *
import dask.dataframe as dd
from app_Lib import manage_directories as md
import datetime as dt
import psutil
from faker import Faker
import random
import pandas_profiling
from sqlalchemy import create_engine
import teradata


def get_result_path(result_path, data_set_name):
    md.create_folder(result_path)
    result_path = result_path + "/" + data_set_name + ".html"
    return result_path

def open_result(relative_result_path):
    full_path = os.path.abspath(relative_result_path)
    webbrowser.open(full_path)

def get_engine(url, schema):
    if schema is None:
        schema = ""
    url_schema = url + schema
    engine = create_engine(url_schema)
    return engine


def source_connection(url, schema):
    engine = get_engine(url, schema)
    connection = engine.connect()
    # results_proxy = connection.execute(query)
    # print('results_proxy::', results_proxy)
    return connection


def get_all_data_from_source(source_engine, url, schema, query, host, user, pw, read_file, sheet_name, csv_delimiter):
    if source_engine == "CSV":
        results_df = pd.read_csv(read_file,delimiter=csv_delimiter)
    elif source_engine == "EXCEL":
        results_df = pd.read_excel(read_file, sheet_name)
    elif source_engine == "JSON":
        results_df = pd.read_json()
    elif source_engine != "TERADATA":
        connection = source_connection(url, schema)
        results_proxy = connection.execute(query)
        all_results = results_proxy.fetchall()

        if all_results:
            results_keys = all_results[0].keys()
            results_df = pd.DataFrame(all_results, columns=results_keys)
        else:
            results_df = pd.DataFrame()
    elif source_engine == "TERADATA":
        results_df = read_from_teradata(query, host, user, pw)

    return results_df


def get_chuncks_of_data_from_source(url, schema, query, fetch_many):
    # print('url:', url)
    connection = source_connection(url, schema)
    results_proxy = connection.execute(query)
    # print(results_proxy)
    more_result = True
    while more_result:
        partial_results = results_proxy.fetchmany(fetch_many)
        if not partial_results:
            more_result = False
        else:
            results_keys = partial_results[0].keys()
            results_df = pd.DataFrame(partial_results, columns=results_keys)
            yield results_df


def df_profile(pd_df, output_file, data_set_name):

    profile = pandas_profiling.ProfileReport(df=pd_df)
    profile.to_file(outputfile=output_file)
    regenerate_html(output_file, data_set_name)


def regenerate_html(output_file, data_set_name : str):
    # Powered by Ahmed Tarek
    file = open(output_file, 'r', encoding='utf-8')
    lines = '\n'.join(file.readlines())
    lines = lines.split('</head>')
    lines[0] += '<style>'
    lines[0] += ' '.join(open('src/bootstrap.min.css').readlines())
    lines[0] += '</style><script>'
    lines[0] += ' '.join(open('src/jquery.min.js').readlines())
    lines[0] += '</script><style>'
    lines[0] += ' '.join(open('src/bootstrap-theme.min.css').readlines())
    lines[0] += '</style><script>'
    lines[0] += ' '.join(open('src/bootstrap.min.js').readlines())
    lines[0] += '</script>'
    lines = '' + lines[0] + '</head>' + lines[1]
    file.close()
    out = open(output_file, 'w', encoding='utf-8')

    data_set_name = data_set_name.upper().split('.')

    try:
        data_set_name[0] = "<h4 style=\"color: #1b6d85; font-weight: bold\">{}</h4>".format(data_set_name[0])
        data_set_name[1] = "<h1 >{}</h1>".format(data_set_name[1])
    except:
        data_set_name[0] = "<h1 >{}</h1>".format(data_set_name[0])

    data_set_name = ' '.join(data_set_name)

    lines = lines.replace("<h1>Overview</h1>", data_set_name)

    out.writelines(lines)
    out.flush()
    out.close()


def gen_mock_data(no_of_rec):
    # ar_SA
    fake = Faker()
    mock_data = []

    for x in range(no_of_rec):
        person_id = x
        first_name = fake.name()
        # print(first_name)
        last_name = fake.name()
        phone_number = '+61-{}-{:04d}-{:04d}'.format(
            random.randint(2, 9),
            random.randint(1, 9999),
            random.randint(1, 9999)
        )
        address = fake.address()
        notes = fake.text()
        some_val_1 = person_id + 1
        some_val_2 = first_name[::-1]  # reversed via slice
        some_val_3 = last_name + first_name
        # randomly leave last two attributes blank for some records
        if random.randint(1, 5) == 1:
            some_val_4 = np.NaN
            some_val_5 = np.NaN
        else:
            some_val_4 = person_id * random.randint(1, 9)
            some_val_5 = random.randint(-9999999, 9999999)
        person_record = {
            'person_id': person_id, 'first_name': first_name, 'last_name': last_name,
            'phone_number': phone_number, 'address': address, 'notes': notes, 'created_date': dt.datetime.now(),'some_val_1': some_val_1, 'some_val_2': some_val_2,
            'some_val_3': some_val_3, 'some_val_4': some_val_4, 'some_val_5': some_val_5
        }
        mock_data.append(person_record)
    print(mock_data[0])
    print(mock_data[1])
    return mock_data


def read_excel(file_path, sheet_name, filter=None, nan_to_empty=True):
    try:
        df = pd.read_excel(file_path, sheet_name)
        df_cols = list(df.columns.values)
        df = df.applymap(lambda x: x.strip() if type(x) is str else x)

        if filter:
            df = df_filter(df, filter, False)

        if nan_to_empty:
            if isinstance(df, pd.DataFrame):
                df = replace_nan(df, '')
                df = df.applymap(lambda x: int(x) if type(x) is float else x)
            else:
                df = pd.DataFrame(columns=df_cols)

    except:
        df = pd.DataFrame()
    return df


def df_filter(df, filter=None, filter_index=True):
    df_cols = list(df.columns.values)
    if filter:
        for i in filter:
            if filter_index:
                df = df[df.index.isin(i[1])]
            else:
                df = df[df[i[0]].isin(i[1])]

    if df.empty:
        df = pd.DataFrame(columns=df_cols)

    return df


def replace_nan(df, replace_with):
    return df.replace(np.nan, replace_with, regex=True)


def get_file_name(file):
    return os.path.splitext(os.path.basename(file))[0]


def get_core_table_columns(Core_tables, Table_name ):
    Core_tables_df = Core_tables.loc[(Core_tables['Layer'] == 'CORE')
                                    & (Core_tables['Table name'] == Table_name)
                                    ].reset_index()
    return Core_tables_df


def single_quotes(string):
    return "'%s'" % string


def list_to_string(list, separator=None, between_single_quotes=0):
    if separator is None:
        prefix = ""
    else:
        prefix = separator
    to_string = prefix.join((single_quotes(str(x)) if between_single_quotes == 1 else str(x)) if x is not None else "" for x in list)

    return to_string


def string_to_dict(sting_dict, separator=' '):
    if sting_dict:
        # ex: Firstname="Sita" Lastname="Sharma" Age=22 Phone=1234567890
        return eval("dict(%s)" % ','.join(sting_dict.split(separator)))


def wait_for_processes_to_finish(processes_numbers, processes_run_status, processes_names):
    count_finished_processes = 0
    no_of_subprocess = len(processes_numbers)

    while processes_numbers:
        for p_no in range(no_of_subprocess):
            if processes_run_status[p_no].poll() is not None:
                try:
                    processes_numbers.remove(p_no)
                    count_finished_processes += 1
                    # print('-----------------------------------------------------------')
                    # print('\nProcess no.', p_no, 'finished, total finished', count_finished_processes, 'out of', no_of_subprocess)
                    print(count_finished_processes, 'out of', no_of_subprocess, 'finished.\t', processes_names[p_no])
                except:
                    pass


def xstr(s):
    if s is None:
        return ''
    return str(s)


def save_to_parquet(pq_df, dataset_root_path, partition_cols=None, string_columns=None):
    if not pq_df.empty:

        # all_object_columns = df.select_dtypes(include='object').columns
        # print(all_object_columns)

        if string_columns is None:
            # string_columns = df.columns
            string_columns = pq_df.select_dtypes(include='object').columns


        for i in string_columns:
            pq_df[i] = pq_df[i].apply(xstr)

        partial_results_table = pa.Table.from_pandas(df=pq_df, nthreads=None)

        pq.write_to_dataset(partial_results_table, root_path=dataset_root_path, partition_cols=partition_cols,
                            use_dictionary=False
                            )
        # flavor = 'spark'
        # print("{:,}".format(len(df.index)), 'records inserted into', dataset_root_path, 'in', datetime.datetime.now() - start_time)


def read_all_from_parquet(dataset, columns, use_threads, filter=None):
    try:
        df = pq.read_table(dataset,
                           columns=columns,
                           use_threads=use_threads,
                           use_pandas_metadata=True).to_pandas()

        if filter:
            df = df_filter(df, filter, False)
    except:
        df = pd.DataFrame()

    return df


def read_all_from_parquet_delayed(dataset, columns=None, filter=None):
    df = dd.read_parquet(path=dataset, columns=columns, engine='pyarrow')
    if filter:
        for i in filter:
            df = df[df[i[0]].isin(i[1])]
    return df


def server_info():
    cpu_per = psutil.cpu_percent(interval=0.5, percpu=False)
    # cpu_ghz = psutil.cpu_freq()
    # io = psutil.disk_io_counters()
    mem_per = psutil.virtual_memory()[2]

    return (cpu_per,mem_per)


def create_udaexec_file(host, user, pw):
    file_name = "udaexec"
    f = WriteFile("", file_name, "ini")

    text = """# Application Configuration
[CONFIG]
appName=DataProfiling
version=2
logConsole=False
dataSourceName=cluster

# Default Data source Configuration
[DEFAULT]
method=odbc
charset=UTF8

# Data source Configuration
[cluster]
system={}
username={}
password={}
driver=Teradata 
"""
    f.write(text.format(host, user, pw))
    f.close()


def read_from_teradata(query, host, user, pw):
    create_udaexec_file(host, user, pw)
    udaExec = teradata.UdaExec()
    with udaExec.connect("${dataSourceName}") as connect:
        df = pd.read_sql(query, connect)

    return df


class WriteFile:
    def __init__(self, file_path, file_name, ext, f_mode="w+", new_line=False):
        self.new_line = new_line
        self.f = open(os.path.join(file_path, file_name + "." + ext), f_mode, encoding="utf-8")

    def write(self, txt, new_line=None):
        self.f.write(txt)
        new_line = self.new_line if new_line is None else None
        self.f.write("\n") if new_line else None

    def close(self):
        self.f.close()


class LogError(WriteFile):
    def __init__(self, log_error_path, file_name_path, error_file_name, error):
        self.log_error_path = log_error_path
        self.log_file_name = "log"
        self.ext = "txt"
        super().__init__(self.log_error_path, self.log_file_name, self.ext, "a+", True)
        self.file_name_path = file_name_path
        self.error_file_name = error_file_name
        self.error = error

    def log_error(self):
        error_separator = "##############################################################################"
        self.write(str(dt.datetime.now()))
        self.write(self.file_name_path)
        self.write(self.error_file_name)
        self.write(self.error)
        self.write(error_separator)


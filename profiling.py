import datetime as dt
import pandas as pd
from app_Lib.functions import gen_mock_data, df_profile, get_engine, get_all_data_from_source, open_result, get_result_path

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


def load_data(url, schema, no_of_rec):

    engine = get_engine(url, schema)
    mock_data = gen_mock_data(no_of_rec)
    mock_df = pd.DataFrame.from_dict(mock_data)

    mock_df.to_sql('mock_data', engine, if_exists="replace")


class DataProfiling:
    def __init__(self, source_engine, url, schema, query, host, port, database, user, pw,
                 save_result_to, data_set_name, read_file = None, sheet_name = None, csv_delimiter = None):
        self.source_engine = source_engine.upper()
        self.url = url
        self.schema = schema
        self.query = query
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.pw = pw
        self.read_file = read_file
        self.sheet_name = sheet_name
        self.csv_delimiter = csv_delimiter
        self.save_result_to = save_result_to
        self.data_set_name = data_set_name
        self.error_message = ""
        self.start_time = dt.datetime.now()
        self.elapsed_time = self.start_time

    def data_profile(self):
        try:
            source_engine = self.source_engine
            df = get_all_data_from_source(source_engine,
                                          self.url, self.schema, self.query, self.host, self.port, self.database, self.user, self.pw,
                                          self.read_file, self.sheet_name, self.csv_delimiter)
            result_path = get_result_path(self.save_result_to, self.data_set_name)
            df_profile(df, result_path, self.data_set_name)
            self.elapsed_time = dt.datetime.now() - self.start_time
            open_result(result_path)
        except:
            self.error_message = "Error!, check the logs please."

#
# if __name__ == '__main__':
#     multiprocessing.freeze_support()
#
#     ###################### inputs ######################
#     source_engine = "teradata"  # "postgres", "csv", "excel", "json"
#     source_engine = "postgres"
#
#     data_set_name = "PG_mock_data"
#     save_result_to = "C:/data_profiling"
#
#     host, port, database, user, pw = "172.19.3.22", "22", "", "edw_prod_u", "edw_prod_u"
#     host, port, database, user, pw = "localhost", "5432", "data_profiling_db", "postgres", "postgres"
#
#     url = None #"postgresql://postgres:postgres@localhost:5432/data_profiling_db"
#
#     pg_url = "postgresql://{username}:{password}@{hostname}:{port}/{database}"
#     TD_url = "teradata://{username}:{password}@{hostname}:{port}/?driver={driverName}"
#     schema = ""
#
#     query = """ select * from stg_layer.CSO_NEW_PERSON sample 10 """
#
#     query = """
#             SELECT address, created_date, first_name, last_name, notes, person_id, phone_number
#                     ,some_val_1, some_val_2, some_val_3, some_val_4, some_val_5
#                     ,mod(person_id + cast(COALESCE(some_val_4,0) as int), 3)  level_
#                     ,case when length(first_name) <= 10 then 'A' else 'B' end name_len_category
#             FROM public.mock_data limit 50000;
#             """
#
#     read_file = "C:/smx_sheets/Production_Citizen_SMX.xlsx"
#     sheet_name = "STG tables"
#     csv_delimiter = ","
#
#     ###################### inputs ######################
#     DP = DataProfiling(source_engine, url, schema, query, host, port, database, user, pw, read_file, sheet_name, csv_delimiter, save_result_to, data_set_name)
#     DP.data_profile()

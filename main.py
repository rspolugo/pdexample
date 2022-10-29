import sys
from pathlib import Path
import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine


# path_to_file=os.path.abspath("part-00000-df19fa56-149b-429f-b7f9-e92911a646bf-c000.csv.gz")
path_to_file="C:/Users/roman/PycharmProjects/pdexample/activity/part-00000-df19fa56-149b-429f-b7f9-e92911a646bf-c000.csv.gz"
# data = pd.read_csv(path_to_file)
# print(data)


# db = create_engine('postgresql://postgres:123456@myhost:5432/postgres')
# Connection parameters, yours will be different


param_dic = {
    "host"      : "localhost",
    "database"  : "postgres",
    "user"      : "postgres",
    "password"  : "123456"
}
def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


conn = connect(param_dic)
data = pd.read_csv(path_to_file)
data.to_sql(data, conn, 'append')





# ----------------------------------------------------------------------------------------
# conn.autocommit = True
# cursor = conn.cursor()
#
# cursor.execute('drop table if exists dataframes')
#
# sql = '''CREATE TABLE dataframes(id int ,
# date DATE, smth1 char(20),smth2 float, smth3 int, smth4 int, smth5 int, smth6 float, smth7 float);'''
#
# cursor.execute(sql)
# data = pd.read_csv(path_to_file)
# data = data[["id","date","smth1","smth2","smth3","smth4","smth5","smth6","smth7"]]
# # converting data to sql
# data.to_sql('dataframes', conn, if_exists='replace')
#
# # fetching all rows
# sql1 = '''select * from dataframes;'''
# cursor.execute(sql1)
# for i in cursor.fetchall():
#     print(i)
#
# conn.commit()
# conn.close()
# -----------------------------------------------------------------------------------------------


# print(new_dataframe)

# def get_files(src_folder_path: str):
#     return [f for f in os.listdir(src_folder_path) if os.path.splitext(f)[1] == ".gz" and isfile(join(src_folder_path, f))]
# def get_dataframe(src_folder_path: str, files: List[str], headers) -> pd.DataFrame:
#     temp_dfs = []
#     for f in files:
#         temp_df = pd.read_csv(f"{src_folder_path}/{f}", names=headers, engine="pyarrow")
#         temp_dfs.append(temp_df)
#
#     return pd.concat(temp_dfs)

# Paths_a = []
# Paths_p = []
# temp_dfs1 = []
# temp_dfs2 = []
# for file in os.listdir("C:/Users/roman/PycharmProjects/pdexample/activity"):
#     if file.endswith(".gz"):
#         Paths_a.append(os.path.join("C:/Users/roman/PycharmProjects/pdexample/activity", file))
# for file in os.listdir("C:/Users/roman/PycharmProjects/pdexample/profiles"):
#     Paths_p.append(os.path.join("C:/Users/roman/PycharmProjects/pdexample/profiles", file))
#
# for i in Paths_a:
#     temp_df1 = pd.read_csv(i)
#     temp_dfs1.append(temp_df1)
#     pd.concat(temp_dfs1)
# for i in Paths_p:
#     temp_df = pd.read_csv(i)
#     temp_dfs2.append(temp_df)
#     pd.concat(temp_dfs2)
#
# print(temp_dfs1.head(10))
# print(temp_dfs2.head(10))

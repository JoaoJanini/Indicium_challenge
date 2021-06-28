
import psycopg2
import pandas as pd
import luigi
import pandas.io.sql as psql
from datetime import datetime
import os

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    print("Connection successful")
    return conn


class PSQLConn(object):
    """Stores the connection to psql."""
    def __init__(self, db, user, password, host):
        self.db = db
        self.user = user
        self.password = password
        self.host = host

    def connect(self):
        connection = psycopg2.connect(
                host=self.host,
                database=self.db,
                user=self.user,
                password=self.password)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection


class ExtractFromDataBase(luigi.Task):
    date = luigi.DateParameter(default=datetime.today())

    database="northwind" 
    user="northwind_user"
    password="thewindisblowing"
    host="127.0.0.1"
    port="5432"

    # Assign credentials here
    cred = PSQLConn(database, user, password, host)
    conn = cred.connect()

    def output(self):
        return self.create_dic_paths()

    def run(self):

        #Faz a conexão com o banco de dados
        conn = connect(self.param_dic)

        list_tables = self.tables_names_list(conn)  
        
        for table in list_tables:
            table_name = table
           
            #Buscar a table utilizando um select statement pelo nome da table e salvar o resultado como um dataframe.
            dataframe = psql.read_sql(f'SELECT * FROM {table_name}', conn)

            #Name of the csv containing the table.
            outname = f'{table_name}.csv'
            outdir = f"./data/postgres/{table_name}/{self.date}"
            if not os.path.exists(outdir):
                os.makedirs(outdir)

            fullname = os.path.join(outdir, outname)  

            #Transformar dataframe em csv e salvar na pasta apropriada.
            path = fullname
            dataframe.to_csv(path, index = False)

    def create_dic_paths(self):
        conn = connect(self.param_dic)

        tables = self.tables_names_list(conn)
        dic_paths = {}

        for table in tables:
            dic_paths[table] = luigi.LocalTarget(f"./data/postgres/{table}/{self.date}/{table}.csv")
        return dic_paths

    def tables_names_list(self, conn):

        #Query que busca o nome de todas as tabelas contidas no banco de dados
        s = ""
        s += "SELECT"
        s += " table_name"
        s += " FROM information_schema.tables"
        s += " WHERE"
        s += " ("
        s += " table_schema = 'public'"
        s += " )"
        s += " ORDER BY table_name;"

        #db_cursor para buscar o nome de todas as tabelas.
        db_cursor = conn.cursor()
        db_cursor.execute(s)
        list_tables = db_cursor.fetchall()
        
        #Faz a query da table toda utilizando os nomes salvos na table_list e cria um objeto dataframe para cada table.
        tables_names = []
        for table in list_tables:
            tables_names.append(table[0])
        return tables_names

    
class ExtractFromCSV(luigi.Task):

    date = luigi.DateParameter(default=datetime.today())

    def output(self):
        return luigi.LocalTarget(f"./data/csv/{self.date}/order_details.csv")

    def run(self):

        order_details_df = pd.read_csv('data/order_details.csv')

        #Name of the csv containing the csv.
        outname = 'orders_csv.csv'
        #Nome do diretório onde será salvo o csv.
        outdir = f"./data/csv/{self.date}"

        #Checar se o diretório já existe, se não, criá-lo.
        if not os.path.exists(outdir):
            os.makedirs(outdir)

        #Fazer o nome do diretório.
        fullname = os.path.join(outdir, outname) 

        path_csv = fullname
        order_details_df.to_csv(path_csv, index = False)

class ExtractLocal(luigi.Task):

    database="OrdersDB" 
    user="postgres"
    password="docker"
    host="127.0.0.1"
    port="5432"

    # Assign credentials here
    cred = PSQLConn(database, user, password, host)

    date = luigi.DateParameter(default=datetime.today())

    def requires(self):
        return {"data_base_tables":ExtractFromDataBase(),
        "orders_csv": ExtractFromCSV()}

    def output(self):
        return luigi.LocalTarget(f"./data/ordens_dos_clientes.csv")

    def run(self):
        conn = connect(self.param_dic)
        # read in file as list﻿
        df_orders_details = pd.read_csv(f"./data/csv/{self.date}/order_details.csv")
        df_orders_table = pd.read_csv(f"./data/postgres/orders/{self.date}/orders.csv")

        df_orders_details.to_sql(f'orders_details_{self.date}', con = conn)
        df_orders_table.to_sql(f'orders_details_{self.date}', con = conn)

        self.orders_query()


    def orders_query_to_json(self,conn):

        with conn.cursor() as curs:
            curs.execute("CREATE dummy_table;")

        with conn.cursor() as curs:
            curs.execute("SELECT * FROM dummy_table;")
            rows = curs.fetchall()

        with self.output().open("w") as out_file:
            for row in rows:
                out_file.write(str(row))
                
        #Query que busca o nome de todas as tabelas contidas no banco de dados
        s = ""
        s += "SELECT"
        s += " table_name"
        s += " FROM information_schema.tables"
        s += " WHERE"
        s += " ("
        s += " table_schema = 'public'"
        s += " )"
        s += " ORDER BY table_name;"

        #db_cursor para buscar o nome de todas as tabelas.
        db_cursor = conn.cursor()
        db_cursor.execute(s)
        list_tables = db_cursor.fetchall()





        


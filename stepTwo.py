import pandas as pd
import luigi
import pandas.io.sql as psql
from datetime import datetime
import PSQLConn as psql
from stepOne import ExtractFromCSV, ExtractFromDataBase
import sqlalchemy
import os

class ExtractLocal(luigi.Task):

    database="db_final" 
    user="postgresUser"
    password="postgrespassword"
    host="127.0.0.1"
    port="5433"

    # Assign credentials here
    cred = psql.PSQLConn(database, user, password, host, port)
    conn = cred.connect()

    engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')


    date = luigi.DateParameter(default=datetime.today())

    def requires(self):
        return { "data_base_tables": ExtractFromDataBase(),
        "orders_csv": ExtractFromCSV()}

    def output(self):
        return luigi.LocalTarget(f"./data/ordens_dos_clientes.csv")

    def run(self):

        # read in file as list﻿
        df_orders_details = pd.read_csv(f"./data/csv/{self.date}/order_details.csv")
        df_orders_table = pd.read_csv(f"./data/postgres/orders/{self.date}/orders.csv")

        df_orders_details.to_sql(f'orders_details', con = self.engine, if_exists = 'append')
        df_orders_table.to_sql(f'orders_table', con = self.engine, if_exists = 'append')


        s = self.s_query()
        dataframe_sql_query = pd.read_sql_query(s, con =self.engine )
        print(dataframe_sql_query.head(40))


        #Name of the csv containing the csv.
        outname = 'order_with_details.csv'
        #Nome do diretório onde será salvo o csv.
        outdir = f"./data/query/{self.date}"

        #Checar se o diretório já existe, se não, criá-lo.
        if not os.path.exists(outdir):
            os.makedirs(outdir)

        #Fazer o nome do diretório.
        fullname = os.path.join(outdir, outname) 

        path_csv = fullname
        dataframe_sql_query.to_csv(path_csv, index = False)


    def s_query(self):
        
        s = ""
        s += "SELECT"
        s += " orders_table.order_id, customer_id,employee_id,order_date,required_date,shipped_date,ship_via,freight,ship_name,ship_address,ship_city,ship_region,ship_postal_code,ship_country,"
        s += "product_id,unit_price,quantity,discount"
        s += " FROM orders_table"
        s += " RIGHT JOIN orders_details"
        s += " ON orders_table.order_id = orders_details.order_id"

        return s
        
    def orders_query_to_json(self):
        s = self.s_query()
        #db_cursor para buscar o nome de todas as tabelas.
        connection2 = self.engine.connect()
        resoverall = connection2.execute(s)

        rows = resoverall.fetchall()
        colunas = resoverall.keys()

        return rows, colunas
    



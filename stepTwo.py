import pandas as pd
import luigi
import pandas.io.sql as psql
from datetime import datetime
import PSQLConn as psql
from stepOne import ExtractFromCSV, ExtractFromDataBase
import sqlalchemy
import helper

class ExtractLocal(luigi.Task):
    # Task that extracts the order_details.csv and the orders.csv from the Local file 
    # to the Final postgres database.
    # The task also returns the result of the query to the final database containing the order and its details. 
    # You can use the parameter Date to define the date you want to run the task on. 
    # The default date is the current date.

    date = luigi.DateParameter(default=datetime.today())

    # Parameters to connect to the final database.
    database="db_final" 
    user="postgresUser"
    password="postgrespassword"
    host="127.0.0.1"
    port="5433"

    # Assign credentials here
    cred = psql.PSQLConn(database, user, password, host, port)
    conn = cred.connect()

    # Alternative way of connecting to the database using the package sqlalchemy. It is the only method
    # which works of the pandas package sql related functions.
    engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')


    def requires(self):
        # Defines the dependency of the task. In this case the dependency are the completion of both 
        # the ExtractFromDataBase task and ExtractFromCSV task.
        return { "data_base_tables": ExtractFromDataBase(self.date),
        "orders_csv": ExtractFromCSV(self.date)}

    def output(self):
        # The task returns the csv resulted from the query.
        return luigi.LocalTarget(f"./data/query/{self.date}/order_with_details.csv")

    def run(self):

        # Read both csv's from local file and put their content into dataframes.
        df_orders_details = pd.read_csv(f"./data/csv/{self.date}/order_details.csv")
        df_orders_table = pd.read_csv(f"./data/postgres/orders/{self.date}/orders.csv")

        # Append the content of the dataframes to their respective tables on the database.
        df_orders_details.to_sql(f'orders_details', con = self.engine, if_exists = 'append')
        df_orders_table.to_sql(f'orders_table', con = self.engine, if_exists = 'append')

        # Stores the result of the challenge query.
        s = self.join_query()
        
        # Makes the query and saves the content to a dataframe
        dataframe_sql_query = pd.read_sql_query(s, con = self.engine )

        # Name of the csv containing the csv.
        outname = 'order_with_details.csv'
        # Nome do diretório onde será salvo o csv.
        outdir = f"./data/query/{self.date}"

        path_csv = helper.createDir(outdir, outname)

        # Store the dataframe as a csv.
        dataframe_sql_query.to_csv(path_csv, index = False)


    def join_query(self):
        # Specifies the content of the join query for the challenge.
        
        s = ""
        s += "SELECT"
        s += " orders_table.order_id, customer_id,employee_id,order_date,required_date,shipped_date,ship_via,freight,ship_name,ship_address,ship_city,ship_region,ship_postal_code,ship_country,"
        s += "product_id,unit_price,quantity,discount"
        s += " FROM orders_table"
        s += " RIGHT JOIN orders_details"
        s += " ON orders_table.order_id = orders_details.order_id"

        return s




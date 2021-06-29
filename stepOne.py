
import pandas as pd
import luigi
import pandas.io.sql as psqlio
from datetime import datetime
import PSQLConn as psql
import helper

class ExtractFromDataBase(luigi.Task):
    """ Task that Extracts the tables from the database given by the challenge to the local files.
    Its output is a dic containing the path for all the tables. 
    You can use the parameter Date to define the date you want to run the task on. 
    The default date is the current date. """

    # Sets the parameter date.
    date = luigi.DateParameter(default=datetime.today())

    # All the credentials to connect to the postgres database.
    database="northwind" 
    user="northwind_user"
    password="thewindisblowing"
    host="127.0.0.1"
    port="5432"

    # Assign credentials here.
    cred = psql.PSQLConn(database, user, password, host, port)
    conn = cred.connect()

    def output(self):
        return self.create_dic_paths()

    def run(self):

        # Gets the name of all the database tables.
        list_tables = self.tables_names_list(self.conn)  
        
        for table in list_tables:
            table_name = table
           
            # Buscar a table utilizando um select statement pelo nome da table e salvar o resultado como um dataframe.
            dataframe = psqlio.read_sql(f'SELECT * FROM {table_name}', self.conn)

            # Name of the csv containing the table.
            outname = f'{table_name}.csv'
            outdir = f"./data/postgres/{table_name}/{self.date}"

            path = helper.createDir(outdir, outname)
            dataframe.to_csv(path, index = False)

    def create_dic_paths(self):
        """ Function which creates a dictionary containing all the database tables names and their respective paths. """

        tables = self.tables_names_list(self.conn)
        dic_paths = {}

        for table in tables:
            dic_paths[table] = luigi.LocalTarget(f"./data/postgres/{table}/{self.date}/{table}.csv")
        return dic_paths

    def tables_names_list(self, conn):
        """ Function which returns a list of all the tables in the database. """

        s = ""
        s += "SELECT"
        s += " table_name"
        s += " FROM information_schema.tables"
        s += " WHERE"
        s += " ("
        s += " table_schema = 'public'"
        s += " )"
        s += " ORDER BY table_name;"

        # db_cursor to lookup the tables' names 
        db_cursor = conn.cursor()
        db_cursor.execute(s)
        list_tables = db_cursor.fetchall()
        
        tables_names = []
        for table in list_tables:
            tables_names.append(table[0])
        return tables_names

    
class ExtractFromCSV(luigi.Task):

    """ Task that Extracts the csv file containing the orders given by the challenge to the local files.
    Its output is a dic containing the path for all the tables. 
    You can use the parameter Date to define the date you want to run the task on. 
    The default date is the current date. """

    date = luigi.DateParameter(default=datetime.today())

    def output(self):
        return luigi.LocalTarget(f"./data/csv/{self.date}/order_details.csv")

    def run(self):

        order_details_df = pd.read_csv('data/order_details.csv')

        # Name of the csv containing the orders.
        outname = 'order_details.csv'
        # Name of the csv where the directory will be saved.
        outdir = f"./data/csv/{self.date}"

        path = helper.createDir(outdir, outname)
        order_details_df.to_csv(path, index = False)



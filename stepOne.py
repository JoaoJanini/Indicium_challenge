
import pandas as pd
import luigi
import pandas.io.sql as psqlio
from datetime import datetime
import os
import PSQLConn as psql


class ExtractFromDataBase(luigi.Task):
    date = luigi.DateParameter(default=datetime.today())

    database="northwind" 
    user="northwind_user"
    password="thewindisblowing"
    host="127.0.0.1"
    port="5432"

    # Assign credentials here
    cred = psql.PSQLConn(database, user, password, host, port)
    conn = cred.connect()

    def output(self):
        return self.create_dic_paths()

    def run(self):

        #Faz a conexão com o banco de dados
        list_tables = self.tables_names_list(self.conn)  
        
        for table in list_tables:
            table_name = table
           
            #Buscar a table utilizando um select statement pelo nome da table e salvar o resultado como um dataframe.
            dataframe = psqlio.read_sql(f'SELECT * FROM {table_name}', self.conn)

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
      
        tables = self.tables_names_list(self.conn)
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
        outname = 'order_details.csv'
        #Nome do diretório onde será salvo o csv.
        outdir = f"./data/csv/{self.date}"

        #Checar se o diretório já existe, se não, criá-lo.
        if not os.path.exists(outdir):
            os.makedirs(outdir)

        #Fazer o nome do diretório.
        fullname = os.path.join(outdir, outname) 

        path_csv = fullname
        order_details_df.to_csv(path_csv, index = False)




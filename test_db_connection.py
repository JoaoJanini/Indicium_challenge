import PSQLConn as psql

database="db_final" 
user="postgresUser"
password="postgrespassword"
host="127.0.0.1"
port="5433"

# Assign credentials here
cred = psql.PSQLConn(database, user, password, host, port)
conn = cred.connect()
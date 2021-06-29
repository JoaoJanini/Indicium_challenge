# Data Pipeline.

To build the pipeline, I used a python framework called Luigi. Luigi let's you create idempotent task, define their outputs and their dependencies. It was developed by Spotify and it is used by many companies. 

For the database, in order to keep the code consistent, I used another postgres.

## Inicializa os bancos de dados.

Junto ao arquivo docker compose fornecido pelo desafio, incluí o comando para criar o banco de dados que usarei no último step.

```bash
docker-compose up
```

## Instala os pacotes do Python necessários para rodar o programa:

```bash
pip3 install requirements.txt
```

## Executar a pipeline toda do zero

```bash
python3 main.py
```

## Execute step 1 /order_details.csv -> LocalFile :

Extracts only the csv to the local file. Specify Date to save information for a specifc date.

```python
PYTHONPATH='.' luigi --local-scheduler stepOne ExtractFromCSV
```

## Execute step 1 /Northwind.db tables -> LocalFile :

Extracts the Northwind's db table to the local file. Specify Date to save information for a specifc date.

```python
PYTHONPATH='.' luigi --local-scheduler stepOne ExtractFromDataBase
```

## Execute step 2:

Executa o segundo step da pipeline, extraindo dos dados locais, guardando no banco de dados novo criado e salvando a nova query. Caso o step one não tenha sido feito ainda, ele também irá rodar o step one. Substitua o parâmetro data pela data atual. The default parameter i

```python
PYTHONPATH='.' luigi --local-scheduler stepTwo ExtractLocals -p todays_date.
```

## Comments:
If the output of a task is found on the file system, the file will not execute again, as the scheduler will consider it as already successeded
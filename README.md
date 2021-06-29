# Data Pipeline.

To build the pipeline, I used a python framework called Luigi. Luigi let's you create idempotent task, define their outputs and their dependencies. It was developed by Spotify and it is used by many companies. 

For the database, in order to keep the code consistent, I used another postgres.

# Inicializa os bancos de dados.

Junto ao arquivo docker compose fornecido pelo desafio, incluí o comando para criar o banco de dados que usarei no último step.

```bash
docker-compose up
```

# Instala os pacotes do Python necessários para rodar o programa:

```bash
pip3 install requirements.txt
```

# Executar a pipeline toda do zero

```bash
python3 main.py
```

# Execute step 1:

Executa o segundo step da pipeline, extraindo dos dados locais, guardando no banco de dados novo criado e salvando a nova query. Caso o step one não tenha sido feito ainda, ele também irá rodar o step one.

```python
PYTHONPATH='.' luigi --module stepTwo ExtractLocal
```

# Execute step 2:

Executa o segundo step da pipeline, extraindo dos dados locais, guardando no banco de dados novo criado e salvando a nova query. Caso o step one não tenha sido feito ainda, ele também irá rodar o step one.

```python
PYTHONPATH='.' luigi --module stepOne ExtractLocal
```
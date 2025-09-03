import pandas as pd
import mysql.connector


colunas = ["city","city_ibge_code","estimated_population","estimated_population_2019","state","last_available_confirmed", "last_available_deaths"]


df = pd.read_csv("caso_full.csv", usecols=colunas) #abre o arquivo e faz a leitura


df = df.dropna() #elimina valores nulos
df = df.drop_duplicates() #elimina valores duplicados



print("inserindo dados")

conexao = mysql.connector.connect(  #cria a conexão com banco de dados
    host="localhost",
    database = "covid",
    port=3306,
    user="root",
    password="root")


cursor = conexao.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS covid (
        city VARCHAR(50),
        city_ibge_code INT,       
        estimated_population INT, 
        estimated_population_2019 INT, 
        last_available_confirmed INT,
        last_available_deaths INT,
        state VARCHAR(2)
    )
""")

sql = "INSERT INTO covid (city, city_ibge_code, estimated_population, estimated_population_2019, last_available_confirmed, last_available_deaths, state) VALUES (%s, %s, %s, %s, %s, %s, %s)"

valores = [tuple(x) for x in df.to_numpy()]


def inserir_em_lotes(cursor, sql, dados, tamanho_lote=1000):
    for i in range(0, len(dados), tamanho_lote):
        chunk = dados[i:i+tamanho_lote]
        cursor.executemany(sql, chunk)


inserir_em_lotes(cursor, sql, valores, tamanho_lote=1000)


#cursor.executemany(sql, valores)

conexao.commit()
cursor.close()
conexao.close()



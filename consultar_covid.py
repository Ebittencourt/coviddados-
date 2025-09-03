import pandas as pd
import mysql.connector

conexao = mysql.connector.connect(  #cria a conexão com banco de dados
    host="localhost",
    database = "covid",
    port=3306,
    user="root",
    password="root")


casos_de_morte = pd.read_sql( #mortes confirmados por cidade
    sql = """
    SELECT 
        city AS cidade,
        MAX(last_available_deaths) AS casos_confirmados
    FROM covid
    GROUP BY city;
""", 
    con = conexao
)

print(casos_de_morte)
casos_de_morte.to_csv('casos_de_morte.csv', index=False)


populacao = pd.read_sql( #populcao confirmados por cidade
    sql = """
    SELECT 
        city AS cidade,
        MAX(estimated_population_2019) AS populacao_estimada_antes,
        MAX(estimated_population) AS populacao_estimada_depois
    FROM covid
    GROUP BY city;
""", 
    con = conexao
)

print(populacao)
populacao.to_csv('populacao.csv', index=False)


maior_cidade = pd.read_sql( #maior numero de casos confirmados por cidade
    sql = """
    SELECT 
        city AS cidade,
        MAX(last_available_confirmed) AS casos_confirmados
    FROM covid
    GROUP BY city
    ORDER BY casos_confirmados DESC
LIMIT 1;
""", 
    con = conexao
)

print(maior_cidade)
maior_cidade.to_csv('maior_cidade.csv', index=False)

menor_cidade = pd.read_sql( #menor numero de casos confirmados por cidade
    sql = """
    SELECT 
        city AS cidade,
        MIN(last_available_confirmed) AS casos_confirmados
    FROM covid
    GROUP BY city
    ORDER BY casos_confirmados DESC
LIMIT 1;
""", 
    con = conexao
)

print(menor_cidade)
menor_cidade.to_csv('menor_cidade.csv', index=False)


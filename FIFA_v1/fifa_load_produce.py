import mysql.connector
import pandas as pd
import pyodbc
from sqlalchemy import create_engine


def load_data_produce():

    def ler_backup(nome_tabela, conexao_mysql):
        cursor = conexao_mysql.cursor()

        consulta = f"SELECT * FROM {nome_tabela}"

        cursor.execute(consulta)

        resultados = cursor.fetchall()

        colunas = [i[0] for i in cursor.description]

        df = pd.DataFrame(resultados, columns=colunas)

        cursor.close()

        return df

    # Configurações de conexão ao MySQL
    conexao_mysql = mysql.connector.connect(
        host="seu host",
        user="seu usuário",
        password="sua senha",
        database="nome do seu abnco de dados"
    )

    # Ler as tabelas desejadas do MySQL
    fifa_pais = ler_backup('FIFA_PAIS', conexao_mysql)
    fifa_liga = ler_backup('FIFA_LIGA', conexao_mysql)
    fifa_clube = ler_backup('FIFA_CLUBE', conexao_mysql)
    fifa_jogador = ler_backup('FIFA_JOGADOR', conexao_mysql)

    # Fechar a conexão do MySQL
    conexao_mysql.close()

    # Dados de conexão ao banco de dados SQL Server
    server = 'seu servidor'
    database = 'nome do seu banco de dados'
    username = 'seu usuário'
    password = 'sua senha'
    driver = '{ODBC Driver 17 for SQL Server}'

    # Configuração da string de conexão para o SQL Server
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(conn_str)

    # Função para inserir DataFrame em uma nova tabela no SQL Server
    def inserir_tabela(df, nome_tabela, conexao_sql):
        cursor = conexao_sql.cursor()

        delete_query = f"DELETE FROM {nome_tabela}"
        cursor.execute(delete_query)
        conexao_sql.commit()
        print(f"Previous data in table '{nome_tabela}' deleted successfully")

        # Substitua 'schema' pelo esquema desejado (opcional)
        engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server')
        df.to_sql(nome_tabela, con=engine, if_exists='replace', index=False)
        print(f"Previous data in table '{nome_tabela}' insert successfully")

        cursor.close()

    # Insere os DataFrames nas respectivas tabelas do SQL Server
    inserir_tabela(fifa_pais, 'FIFA_PAIS', conn)
    inserir_tabela(fifa_liga, 'FIFA_LIGA', conn)
    inserir_tabela(fifa_clube, 'FIFA_CLUBE', conn)
    inserir_tabela(fifa_jogador, 'FIFA_JOGADOR', conn)

    # Fechar conexão
    conn.close()


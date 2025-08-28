import os
import time
import pandas as pd
import pymysql
from dotenv import load_dotenv

# ==============================
# CONFIGURAÇÕES
# ==============================
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Diretório monitorado
WATCH_DIR = r"C:\Users\SeuUsuario\Documents\pasta_csv"  # altere para o seu caminho
PROCESSED_DIR = os.path.join(WATCH_DIR, "processados")

# Criar pasta de processados se não existir
os.makedirs(PROCESSED_DIR, exist_ok=True)

# ==============================
# CONEXÃO MYSQL
# ==============================
def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

# ==============================
# FUNÇÃO INSERIR SAÍDAS
# ==============================
def insert_saida(df, fkPlataforma, conn):
    saidas = df[['numeroPedido', 'dtVenda', 'precoVenda', 'totalDesconto']].drop_duplicates()

    with conn.cursor() as cursor:
        for _, row in saidas.iterrows():
            sql = """
                INSERT INTO Saida (idEmpresa, idPlataforma, idTipoSaida, numeroPedido, dtVenda, precoVenda, totalDesconto, idStatusVenda)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                1,  # fkEmpresa fixo
                fkPlataforma,
                1,  # fkTipoSaida fixo
                row['numeroPedido'],
                row['dtVenda'],
                row['precoVenda'],
                row['totalDesconto'],
                1   # fkStatusVenda fixo
            ))
    conn.commit()

# ==============================
# FUNÇÃO INSERIR ITENS SAÍDA
# ==============================
def insert_itens_saida(df, fkPlataforma, conn):
    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            # Buscar ids
            cursor.execute("SELECT idProduto FROM Produto WHERE nomeProduto = %s", (row['nomeProduto'],))
            produto = cursor.fetchone()
            if not produto:
                raise Exception(f"Produto não encontrado: {row['nomeProduto']}")
            idProduto = produto['idProduto']

            cursor.execute("SELECT idCaracteristica, idTipoCaracteristica FROM Caracteristica WHERE nomeCaracteristica = %s", (row['caracteristicaProduto'],))
            caract = cursor.fetchone()
            if not caract:
                raise Exception(f"Característica não encontrada: {row['caracteristicaProduto']}")
            idCaracteristica = caract['idCaracteristica']
            idTipoCaracteristica = caract['idTipoCaracteristica']

            cursor.execute("""
                SELECT idProdutoCaracteristica 
                FROM ProdutoCaracteristica
                WHERE idProduto = %s AND idCaracteristica = %s AND idTipoCaracteristica = %s
            """, (idProduto, idCaracteristica, idTipoCaracteristica))
            prod_caract = cursor.fetchone()
            if not prod_caract:
                raise Exception(f"ProdutoCaracteristica não encontrada para produto {idProduto} e caract {idCaracteristica}")
            idProdutoCaracteristica = prod_caract['idProdutoCaracteristica']

            # Buscar idSaida pelo numeroPedido
            cursor.execute("SELECT idSaida FROM Saida WHERE numeroPedido = %s", (row['numeroPedido'],))
            saida = cursor.fetchone()
            if not saida:
                raise Exception(f"Saída não encontrada para numeroPedido {row['numeroPedido']}")
            idSaida = saida['idSaida']

            # Inserir item
            sql = """
                INSERT INTO ItensSaida (idSaida, idPlataforma, quantidade, idProdutoCaracteristica, idCaracteristica, idTipoCaracteristica, idProduto)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                idSaida,
                fkPlataforma,
                row['quantidade'],
                idProdutoCaracteristica,
                idCaracteristica,
                idTipoCaracteristica,
                idProduto
            ))
    conn.commit()

# ==============================
# PROCESSAR UM ARQUIVO
# ==============================
def process_file(filepath):
    print(f"Processando arquivo: {filepath}")
    filename = os.path.basename(filepath)

    # Extrair fkPlataforma do nome do arquivo
    fkPlataforma = int(filename.split("_")[2])

    df = pd.read_csv(filepath)

    conn = get_connection()

    # Inserir Saídas
    insert_saida(df, fkPlataforma, conn)

    # Inserir ItensSaida
    insert_itens_saida(df, fkPlataforma, conn)

    conn.close()

    # Mover para pasta processados
    os.rename(filepath, os.path.join(PROCESSED_DIR, filename))
    print(f"Arquivo {filename} processado com sucesso e movido para {PROCESSED_DIR}")

# ==============================
# MONITORAR DIRETÓRIO
# ==============================
def main():
    print(f"Monitorando pasta: {WATCH_DIR}")
    while True:
        for file in os.listdir(WATCH_DIR):
            if file.endswith(".csv"):
                filepath = os.path.join(WATCH_DIR, file)
                process_file(filepath)
        time.sleep(10)  # verifica a cada 10s

if __name__ == "__main__":
    main()

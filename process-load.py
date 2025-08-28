import os
import json
import boto3
import pymysql
import pandas as pd
from io import StringIO
from dotenv import load_dotenv

# ==============================
# CARREGAR VARIÁVEIS DO .env
# ==============================
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

s3 = boto3.client('s3')

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
# INSERIR SAÍDAS
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
# INSERIR ITENS SAÍDA
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
# HANDLER PRINCIPAL
# ==============================
def lambda_handler(event, context):
    try:
        # Pegar info do evento S3
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        print(f"Processando arquivo: s3://{bucket}/{key}")

        # Extrair fkPlataforma do nome do arquivo
        # Exemplo: "Order.all.20250101_20250131_1_20250819_173843_processado.csv"
        fkPlataforma = int(key.split("_")[2])  # assumindo que sempre está nessa posição

        # Baixar arquivo
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(body))

        conn = get_connection()

        # Inserir Saídas
        insert_saida(df, fkPlataforma, conn)

        # Inserir ItensSaida
        insert_itens_saida(df, fkPlataforma, conn)

        conn.close()

        return {
            'statusCode': 200,
            'body': json.dumps(f"Arquivo {key} processado com sucesso.")
        }

    except Exception as e:
        print(f"Erro: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }

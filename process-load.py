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
                INSERT INTO Saida (id_empresa, id_plataforma, id_tipo_saida, numero_pedido, dt_venda, preco_venda, total_desconto, id_status_venda)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                1,  # id_empresa fixo
                fkPlataforma,
                1,  # id_tipo_saida fixo
                row['numeroPedido'],
                row['dtVenda'],
                row['precoVenda'],
                row['totalDesconto'],
                1   # id_status_venda fixo
            ))
    conn.commit()

# ==============================
# INSERIR ITENS SAÍDA
# ==============================
def insert_itens_saida(df, fkPlataforma, conn):
    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            # Buscar ids
            cursor.execute("SELECT id_produto FROM Produto WHERE nome_produto = %s", (row['nomeProduto'],))
            produto = cursor.fetchone()
            if not produto:
                raise Exception(f"Produto não encontrado: {row['nomeProduto']}")
            idProduto = produto['id_produto']

            cursor.execute("SELECT id_caracteristica, id_tipo_caracteristica FROM Caracteristica WHERE nome_caracteristica = %s", (row['caracteristicaProduto'],))
            caract = cursor.fetchone()
            if not caract:
                raise Exception(f"Característica não encontrada: {row['caracteristicaProduto']}")
            idCaracteristica = caract['id_caracteristica']
            idTipoCaracteristica = caract['id_tipo_caracteristica']

            cursor.execute("""
                SELECT id_produto_caracteristica 
                FROM ProdutoCaracteristica
                WHERE id_produto = %s AND id_caracteristica = %s AND id_tipo_caracteristica = %s
            """, (idProduto, idCaracteristica, idTipoCaracteristica))
            prod_caract = cursor.fetchone()
            if not prod_caract:
                print(f"⚠️  ProdutoCaracteristica não encontrada para produto {idProduto} e caract {idCaracteristica}")
                print(f"    Criando relacionamento automaticamente...")
                # Criar o relacionamento automaticamente
                cursor.execute("""
                    INSERT INTO ProdutoCaracteristica (id_produto, id_caracteristica, id_tipo_caracteristica)
                    VALUES (%s, %s, %s)
                """, (idProduto, idCaracteristica, idTipoCaracteristica))
                idProdutoCaracteristica = cursor.lastrowid
                print(f"    ✅ ProdutoCaracteristica criada com ID: {idProdutoCaracteristica}")
            else:
                idProdutoCaracteristica = prod_caract['id_produto_caracteristica']

            # Buscar idSaida pelo numeroPedido
            cursor.execute("SELECT id_saida FROM Saida WHERE numero_pedido = %s", (row['numeroPedido'],))
            saida = cursor.fetchone()
            if not saida:
                raise Exception(f"Saída não encontrada para numeroPedido {row['numeroPedido']}")
            idSaida = saida['id_saida']

            # Verificar se tem coluna quantidade
            if 'quantidade' in df.columns and pd.notna(row['quantidade']):
                quantidade = row['quantidade']
            else:
                quantidade = 1
                print(f"    ⚠️  Usando quantidade padrão: 1")

            # Inserir item
            sql = """
                INSERT INTO ItensSaida (id_saida, id_plataforma, quantidade, id_produto_caracteristica, id_caracteristica, id_tipo_caracteristica, id_produto)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                idSaida,
                fkPlataforma,
                quantidade,
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

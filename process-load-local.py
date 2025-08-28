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
WATCH_DIR = r"C:\Users\Ayrton Casa\Documents\SPTech\2025\PI\Projeto\bucket-trusted"  # altere para o seu caminho
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
    print(f"🚀 Processando arquivo: {filepath}")
    filename = os.path.basename(filepath)
    
    try:
        # Extrair fkPlataforma do nome do arquivo
        print(f"📝 Nome do arquivo: {filename}")
        partes = filename.split("_")
        print(f"   Partes: {partes}")
        
        # CORREÇÃO: Identificar posição correta do fkPlataforma
        if filename.startswith("Order.all"):
            # Para Order.all.20250101_20250131_1_20250828_120018_processado.csv
            # fkPlataforma está na posição 2 (valor = 1)
            fkPlataforma = int(partes[2])
        elif filename.startswith("Vendas-"):
            # Para Vendas-de6809a0-d616-40f2-8124-c1b3165b67b9_2_20250828_120021_processado.csv  
            # fkPlataforma está na posição 1 (valor = 2)
            fkPlataforma = int(partes[1])
        else:
            raise Exception(f"Formato de arquivo não reconhecido: {filename}")
            
        print(f"   fkPlataforma: {fkPlataforma}")

        print(f"📖 Lendo CSV...")
        
        # CORREÇÃO: Detectar separador correto
        with open(filepath, 'r', encoding='utf-8') as f:
            primeira_linha = f.readline()
            if ';' in primeira_linha and ',' not in primeira_linha:
                separador = ';'
                print(f"   Detectado separador: ';'")
            else:
                separador = ','
                print(f"   Detectado separador: ','")
        
        df = pd.read_csv(filepath, sep=separador)
        print(f"   Linhas: {len(df)}")
        print(f"   Colunas disponíveis: {list(df.columns)}")
        
        # Mostrar primeiras linhas para debug
        print(f"   Primeiras 3 linhas:")
        for i in range(min(3, len(df))):
            print(f"     {dict(df.iloc[i])}")

        print(f"🔌 Conectando ao banco...")
        conn = get_connection()

        print(f"📊 Inserindo saídas...")
        insert_saida(df, fkPlataforma, conn)

        print(f"📦 Inserindo itens...")
        insert_itens_saida(df, fkPlataforma, conn)

        conn.close()
        print(f"✅ Dados inseridos com sucesso")

        # Mover para pasta processados
        new_path = os.path.join(PROCESSED_DIR, filename)
        print(f"📁 Movendo para: {new_path}")
        os.rename(filepath, new_path)
        print(f"✅ Arquivo movido com sucesso")
        
    except Exception as e:
        print(f"❌ ERRO DETALHADO: {e}")
        raise

# ==============================
# MONITORAR DIRETÓRIO
# ==============================
def main():
    print(f"Monitorando pasta: {WATCH_DIR}")
    while True:
        try:
            arquivos = os.listdir(WATCH_DIR)
            csvs = [f for f in arquivos if f.endswith(".csv")]
            
            print(f"Arquivos CSV encontrados: {csvs}")
            
            for file in csvs:
                filepath = os.path.join(WATCH_DIR, file)
                print(f"Tentando processar: {file}")
                
                try:
                    process_file(filepath)
                    print(f"✅ {file} processado com sucesso")
                except Exception as e:
                    print(f"❌ ERRO ao processar {file}: {e}")
                    print(f"   Arquivo NÃO foi movido para processados")
                    # Continue para próximo arquivo mesmo com erro
                    continue
                    
        except Exception as e:
            print(f"❌ Erro ao listar diretório: {e}")
            
        time.sleep(10)  # verifica a cada 10s

if __name__ == "__main__":
    main()

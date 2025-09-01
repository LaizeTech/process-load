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
                INSERT INTO Saida (id_empresa, id_plataforma, id_tipo_saida, numero_pedido, dt_venda, preco_venda, total_desconto, id_status_venda)
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
                # CORREÇÃO: Criar o relacionamento automaticamente
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

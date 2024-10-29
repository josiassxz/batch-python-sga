import mysql.connector
from mysql.connector import Error
import configparser
import logging
import sga_clean
import pandas as pd

# Configurar logging para console e arquivo
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('automacao_sql.log'),
                        logging.StreamHandler()
                    ])

def connect_to_database(host, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            logging.info(f"Conectado com sucesso ao banco de dados: {database}")
            return connection
    except Error as e:
        logging.error(f"Erro ao conectar ao banco de dados {database}: {e}")
    return None

def execute_query(connection, query):
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        logging.info(f"Consulta executada com sucesso. {len(results)} registros retornados.")
        return results
    except Error as e:
        logging.error(f"Erro ao executar a consulta: {e}")
    return None

def insert_data_to_datalake(connection, data, id_unidade_origem):
    try:
        cursor = connection.cursor()
        query = """
        INSERT INTO atendimento (
            id_unidade, nome_unidade, nome_atendente, servico, tipo_prioridade,
            sigla_senha, dt_cheg, dt_cha, dt_ini, dt_fim, tempo_espera, tempo_atendimento, tempo_total,
            id_unidade_origem
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        for index, row in data.iterrows():
            values = (
                row['id_unidade'], row['nome'], row['nome_atendente'],
                row['servico'], row['tipo_prioridade'], row['sigla_senha'],
                row['dt_cheg'], row['dt_cha'], row['dt_ini'], row['dt_fim'],
                row['tempo_espera'], row['tempo_atendimento'], row['tempo_total'],
                id_unidade_origem
            )
            cursor.execute(query, values)
        connection.commit()
        cursor.close()
        logging.info(f"Dados inseridos com sucesso no datalake. Origem: {id_unidade_origem}")
    except Error as e:
        logging.error(f"Erro ao inserir dados no datalake: {e}")

def main():
    logging.info("Iniciando o processo de automação...")
    
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
        logging.info("Arquivo de configuração lido com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo de configuração: {e}")
        return

    if 'datalake' not in config:
        logging.error("Configuração do datalake não encontrada no arquivo config.ini")
        return

    datalake_config = config['datalake']
    required_keys = ['host', 'user', 'password', 'database']
    if not all(key in datalake_config for key in required_keys):
        logging.error(f"Configuração do datalake está incompleta. Chaves necessárias: {', '.join(required_keys)}")
        return

    datalake_connection = connect_to_database(
        datalake_config['host'],
        datalake_config['user'],
        datalake_config['password'],
        datalake_config['database']
    )

    if not datalake_connection:
        logging.error("Não foi possível conectar ao datalake. Encerrando o script.")
        return

    query = """
    SELECT ha.id, unidades.nome, unidades.id as id_unidade, usuarios.nome AS nome_atendente, 
           servicos.nome AS servico, prioridades.nome AS tipo_prioridade, 
           ha.sigla_senha, ha.dt_cheg, ha.dt_cha, ha.dt_ini, ha.dt_fim  
    FROM view_historico_atendimentos ha
    INNER JOIN prioridades ON prioridades.id = ha.prioridade_id
    INNER JOIN usuarios ON usuarios.id = usuario_tri_id
    INNER JOIN servicos ON servicos.id = ha.servico_id
    INNER JOIN unidades ON unidades.id = ha.unidade_id
    WHERE DATE(dt_cheg) = CURDATE() - INTERVAL 1 DAY;
    """

    for db_name, db_config in config.items():
        if db_name != 'datalake':
            logging.info(f"Processando banco de dados: {db_name}")
            if not all(key in db_config for key in required_keys + ['id_unidade_origem']):
                logging.error(f"Configuração incompleta para o banco de dados {db_name}. Pulando.")
                continue

            connection = connect_to_database(
                db_config['host'],
                db_config['user'],
                db_config['password'],
                db_config['database']
            )
            
            if connection:
                results = execute_query(connection, query)
                if results:
                    results = sga_clean.do_clean(pd.DataFrame(results))
                    insert_data_to_datalake(datalake_connection, results, db_config['id_unidade_origem'])
                else:
                    logging.warning(f"Nenhum resultado retornado da consulta para {db_name}")
                connection.close()
            else:
                logging.warning(f"Não foi possível conectar ao banco de dados: {db_name}")

    datalake_connection.close()
    logging.info("Processo de automação concluído com sucesso.")

if __name__ == "__main__":
    main()
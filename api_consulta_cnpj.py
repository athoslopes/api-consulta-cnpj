import requests
import pandas as pd
import os
import time
from datetime import datetime

def ler_cnpjs_csv(nome_arquivo, limite=None):
    # Monta o caminho completo do arquivo CSV
    caminho_arquivo = os.path.join(os.path.dirname(__file__), 'arquivo_base', nome_arquivo)

    try:
        # Tenta ler o arquivo CSV
        df = pd.read_csv(caminho_arquivo)
        if 'CNPJs' not in df.columns:
            raise ValueError("Não encontrei a coluna 'CNPJs' no arquivo.")

        # Remove entradas vazias e limita o número de CNPJs se necessário
        cnpjs = df['CNPJs'].dropna().tolist()
        if limite is not None:
            cnpjs = cnpjs[:limite]
        return cnpjs
    except FileNotFoundError:
        print(f"Não consegui encontrar o arquivo {nome_arquivo}.")
        return []
    except Exception as e:
        print(f"Deu um problema ao tentar ler o arquivo CSV: {e}")
        return []

def consultar_api(cnpj):
    url = 'https://api.infosimples.com/api/v2/consultas/receita-federal/cnpj'
    args = {
        "cnpj": cnpj,
        "mobile_sem_login": "",
        "login_cpf": "",
        "login_senha": "",
        "token": "B9km9f8wfeJbfZK3CNefWmDrzjJbAEIuJhtQ4ZlZ",
        "timeout": 300
    }

    try:
        # Faz a requisição para a API
        response = requests.post(url, args)
        response_json = response.json()
        response.close()

        # Verifica o resultado da API
        print(f"Resposta da API para o CNPJ {cnpj}: {response_json}")

        if response_json.get('code') == 200:
            data = response_json.get('data', [{}])[0]
            return {
                'CNPJ': cnpj,
                'Razao Social': data.get('razao_social', 'Não disponível'),
                'Nome Fantasia': data.get('nome_fantasia', 'Não disponível'),
                'Logradouro': data.get('endereco_logradouro', 'Não disponível'),
                'Numero': data.get('endereco_numero', 'Não disponível'),
                'Bairro': data.get('endereco_bairro', 'Não disponível'),
                'CEP': data.get('endereco_cep', 'Não disponível'),
                'Municipio': data.get('endereco_municipio', 'Não disponível'),
                'UF': data.get('endereco_uf', 'Não disponível'),
                'Situacao Cadastral': data.get('situacao_cadastral', 'Não disponível'),
                'Natureza Juridica': data.get('natureza_juridica', 'Não disponível'),
                'Atividade Economica': data.get('atividade_economica', 'Não disponível'),
                'Matriz/Filial': data.get('matriz_filial', 'Não disponível'),
                'Data Situacao Cadastral': data.get('situacao_cadastral_data', 'Não disponível'),
            }
        else:
            return {
                'CNPJ': cnpj,
                'Erro': f"Código: {response_json.get('code', 'Não disponível')}, Mensagem: {response_json.get('code_message', 'Não disponível')}"
            }
    except Exception as e:
        return {'CNPJ': cnpj, 'Erro': str(e)}

def salvar_dados_csv(dados):
    if not dados:
        print("Parece que não há dados para salvar.")
        return

    # Define o nome do arquivo com base na data atual
    data_atual = datetime.now().strftime('%d_%m_%Y')
    nome_arquivo = f"resultado_consulta_{data_atual}.csv"

    df = pd.DataFrame(dados)
    df.to_csv(nome_arquivo, index=False)
    print(f"Os dados foram salvos em {nome_arquivo}")

def registrar_log(cnpjs, resultados):
    """
    Registra o progresso da consulta em um arquivo de log.

    Args:
    cnpjs (list): Lista de CNPJs que foram consultados.
    resultados (list): Lista de resultados das consultas.
    """
    caminho_log = os.path.join(os.path.dirname(__file__), 'log', 'consulta.log')
    os.makedirs(os.path.dirname(caminho_log), exist_ok=True)

    hora_consulta = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    with open(caminho_log, 'a') as arquivo_log:
        arquivo_log.write(f"************** Início da consulta **************\n")

        for cnpj, resultado in zip(cnpjs, resultados):
            status = 'Sucesso' if 'Erro' not in resultado else 'Erro'
            arquivo_log.write(f"{hora_consulta} - CNPJ: {cnpj} - Status: {status}\n")

        arquivo_log.write(f"______________ Fim da consulta _______________\n")

def main():
    # Lê os CNPJs do arquivo CSV, limitando a 20 CNPJs
    cnpjs = ler_cnpjs_csv('input.csv', limite=20)

    if not cnpjs:
        print("Nenhum CNPJ encontrado para consulta.")
        return

    todos_dados = []
    resultados = []

    # Consulta a API para cada CNPJ e coleta os dados
    for cnpj in cnpjs:
        dados = consultar_api(cnpj)
        resultados.append(dados)
        if 'Erro' in dados:
            status = 'Erro ao consultar'
        else:
            status = 'Sucesso'
            todos_dados.append(dados)
        # Registra o status da consulta no log
        registrar_log([cnpj], [dados])
        # Aguarda 2 segundos antes da próxima consulta
        time.sleep(2)

    # Registra o início e o fim da consulta em um único log
    registrar_log(cnpjs, resultados)

    # Salva os dados coletados em um arquivo CSV
    salvar_dados_csv(todos_dados)

if __name__ == "__main__":
    main()

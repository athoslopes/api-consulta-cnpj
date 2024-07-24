import requests
import pandas as pd
import os
import time
from datetime import datetime


def ler_cnpjs_csv(nome_arquivo, limite=None):
    caminho_arquivo = os.path.join(os.path.dirname(__file__), 'arquivo_base', nome_arquivo)

    try:
        df = pd.read_csv(caminho_arquivo)
        if 'CNPJs' not in df.columns:
            raise ValueError("A coluna 'CNPJs' não foi encontrada no arquivo CSV.")

        cnpjs = df['CNPJs'].dropna().tolist()  # Remove valores NaN e converte para lista
        if limite is not None:
            cnpjs = cnpjs[:limite]  # Limita a quantidade de CNPJs
        return cnpjs
    except FileNotFoundError:
        print(f"Arquivo {nome_arquivo} não encontrado.")
        return []
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
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
        response = requests.post(url, args)
        response_json = response.json()
        response.close()

        if response_json['code'] == 200:
            data = response_json['data'][0]
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
                'Erro': f"Código: {response_json['code']}, Mensagem: {response_json.get('code_message', 'Não disponível')}"
            }
    except Exception as e:
        return {'CNPJ': cnpj, 'Erro': str(e)}


def salvar_dados_csv(dados):
    if not dados:
        print("Nenhum dado para salvar.")
        return

    # Criar o nome do arquivo com a data atual
    data_atual = datetime.now().strftime('%d_%m_%Y')
    nome_arquivo = f"resultado_consulta_{data_atual}.csv"

    df = pd.DataFrame(dados)
    df.to_csv(nome_arquivo, index=False)
    print(f"Dados salvos em {nome_arquivo}")


def registrar_log(cnpj, status):
    caminho_log = os.path.join(os.path.dirname(__file__), 'log', 'consultas.log')

    try:
        with open(caminho_log, 'a') as f:
            f.write(f"{time.strftime('%d/%m/%Y %H:%M:%S')} - CNPJ: {cnpj} - Status: {status}\n")
    except Exception as e:
        print(f"Erro ao registrar log: {e}")


def main():
    cnpjs = ler_cnpjs_csv('input.csv', limite=3)

    if not cnpjs:
        print("Nenhum CNPJ encontrado para consulta.")
        return

    todos_dados = []

    for cnpj in cnpjs:
        registrar_log(cnpj, 'Início da consulta')
        dados = consultar_api(cnpj)
        todos_dados.append(dados)
        status = dados.get('Erro', 'Sucesso')
        registrar_log(cnpj, status)
        registrar_log(cnpj, 'Fim da consulta')
        time.sleep(2)  # Aguarda 2 segundos entre as consultas

    salvar_dados_csv(todos_dados)


if __name__ == "__main__":
    main()

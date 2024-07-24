
from api_consulta_cnpj import *


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

import requests
import json

# URL do endpoint da API que fornece os dados dos ônibus
API_URL = "https://dados.mobilidade.rio/gps/sppo"

def search_bus_data():
    """
    Busca os dados mais recentes da API de GPS dos ônibus do Rio.
    Retorna os dados em formato de dicionário Python ou None em caso de erro.
    """
    print("Iniciando a busca de dados na API...")
    try:
        # A requisição GET é feita aqui. O timeout é uma boa prática.
        response = requests.get(API_URL, timeout=30)

        # Verifica se a requisição foi bem-sucedida (código de status 200 OK)
        response.raise_for_status() 
        
        print("Dados recebidos com sucesso!")
        # A biblioteca requests tem um método .json() que já converte a resposta
        # da API (que está em formato de texto JSON) para um objeto Python (dicionário/lista)
        return response.json()

    except requests.exceptions.HTTPError as errh:
        print(f"Erro HTTP: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Erro de Conexão: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Erro de Timeout: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Ocorreu um erro: {err}")
    
    return None

def main():
    """
    Função principal do programa.
    """
    dados_onibus = search_bus_data() # Supondo que o nome da função é este

    # A verificação agora deve checar se recebemos dados e se são uma lista
    if dados_onibus != None:
        assert isinstance(dados_onibus, list), "Os dados retornados não são uma lista como esperado."

        print(f"\nEncontrados {len(dados_onibus)} ônibus em tempo real.")
        # Se a lista estiver vazia, não há o que fazer.
        if not dados_onibus:
            print("Nenhum dado de ônibus foi retornado pela API no momento.")
            return

        # Vamos inspecionar os dados dos 3 primeiros ônibus como exemplo
        print("\nExemplo de dados dos 3 primeiros ônibus:")
        for onibus_info in dados_onibus[:3]:
            assert isinstance(onibus_info, dict), "Cada item na lista deve ser um dicionário."

            # 'onibus_info' já é um dicionário com as informações corretas
            # Acessamos os valores diretamente pelas chaves
            linha = onibus_info.get('linha', 'N/A')
            ordem = onibus_info.get('ordem', 'N/A')
            velocidade = onibus_info.get('velocidade', 'N/A')
            
            print(f"  - Linha: {linha}, Ordem: {ordem}, Velocidade: {velocidade} km/h")

        # Exemplo de como salvar os dados em um arquivo JSON
        nome_arquivo = "LargeFiles/bus_data.json"
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            # A lista de dicionários já é um formato JSON válido e muito legível
            json.dump(dados_onibus, f, ensure_ascii=False, indent=2)
        print(f"\nTodos os dados brutos foram salvos em '{nome_arquivo}'")

if __name__ == "__main__":
    main()
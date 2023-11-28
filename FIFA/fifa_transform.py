import pandas as pd
from azure.storage.blob import BlobServiceClient,BlobPrefix
from azure.core.pipeline.transport import RequestsTransport
from io import BytesIO
import io
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


account_name = 'nome do seu datalake'
account_key = 'sua chave'
containername_raw = 'nome do container'
containername_produce = 'nome do container'
transport = RequestsTransport(connection_verify = False)


def tratar_dados():

    def ler_blob_1_sheet(account_name, account_key, containername_raw):
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                                credential=account_key,
                                                transport=transport)
        container_client = blob_service_client.get_container_client(containername_raw)

        dfs = []  # Lista para armazenar os dataframes de cada arquivo

        # Listando blobs no container
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            blob_name = blob.name

            # Download do blob e leitura do DataFrame
            blob_client = container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().readall()
            df = pd.read_excel(io.BytesIO(blob_data), header=0, engine="openpyxl")

            # Adicionando o DataFrame à lista
            dfs.append(df)

        return dfs
    data_list = ler_blob_1_sheet(account_name, account_key, containername_raw)

    def tratar_dados_liga(data_list):

        liga_data = data_list[2].copy()

        # Preenchimento de valores NaN
        liga_data.fillna({'Genero': '-'}, inplace=True)

        def list_blobs(account_name, account_key, container_name, directory):
            """
            Lista os blobs dentro de um diretório específico em um contêiner do Azure Blob Storage.

            Args:
            - account_name: Nome da conta do Azure Blob Storage.
            - account_key: Chave de acesso à conta do Azure Blob Storage.
            - container_name: Nome do contêiner que contém os blobs.
            - directory: Diretório dentro do contêiner para listar os blobs.

            Returns:
            - Lista de nomes de blobs presentes no diretório especificado.
            """
            transport = RequestsTransport(connection_verify=False)

            with BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net",
                credential=account_key,
                transport=transport
            ) as blob_service_client:
                container_client = blob_service_client.get_container_client(container_name)
                blobs = container_client.list_blobs(name_starts_with=directory)
                file_list = [blob.name for blob in blobs if not isinstance(blob, BlobPrefix)]
                return file_list

        def create_dataframe(file_list, container_name, directory, account_name):
            """
            Cria um DataFrame do Pandas com IDs e URLs correspondentes aos blobs.

            Args:
            - file_list: Lista de nomes de blobs.
            - container_name: Nome do contêiner do Azure Blob Storage.
            - directory: Diretório dentro do contêiner onde os blobs estão localizados.
            - account_name: Nome da conta do Azure Blob Storage.

            Returns:
            - DataFrame Pandas contendo colunas 'ID' (nomes de blobs) e 'url' (URLs para os blobs).
            """
            # Remover o diretório da lista de arquivos, se presente
            file_list = [file for file in file_list if file != f"{directory}/"]

            df = pd.DataFrame({'ID': file_list})
            df['ID'] = df['ID'].str.replace(f'{directory}/', '').str.replace('.jpg', '')

            url_prefix = f"https://{account_name}.blob.core.windows.net/{container_name}/{directory}/"
            df['url'] = url_prefix + df['ID'] + '.jpg'
            return df

        def main(liga_data):
            """
            Função principal para listar blobs de um diretório específico e criar um DataFrame com IDs e URLs.
            """
            account_name = 'nome do seu datalake'
            account_key = 'sua chave'
            container_name = 'nome do container'
            directory = 'nome do diretorio'

            try:
                file_list = list_blobs(account_name, account_key, container_name, directory)
                df = create_dataframe(file_list, container_name, directory, account_name)
                
                # Remova a primeira linha (se for 'pais') do DataFrame
                df = df[df['ID'] != 'liga']

                liga_data['ID Liga'] = liga_data['ID Liga'].astype(str)
                df['ID'] = df['ID'].astype(str)

                liga_data = liga_data.merge(df[['ID', 'url']], left_on='ID Liga', right_on='ID', how='left')
                # Excluindo a coluna 'ID'
                liga_data.drop(columns=['ID'], inplace=True)

                # Renomeando a coluna 'url' para 'Imagem Pais'
                liga_data.rename(columns={'url': 'Imagem Liga'}, inplace=True)

                print(liga_data)
            except Exception as e:
                print(f"An error occurred: {e}")

            return liga_data  # Retorna a variável modificada

        liga_data = main(liga_data)





        return liga_data
    liga = tratar_dados_liga(data_list)


    def tratar_dados_jogadores(data_list):

        jogadores_data = data_list[1]

        def modelagem_jogadores(jogadores_data):

            # Preenchimento de valores NaN
            jogadores_data.fillna({'Atributo de Goleiro': 0, 'IMC': 0, 'Peso':0, 'Genero':'-'}, inplace=True)

            # Conversão de tipos
            jogadores_data['Atributo de Goleiro'] = jogadores_data['Atributo de Goleiro'].astype(int)
            jogadores_data['Altura'] = jogadores_data['Altura'] / 100

            # Cálculo do IMC
            def calcular_imc(row):
                if row['Peso'] == 0 and row['Altura'] == 0:
                    return 0
                elif row['Peso'] != 0 and row['Altura'] == 0:
                    return 0
                else:
                    return round(row['Peso'] / (row['Altura'] ** 2), 2)

            jogadores_data['IMC'] = jogadores_data.apply(calcular_imc, axis=1)

            # Classificação do IMC
            def classificar_imc(imc):
                if imc == 0:
                    return "Não se Aplica"
                elif 0 < imc < 18.5:
                    return "Abaixo do Peso"
                elif 18.5 <= imc < 24.9:
                    return "Peso Normal"
                elif 24.9 <= imc < 29.9:
                    return "Sobrepeso"
                else:
                    return "Obeso"

            jogadores_data['Classificacao IMC'] = jogadores_data['IMC'].apply(classificar_imc)

            return jogadores_data
        jogadores_data = modelagem_jogadores(jogadores_data)

        def list_blobs(account_name, account_key, container_name, directory):
            """
            Lista os blobs dentro de um diretório específico em um contêiner do Azure Blob Storage.

            Args:
            - account_name: Nome da conta do Azure Blob Storage.
            - account_key: Chave de acesso à conta do Azure Blob Storage.
            - container_name: Nome do contêiner que contém os blobs.
            - directory: Diretório dentro do contêiner para listar os blobs.

            Returns:
            - Lista de nomes de blobs presentes no diretório especificado.
            """
            transport = RequestsTransport(connection_verify=False)

            with BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net",
                credential=account_key,
                transport=transport
            ) as blob_service_client:
                container_client = blob_service_client.get_container_client(container_name)
                blobs = container_client.list_blobs(name_starts_with=directory)
                file_list = [blob.name for blob in blobs if not isinstance(blob, BlobPrefix)]
                return file_list

        def create_dataframe(file_list, container_name, directory, account_name):
            """
            Cria um DataFrame do Pandas com IDs e URLs correspondentes aos blobs.

            Args:
            - file_list: Lista de nomes de blobs.
            - container_name: Nome do contêiner do Azure Blob Storage.
            - directory: Diretório dentro do contêiner onde os blobs estão localizados.
            - account_name: Nome da conta do Azure Blob Storage.

            Returns:
            - DataFrame Pandas contendo colunas 'ID' (nomes de blobs) e 'url' (URLs para os blobs).
            """
            # Remover o diretório da lista de arquivos, se presente
            file_list = [file for file in file_list if file != f"{directory}/"]

            df = pd.DataFrame({'ID': file_list})
            df['ID'] = df['ID'].str.replace(f'{directory}/', '').str.replace('.jpg', '')

            url_prefix = f"https://{account_name}.blob.core.windows.net/{container_name}/{directory}/"
            df['url'] = url_prefix + df['ID'] + '.jpg'
            return df

        def main(jogadores_data):
            """
            Função principal para listar blobs de um diretório específico e criar um DataFrame com IDs e URLs.
            """
            account_name = 'nome do seu datalake'
            account_key = 'sua chave'
            container_name = 'nome do container'
            directory = 'nome do diretorio'

            try:
                file_list = list_blobs(account_name, account_key, container_name, directory)
                df = create_dataframe(file_list, container_name, directory, account_name)
                
                # Remova a primeira linha (se for 'pais') do DataFrame
                df = df[df['ID'] != 'jogador']

                jogadores_data['ID'] = jogadores_data['ID'].astype(str)
                df['ID'] = df['ID'].astype(str)

                jogadores_data = jogadores_data.merge(df[['ID', 'url']], left_on='ID', right_on='ID', how='left')
                # Excluindo a coluna 'ID'
                jogadores_data.drop(columns=['ID'], inplace=True)

                # Renomeando a coluna 'url' para 'Imagem Pais'
                jogadores_data.rename(columns={'url': 'Imagem Jogador'}, inplace=True)

                print(jogadores_data)
            except Exception as e:
                print(f"An error occurred: {e}")

            return jogadores_data  # Retorna a variável modificada

        jogadores_data = main(jogadores_data)
        return jogadores_data

    jogadores = tratar_dados_jogadores(data_list)


    def tratar_dados_pais(data_list):

        pais_data = data_list[3].copy()

        def list_blobs(account_name, account_key, container_name, directory):
            """
            Lista os blobs dentro de um diretório específico em um contêiner do Azure Blob Storage.

            Args:
            - account_name: Nome da conta do Azure Blob Storage.
            - account_key: Chave de acesso à conta do Azure Blob Storage.
            - container_name: Nome do contêiner que contém os blobs.
            - directory: Diretório dentro do contêiner para listar os blobs.

            Returns:
            - Lista de nomes de blobs presentes no diretório especificado.
            """
            transport = RequestsTransport(connection_verify=False)

            with BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net",
                credential=account_key,
                transport=transport
            ) as blob_service_client:
                container_client = blob_service_client.get_container_client(container_name)
                blobs = container_client.list_blobs(name_starts_with=directory)
                file_list = [blob.name for blob in blobs if not isinstance(blob, BlobPrefix)]
                return file_list

        def create_dataframe(file_list, container_name, directory, account_name):
            """
            Cria um DataFrame do Pandas com IDs e URLs correspondentes aos blobs.

            Args:
            - file_list: Lista de nomes de blobs.
            - container_name: Nome do contêiner do Azure Blob Storage.
            - directory: Diretório dentro do contêiner onde os blobs estão localizados.
            - account_name: Nome da conta do Azure Blob Storage.

            Returns:
            - DataFrame Pandas contendo colunas 'ID' (nomes de blobs) e 'url' (URLs para os blobs).
            """
            # Remover o diretório da lista de arquivos, se presente
            file_list = [file for file in file_list if file != f"{directory}/"]

            df = pd.DataFrame({'ID': file_list})
            df['ID'] = df['ID'].str.replace(f'{directory}/', '').str.replace('.jpg', '')

            url_prefix = f"https://{account_name}.blob.core.windows.net/{container_name}/{directory}/"
            df['url'] = url_prefix + df['ID'] + '.jpg'
            return df

        def main(pais_data):
            """
            Função principal para listar blobs de um diretório específico e criar um DataFrame com IDs e URLs.
            """
            account_name = 'nome do seu datalake'
            account_key = 'sua chave'
            container_name = 'nome do container'
            directory = 'nome do diretorio'

            try:
                file_list = list_blobs(account_name, account_key, container_name, directory)
                df = create_dataframe(file_list, container_name, directory, account_name)
                
                # Remova a primeira linha (se for 'pais') do DataFrame
                df = df[df['ID'] != 'pais']

                pais_data['ID Nacionalidade'] = pais_data['ID Nacionalidade'].astype(str)
                df['ID'] = df['ID'].astype(str)

                pais_data = pais_data.merge(df[['ID', 'url']], left_on='ID Nacionalidade', right_on='ID', how='left')
                # Excluindo a coluna 'ID'
                pais_data.drop(columns=['ID'], inplace=True)

                # Renomeando a coluna 'url' para 'Imagem Pais'
                pais_data.rename(columns={'url': 'Imagem Pais'}, inplace=True)

                print(pais_data)
            except Exception as e:
                print(f"An error occurred: {e}")

            return pais_data  # Retorna a variável modificada

        pais_data = main(pais_data)
        return pais_data
    pais = tratar_dados_pais(data_list)


    def tratar_dados_clube(data_list):

        clube_data = data_list[0].copy()

        def list_blobs(account_name, account_key, container_name, directory):
            """
            Lista os blobs dentro de um diretório específico em um contêiner do Azure Blob Storage.

            Args:
            - account_name: Nome da conta do Azure Blob Storage.
            - account_key: Chave de acesso à conta do Azure Blob Storage.
            - container_name: Nome do contêiner que contém os blobs.
            - directory: Diretório dentro do contêiner para listar os blobs.

            Returns:
            - Lista de nomes de blobs presentes no diretório especificado.
            """
            transport = RequestsTransport(connection_verify=False)

            with BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net",
                credential=account_key,
                transport=transport
            ) as blob_service_client:
                container_client = blob_service_client.get_container_client(container_name)
                blobs = container_client.list_blobs(name_starts_with=directory)
                file_list = [blob.name for blob in blobs if not isinstance(blob, BlobPrefix)]
                return file_list

        def create_dataframe(file_list, container_name, directory, account_name):
            """
            Cria um DataFrame do Pandas com IDs e URLs correspondentes aos blobs.

            Args:
            - file_list: Lista de nomes de blobs.
            - container_name: Nome do contêiner do Azure Blob Storage.
            - directory: Diretório dentro do contêiner onde os blobs estão localizados.
            - account_name: Nome da conta do Azure Blob Storage.

            Returns:
            - DataFrame Pandas contendo colunas 'ID' (nomes de blobs) e 'url' (URLs para os blobs).
            """
            # Remover o diretório da lista de arquivos, se presente
            file_list = [file for file in file_list if file != f"{directory}/"]

            df = pd.DataFrame({'ID': file_list})
            df['ID'] = df['ID'].str.replace(f'{directory}/', '').str.replace('.jpg', '')

            url_prefix = f"https://{account_name}.blob.core.windows.net/{container_name}/{directory}/"
            df['url'] = url_prefix + df['ID'] + '.jpg'
            return df

        def main(clube_data):
            """
            Função principal para listar blobs de um diretório específico e criar um DataFrame com IDs e URLs.
            """
            account_name = 'nome do seu datalake'
            account_key = 'sua chave'
            container_name = 'nome do container'
            directory = 'nome do diretorio'

            try:
                file_list = list_blobs(account_name, account_key, container_name, directory)
                df = create_dataframe(file_list, container_name, directory, account_name)
                
                # Remova a primeira linha (se for 'pais') do DataFrame
                df = df[df['ID'] != 'pais']

                clube_data['ID Clube'] = clube_data['ID Clube'].astype(str)
                df['ID'] = df['ID'].astype(str)

                clube_data = clube_data.merge(df[['ID', 'url']], left_on='ID Clube', right_on='ID', how='left')
                # Excluindo a coluna 'ID'
                clube_data.drop(columns=['ID'], inplace=True)

                # Renomeando a coluna 'url' para 'Imagem Pais'
                clube_data.rename(columns={'url': 'Imagem Clube'}, inplace=True)

                print(clube_data)
            except Exception as e:
                print(f"An error occurred: {e}")

            return clube_data  # Retorna a variável modificada

        clube_data = main(clube_data)
        return clube_data
    clube = tratar_dados_clube(data_list)




    def salvar_arquivo_produce(df, blob_name):
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
            transport=transport
        )
        container_client = blob_service_client.get_container_client(containername_produce)
        blob_client = container_client.get_blob_client(blob_name)

        output = io.BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        blob_client.upload_blob(output, overwrite=True)
    salvar_arquivo_produce(pais, 'nacao.xlsx')
    salvar_arquivo_produce(liga, 'liga.xlsx')
    salvar_arquivo_produce(clube, 'clube.xlsx')
    salvar_arquivo_produce(jogadores, 'jogadores.xlsx')

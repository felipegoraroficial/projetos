# Pipeline de Dados da FIFA

Este repositório exemplifica um fluxo de trabalho que desenvolvi para automatizar a transformação de dados da FIFA, indo desde arquivos Excel e apresentações em PowerPoint armazenados localmente até uma estrutura de dados na nuvem Azure. Como exemplo, utilizei dados de jogadores de futebol do jogo EA FC.

O ambiente foi montado em uma máquina virtual com Ubuntu, usando a VirtualBox da Oracle como plataforma.

O fluxo foi construído inteiramente em Python, sendo orquestrado via Apache Airflow localmente. Utilizei um banco de dados MySQL local como backup, e na Azure, um Data Lake junto a um banco de dados MySQL para conexões em ferramentas de BI.

O objetivo central é acessar a API do site futeDB, que contém informações dos jogadores de futebol do jogo EA FC. Os dados brutos são inicialmente armazenados no contêiner "Raw" do Data Lake da Azure e, após tratamento e limpeza, são salvos no contêiner "Produce". Esses dados tratados são então inseridos tanto no banco de dados local MySQL como no MySQL na Azure, o qual será utilizado para a criação de dashboards em ferramentas de BI.

Pontos de destaque:
- Apesar da possibilidade de utilizar o Data Factory da Azure para transferir dados do Data Lake para o banco MySQL, optei por não empregar esses recursos para minimizar a complexidade e os custos do pipeline.

Bibliotecas utilizadas: pandas, request, json, azure.storage.blob, io, mysql.connector, sqlalchemy, airflow, datetime.

Versão do Python: 3.10.12 64-bit.
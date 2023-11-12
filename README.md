# Pipeline de Dados da FIFA

Criei este projeto para demonstrar 
o fluxo desenvolvido por mim no meu 
trabalho. Saímos de arquivos 
Excel armazenados em pastas de trabalho 
em computadores locais e apresentações 
criadas via PowerPoint para uma 
estrutura de dados automatizada na 
nuvem da Azure. Para ilustrar os 
resultados do meu trabalho, 
reproduzi uma versão utilizando 
dados de jogadores de futebol do 
jogo de videogame EA FC.

O ambiente foi configurado em minha 
máquina virtual com o sistema 
operacional Ubuntu na VirtualBox da Oracle.

O fluxo foi totalmente desenvolvido 
em Python, orquestrado via 
Apache Airflow (localhost), utilizando 
um banco de dados MySQL (localhost) 
como backup e um Data Lake da Azure, 
além de um banco de dados MySQL na Azure 
para conexões em ferramentas de BI.

O objetivo do ambiente é acessar a API 
do site futeDB, que contém informações 
dos jogadores de futebol do jogo EA FC. 
Os dados brutos são armazenados em um 
contêiner do Data Lake da Azure, 
denominado "Raw". Em seguida, os dados 
são tratados e limpos, sendo o dataframe 
salvo em outro contêiner chamado "Produce", 
onde os dados tratados são armazenados. 
Posteriormente, esses dados tratados são 
inseridos em um banco de dados local do 
MySQL, que servirá como nosso banco de 
dados de backup. O mesmo procedimento 
será realizado para o banco de dados MySQL 
no ambiente da Azure, que será utilizado 
para conectar ferramentas de BI na criação 
de dashboards.

Pontos importantes a serem considerados:
- É possível utilizar o Apache Airflow e 
transferir dados do Data Lake para o 
banco de dados MySQL da Azure usando o 
Data Factory da própria Azure. No entanto, 
optei por não utilizar esses recursos 
para economizar na produção deste pipeline.

Bibliotecas utilizadas: pandas, request, 
json, azure.storage.blob, io, 
mysql.connector, sqlalchemy, airflow, 
datetime.

Versão do Python: 3.10.12 64-bit.
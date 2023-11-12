# pipeline dados FIFA

Criei esse projeto pra demonstrar o fluxo criado
por mim no meu trabalho onde saimos de arquivos
excel armazenado em psta de trabalho em computadores locais
e apresentacoes criadas via power point para uma
estrutura de dados automatizado em nuvem da Azure
e para demonstrar os frutos do meu trabalho, criei
uma replica do que temos utilizandos dados de jogadores
de futebol do jogo de videogame EA FC.

O ambiente foi criado em minha maquina 
virtual com o sistema operacional Ubunto
na virtual box da oracle.

O fluxo foi criado totalmente em python,
orquestrado via apache airflow (localhos),
banco de dados do MySQL (local host) que será
usado como banco de dados de backup,
datalake da Azure e banco de dados MySQL da Azure
que será utilizado para conexoes em ferramentas de BI.

O objetivo do ambiente é acessar a API
do site futeDB com informações dos jogadores
de futebol do jogo da EA FC, armazenar os dados
cru em um container do datalake da Azure
que chamei de Raw e em seguida tratar e limpe oa dados
e salvar o dataframe em outro container
produce que é onde irei armazenar os dados
que foram tratados. Com os dados tratados,
irei inserir-los em um banco de dados local
do MySQL que será nosso banco de dados back
e tambem iremos fazer o mesmo para o banco
de dados do MySQL do ambiente da Azure onde este ultimo
usaremos para conectar em ferramentas de BI
para criaçao de dashboards.

Alguns pontos importantes:
é possivel utilizar o apache airflow e subir
dados do datalake para o banco de dados MySQL
da azure usando o DataFactory da propria Azure porém,
nao irei utilizar esses recursos pra economizar 
na produção deste pipeline.

bibliotecas utilizadas:
pandas, request, json,azure.storage.blob,
io, mysql.connector, sqlalchemy,airflow,
datetime

python 3.10.12 64-bit





# FIFA Data Pipeline

I created this project to showcase the workflow I developed in my job. We transitioned from Excel files stored in local computer folders and PowerPoint presentations to an automated data structure in the Azure cloud. To illustrate the outcomes of my work, I replicated a version using player data from the EA FC video game.

The environment was set up on my virtual machine with the Ubuntu operating system in Oracle's VirtualBox.

The entire workflow was developed in Python, orchestrated by Apache Airflow (localhost), utilizing a MySQL database (localhost) as a backup and an Azure Data Lake, along with a MySQL database in Azure for connections in BI tools.

The environment's objective is to access the futeDB website API, which contains information about the EA FC video game's football players. Raw data is stored in an Azure Data Lake container named "Raw". Then, the data is processed and cleaned, with the dataframe saved in another container called "Produce," where the treated data is stored. Subsequently, this treated data is inserted into a local MySQL database, which serves as our backup database. The same procedure will be performed for the MySQL database in the Azure environment, used to connect BI tools in creating dashboards.

Important points to consider:

It is possible to use Apache Airflow and transfer Data Lake data to Azure's MySQL database using Azure's Data Factory. However, I chose not to use these resources to save on the production of this pipeline.
Libraries used: pandas, request, json, azure.storage.blob, io, mysql.connector, sqlalchemy, airflow, datetime.

Python: 3.10.12 64-bit.

Dashboard: https://bit.ly/fifa-dashboard

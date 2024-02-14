# FIFA_v2
In January 2024, I took a course on Fabric and dbt and decided to put into practice what I learned in the FIFA_v1 project (link: https://github.com/felipegoraroficial/projetos/tree/main/FIFA_v1).

In the V1 version repository, we had an environment with various resources including: Ubuntu VM, Blob Storage Gen 2, Azure SQL Server Database, Apache Airflow, and MySQL. For this new version, I used only the Microsoft Fabric environment where I have a LakeHouse, WareHouse, and a Data Pipeline, I used SQL, Python, and PySpark in this project.

Fabric:

- The first step was to organize the “raw” and “silver” repositories where in the raw folder are json format files referring to the extraction of data from the FutDB API and in the silver folder I transform the data from the files into a table in a csv file.
- After transforming the data into CSV, I use PySpark to read the data, organize and filter columns that I will use in the Data WareHouse and transform the dataframes into Delta Lake tables in my Fabric LakeHouse.
- When I have my LakeHouse organized, I start building my WareHouse and data modeling and after that, I build the Data Pipeline where I organize each stage from extraction to data storage in my Data WareHouse.
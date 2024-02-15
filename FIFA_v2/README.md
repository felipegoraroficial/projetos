# FIFA_v2
In January 2024, I took a course on Fabric and decided to put into practice what I learned in the FIFA_v1 project (link: https://github.com/felipegoraroficial/projetos/tree/main/FIFA_v1).

In the V1 version repository, I had an environment with various resources including: Ubuntu VM, Blob Storage Gen 2, Azure SQL Server Database, Apache Airflow, and MySQL. For this new version, I used only the Microsoft Fabric environment where I have a LakeHouse, WareHouse, and a Data Pipeline and, I used SQL, Python, and PySpark in this project.

MS Fabric:

- The first step was to organize the “raw” and “silver” repositories where in the raw folder are json format files referring to the extraction of data from the FutDB API and in the silver folder I transform the data from the files into a table in a csv file.
- After transforming the data into CSV, I use PySpark to read the data, organize and filter columns that I will use in the Data WareHouse and transform the dataframes into Delta Lake tables in my Fabric LakeHouse.
- When I have my LakeHouse organized, I start building my WareHouse and data modeling and after that, I build the Data Pipeline where I organize each stage from extraction to data storage in my Data WareHouse.

Conclusion:

In this project, I opted to employ the ELT method instead of ETL, a choice that, from my point of view, resulted in more effective control over the extracted data and the transformation process. One of the main advantages is the ELT approach, which keeps the extracted data in its original format and performs the transformation after loading. This facilitates adjustments and corrections in case of bugs, without the need to rerun the entire pipeline, as would be the case with the ETL method.

I observed a significant improvement in the pipeline's execution time, with all data available in the Data Warehouse in less than 5 minutes. Despite the slight delay caused by the initialization of the PySpark cluster in the flow, the process has been extremely efficient.

One of the aspects I most appreciated when managing Microsoft Fabric resources was the centralization and native integration of all the tools. This greatly simplified the creation and management of resources, reducing my workload.

Finally, I highlight the introduction of Delta Lake in the Lakehouse, a tool that I was initially unfamiliar with, but now recognize as extremely powerful. The table history feature offered by Delta Lake is particularly impressive, a functionality absent in the previous project. I am looking forward to exploring this tool further, as I believe it can be highly effective in day-to-day work, bringing a host of benefits.


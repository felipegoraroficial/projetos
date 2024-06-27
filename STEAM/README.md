### Steam - Databricks Pipeline - Google Cloud


## Introduction: 
In this personal project, I utilize Databricks on Google Cloud Platform (GCP). I recognized the need to incorporate the Databricks tool into my daily work routine, and since January 2024, I have begun practicing with the Databricks environment to handle large volumes of data. Additionally, Databricks allows me to integrate scripts with a Git version control system, facilitates the administration and onboarding of new users to the environment, and enables the rapid scaling of the cluster as needed.

An interesting strategy I incorporated into this project was to store the extracted data in the "raw" folder of the datalake (bucket) in JSON format, with the filename corresponding to the extraction date. This is done instead of overwriting old data with new data and, at the end, instead of appending to the table within the data warehouse, I handle duplicates, keeping only the most recent values. I believe this approach saves costs by avoiding overloading the data warehouse with duplicate information.

![Steam](https://github.com/felipegoraroficial/projetos/assets/138418925/4405a38f-f516-4420-adbc-d506dab94a28)

## Data Extraction:
The heart of the project is the daily extraction of details about games, DLCs, and other content directly from the Steam API, as well as specific data from my user profile, such as the time spent on each game. This information is collected in JSON format and stored in the 'raw' folder of a bucket, organized by extraction date.

## Unification and Filtering: 
After collection, the JSON files are grouped and unified. In this process, I select the relevant information I want to analyze and save it again in a structured JSON file in the 'bronze' folder of the bucket.

## Transformation and Cleaning: 
Using PySpark, I transform these unified data into a dataframe, where I perform a series of treatments to ensure data quality. This includes cleaning null values, defining schemas, expanding nested columns, and converting prices from pounds to reais. The final result is a clean and structured dataframe, saved in CSV format in the 'silver' folder.

## Duplication and Final Storage: 
The final step involves filtering the dataframe to remove duplicates, keeping only the most recent records based on ID. The data is then stored in Delta Lake format in the 'gold' folder of the bucket and also loaded into the Databricks Hive Metastore.

![steam games](https://github.com/felipegoraroficial/projetos/assets/138418925/78dc98f7-0854-478f-9610-5cbe2bb43c7b)

## Automation and Monitoring: 
To ensure this process occurs daily without failures, I configured workflows in Databricks that follow the steps mentioned above. Additionally, I created automated email alerts to notify about the completion of the workflows, any delays, or execution failures.

![steam_game](https://github.com/felipegoraroficial/projetos/assets/138418925/9f0463af-a9ab-47fa-8cbb-72abc1a75877)
![steam_user](https://github.com/felipegoraroficial/projetos/assets/138418925/2fedb2f4-7e59-4efd-b684-6745aebf0c67)

![notifi](https://github.com/felipegoraroficial/projetos/assets/138418925/b980fda3-a63a-4a62-9f16-f1fc008fb43f)  ![trigger](https://github.com/felipegoraroficial/projetos/assets/138418925/49df47a1-bc7a-469c-8ed1-7c1fe8d05ff2) ![computer](https://github.com/felipegoraroficial/projetos/assets/138418925/dcc79ce8-857b-4964-8bd7-cc68fc6cde7e)

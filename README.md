# HR-Data-Migration
**Business Scenario:** The HR department of a leading Healthcare facility, operating across multiple locations in Nigeria, is actively seeking an experienced ETL Developer. In this role, you will play a crucial part in spearheading the creation of an efficient data pipeline to facilitate the migration of absence, misconduct, and overtime data from flat files to our central data warehouse. Additionally, you will be responsible for automating the ETL process to ensure the continuous ingestion of data into the data warehouse, enabling streamlined analysis and data reporting.


**Steps Taken to Migrate the HR data into the Central Data Warehouse**
1. **Data Profiling:** In order to ensure data alignment with the business requirements, my first step involved conducting a thorough data profiling process. This process focused on assessing data quality, completeness, accuracy, and consistency. During this profiling, a notable finding was that all the HR data contained duplicate entries. To address this issue, I planned to incorporate deduplication within my SQL script, thereby resolving the problem and improving the overall data integrity.
   
2. **SQL Scripting:** After completing the data profiling process, my next step involved developing optimized SQL scripts. These scripts were designed to extract data from the flat files and load them into a staging environment. The purpose of the staging environment was to provide a controlled space where data could be cleaned and transformed before being loaded into the final data warehouse.

Within the staging environment, I implemented various transformations to ensure data quality and consistency. These transformations included deduplicating the data to remove any duplicate entries, applying the appropriate constraints and data types, and implementing Slowly Changing Dimensions (SCDs) for tracking historical changes.

Once the necessary data cleansing and transformations were applied in the staging environment, I proceeded to write SQL scripts to load the transformed data into the data warehouse. These scripts ensured that the data was properly mapped and integrated into the warehouse's structure, following the defined data models and schema.

By following this process, I aimed to ensure that the data in the final data warehouse was clean, accurate, and aligned with the business requirements. The staging environment played a crucial role in facilitating data cleaning and transformation, allowing for a seamless and controlled data integration process into the data warehouse (You can check out the SQL scripts in this repository).

3. **Construct the ETL Pipeline:** To construct my ETL pipelines, I employed the SSIS (SQL Server Integration Services) tool within Visual Studio. During the development process, I made use of various SSIS components to create efficient data flows. These components included OLE DB and Flat File connection managers, Foreach container, Fuzzy Lookup, Lookup, Merge Join, data flow tasks, File System task, Merge, Conversion, and Derived Column.

    OLE DB and Flat File connection managers allowed me to establish connections with both relational databases and flat files as data sources.
    The Foreach container enabled me to iterate over a collection of files or objects, facilitating the handling of multiple data sources or files efficiently.
    I leveraged the Fuzzy Lookup and Lookup components to perform data matching and lookup operations, ensuring accurate data integration and enrichment.
    The Merge Join component aided in combining data from multiple sources based on specified join conditions.
    Data flow tasks played a vital role in the extraction, transformation, and loading of data within the pipelines.
    The File System task enabled interaction with the file system, empowering tasks such as file movement or copying.
    The Merge component supported the merging of multiple datasets into a single output dataset.
    Conversion and Derived Column components were utilized for data transformation and manipulation during the extraction and loading processes.


To provide a visual representation of the control flow used to extract overtime data from CSV files, Figure 1 was created. This diagram demonstrates the logical flow and sequence of tasks involved in the extraction process.
![image](https://github.com/okwoli200/HR-Data-Migration/assets/99350558/13739c09-76ee-41bc-824b-2f8e02e86003)
Figure 1


Additionally, Figure 2 illustrates the pipeline used to load the data into the staging environment. This diagram visually represents the steps involved in the transformation and loading of data into the staging area.


 ![image](https://github.com/okwoli200/HR-Data-Migration/assets/99350558/a4e1a8a2-8939-4b19-9a53-62d8552b5f6a)
Figure 2.

Lastly, Figure 3 showcases the data flow from the staging environment into the data warehouse, outlining the process of integrating the transformed data into the target data warehouse.


![image](https://github.com/okwoli200/HR-Data-Migration/assets/99350558/91621f04-3c93-477f-b4b4-ec0709bd7096)
Figure 3.

By utilizing these SSIS components in Visual Studio, I successfully built robust and efficient ETL pipelines. These pipelines facilitated seamless data extraction, transformation, and loading, ensuring the integration of data into the target data warehouse or destination with precision.

4. **Automate the ETL process:** To ensure the automatic loading of new data into the data warehouse, I implemented a control flow script that facilitated both initial full loads and subsequent incremental loads. This script ensured the seamless integration of new data into the existing dataset.

To automate the process, I leveraged the SQL Server Agent, which allowed me to schedule and execute the data loading tasks automatically at specified intervals.

Furthermore, to track the success and quality of the data loading process, I incorporated metrics within my SQL scripts. These metrics included precount (count of records before loading), descount (count of records discarded during loading), currentcount (count of records currently in the data warehouse), precount (count of records before loading), and postcount (count of records after loading). These metrics provided valuable insights into the data loading process and allowed for the identification of any data failures or discrepancies.

By utilizing the control flow script, SQL Server Agent scheduling, and incorporating metrics, I ensured the continuous and automated ingestion of new data into the data warehouse. This approach enhanced the efficiency, accuracy, and reliability of the data loading process, enabling comprehensive data analysis and reporting.

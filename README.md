# metadata-crawler-poc
POC for adding and updating AWS S3 object and AWS Glue Data Catalog tables

# glue_metadata_config
Configuration file for AWS login

# glue_metadata_crawler
- Lists metadata based on the details provided in configuration file. 

- Updates column metadata based on the key values  provided in 'new_values.json' file


# Running Crawler Locally



For checking missing table metadata follow below steps:


1. First run the code by commenting below code in main(): function

        update_table_metadata = add_update_missing_values(glue_client, database_name, table_name,
                                                          default_table_metadata)
        write_metadata_and_missing_values(update_table_metadata, update_table_metadata.get('missing_columns', {}),
                                          output_file="updated_metadata.json")

2. After running code, it will generate file named 'default_metadata.json' with default metadat under 'default_values' and missing metadata columns under 'missing_columns'


                     {
                        "default_values": {
                           "column-name": "column-comment",
                       "missing_columns": {
                           "column-name": "",
                       }
                     }




For updating metadata for missing columns (comment section of the column) follow below steps:
            
1. Follow Step 1 and 2 from  above 'For checking missing table metadata' section
2. Get all missing columns from Step 2 in  above 'For checking missing table metadata' section and add those columns in 'new_values.json' file
           
                       {
                           "column-name": "comment to add/update",
                        }
                     
3. Uncomment below code in main(): function and run code.

        update_table_metadata = add_update_missing_values(glue_client, database_name, table_name,
                                                          default_table_metadata)
        write_metadata_and_missing_values(update_table_metadata, update_table_metadata.get('missing_columns', {}),
                                          output_file="updated_metadata.json")


4. After successful running of code, it will generate file 'default_metadata.json' and 'updated_metadata.json' with recently added or updated value

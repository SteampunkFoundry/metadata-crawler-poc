import json

import boto3
import logging
import glue_metadata_config
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Retrieve AWS credentials and configuration from the glue_metadata_config module
def get_aws_token():
    return (
        glue_metadata_config.AWS_ACCESS_KEY_ID,
        glue_metadata_config.AWS_SECRET_ACCESS_KEY,
        glue_metadata_config.REGION_NAME,
        glue_metadata_config.BUCKET_NAME,
        glue_metadata_config.DATABASE_NAME,
        glue_metadata_config.CATALOG_ID,
        glue_metadata_config.OBJECT_NAME,
        glue_metadata_config.TABLE_NAME
    )


#  Creates an AWS S3 client using the provided credentials and returns it.
def login_to_aws_s3():
    try:
        s3_client = boto3.Session(profile_name='fpac').client(
            's3'
        )
        return s3_client
    except Exception as e:
        raise ConnectionError("Error connecting to AWS S3 Bucket : ", str(e))


# Creates an AWS Glue client using the provided credentials and returns it
def login_to_aws_glue():
    try:
        glue_client = boto3.Session(profile_name='fpac').client(
            'glue'
        )
        return glue_client
    except Exception as e:
        raise ConnectionError("Error connecting to AWS Glue : ", str(e))


def serialize_datetime(obj):
    """
    Helper function to serialize datetime objects.

    This function checks if the provided object is an instance of the datetime class.
    Parameters:
        obj (Any): The object to be serialized.

    Returns:
        str: A JSON-compatible representation of the datetime object.

    Raises:
        TypeError: If the provided object is not an instance of the datetime class.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Object of type '{}' is not JSON serializable".format(type(obj)))


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        """
        Override the default method of json.JSONEncoder.

        Parameters:
            obj (Any): The Python object to be serialized.

        Returns:
            str: A JSON-compatible representation of the object.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


# Retrieves all metadata associated with a specific table in the AWS Glue Data Catalog.
def get_table_metadata(database_name, table_name):
    """
    Retrieves all metadata associated with a table in AWS Glue Data Catalog.

    Parameters:
        database_name (str): The name of the database where the table resides.
        table_name (str): The name of the table for which to retrieve metadata.

    Returns:
        dict: A dictionary containing all table metadata.
    """
    glue_client = login_to_aws_glue()
    access_key, secret_key, region_name, bucket_name, database_name, catalog_id, object_name, table_name = get_aws_token()

    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name, CatalogId=catalog_id)

        # Extract all metadata
        metadata = response['Table']

        # Check for missing values
        missing_values = [key for key, value in metadata.items() if value is None]

        # Write metadata and missing values to a single file
        write_metadata_and_missing_values(metadata, missing_values)

        return metadata

    except glue_client.exceptions.EntityNotFoundException:
        print(f"***** Table '{table_name}' not found in the database '{database_name}'.")
        return None
    except Exception as e:
        print(f"Error occurred: {e}")
        return None


def write_metadata_and_missing_values(metadata, missing_values, output_file="default_metadata.json"):
    """
    Write metadata and missing column information to a JSON file.

    Parameters:
        metadata (dict): A dictionary containing table metadata.
        missing_values (list): A list of column names with missing values.
        output_file (str, optional): The name of the output JSON file. Default is "metadata_output.json".

    Returns:
        None
    """
    try:
        default_values = {}
        missing_columns = {}

        for column in metadata.get('StorageDescriptor', {}).get('Columns', []):
            name = column.get('Name')
            comment = column.get('Comment')
            if comment is None or comment.strip() == "":
                default_values[name] = ""
            else:
                default_values[name] = comment

            if name in missing_values and (comment is None or comment.strip() == ""):
                missing_columns[name] = "default comment missing"

        # Add any missing columns with default comment missing to the 'missing_columns' section
        for column in default_values:
            if column not in missing_columns and default_values[column] == "":
                missing_columns[column] = ""

        data = {
            "default_values": default_values,
            "missing_columns": missing_columns
        }

        with open(output_file, 'w') as f:
            json.dump(data, f, indent=4, default=serialize_datetime)

    except Exception as e:
        print(f" ***** Error during metadata writing: {e}")


def add_update_missing_values(glue_client, database_name, table_name, metadata):
    """
    Add or update missing column values in the table's metadata.

    Parameters:
        glue_client: Boto3 Glue client.
        database_name (str): The name of the database where the table resides.
        table_name (str): The name of the table for which to update missing values.
        metadata (dict): A dictionary containing the metadata.

    Returns:
        dict: A dictionary containing the updated missing columns.
    """
    # Get the columns with empty 'Comment' values
    missing_columns = {}
    for col in metadata['StorageDescriptor']['Columns']:
        comment = col.get('Comment')
        if comment is None or col['Comment'].strip() == "":
            missing_columns[col['Name']] = ""

    if not missing_columns:
        print("***** No missing columns with empty 'Comment' found. *****")
        return {}

    # Update the missing columns with their new values from the external JSON file
    with open("new_values.json", "r") as json_file:
        new_values = json.load(json_file)

    for column, new_comment in missing_columns.items():
        if column in new_values:
            new_comment = new_values[column]

        # Update the value of the missing column
        for col in metadata['StorageDescriptor']['Columns']:
            if col['Name'] == column:
                col['Comment'] = new_comment

    # Prepare the TableInput parameter with only the columns attribute to be updated
    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': metadata['StorageDescriptor']['Columns']
        }
    }

    # Update the table in Glue Data Catalog with the new column values
    glue_client.update_table(DatabaseName=database_name, TableInput=table_input)

    print("********** Missing column values updated successfully! **********")
    return metadata


def main():
    try:
        access_key, secret_key, region_name, bucket_name, database_name, catalog_id, object_name, table_name = get_aws_token()

        # Login to AWS S3
        s3_client = login_to_aws_s3()

        # Login to AWS Glue
        glue_client = login_to_aws_glue()

        # Get table metadata
        default_table_metadata = get_table_metadata(database_name, table_name)

        """
        Writes metadata and missing columns to a JSON output file

        Comment below code block to list all the default values from  particular table mentioned in config file
        Run below code to add or update missing column comment values for particular table mentioned in config file
        """
        update_table_metadata = add_update_missing_values(glue_client, database_name, table_name,
                                                          default_table_metadata)

        write_metadata_and_missing_values(update_table_metadata, update_table_metadata.get('missing_columns', {}),
                                          output_file="updated_metadata.json")

    except Exception as e:
        logging.exception("An error occurred: %s", str(e))


if __name__ == "__main__":
    main()

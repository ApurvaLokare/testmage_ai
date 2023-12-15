from pandas import DataFrame
from mage_ai.data_preparation.models.file import File
from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.data_preparation.models.block import Block, run_blocks, run_blocks_sync
from mage_ai.io.s3 import S3
from os import path
from pyspark.sql.types import StructType,StringType,IntegerType,StructField,TimestampType
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
@data_loader
def load_from_s3_bucket(**kwargs):
    """
    Template for loading data from a S3 bucket.
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    s3_bucket = 'opensource-etl-demofiles'
    s3_path_prefix = 's3://opensource-etl-demofiles'
    main_schema=StructType([
        StructField("id",IntegerType(),True),
        StructField("PMID",IntegerType(),True),
        StructField("AND_ID",IntegerType(),True),
        StructField("AuOrder",IntegerType(),True),
        StructField("LastName",StringType(),True),
        StructField("ForeName",StringType(),True),
        StructField("Initials",StringType(),True),
        StructField("Suffix",StringType(),True),
        StructField("AuNum",IntegerType(),True),
        StructField("PubYear",IntegerType(),True),
        StructField("BeginYear",IntegerType(),True)
    ])
    author_df = (kwargs['spark'].read
        .format('csv')
        .option('header', 'true')
        .schema(main_schema)
        .option('inferSchema', 'true')
        .option('delimiter', ',')
        .load(f's3://opensource-etl-demofiles/Author_list.csv')
    )
    return author_df
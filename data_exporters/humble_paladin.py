if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_s3(df, **kwargs):
    """
    Template for exporting data to a S3 bucket.
    """
    s3_bucket = 'opensource-etl-demofiles'
    s3_path_prefix = 's3://opensource-etl-demofiles'
    df_single_partition = df.repartition(1)
    (
        df_single_partition.write
        .option('header', 'True')
        .mode('overwrite')
        # Use csv or parquet format
        .csv(f's3://opensource-etl-demofiles/huge-data-output')
    )
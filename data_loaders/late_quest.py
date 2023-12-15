if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_s3_bucket(**kwargs):
    """
    Template for loading data from a S3 bucket.
    """
    s3_bucket = 'opensource-etl-demofiles'
    s3_path_prefix = '3.1-data-sheet-udemy-courses-design-courses.csv'
    df = (kwargs['spark'].read
        .format('csv')
        .option('header', 'true')
        .option('inferSchema', 'true')
        .option('delimiter', ',')
        .load(f's3://{s3_bucket}/{s3_path_prefix}')
    )

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

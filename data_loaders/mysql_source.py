from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader, ConfigKey
#from mage_ai.io.mysql import MySQL
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_mysql(*args, **kwargs):
    """
    Template for loading data from a MySQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#mysql
    """
    # query = 'select * from demo.ansh_nyc_taxi_data'  # Specify your SQL query here
    # config_path = path.join(get_repo_path(), 'io_config.yaml')
    # config_profile = 'ansh_property'
    # ConfigFileLoader(config_path, config_profile)
    # config = ConfigFileLoader(config_path, config_profile)

    df = (kwargs['spark'].read
        .format('jdbc')
        .option("url", f"jdbc:mysql://15.206.151.99:3306") \
        .option("dbtable", 'ansh_nyc_taxi_data') \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("user", "test") \
        .option("password", "test@123") \
        .load()

    )
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
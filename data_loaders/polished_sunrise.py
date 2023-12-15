from pandas import DataFrame
import io
import pandas as pd
import requests


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


def data_from_internet():
    url = 'https://raw.githubusercontent.com/kunal-awachar/Udemy-data/main/3.1-data-sheet-udemy-courses-business-courses.csv'

    response = requests.get(url)
    return pd.read_csv(io.StringIO(response.text), sep=',')


@data_loader
def load_data(**kwargs) -> DataFrame:
    business_df = kwargs['spark'].createDataFrame(data_from_internet())

    return business_df
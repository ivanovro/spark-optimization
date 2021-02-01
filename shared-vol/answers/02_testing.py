%%run_pytest[clean]

import os

from pyspark.sql import functions as sf

import pandas as pd
import pandas.testing as pd_test

import pytest


@pytest.fixture(scope="session")
def heroes_df(spark):
    """Small static dataset containing heroes data."""
    heroes_path = os.path.join(DATA_DIR, 'heroes.csv')
    return spark.read.csv(heroes_path, header=True)

def filter_by_role(heroes, role):
    return heroes.filter(sf.col("role") == role)


def test_filter_by_role(heroes_df):
    """Tests the filter_by_role function."""
    filtered = filter_by_role(heroes_df, role="Warrior")
    assert filtered.count() == 15
    
    
def count_roles(heroes):
    return heroes.groupby("role").count()
    
    
def test_count_roles(heroes_df):
    """Tests the count_roles function."""
    counts = count_roles(heroes_df)
    expected = pd.DataFrame({
        "role": ["Assassin", "Specialist", "Support", "Warrior"],
        "count": [16, 10, 9, 15]
    })
    pd_test.assert_frame_equal(counts.sort("role").toPandas(), expected)

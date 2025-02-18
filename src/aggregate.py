import cols
from pyspark.sql.functions import *
from typing import List
import pandas as pd
from pyspark.sql.dataframe import DataFrame as DF
from pyspark.sql.window import Window



def aggregate_spatially( sdf: DF, year: int, value_col_name: str, area_col_name: str, full_area_list: DF ) -> pd.DataFrame:
    """
    Returns a pandas DataFrame containing the mean, standard deviation, and number of considered stations for each *area* in the given *year*.
    There might be areas without measurements. These are also included (via full_area_list), but have NaNs in both mean and stddev, as well
    as 0 in the number of stations. Also areas with data maz have a NaN in stddev, in case they have only one datum.

    ---
    sdf: Spark DataFrame
    Spark Data frame containing all measurements.

    year: int
    The year to be considered.

    value_col_name: str
    Name of the column with the data of interest. There are several eligible columns.

    area_col_name:
    The column of interest in the input DataFrame. The column contains the area names. There are several of eligible columns, one for each level
    of detail.

    full_area_list: Spark DataFrame
    One-columned Spark DataFrame containung the list of all possible area names.

    ---
    returns: pd.DataFarme
    A pandas with data for visualization.
    """

    # pdf = sdf.where( (col(area_col_name).isNotNull()) & (col(cols.year) == year) ) \
    pdf = sdf.where( col(area_col_name).isNotNull() ) \
        .where( col(cols.year) == year ) \
        .where( col(value_col_name).isNotNull() ) \
        .groupby( area_col_name ) \
        .agg(
            mean( value_col_name ).alias( cols.mean ),
            stddev( value_col_name ).alias( cols.stddev ),
            count( value_col_name ).alias( cols.stations )
        ) \
        .withColumnRenamed( area_col_name, cols.area ) \
        .join( full_area_list, on = cols.area, how = 'outer' ) \
        .fillna( 0, cols.stations ) \
        .toPandas()

    return pdf



def aggregate_temporarly( sdf: DF, value_col_name: str, area: str, area_col_name: str, sdf_years: DF ) -> pd.DataFrame:
    """
    Returns a pandas DataFrame containing the mean, standard deviation, and number of considered stations for each *year* in the given *area*.
    There might be years without measurements. These are also included (via sdf_years), but have NaNs in both mean and stddev, as well
    as 0 in the number of stations. Also, years with data may have a NaN in stddev, in case they have only one datum.

    ---
    sdf: Spark DataFrame
    Spark Data frame containing all measurements.

    value_col_name: str
    Name of column with the data of interest.

    area: str
    The area to be considered.

    area_col_name:
    The column of interest in the input DataFrame. The column contains the area names. There are several of eligible columns, one for each level
    of detail.

    sdf_years: Spark DataFrame
    One-columned Spark DataFrame containing the list of all possible years.

    ---
    returns: pd.DataFarme
    A pandas with data for visualization.
    """

    pdf = sdf.where( col(area_col_name) == area ) \
            .groupby( cols.year ) \
            .agg(
                mean( value_col_name ).alias( cols.mean ),
                stddev( value_col_name ).alias( cols.stddev ),
                count( value_col_name ).alias( cols.stations )
            ) \
            .join( sdf_years, on=cols.year, how='outer' ) \
            .fillna( 0, cols.stations ) \
            .sort( cols.year, ascending=True ) \
            .toPandas()
    
    return pdf



def aggregate_running_mean( sdf: DF, value_col_name: str, area: str, area_col_name: str, sdf_years: DF ) -> pd.DataFrame:
    """
    Computes a 5-year running mean for the given area for each year.

    ---
    sdf: Spark DataFrame
    Spark Data frame containing all measurements.

    value_col_name: str
    Name of the column with the data.

    area: str
    The area to be considered.

    area_col_name:
    The column of interest in the input DataFrame. The column contains the area names. There are several of eligible columns, one for each level
    of detail.

    sdf_years: Spark DataFrame
    One-columned Spark DataFrame containing the list of all possible years.

    ---
    returns: pd.DataFarme
    A pandas DataFarme with data for visualization.
    """
    # 7-year-window from six years ago to this years
    window = Window.orderBy( cols.year ).rangeBetween(-6, Window.currentRow)    
    pdf = sdf.where( col(area_col_name) == area ) \
            .withColumn( cols.running, mean( col(value_col_name) ).over(window) ) \
            .drop_duplicates( [cols.year] ) \
            .select( [cols.year, cols.running] ) \
            .join( sdf_years, on = cols.year, how = 'outer') \
            .sort( cols.year, ascending=True ) \
            .toPandas()
    
    return pdf



def aggregate_differences( sdf: DF, area: str, area_col_name: str, sdf_years: DF ) -> pd.DataFrame:
    """
    Returns a pandas DataFrame containing the mean, standard deviation, and number of considered stations of the temperature differences to the pivot
    temperatures for each *year* in the given *area*. The pivot temperatures is individual to each station. There might be years without measurements.
    These are also included (via sdf_years), but have NaNs in both mean and stddev, as well as 0 in the number of stations. Also, years with data may
    have a NaN in stddev, in case they have only one datum.

    ---
    sdf: Spark DataFrame
    Spark Data frame containing all measurements.

    area: str
    The area to be considered.

    area_col_name:
    The column of interest in the input DataFrame. The column contains the area names. There are several of eligible columns, one for each level
    of detail.

    sdf_years: Spark DataFrame
    One-columned Spark DataFrame containing the list of all possible years.

    ---
    returns: pd.DataFarme
    A pandas DataFarme with data for visualization.
    """
    pdf = sdf.where( col(area_col_name) == area ) \
            .withColumn( cols.diff, col(cols.temperature) - col(cols.pivot_temp) ) \
            .groupby( cols.year ) \
            .agg(
                mean( cols.diff ).alias( cols.mean ),
                stddev( cols.diff ).alias( cols.stddev ),
                count( cols.diff ).alias( cols.stations )
            ) \
            .join( sdf_years, on=cols.year, how='outer' ) \
            .fillna( 0, cols.stations ) \
            .sort( cols.year, ascending=True ) \
            .toPandas()
    
    return pdf



def get_pivotal_mean( df: DF, start_year: int, end_year: int ) -> DF:
    """
    Computes a pivotal reference temperature for each station by averaging the
    stations measurements within the given time frame.

    ---
    df: spark DataFrame
    The temperature measurements.

    start_year: int
    The year the computation of the mean starts in (inclusive).

    end_year: int
    The year the computation of the mean ends in (inclusive).

    ---
    returns: spark DataFrame
    A DataFrame containing the station id and its pivotal reference temperature in each row. Note that the temperature is 'null'
    if no measurements are available for a station in the given interval.
    """
    pivots = df.where( col(cols.year) >= start_year ) \
            .where( col(cols.year) <= end_year) \
            .groupBy( cols.sdo_id ) \
            .agg(
                mean( cols.temperature ).alias( cols.pivot_temp )
            )
    
    return pivots

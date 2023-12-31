import streamlit as st
import pandas as pd
import geopandas as gpd
import pyproj
import plotly.express as px
import requests
from io import BytesIO
import zipfile
import tempfile

@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def load_and_process_data():
    final_df = pd.DataFrame()

    # Process taxi data
    for i in range(1, 13):  # You can adjust the range for the months you want to include
        month = str(i).zfill(2)
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-{month}.parquet'
        df = pd.read_parquet(url)

        df['pickup_date'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.date

        df_taxi_grouped = df.groupby(['pickup_date', 'PULocationID', 'DOLocationID'], as_index=False).agg(
            total_trips=('pickup_date', 'count'),
            passenger_count_mean=('passenger_count', 'mean'),
            passenger_count_sum=('passenger_count', 'sum'),
            trip_distance_mean=('trip_distance', 'mean'),
            trip_distance_sum=('trip_distance', 'sum'),
            PULocationID=('PULocationID', 'first'),
            DOLocationID=('DOLocationID', 'first'),
            payment_type=('payment_type', 'median'),
            fare_amount_mean=('fare_amount', 'mean'),
            fare_amount_sum=('fare_amount', 'sum'),
            extra=('extra', 'sum'),
            tip_amount_mean=('tip_amount', 'mean'),
            tip_amount_sum=('tip_amount', 'sum'),
            tolls_amount=('tolls_amount', 'sum'),
            total_amount_mean=('total_amount', 'mean'),
            total_amount_sum=('total_amount', 'sum'),
            congestion_surcharge_mean=('congestion_surcharge', 'mean'),
            congestion_surcharge_sum=('congestion_surcharge', 'sum'),
            airport_fee_mean=('airport_fee', 'mean'),
            airport_fee_sum=('airport_fee', 'sum')
        )

        final_df = pd.concat([final_df, df_taxi_grouped])

    # Filter for year 2022
    final_df['pickup_date'] = pd.to_datetime(final_df['pickup_date'])
    final_df_2022 = final_df[final_df['pickup_date'].dt.year >= 2022].copy()
    final_df_2022.loc[:, 'tip_ratio'] = final_df_2022['tip_amount_sum'] / final_df_2022['total_amount_sum']

    # Download and process shapefile
    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip'
    response = requests.get(url)
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            z.extractall(temp_dir)  # Extract all files to the temporary directory
            shapefile = gpd.read_file(f"{temp_dir}/taxi_zones.shp")

    # Transform CRS
    shapefile.to_crs(pyproj.CRS.from_epsg(4326), inplace=True)

    # Download borough lookup
    borough_lookup_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
    borough_lookup = pd.read_csv(borough_lookup_url)

    # Merge datasets
    merged_data = pd.merge(final_df_2022, borough_lookup, left_on='PULocationID', right_on='LocationID')
    merged_data_gps = pd.merge(merged_data, shapefile, left_on='Zone', right_on='zone')

    merged_data_gps_grouped = merged_data_gps.groupby('zone')[['zone', 'borough', 'geometry', 'total_amount_sum', 'tip_ratio']].agg({
        'total_amount_sum': 'sum',
        'tip_ratio': 'mean',
        'geometry': 'first',
        'borough': 'max'
    })

    # Create GeoDataFrame
    geo_df = gpd.GeoDataFrame(merged_data_gps_grouped, geometry=merged_data_gps_grouped.geometry)

    # Reset index for plotting with Plotly
    geo_df.reset_index(inplace=True)

    # Filter for Manhattan borough if necessary
    geo_df_manhattan = geo_df[geo_df['borough'] == 'Manhattan']

    return geo_df_manhattan

# Streamlit interface
st.title('NYC Taxi Data Visualization')

# Use a button to trigger the data loading and processing
if st.button('Load Data and Generate Map'):
    # Load your data
    geo_df_manhattan = load_and_process_data()

    # Display the map
    try:
        # Assuming geo_df_manhattan is returned from your data processing function
        fig = px.choropleth_mapbox(
            geo_df_manhattan,
            geojson=geo_df_manhattan.geometry,
            locations=geo_df_manhattan.index,
            color='total_amount_sum',
            color_continuous_scale="Viridis",
            range_color=(0, max(geo_df_manhattan.total_amount_sum)),
            mapbox_style="open-street-map",
            zoom=10,
            center={"lat": 40.7128, "lon": -74.0060},
            opacity=0.5,
            labels={'total_amount_sum': 'Total Amount'}
        )
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        st.plotly_chart(fig)

    except Exception as e:
        st.error(f"An error occurred: {e}")

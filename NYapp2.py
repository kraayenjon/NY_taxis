import streamlit as st
import pandas as pd
import geopandas as gpd
import pyproj
import plotly.express as px
import plotly.graph_objects as go
import requests
from io import BytesIO
import zipfile
import tempfile
import calendar

# Wrap your data processing in a function that Streamlit can cache to avoid reloading on every interaction.
@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def load_and_process_data():
    final_df = pd.DataFrame()

    # Process taxi data
    for i in range(1, 13):  # You can adjust the range for the months you want to include
        month = str(i).zfill(2)
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-{month}.parquet'
        selected_columns = ['tpep_pickup_datetime', 'passenger_count', 'trip_distance', 
                           'PULocationID','DOLocationID', 'fare_amount', 'tip_amount',
                           'total_amount']

        df = pd.read_parquet(url, columns=selected_columns)

        df['pickup_date'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.date

        df_taxi_grouped = df.groupby(['pickup_date', 'PULocationID', 'DOLocationID'], as_index=False).agg(
            total_trips=('pickup_date', 'count'),
            #passenger_count_mean=('passenger_count', 'mean'),
            passenger_count_sum=('passenger_count', 'sum'),
            #trip_distance_mean=('trip_distance', 'mean'),
            trip_distance_sum=('trip_distance', 'sum'),
            PULocationID=('PULocationID', 'first'),
            DOLocationID=('DOLocationID', 'first'),
            #payment_type=('payment_type', 'median'),
            fare_amount_mean=('fare_amount', 'mean'),
            fare_amount_sum=('fare_amount', 'sum'),
            #extra=('extra', 'sum'),
            #tip_amount_mean=('tip_amount', 'mean'),
            tip_amount_sum=('tip_amount', 'sum'),
            #tolls_amount=('tolls_amount', 'sum'),
           # total_amount_mean=('total_amount', 'mean'),
            total_amount_sum=('total_amount', 'sum'),
            #congestion_surcharge_mean=('congestion_surcharge', 'mean'),
            #congestion_surcharge_sum=('congestion_surcharge', 'sum'),
            #airport_fee_mean=('airport_fee', 'mean'),
            #airport_fee_sum=('airport_fee', 'sum')
        )

        final_df = pd.concat([final_df, df_taxi_grouped])

    # Filter for year 2022
    final_df['pickup_date'] = pd.to_datetime(final_df['pickup_date'])
    #final_df['month'] = final_df['pickup_date'].dt.month
    final_df_2022 = final_df[final_df['pickup_date'].dt.year >= 2022].copy()
    final_df_2022.loc[:, 'tip_ratio'] = final_df_2022['tip_amount_sum'] / final_df_2022['fare_amount_sum']

    # Download and process shapefile
    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip'
    response = requests.get(url)
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            z.extractall(temp_dir)  # Extract all files to the temporary directory
            shapefile = gpd.read_file(f"{temp_dir}/taxi_zones.shp")

    # filtering shapefile
    shapefile = shapefile.drop(columns=['OBJECTID', 'Shape_Leng', 'Shape_Area', 'LocationID', 'borough'], axis=1)
    
    # Transform CRS
    shapefile.to_crs(pyproj.CRS.from_epsg(4326), inplace=True)

    # Download borough lookup
    borough_lookup_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
    borough_lookup = pd.read_csv(borough_lookup_url,  usecols=['LocationID', 'Borough', 'Zone'])  

    # Merge datasets
    merged_data = pd.merge(final_df_2022, borough_lookup, left_on='PULocationID', right_on='LocationID')

    # dropping columns
    merged_data = merged_data.drop(columns= [
       'PULocationID', 'DOLocationID',
       'fare_amount_sum',  'tip_amount_sum','LocationID'], axis=1)

    # merging with shapefile to get the geometry
    merged_data_gps = pd.merge(merged_data, shapefile, left_on='Zone', right_on='zone')

    #filtering 
    merged_data_gps = merged_data_gps.drop(columns=['zone'] , axis=1)
    
    
    merged_data_gps_grouped = merged_data_gps.groupby('Zone')[['Zone', 'Borough', 'geometry', 'total_amount_sum', 'tip_ratio']].agg({
        'total_amount_sum': 'sum',
        'tip_ratio': 'mean',
        'geometry': 'first',
        'Borough': 'max'
    })

    # Create GeoDataFrame
    geo_df = gpd.GeoDataFrame(merged_data_gps_grouped, geometry=merged_data_gps_grouped.geometry)

    # Reset index for plotting with Plotly
    geo_df.reset_index(inplace=True)

    # Filter for Manhattan borough if necessary
    geo_df_manhattan = geo_df[geo_df['Borough'] == 'Manhattan']

    return geo_df_manhattan,merged_data



# Streamlit interface
st.title('NYC Taxi Rides Analysis')

# Define tabs
tabs = st.tabs(["Project Info", "EDA", "Map", "Graph"])

# Load your data
geo_df_manhattan,df = load_and_process_data()


# Project Info tab
with tabs[0]:
    st.title('Project Info')
    project_info = """
This project centers on New York City's yellow taxi ride data, sourced from the Taxi and Limousine Commission (TLC).   The dataset contains crucial details like pick-up/drop-off times, locations, distances, fares, and payment methods, providing a comprehensive foundation for in-depth data analysis.   

The background script aggregates monthly NYC yellow taxi ride data for the year 2022 returning daily summaries, categorizing by pick-up/drop-off locations, delivering both totals and averages across various metrics.
    """
    st.markdown(project_info)
    st.subheader("Contributors")
    st.markdown("[Encarna Sanchez](https://www.linkedin.com/in/encarna-s-18385414/) | [Jonathan Kraayenbrink](https://www.linkedin.com/in/jonathan-kraayenbrink/) | [Mauricio Cortes](https://www.linkedin.com/in/mauricio-cortes-b2478a3b/) | [Misaki Sagara](https://www.linkedin.com/in/misaki-sagara-640731110/)")
    
# EDA tab
with tabs[1]:
    st.title('Exploratory Data Analysis')

    total_rides = df['total_trips'].sum()
    total_fares = df['total_amount_sum'].sum()
    avg_fare  = df['fare_amount_mean'].mean()
    total_passengers = df['passenger_count_sum'].sum()

    total_rides_formatted = '{:.1f}M'.format(total_rides/1000000)
    total_fares_formatted = '{:.1f}M'.format(total_fares/1000000)
    avg_fare_formatted = '{:.2f}'.format(avg_fare)  
    total_passengers_formatted = '{:.1f}M'.format(total_passengers/1000000)
    
    
    col1, col2, col3, col4 = st.columns(4)    
    col1.metric("Total Rides", total_rides_formatted)
    col2.metric("Total Passengers", total_passengers_formatted)
    col3.metric("Total Fares", total_fares_formatted)
    col4.metric('Avg Fare', avg_fare_formatted)

    #grouping by month
    df['pickup_date'] = pd.to_datetime(df['pickup_date'])
    df['month'] = df['pickup_date'].dt.month
    
    monthly_trips = df.groupby('month', as_index=False).agg({'total_trips': 'sum'})
    
    monthly_trips['total_trips_M'] = monthly_trips['total_trips']/1000000

    
    st.header('Total Trips by Month (in millions)')
    st.area_chart(data=monthly_trips, x='month', y='total_trips_M', color=None, width=0, height=0, use_container_width=True)

    
    # Assuming 'month' is a column representing the month
    trips_per_month = df.groupby(['Zone', 'month'], as_index=False)['total_trips'].sum()

    # Group by 'zone' and aggregate total trips as a list
    trips_per_month_grouped = trips_per_month.groupby('Zone')['total_trips'].apply(list).reset_index()

    # Rename the column to 'total_trips_per_month'
    trips_per_month_grouped = trips_per_month_grouped.rename(columns={'total_trips': 'total_trips_per_month'})

    # Merge the list of total trips per month back into the original grouped DataFrame
    merged_data_grouped = pd.merge(df, trips_per_month_grouped, on='Zone', how='left')
    
    df_grouped = merged_data_grouped.groupby('Zone')[['Zone', 'Borough', 'total_amount_sum','passenger_count_sum', 'trip_distance_sum','total_trips_per_month']].agg({'total_amount_sum':'sum', 'passenger_count_sum':'sum', 'trip_distance_sum':'sum', 'Borough':'first', 'total_trips_per_month':'first'})

    st.subheader('Insights on Pick-Up Zones')
    st.write("Explore the table below for key insights into diverse pick-up zones in New York. Our analysis, organized by pick-up zones, reveals trends and various metrics.")
    
    st.data_editor(
    df_grouped,
    column_config={
        "total_trips_per_month": st.column_config.LineChartColumn(
            "Total Trips (per month)",
            width="medium",
            help="The total trips during 2022",
            y_min=0,
            y_max=100,
         ),
    },
    hide_index=False, 
    use_container_width=True)


# Map tab
with tabs[2]:
    st.title('Heatmap of NYC Taxis Rides')
    st.markdown("Explore this interactive map to discover the most frequented locations for taxi pickups in New York.")

    # Define a container for the map
    map_container = st.container()


        # Display the map
    try:
            
        fig = px.choropleth_mapbox(
                geo_df_manhattan,
                geojson=geo_df_manhattan.geometry,
                locations=geo_df_manhattan.index, 
                color='total_amount_sum',
                color_continuous_scale="Viridis",
                range_color=(0, max(geo_df_manhattan.total_amount_sum)),
                mapbox_style="open-street-map",
                zoom=10,
                center = {"lat": 40.7831, "lon":-73.9654}, #central park
                opacity=0.5,
                labels={'total_amount_sum': 'Total Amount', 'Zone':'Zone', 'tip_ratio':'Tip Ratio'}
            )
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

            # Display the map inside the map container
        with map_container:
                st.plotly_chart(fig)

    except Exception as e:
            st.error(f"An error occurred: {e}")


# Graph tab
with tabs[3]:
    st.title('Graph')
    st.write("Discover the relationships between pick-up and drop-off boroughs in the following Sankey diagram. Each flow represents the volume of taxi trips between specific borough pairs, providing a visual representation of the transportation dynamics across New York City.")
    
    # Define the CSV file path
    csv_file_path = 'https://raw.githubusercontent.com/kraayenjon/NY_taxis/main/graph_ny_taxis.csv'
    try:
        # Read data from the predefined CSV file
        df = pd.read_csv(csv_file_path)

        # Create a unique list of all boroughs
        all_boroughs = list(set(df['Borough_PU'].tolist() + df['Borough_DO'].tolist()))

# Specify colors for each borough based on a predefined color list
        color_list = [
    "rgba(31, 119, 180, 0.8)",
    "rgba(255, 127, 14, 0.8)",
    "rgba(44, 160, 44, 0.8)",
    "rgba(214, 39, 40, 0.8)",
    "rgba(148, 103, 189, 0.8)",
    "rgba(140, 86, 75, 0.8)",
    "rgba(227, 119, 194, 0.8)"
]

# Create a dictionary to map boroughs to colors
        color_mapping = {borough: color_list[i] for i, borough in enumerate(all_boroughs)}

# Create nodes using unique boroughs
        nodes = [{'pad': 15, 'thickness': 15, 'line': dict(color='black', width=0.5), 'label': borough, 'color': color_mapping[borough]}
         for borough in all_boroughs]

# Create links using the DataFrame
        links = []
        for index, row in df.iterrows():
            links.append({
        'source': all_boroughs.index(row['Borough_PU']),
        'target': len(all_boroughs) + all_boroughs.index(row['Borough_DO']),
        'value': row['total_trips'],
        'label': f"{row['Borough_PU']} to {row['Borough_DO']}",
        'color': color_mapping[row['Borough_PU']]
    })
        # Create the Sankey diagram
        fig = go.Figure(data=[go.Sankey(
    valueformat=".0f",
    valuesuffix=" trips",
    node=dict(
        pad=15,
        thickness=15,
        line=dict(color='black', width=0.5),
        label=[node['label'] for node in nodes],
        color=[node['color'] for node in nodes]
    ),
    link=dict(
        source=[link['source'] for link in links],
        target=[link['target'] for link in links],
        value=[link['value'] for link in links],
        label=[link['label'] for link in links],
        color=[link['color'] for link in links]
    )
)])

        # Update layout
        fig.update_layout(title_text="Total Trips between Boroughs", font_size=10)

        # Display the Sankey diagram
        st.plotly_chart(fig)

    except Exception as e:
        st.error(f"An error occurred: {e}")


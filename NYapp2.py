# Streamlit interface
st.title('NYC Taxi Data Visualization')

# Define a container for the title
title_container = st.container()

# Define a container for the map
map_container = st.container()

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

        # Display the map inside the map container
        with map_container:
            st.plotly_chart(fig)

    except Exception as e:
        st.error(f"An error occurred: {e}")

# Display the title outside the container
with title_container:
    st.title('NYC Taxi Data Visualization')

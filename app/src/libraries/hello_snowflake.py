# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title("Hello Snowflake - Streamlit Edition")
st.write(
   """The following data is from the ORDERS table in the application package.
   """
)

# Get the current credentials
session = get_active_session()

#  Create an example data frame
data_frame = session.sql("SELECT * FROM app_instance_schema.orders_v limit 100;")

# Execute the query and convert it into a Pandas data frame
queried_data = data_frame.to_pandas()

# Display the Pandas data frame as a Streamlit data frame.
st.dataframe(queried_data, use_container_width=True)
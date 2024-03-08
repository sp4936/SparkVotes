import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import simplejson as json
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh
import psycopg2
import urllib.parse


# Function to create a Kafka consumer
def create_kafka_consumer(topic_name):
    # Set up a Kafka consumer with specified topic and configurations
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer


# Function to fetch voting statistics from PostgreSQL database
@st.cache_data
def fetch_voting_stats():
    # Connect to PostgreSQL database
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # Fetch total number of voters
    cur.execute("""
        SELECT count(*) voters_count FROM voters
    """)
    voters_count = cur.fetchone()[0]

    # Fetch total number of candidates
    cur.execute("""
        SELECT count(*) candidates_count FROM candidates
    """)
    candidates_count = cur.fetchone()[0]

    return voters_count, candidates_count



# Function to fetch data from Kafka
def fetch_data_from_kafka(consumer):
    # Poll Kafka consumer for messages within a timeout period
    messages = consumer.poll(timeout_ms=1000)
    data = []

    # Extract data from received messages
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data





# Function to split a dataframe into chunks for pagination
@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    df = [input_df.loc[i: i + rows - 1, :] for i in range(0, len(input_df), rows)]
    return df





# Function to update data displayed on the dashboard
def update_data():
    # Placeholder to display last refresh time
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Fetch voting statistics
    voters_count, candidates_count = fetch_voting_stats()


    # Fetch data from Kafka on aggregated votes per candidate
    consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    data = fetch_data_from_kafka(consumer)
    results = pd.DataFrame(data)

    # Identify the leading candidate
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    # Display leading candidate information
    st.markdown("""---""")
    st.header('Leading Candidate')
    col1, col2 = st.columns(2)
    
    #------------------------------------------------------------------------------update----------------
    with col1:
        decoded_url = urllib.parse.unquote_plus(leading_candidate['photo_url'])
        #st.image(leading_candidate['photo_url'], width=200)
        st.image(decoded_url,width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader("Total Vote: {}".format(leading_candidate['total_votes']))

    # Display statistics and visualizations
    st.markdown("""---""")
    st.header('Statistics')
    results = results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']]
    results = results.reset_index(drop=True)
    col1, col2 = st.columns(2)



    # Display table with candidate statistics
    st.table(results)


    # Update the last refresh time
    st.session_state['last_update'] = time.time()
    
    
    

# Sidebar layout
def sidebar():
    # Initialize last update time if not present in session state
    if st.session_state.get('last_update') is None:
        st.session_state['last_update'] = time.time()

    # Slider to control refresh interval
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    # Button to manually refresh data
    if st.sidebar.button('Refresh Data'):
        update_data()

# Title of the Streamlit dashboard
st.title('Real-time Election Dashboard')
topic_name = 'aggregated_votes_per_candidate'

# Display sidebar
sidebar()


#--------------------------------------------------Initialise------------------------------------------------

# Update and display data on the dashboard
update_data()



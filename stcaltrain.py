import requests
import pandas as pd
import streamlit as st
import pytz
import datetime
from streamlit_extras.badges import badge
from functions.ct_functions import (
    get_schedule,
    assign_train_type,
    is_northbound,
)
from geopy.distance import geodesic
import json

st.set_page_config(page_title="Caltrain Platform", page_icon="🚆", layout="wide")

@st.cache_resource(ttl="60s")
def ping_train() -> dict:
    # URL for the 511 Transit API
    url = f"https://api.511.org/transit/VehicleMonitoring?api_key={st.secrets['511_key']}&agency=CT"

    # Making the request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Decode using utf-8-sig to handle UTF-8 BOM
        decoded_content = response.content.decode("utf-8-sig")

        # Parse the decoded content into JSON
        data = json.loads(decoded_content)
    else:
        return False

    if data["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"].get("VehicleActivity") is None:
        return False
    else:
        api_live_responsetime = data["Siri"]["ServiceDelivery"]["ResponseTimestamp"]
        api_live_responsetime = datetime.datetime.strptime(api_live_responsetime, '%Y-%m-%dT%H:%M:%SZ') \
            .replace(tzinfo=pytz.utc) \
            .astimezone(pytz.timezone('US/Pacific')) \
            .strftime('%I:%M %p')
        return data

API_RESPONSE_DATA = ping_train()

def create_caltrain_dfs(data: dict) -> pd.DataFrame:
    """Ping 511 API and reformat the data"""
    trains = []

    for train in data["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"]["VehicleActivity"]:
        train_obj = train["MonitoredVehicleJourney"]

        if train_obj.get("OnwardCalls") is None:
            continue

        next_stop_df = pd.DataFrame(
            [
                [
                    train_obj["MonitoredCall"]["StopPointName"],
                    train_obj["MonitoredCall"]["StopPointRef"],
                    train_obj["MonitoredCall"]["AimedArrivalTime"],
                    train_obj["MonitoredCall"]["ExpectedArrivalTime"],
                    train_obj["MonitoredCall"]["AimedDepartureTime"],
                ]
            ],
            columns=["stop_name", "stop_id", "aimed_arrival_time", "expected_arrival_time","AimedDepartureTime"],
        )
        destinations_df = pd.DataFrame(
            [
                [
                    stop["StopPointName"],
                    stop["StopPointRef"],
                    stop["AimedArrivalTime"],
                    stop["ExpectedArrivalTime"],
                    stop["AimedDepartureTime"]
                ]
                for stop in train_obj["OnwardCalls"]["OnwardCall"]
            ],
            columns=["stop_name", "stop_id", "aimed_arrival_time", "expected_arrival_time","AimedDepartureTime"],
        )
        destinations_df = pd.concat([next_stop_df, destinations_df])
        destinations_df["id"] = train_obj["VehicleRef"]
        destinations_df["origin"] = train_obj["OriginName"]
        destinations_df["origin_id"] = train_obj["OriginRef"]
        destinations_df["direction"] = train_obj["DirectionRef"] + "B"
        destinations_df["line_type"] = train_obj["PublishedLineName"]
        destinations_df["destination"] = train_obj["DestinationName"]
        destinations_df["train_longitude"] = train_obj["VehicleLocation"]["Longitude"]
        destinations_df["train_latitude"] = train_obj["VehicleLocation"]["Latitude"]

        destinations_df = destinations_df[
            [
                "id",
                "origin",
                "origin_id",
                "direction",
                "line_type",
                "destination",
                "stop_name",
                "stop_id",
                "aimed_arrival_time",
                "expected_arrival_time",
                "AimedDepartureTime",
                "train_longitude",
                "train_latitude",
            ]
        ]
        destinations_df["stops_away"] = destinations_df.index
        trains.append(destinations_df)
    trains_df = pd.concat(trains)

    # Change to the correct types
    trains_df["aimed_arrival_time"] = pd.to_datetime(trains_df["aimed_arrival_time"])
    trains_df["expected_arrival_time"] = pd.to_datetime(trains_df["expected_arrival_time"])
    trains_df["AimedDepartureTime"] = pd.to_datetime(trains_df["AimedDepartureTime"])
    trains_df["train_longitude"] = trains_df["train_longitude"].astype(float)
    trains_df["train_latitude"] = trains_df["train_latitude"].astype(float)
    trains_df["stop_id"] = trains_df["stop_id"].astype(float)
    trains_df["origin_id"] = trains_df["origin_id"].astype(float)

    # Import the stop_ids and add their coordinates to the dataframe
    stop_ids = pd.read_csv("stop_ids.csv")

    # Combine the stop IDs
    sb_trains_df = pd.merge(trains_df, stop_ids, left_on="stop_id", right_on="stop1", how="inner")
    nb_trains_df = pd.merge(trains_df, stop_ids, left_on="stop_id", right_on="stop2", how="inner")
    trains_df = pd.concat([sb_trains_df, nb_trains_df])

    trains_df["distance"] = trains_df.apply(
        lambda x: geodesic((x["train_latitude"], x["train_longitude"]), (x["lat"], x["lon"])).miles,
        axis=1,
    )
    trains_df["distance"] = trains_df["distance"].round(1).astype("str") + " mi"
    trains_df["Departure Time"] = trains_df["expected_arrival_time"]
    trains_df["Scheduled Time"] = trains_df["aimed_arrival_time"]
    trains_df["Current Time"] = datetime.datetime.now(pytz.timezone("UTC"))
    trains_df["ETA"] = trains_df["Departure Time"] - trains_df["Current Time"]
    trains_df["ScheduledETA"] = trains_df["Scheduled Time"] - trains_df["Current Time"]
    trains_df["AimedDepartureTimeETA"] = trains_df["AimedDepartureTime"] - trains_df["Current Time"]
    trains_df["Train #"] = trains_df["id"]
    trains_df["Direction"] = trains_df["direction"]
    return trains_df

def clean_up_df(data: pd.DataFrame) -> pd.DataFrame:
    """Clean up the dataframe for display"""
    # Filter for desired columns
    #data = data[["Train #", "Departure Time", "Scheduled Time", "ETA", "distance", "stops_away"]] #Train Type
    data["ETA"] = data["ETA"].apply(lambda x: int(x.total_seconds() / 60))
    data["ETA"] = data["ETA"].astype("str") + " min"
    data["ScheduledETA"] = data["ScheduledETA"].apply(lambda x: int(x.total_seconds() / 60))
    data["ScheduledETA"] = data["ScheduledETA"].astype("str") + " min"
    data["AimedDepartureTimeETA"] = data["AimedDepartureTimeETA"].apply(lambda x: int(x.total_seconds() / 60))
    data["AimedDepartureTimeETA"] = data["AimedDepartureTimeETA"].astype("str") + " min"

    # data["ETA"] = data["ETA"].apply(lambda x: f"{int(x // 60)} hr {int(x % 60)} min")
    data["API Time"] = data.apply(lambda row: f"{row['Departure Time']} // Train in {row['ETA']}", axis=1)
    data["Scheduled Time"] = data.apply(lambda row: f"{row['Departure Time']} // Train in {row['ScheduledETA']}", axis=1)
    data["AimedDepartureTime"] = data.apply(lambda row: f"{row['AimedDepartureTime']} // Train in {row['AimedDepartureTimeETA']}", axis=1)


    #Select columns desired
    data = data[["Train #", "API Time", "AimedDepartureTime", "distance", "stopsaway2"]] 

    # Rename the columns
    data.columns = [
        "Train #",
        #"Train Type",
        "API Arrival",
        "Scheduled Depature",
        #"Scheduled Arrival",
        #"ETA",
        "Distance to Station",
        "Stops Away",
        #"API Time"
    ]

    data = data.T
    data.columns = data.iloc[0]
    data = data.drop(data.index[0])

    return data

if API_RESPONSE_DATA is not False:
    caltrain_data = create_caltrain_dfs(API_RESPONSE_DATA)
else:
    caltrain_data = False

# get current time in US/Pacific and format as HH:MM AM/PM
pacific = pytz.timezone("US/Pacific")
current_time2 = datetime.datetime.now(pacific)
current_time = current_time2.strftime("%I:%M %p")

caltrain_stations = pd.read_csv("stop_ids.csv")
col1, col2 = st.columns([2, 1])

with st.expander('Change Stations and Schedule Type', expanded=False):
    col1, col2 = st.columns(2)  # Define two columns for layout

    # Select origin station
    chosen_station = col1.selectbox("Choose Origin Station", caltrain_stations["stopname"], index=8)  # Index 8 is San Mateo

    # Select destination station
    chosen_destination = col1.selectbox(
        "Choose Destination Station", ["--"] + caltrain_stations["stopname"].tolist(), index=0
    )

api_working = True if type(caltrain_data) == pd.DataFrame else False
scheduled = False

# Allow switch between live data and scheduled data
if api_working:
    display = col1.radio(
        "Show trains",
        ["Live", "Scheduled"],
        horizontal=True,
        help="Live shows only trains that have already left the station",
    )
    schedule_chosen = True
else:
    display = col1.radio(
        "Show trains",
        ["Live", "Scheduled"],
        horizontal=True,
        help="Live shows only trains that have already left the station",
        index=1,
        disabled=True,
    )
    schedule_chosen = False

col1, col2 = st.columns([2, 1])

if display == "Scheduled":
    scheduled = True
    if schedule_chosen:
        col1.info("📆 Pulling the current schedule from the Caltrain website...")
    else:
        col1.error(
            "❌ Caltrain Live Map API is currently down. Pulling the current schedule from the Caltrain website instead..."
        )
    # If the chosen destination is before the chosen station, then the direction is southbound
    if chosen_destination != "--" and chosen_destination != chosen_station:
        if is_northbound(chosen_station, chosen_destination):
            caltrain_data = get_schedule("northbound", chosen_station, chosen_destination)
        else:
            caltrain_data = get_schedule("southbound", chosen_station, chosen_destination)

    else:
        caltrain_data = pd.concat(
            [
                get_schedule("northbound", chosen_station, chosen_destination),
                get_schedule("southbound", chosen_station, chosen_destination),
            ]
        )

    # Sort by ETA
    caltrain_data = caltrain_data.sort_values(by=["ETA"])
    caltrain_data["Train #"] = caltrain_data["Train #"].map(lambda c: f"{assign_train_type(c)}-{c}")
    caltrain_data_nb = caltrain_data.query("Direction == 'NB'").drop("Direction", axis=1)
    caltrain_data_sb = (
        caltrain_data.query("Direction == 'SB'").drop("Direction", axis=1).reset_index(drop=True)
    )
    # Reset the index to 1, 2, 3.
    caltrain_data_nb.index = caltrain_data_nb.index + 1
    caltrain_data_sb.index = caltrain_data_sb.index + 1

    col1, col2 = st.columns([2, 1])

    # Display the dataframes split by Train #, Scheduled Departure, Current Stop and the other columns
    col1.subheader(f"Northbound Trains - {current_time}")
    nb_data = caltrain_data_nb.T
    nb_data.columns = nb_data.iloc[0]
    nb_data = nb_data.drop(nb_data.index[0])
    col1.dataframe(nb_data, use_container_width=True)

    col1.subheader(f"Southbound Trains - {current_time}")
    sb_data = caltrain_data_sb.T
    sb_data.columns = sb_data.iloc[0]
    sb_data = sb_data.drop(sb_data.index[0])
    col1.dataframe(sb_data, use_container_width=True)

else:
    api_live_responsetime = API_RESPONSE_DATA["Siri"]["ServiceDelivery"]["ResponseTimestamp"]
    api_live_responsetime = datetime.datetime.strptime(api_live_responsetime, '%Y-%m-%dT%H:%M:%SZ') \
        .replace(tzinfo=pytz.utc) \
        .astimezone(pytz.timezone('US/Pacific')) \
        .strftime('%I:%M %p')
    col1.info(f"✅ Caltrain API is up and running 🚂 Time via API is {api_live_responsetime}")
    caltrain_data["Train Type"] = caltrain_data["Train #"].apply(lambda x: assign_train_type(x))
    caltrain_data["Train #"] = caltrain_data["Train #"].map(lambda c: f"{assign_train_type(c)}-{c}")

    caltrain_data["Departure Time"] = (
        pd.to_datetime(caltrain_data["Departure Time"])
        .dt.tz_convert("US/Pacific")
        .dt.strftime("%I:%M %p")
    )
    caltrain_data["Scheduled Time"] = (
        pd.to_datetime(caltrain_data["Scheduled Time"])
        .dt.tz_convert("US/Pacific")
        .dt.strftime("%I:%M %p")
    )
    caltrain_data["AimedDepartureTime"] = (
        pd.to_datetime(caltrain_data["AimedDepartureTime"])
        .dt.tz_convert("US/Pacific")
        .dt.strftime("%I:%M %p")
    )

    # # find the index of the min aimed departure per id
    idx = caltrain_data.sort_values(['id','AimedDepartureTime','stop_name']).groupby('id').head(1).index

    # # create a mapping id -> stop_name at that min time
    first_stops = caltrain_data.loc[idx, ['id', 'stop_name']].set_index('id')['stop_name']
    first_stops = caltrain_data.loc[idx, ['id', 'stop_name']].drop_duplicates(subset='id', keep='first').set_index('id')['stop_name']
    first_stops
    #first_stops['stop_name'] = first_stops['stop_name'].astype(str).str.replace(r'\s*Caltrain Station\s+(Northbound|Southbound)\s*$', '', regex=True, case=False).str.strip()

    # # map back to the original df
    caltrain_data['stopsaway2'] = caltrain_data['id'].map(first_stops)
    caltrain_data['stopsaway2'] = caltrain_data['stopsaway2'].astype(str).str.replace(r'\s*Caltrain Station\s+(Northbound|Southbound)\s*$', '', regex=True, case=False).str.strip()
    caltrain_data["stopsaway2"] = caltrain_data["stops_away"].astype(str) + " // " + caltrain_data["stopsaway2"]

    # Filter for destinations
    valid_destinations = [
        "San Francisco",
        "Tamien",
        "San Jose Diridon",
    ]
    if chosen_destination != "--" and chosen_destination not in valid_destinations:
        destinations = caltrain_data[caltrain_data["stopname"] == chosen_destination]["id"]
        caltrain_data = caltrain_data[caltrain_data["id"].isin(destinations)]

    # Remove NB or SB depending on the direction
    if chosen_destination != "--" and chosen_destination != chosen_station:
        if is_northbound(chosen_station, chosen_destination):
            caltrain_data = caltrain_data.query("direction == 'NB'")
        else:
            caltrain_data = caltrain_data.query("direction == 'SB'")

    # # Display the dataframes split by Train #, Scheduled Departure, Current Stop and the other columns

    # Northbound Trains
    col1.subheader(f"Northbound Trains - {current_time}")
    nb_trains = caltrain_data.query("Direction == 'NB'").drop("Direction", axis=1)
    nb_trains = nb_trains.sort_values(by=["ETA"])
    nb_trains = nb_trains[nb_trains["stopname"] == chosen_station]

    if nb_trains.empty:
        col1.info("No trains")
    else:
        col1.dataframe(clean_up_df(nb_trains), use_container_width=True)

    # Southbound trains
    col1.subheader(f"Southbound Trains - {current_time}")
    sb_data = caltrain_data.query("direction == 'SB'").drop("direction", axis=1)
    sb_data = sb_data.sort_values(by=["ETA"])
    sb_data = sb_data[sb_data["stopname"] == chosen_station]

    if len(sb_data) == 0:
        col1.info("No trains")
    else:
        col1.dataframe(clean_up_df(sb_data), use_container_width=True)

# Definitions
col1.markdown("---")
col1.subheader("Definitions")
col1.markdown(
    """
1. **API Arrival** - Expected Arrival Time of Train based on 511 API
2. **Scheduled Depature** - Aimed Depature from Station (Should match the schedule of the train)
3. **Distance to Station** - The distance from the train to the **Origin** station.
4. **Stops Away** - The number of stops until the train reaches the **Origin** station.
"""
)

col1.subheader("About")
col1.markdown(
    """
- This app pulls _real-time_ data from the [511 API](https://511.org/open-data). It was created to solve the issue of arriving at the Caltrain station while the train is behind schedule. This app will tell you when the next train is leaving, and about how long it will take to arrive at the station.

- **Note:** If the caltrain API is down or there aren't any trains moving, then the app will pull the current schedule from the Caltrain website instead.

- Fork from [caltrain/tyler-simons](https://github.com/tyler-simons/caltrain/tree/main) with substaintial changes
"""
)

col1, col2 = st.columns([2, 1])
with col1:
    st.markdown("---")

col1, col2 = st.columns([1, 1])

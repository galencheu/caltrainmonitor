import requests
import pandas as pd
import numpy as np
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
    url = f"https://api.511.org/transit/VehicleMonitoring?api_key={st.secrets['511_key']}&agency=CT"
    response = requests.get(url)
    if response.status_code != 200:
        return False

    decoded_content = response.content.decode("utf-8-sig")
    data = json.loads(decoded_content)

    if data["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"].get("VehicleActivity") is None:
        return False
    return data

API_RESPONSE_DATA = ping_train()

def create_caltrain_dfs(data: dict) -> pd.DataFrame:
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
            columns=["stop_name", "stop_id", "aimed_arrival_time",
                     "expected_arrival_time", "AimedDepartureTime"],
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
            columns=["stop_name", "stop_id", "aimed_arrival_time",
                     "expected_arrival_time", "AimedDepartureTime"],
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
        destinations_df["stops_away"] = destinations_df.index

        trains.append(destinations_df)

    trains_df = pd.concat(trains)

    trains_df["aimed_arrival_time"] = pd.to_datetime(trains_df["aimed_arrival_time"])
    trains_df["expected_arrival_time"] = pd.to_datetime(trains_df["expected_arrival_time"])
    trains_df["AimedDepartureTime"] = pd.to_datetime(trains_df["AimedDepartureTime"])
    trains_df["train_longitude"] = trains_df["train_longitude"].astype(float)
    trains_df["train_latitude"] = trains_df["train_latitude"].astype(float)
    trains_df["stop_id"] = trains_df["stop_id"].astype(float)
    trains_df["origin_id"] = trains_df["origin_id"].astype(float)

    stop_ids = pd.read_csv("stop_ids.csv")

    sb_trains_df = pd.merge(trains_df, stop_ids, left_on="stop_id",
                            right_on="stop1", how="inner")
    nb_trains_df = pd.merge(trains_df, stop_ids, left_on="stop_id",
                            right_on="stop2", how="inner")
    trains_df = pd.concat([sb_trains_df, nb_trains_df])

    trains_df["distance"] = trains_df.apply(
        lambda x: geodesic((x["train_latitude"], x["train_longitude"]),
                           (x["lat"], x["lon"])).miles,
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
    data["ETA"] = data["ETA"].apply(lambda x: int(x.total_seconds() / 60))
    data["ETA_COMPARE"] = data["ETA"]
    data["ETA"] = data["ETA"].astype(str) + " min"

    data["ScheduledETA"] = data["ScheduledETA"].apply(lambda x: int(x.total_seconds() / 60))
    data["ScheduledETA_COMPARE"] = data["ScheduledETA"]
    data["delayed"] = np.where(data["ETA_COMPARE"] > data["ScheduledETA_COMPARE"] + 1,
                               '!!!!!--  I SLOW  --!!!!!', '')
    data["ScheduledETA"] = data["ScheduledETA"].astype(str) + " min"

    data["AimedDepartureTimeETA"] = data["AimedDepartureTimeETA"].apply(
        lambda x: int(x.total_seconds() / 60))
    data["AimedDepartureTimeETA"] = data["AimedDepartureTimeETA"].astype(str) + " min"

    data["API Time"] = data.apply(
        lambda row: f"{row['Departure Time']} // Train in {row['ETA']}", axis=1)
    data["Scheduled Time"] = data.apply(
        lambda row: f"{row['Departure Time']} // Train in {row['ScheduledETA']}", axis=1)
    data["AimedDepartureTime"] = data.apply(
        lambda row: f"{row['AimedDepartureTime']} // Train in {row['AimedDepartureTimeETA']}", axis=1)

    data = data[["Train #", "API Time", "AimedDepartureTime", "delayed", "stopsaway2"]]
    data.columns = ["Train #", "API Arrival", "Scheduled Depature", "Delayed", "Stops Away"]

    data = data.T
    data.columns = data.iloc[0]
    data = data.drop(data.index[0])
    return data


if API_RESPONSE_DATA is not False:
    caltrain_data = create_caltrain_dfs(API_RESPONSE_DATA)
else:
    caltrain_data = False


pacific = pytz.timezone("US/Pacific")
current_time = datetime.datetime.now(pacific).strftime("%I:%M %p")

caltrain_stations = pd.read_csv("stop_ids.csv")


# ----------------------------
#  SETTINGS UI (NO COLUMNS)
# ----------------------------
# with st.expander("Change Stations and Schedule Type", expanded=False):
#     chosen_station = st.selectbox("Choose Origin Station",
#                                   caltrain_stations["stopname"], index=8)

#     chosen_destination = st.selectbox("Choose Destination Station",
#                                       ["--"] + caltrain_stations["stopname"].tolist(),
#                                       index=0)

with st.sidebar:
    st.header("Settings")
    chosen_station = st.selectbox("Choose Origin Station",
                                  caltrain_stations["stopname"], index=8)

    chosen_destination = st.selectbox("Choose Destination Station",
                                      ["--"] + caltrain_stations["stopname"].tolist(),
                                      index=0)

    api_working = isinstance(caltrain_data, pd.DataFrame)
    scheduled = False

    if api_working:
        display = st.radio(
            "Show trains",
            ["Live", "Scheduled"],
            horizontal=True,
            help="Live shows only trains that have already left the station",
        )
    else:
        display = st.radio(
            "Show trains",
            ["Live", "Scheduled"],
            horizontal=True,
            index=1,
            disabled=True
        )

# -------------------------------------
#  SCHEDULE VIEW
# -------------------------------------
if display == "Scheduled":
    st.warning("📆 Pulling the current schedule from the Caltrain website...")

    if chosen_destination != "--" and chosen_destination != chosen_station:
        if is_northbound(chosen_station, chosen_destination):
            caltrain_data = get_schedule("northbound", chosen_station, chosen_destination)
        else:
            caltrain_data = get_schedule("southbound", chosen_station, chosen_destination)
    else:
        caltrain_data = pd.concat([
            get_schedule("northbound", chosen_station, chosen_destination),
            get_schedule("southbound", chosen_station, chosen_destination)
        ])

    caltrain_data = caltrain_data.sort_values(by=["Scheduled"])
    caltrain_data["Train"] = caltrain_data["Train"].map(
        lambda c: f"{assign_train_type(c)}-{c}")

    # NORTHBOUND
    st.subheader(f"Northbound Trains - {current_time}")
    nb_data = caltrain_data[caltrain_data["label"].str.contains('Northbound', case=False)].copy()
    nb_data.sort_values(["time_clean"], inplace=True)
    nb_data = nb_data[nb_data["station"] == chosen_station].T #nb_data.T
    nb_data.columns = nb_data.iloc[0]
    st.dataframe(nb_data.drop(nb_data.index[[0, 2]]), use_container_width=True)

    # SOUTHBOUND
    st.subheader(f"Southbound Trains - {current_time}")
    sb_data = caltrain_data[caltrain_data["label"].str.contains('Southbound', case=False)].copy()
    sb_data.sort_values(["time_clean"], inplace=True)
    sb_data = sb_data[sb_data["station"] == chosen_station].T #sb_data.T
    sb_data.columns = sb_data.iloc[0]
    st.dataframe(sb_data.drop(sb_data.index[[0, 2]]), use_container_width=True)

# -------------------------------------
#  LIVE VIEW
# -------------------------------------
else:
    api_live_responsetime = API_RESPONSE_DATA["Siri"]["ServiceDelivery"]["ResponseTimestamp"]
    api_live_responsetime_dt = datetime.datetime.strptime(api_live_responsetime, '%Y-%m-%dT%H:%M:%SZ') \
        .replace(tzinfo=pytz.utc) \
        .astimezone(pytz.timezone('US/Pacific'))
    api_live_responsetime =  api_live_responsetime_dt.strftime('%I:%M %p')


    pacific = pytz.timezone("US/Pacific")
    current_time_dt = datetime.datetime.now(pacific)
    api_hi_time = current_time_dt + datetime.timedelta(seconds=90)
    api_lo_time = current_time_dt - datetime.timedelta(seconds=90)

    #If API time and Actual Time go out of Sync
    if api_live_responsetime_dt < api_hi_time and api_live_responsetime_dt > api_lo_time:
        st.info(f"✅ Caltrain API is up 🚂 (API Time: {api_live_responsetime})")
    else:
        st.error(f"❌ Caltrain API Time is off by {api_live_responsetime_dt - current_time_dt} minutes")

    caltrain_data["Train Type"] = caltrain_data["Train #"].apply(assign_train_type)
    caltrain_data["Train #"] = caltrain_data["Train #"].map(
        lambda c: f"{assign_train_type(c)}-{c}")

    caltrain_data["Departure Time"] = pd.to_datetime(
        caltrain_data["Departure Time"]).dt.tz_convert("US/Pacific").dt.strftime("%I:%M %p")
    caltrain_data["Scheduled Time"] = pd.to_datetime(
        caltrain_data["Scheduled Time"]).dt.tz_convert("US/Pacific").dt.strftime("%I:%M %p")
    caltrain_data["AimedDepartureTime"] = pd.to_datetime(
        caltrain_data["AimedDepartureTime"]).dt.tz_convert("US/Pacific").dt.strftime("%I:%M %p")

    caltrain_data = caltrain_data.reset_index(drop=True)

    idx = (caltrain_data
           .sort_values(["id", "aimed_arrival_time"])
           .groupby("id")
           .head(1)
           .index)

    first_stops = caltrain_data.loc[idx, ["id", "stop_name"]].drop_duplicates().set_index("id")["stop_name"]
    caltrain_data["stopsaway2"] = caltrain_data["id"].map(first_stops)
    caltrain_data["stopsaway2"] = caltrain_data["stopsaway2"].astype(str).str.replace(
        r'\s*Caltrain Station\s+(Northbound|Southbound)\s*$',
        '', regex=True
    ).str.strip()
    caltrain_data["stopsaway2"] = (
        caltrain_data["stops_away"].astype(str)
        + " // " + caltrain_data["stopsaway2"]
        + " // " + caltrain_data["distance"]
    )

    valid_destinations = ["San Francisco", "Tamien", "San Jose Diridon"]

    if chosen_destination not in ["--"] + valid_destinations:
        dest_ids = caltrain_data[caltrain_data["stopname"] == chosen_destination]["id"]
        caltrain_data = caltrain_data[caltrain_data["id"].isin(dest_ids)]

    if chosen_destination != "--" and chosen_destination != chosen_station:
        if is_northbound(chosen_station, chosen_destination):
            caltrain_data = caltrain_data.query("direction == 'NB'")
        else:
            caltrain_data = caltrain_data.query("direction == 'SB'")

    # NORTHBOUND
    st.subheader(f"Northbound Trains - {current_time}")
    nb_trains = caltrain_data.query("Direction == 'NB'").drop("Direction", axis=1)
    nb_trains = nb_trains[nb_trains["stopname"] == chosen_station].sort_values("ETA")

    if nb_trains.empty:
        st.info("No trains northbound.")
    else:
        st.dataframe(clean_up_df(nb_trains), use_container_width=True)

    # SOUTHBOUND
    st.subheader(f"Southbound Trains - {current_time}")
    sb_trains = caltrain_data.query("direction == 'SB'").drop("direction", axis=1)
    sb_trains = sb_trains[sb_trains["stopname"] == chosen_station].sort_values("ETA")

    if sb_trains.empty:
        st.info("No trains southbound.")
    else:
        st.dataframe(clean_up_df(sb_trains), use_container_width=True)


# -------------------------
# DEFINITIONS & ABOUT
# -------------------------
st.markdown("---")
st.subheader("Definitions")
st.markdown("""
1. **API Arrival** — Expected arrival time from the 511 API  
2. **Scheduled Depature** — Aimed departure from the schedule  
3. **Delayed** — Triggered when API ETA is behind schedule  
4. **Stops Away** — Stops until origin // nearest major station // distance  
""")

st.subheader("About")
st.markdown("""
- This app provides **real-time Caltrain status** using the 511 API.  
- If real-time data is unavailable, the app automatically switches to the live Caltrain schedule.  
- Forked from the original project by Tyler Simons with major enhancements.
""")

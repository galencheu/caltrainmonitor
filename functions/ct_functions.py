import requests
import pandas as pd
import streamlit as st
import pytz
import datetime
from streamlit_extras.badges import badge
from bs4 import BeautifulSoup
import re

def to_time(seconds):
    delta = datetime.timedelta(seconds=seconds)
    return (datetime.datetime.utcfromtimestamp(0) + delta).strftime("%H:%M")


def create_train_df(train):
    # Create a dataframe for the train where each stop has arrival and departure times
    stops_df = pd.json_normalize(train["TripUpdate"]["StopTimeUpdate"])
    # If Arrival.Time is not in the columns, return None
    if "Arrival.Time" not in stops_df.columns:
        return None
    stops_df["train_num"] = train["TripUpdate"]["Trip"]["TripId"]
    stops_df["direction"] = train["TripUpdate"]["Trip"]["DirectionId"]
    # Fill in missing Arrival.Time values with Departure.Time
    stops_df["Arrival.Time"] = stops_df["Arrival.Time"].fillna(stops_df["Departure.Time"])

    # Convert the arrival and departure times to datetime objects with pacfic timezone in the format strftime ("%-I:%M:%S %p")
    tz = pytz.timezone("US/Pacific")
    stops_df["arrival_time"] = stops_df["Arrival.Time"].apply(
        lambda x: datetime.datetime.fromtimestamp(x, tz).strftime("%I:%M %p")
    )
    stops_df["departure_time"] = stops_df["Departure.Time"].apply(
        lambda x: datetime.datetime.fromtimestamp(x, tz).strftime("%I:%M %p")
    )
    # drop where Arrival.Time is null
    # Drop the arrival and departure times in seconds
    stops_df.drop(["Arrival.Time", "Departure.Time"], axis=1, inplace=True)
    return stops_df


# Add train type where locals are 100s and 200s, limited is 300s through 600s and bullets are 700s
# 1XX is Local, 4XX is Limited, 5XX is Express, 6XX is Weekend Local
def assign_train_type(x):
    if x.startswith("1"):
        return "Local"
    if x.startswith("4"):
        return "Limited"
    if x.startswith("5"):
        return "Express"
    if x.startswith("6"):
        return "Weekend"
    if x.startswith("1"):
        return "SouthCounty"
    else:
        return "Contact Dev"


def build_caltrain_df(stopname):
    # tz = pytz.timezone("US/Pacific")

    # read in the station list and get the matching urlname
    stops_df = pd.read_csv("stop_ids.csv")

    # Get the urlname for the chosen station
    chosen_station_urlname = stops_df[stops_df["stopname"] == stopname]["urlname"].tolist()[0]

    curr_timestamp = datetime.datetime.utcnow().strftime("%s")

    curr_timestamp = int(curr_timestamp) * 1000
    # ping_url = f"https://www.caltrain.com/files/rt/vehiclepositions/CT.json?time={curr_timestamp}"
    ping_url = f"https://www.caltrain.com/gtfs/stops/{chosen_station_urlname}/predictions"
    real_time_trains = requests.get(ping_url).json()

    # Assuming `json_data` is the JSON object you provided
    json_data = real_time_trains

    # Initialize a list to collect data
    data = []
    lat_lons = []
    # Loop through the 'data' part of the JSON
    for entry in json_data["data"]:
        # Each 'entry' corresponds to a 'stop' and its 'predictions'
        lat_lon = entry.get("stop", {}).get("field_location", {})[0].get("latlon")
        lat_lons.append(lat_lon)

        stop_predictions = entry.get("predictions", [])
        for prediction in stop_predictions:
            trip_update = prediction.get("TripUpdate", {})
            trip = trip_update.get("Trip", {})
            stop_time_updates = trip_update.get("StopTimeUpdate", [])

            for stop_time_update in stop_time_updates:
                # Extract the required information
                train_number = trip.get("TripId")
                route_id = trip.get("RouteId")
                stop_id = stop_time_update.get("StopId")

                # Convert the arrival and departure timestamps to human-readable format
                arrival_timestamp = stop_time_update.get("Arrival", {}).get("Time")
                departure_timestamp = stop_time_update.get("Departure", {}).get("Time")

                eta = (
                    datetime.datetime.fromtimestamp(arrival_timestamp).strftime("%Y-%m-%d %H:%M:%S")
                    if arrival_timestamp
                    else None
                )
                departure = (
                    datetime.datetime.fromtimestamp(departure_timestamp).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    if departure_timestamp
                    else None
                )

                # Get the route type from the 'meta' part using the route_id
                route_info = json_data["meta"]["routes"].get(route_id, {})
                train_type = next(
                    (item.get("value") for item in route_info.get("title", [])), "Unknown"
                )

                # Append the collected data to the list
                data.append(
                    {
                        "Train Number": train_number,
                        "Train Type": train_type,
                        "ETA": eta,
                        "Departure": departure,
                        "Route ID": route_id,
                        "Stop ID": stop_id,
                    }
                )
    # Create a DataFrame from the collected data
    df = pd.DataFrame(data)
    if df.empty:
        return pd.DataFrame()

    # If ETA is null, it means the train is there already, use the departure time instead
    df["ETA"] = df["ETA"].fillna(df["Departure"])

    pacific = pytz.timezone("US/Pacific")
    current_time2 = datetime.datetime.now(pacific)

    lt = (
        df["ETA"]
        .apply(
            lambda x: (datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S") - current_time2)
        )
        .to_list()
    )
    lt = [i.total_seconds() for i in lt]
    # Change to hours:minutes
    lt = [str(datetime.timedelta(seconds=i))[0:4] for i in lt]
    # Remove negatives
    lt = [i if i[0] != "-" else "-" for i in lt]
    df["departs_in"] = lt
    # If the stopID is even, the train is northbound, otherwise it is southbound -- make this a string
    df["direction"] = df["Stop ID"].apply(lambda x: "SB" if int(x) % 2 == 0 else "NB")

    return df


def is_northbound(chosen_station, chosen_destination):
    """
    Returns True if the chosen destination is before
    the chosen station in the list of stations
    """
    stops = pd.read_csv("stop_ids.csv")
    station_index = stops[stops["stopname"] == chosen_station].index[0]
    destination_index = stops[stops["stopname"] == chosen_destination].index[0]
    return station_index > destination_index


def ping_caltrain(station, destination):
    # try:
    ct_df = build_caltrain_df(station)
    # except:
    # return False
    if ct_df.empty:
        return pd.DataFrame(columns=["Train #", "Direction", "Departure Time", "ETA"])

    # Move num_stops to the end
    ct_df = ct_df[["Train Number", "direction", "Departure", "departs_in"]]
    # Change column names to TN, Dir, Dep
    ct_df.columns = [
        "Train #",
        "Direction",
        "Departure Time",
        "ETA",
    ]

    # Clean up the dataframe
    ct_df.dropna(inplace=True)
    ct_df = ct_df[ct_df["ETA"] != "-"]

    deps = [
        datetime.datetime.strptime(i, "%Y-%m-%d %H:%M:%S").strftime("%I:%M %p")
        for i in ct_df["Departure Time"].tolist()
    ]
    ct_df["Departure Time"] = deps

    # Check the destination and if it's before the station, then the direction is southbound
    nb_sched = get_schedule("northbound", station, destination, rows_return=100)
    sb_sched = get_schedule("southbound", station, destination, rows_return=100)

    if destination != "--" and destination != station:
        if is_northbound(station, destination):
            sched = nb_sched
            ct_df = ct_df[ct_df["Direction"] == "NB"]
        else:
            sched = sb_sched
            ct_df = ct_df[ct_df["Direction"] == "SB"]
    else:
        sched = pd.concat([nb_sched, sb_sched])

    sched["ETA_sched"] = sched["ETA"]
    sched = sched[["Train #", "ETA_sched"]]
    merged = ct_df.merge(sched, how="inner", on=["Train #"], suffixes=("_test", "_sched"))

    # Calculate the difference between the scheduled and real time
    merged["diff"] = [
        datetime.datetime.strptime(i, "%H:%M") - datetime.datetime.strptime(j, "%H:%M")
        for i, j in zip(merged["ETA"], merged["ETA_sched"])
        if i != "-" and j != "-"
    ]
    merged["total_seconds"] = [i.total_seconds() for i in merged["diff"]]

    # Change the minutes to a time
    merged["diff"] = [to_time(i) for i in merged["total_seconds"].tolist()]

    return ct_df


def get_schedule(datadirection, chosen_station, chosen_destination=None, rows_return=5):
    if chosen_destination == "--" or chosen_station == chosen_destination:
        chosen_destination = None

    # Pull the scheduled train times from this url
    url = "https://www.caltrain.com/?active_tab=route_explorer_tab"

    # Get the html from the url
    html = requests.get(url).content

    # Parse the html
    soup = BeautifulSoup(html, "lxml")

    # Get the table from the html
    tables = soup.find_all("table")

    # Get the rows from the table
    table1 = tables[0]
    table2 = tables[1]
    table3 = tables[2]
    table4 = tables[3]

    def parse_table(table):
        rows = table.find_all("tr")
        table_data = []
        for row in rows:
            cols = row.find_all(["td", "th"])  # grab both header and data cells
            cols = [col.get_text(strip=True) for col in cols]
            table_data.append(cols)
        return table_data


    # Get the data from the rows
    data1 = parse_table(table1)
    data2 = parse_table(table2)
    data3 = parse_table(table3)
    data4 = parse_table(table4)

    # Convert the data to a dataframe
    def table_to_df(table_data):
        dataset_name = table_data[0][0]
        train_numbers = table_data[1][2:]  # Train numbers start at column 2

        station_rows = [
            row for row in table_data[2:]
            if any(cell.strip() for cell in row)
        ]

        flattened = []

        for row in station_rows[2:]:
            zone = row[0]
            station_name = row[1]
            times = row[2:]

            for train_num, time in zip(train_numbers, times):
                if time != '--' and time.strip() != '':
                    flattened.append([
                        dataset_name,
                        zone,
                        train_num,
                        station_name,
                        time
                    ])
        df = pd.DataFrame(
        flattened,
        columns=["dataset", "zone", "Train", "station", "time"]
        )
        return df

    df1 = table_to_df(data1)
    df2 = table_to_df(data2)
    df3 = table_to_df(data3)
    df4 = table_to_df(data4)

    # Union all four DataFrames
    list_of_dataframes = [df1, df2, df3, df4]
    df_union = pd.concat(list_of_dataframes, ignore_index=True)

    def parse_time(time_str, next_day_cutoff=4):
        """
        - If the time is past midnight (hour < next_day_cutoff), it assigns date + 1.
        - next_day_cutoff: hour threshold to consider as "next day" (default 4 AM)
        """
        if pd.isna(time_str):
            return None
        
        # Normalize shorthand 'a'/'p' to 'am'/'pm'
        time_str = time_str.strip().lower()
        time_str = re.sub(r'(?<=\d)a$', 'am', time_str)
        time_str = re.sub(r'(?<=\d)p$', 'pm', time_str)
        
        # Parse into datetime.time
        t = datetime.datetime.strptime(time_str, "%I:%M%p").time()
        
        # Combine with today
        dt = datetime.datetime.combine(datetime.date.today(), t)
        
        # If time is past midnight (hour < next_day_cutoff), add 1 day
        if dt.hour < next_day_cutoff:
            dt += datetime.timedelta(days=1)
        
        # Localize to Pacific Time
        pacific = pytz.timezone("US/Pacific")
        dt_pacific = pacific.localize(dt)
        
        return dt_pacific

    def simplify_service_label(text):
        """
        Converts strings like:
        'Northbound Service - Weekend Service to San Jose'
        into:
        'Northbound Weekend'
        """
        if pd.isna(text):
            return None
        
        match = re.search(r'(\w+bound).*?(Weekday|Weekend)', text, re.IGNORECASE)
        
        if match:
            direction = match.group(1).capitalize()
            day_type = match.group(2).capitalize()
            return f"{direction} {day_type}"
        
        return None  # or return original text if you prefer

    df_union["time_clean"] = df_union["time"].apply(parse_time)
    df_union['label'] = df_union['dataset'].apply(simplify_service_label)
    df_clean = df_union.drop(columns=['dataset', 'time', 'zone'])

    #Need to localize for streamlit servers
    pacific = pytz.timezone("US/Pacific")
    current_time2 = datetime.datetime.now(pacific)
    #weekday = True if current_time2.weekday() < 5 else False
    df_future = df_clean[df_clean["time_clean"] >= current_time2].copy()
    df_future.sort_values(["Train", "time_clean"], inplace=True)

    current_day_type = "Weekend" if current_time2.weekday() >= 5 else "Weekday"  # Saturday=5, Sunday=6
    df_future_wk = df_future[df_future["label"].str.contains(current_day_type, case=False)].copy()
    df_future_wk = df_future_wk[df_future_wk["time_clean"] >= current_time2].copy()

    df_future_wk["ETA"] = df_future_wk['time_clean'] - current_time2
    df_future_wk["ETA"] = df_future_wk["ETA"].apply(lambda td: f"{int(td.total_seconds() // 60)} mins")
    df_future_wk["Scheduled"] = df_future_wk["ETA"] + " // " + df_future_wk["time_clean"].dt.strftime("%I:%M %p")
    df_future_wk_final = df_future_wk.drop(columns = ["time_clean", "ETA"])

    if datadirection == "northbound":
        df_future_wk_nb = df_future_wk_final[df_future_wk_final["label"].str.contains('Northbound', case=False)].copy()
        df = df_future_wk_nb#.head(rows_return)
    if datadirection == "southbound":
        df_future_wk_sb = df_future_wk_final[df_future_wk_final["label"].str.contains('Southbound', case=False)].copy()
        df = df_future_wk_sb#.head(rows_return)

    return df

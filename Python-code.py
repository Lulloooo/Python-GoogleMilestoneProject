#import the needed packages
import numpy as np
import datetime as dt
import pandas as pd
#function to compute the mode of a variable
def Mode(x):
    return x.value_counts().idxmax()
#loading and setting the dataset from q2 (2019) to q1 (2020)
q2 = pd.read_csv("in_data/2019q2.csv")
q3 = pd.read_csv("in_data/2019q3.csv")
q4 = pd.read_csv("in_data/2019q4.csv")
q1 = pd.read_csv("in_data/2020q1.csv")
#rename various column
q4 = q4.rename(columns={"trip_id" : "ride_id",
                        "bikeid" : "rideable_type",
                        'start_time': 'started_at',
                        'end_time': 'ended_at',
                        'from_station_name': 'start_station_name',
                        'from_station_id': 'start_station_id',
                        'to_station_name': 'end_station_name',
                        'to_station_id': 'end_station_id',
                        'usertype': 'member_casual'})
q3 = q3.rename(columns={'trip_id': 'ride_id',
                        'bikeid': 'rideable_type',
                        'start_time': 'started_at',
                        'end_time': 'ended_at',
                        'from_station_name': 'start_station_name',
                        'from_station_id': 'start_station_id',
                        'to_station_name': 'end_station_name',
                        'to_station_id': 'end_station_id',
                        'usertype': 'member_casual'})
q2 = q2.rename(columns={'01 - Rental Details Rental ID': 'ride_id',
                        '01 - Rental Details Bike ID': 'rideable_type',
                        '01 - Rental Details Local Start Time': 'started_at',
                        "01 - Rental Details Duration In Seconds Uncapped": "tripduration",
                        '01 - Rental Details Local End Time': 'ended_at',
                        '03 - Rental Start Station Name': 'start_station_name', 
                        '03 - Rental Start Station ID': 'start_station_id',
                        '02 - Rental End Station Name': 'end_station_name', 
                        '02 - Rental End Station ID': 'end_station_id',
                        'User Type': 'member_casual'})
#inspect df and look for incongruencies
print(q2.info())
print(q3.info())
print(q4.info())
print(q1.info())
#convert ride_id and rideable_type into "string" type for every df
q4["ride_id"] = q4["ride_id"].astype(str)
q4["ride_id"] = q4["ride_id"].astype(str)
#
q3["ride_id"] = q3["ride_id"].astype(str)
q3["rideable_type"] = q3["rideable_type"].astype(str)
#
q2["ride_id"] = q2["ride_id"].astype(str)
q2["rideable_type"] = q2["rideable_type"].astype(str)
#add a "quarter" col (to know what quarter it is)
q2["quarter"] = ["2"] * 1108163
q3["quarter"] = ["3"] * 1640718
q4["quarter"] = ["4"] * 704054
q1['quarter'] = ['1'] * 426887
#drop not useful cols
q2b = q2.drop(columns = ["Member Gender", "05 - Member Details Member Birthday Year", "rideable_type", "tripduration"])
q3b = q3.drop(columns = ["birthyear", "gender", "rideable_type", "tripduration"])
q4b = q4.drop(columns = ["birthyear", "gender", "rideable_type", "tripduration"])
q1b = q1.drop(columns = ["end_lat", "end_lng", "start_lat", "start_lng", "rideable_type"])
#check the df after these changes
print(q2b.info())
print(q3b.info())
print(q4b.info())
print(q1b.info())
#merge dfs (to have a whole year of data altogether)
trips = pd.concat([q2b,q3b,q4b,q1b], ignore_index= True)
#keep only useufl col
trips = trips[['ride_id',
               'started_at',
               'ended_at',
               'start_station_name',
               'start_station_id',
               'end_station_name',
               'end_station_id',
               'member_casual', 
               "quarter"]]
#convert started_at and ended_at into datetime
trips["starting_time"] = pd.to_datetime(trips["started_at"])
trips["ending_time"] = pd.to_datetime(trips["ended_at"])
############################# PROCESSING ###############################
#check for incosistencies
#"member_casual" col replace "subscriber" with "member and "customer" with "casual"
# Before 2020, Divvy used different labels for these two types of riders
#make the df consistent with their current names
# Count how many observations fall under each usertype
print(trips["member_casual"].value_counts())
#re-assign to the desired values (member & casual)
trips["member_casual"] = trips["member_casual"].replace({"Subscriber": "member", "Customer": "casual"})
#check to make sure the proper obs number were re-assigned
print(trips["member_casual"].value_counts())
#add col that lists the date, month, day & year of each ride
#this gives the possible to aggregate ride-data for each mont, day or year
trips["date"] = pd.to_datetime(trips["started_at"]).dt.date
trips["month"] = pd.to_datetime(trips["started_at"]).dt.strftime("%m")
trips["day"] = pd.to_datetime(trips["started_at"]).dt.strftime("%d")
trips["year"] = pd.to_datetime(trips["started_at"]).dt.strftime("%Y")
trips["day_of_week"] = pd.to_datetime(trips["started_at"]).dt.strftime("%A")
#build a col "reverse" for starting_time > ending_time
trips["reverse"] = np.where(trips["starting_time"] > trips["ending_time"], "Yes", "No")
#build a col no_time for starting_time = ending_time
trips["no_time"] = np.where(trips["starting_time"] == trips["ending_time"], "Yes", "No")
#count "Yes" in no_time and reverse cols
print(trips["no_time"].value_counts()) #<60 sec: 93 obs
print(trips["reverse"].value_counts()) #starting_time > ending_time: 130 obs
#create the "same_station" col if start_station_id = end_station_id
trips["same_station"] = np.where(trips["start_station_id"] == trips["end_station_id"], "Yes", "No")
print(trips["same_station"].value_counts()) #same station: 165446
#create the "error" col for those start_time > end_time AND no_time = "No"
trips["errors"] = np.where((trips["reverse"] == "Yes") & (trips["no_time"] == "No"), "Yes", "No")
print(trips["errors"].value_counts()) #Errors 130
############################# CLEANING ###############################
#drop data where errors = "yes" (i.e 130 obs)
trips = trips[trips['errors'] != "Yes"]
#add a trip_duration col
trips.loc[:, "trip_duration"] = trips["ending_time"] - trips["starting_time"]
#check for NaN in trip_duration
trips["trip_duration"].isnull().values.any() #No NaN
#check the entire df
trips.isnull().values.any()
#count how many and display the cols
trips.isnull().sum()
#drop NaN in trips
trips = trips.dropna(subset=["end_station_name"])
#check if them have been removed
trips.isnull().sum()
#add col short_trip for trips < 60sec
trips["short_trip"] = np.where(trips["trip_duration"] <= pd.Timedelta(seconds=60), "Yes", "No")
print(trips["short_trip"].value_counts()) #short trips: 7579 obs
#add user_error col for trips < 60 sec and starting and ending from the same station
trips["user_error"] = np.where((trips["short_trip"] == "Yes") & (trips["start_station_id"] == trips["end_station_id"]), "Yes", "No")
#drop user errors
trips = trips[trips["user_error"] == "No"]
#drop the rows where the start_station is not specified
trips = trips[trips["start_station_id"] != "675"]
#drop the rows where the end_station is not specified
trips = trips[trips["end_station_id"] != "675"]
#check the datatype of trip_duration
print(trips["trip_duration"].dtype)
#change trip_duration into seconds
trips["trip_duration"] = trips["trip_duration"].dt.total_seconds()
#change it from factor to numeric
trips["trip_duration"] = trips["trip_duration"].astype(str).astype(float)
#check it
print(trips["trip_duration"].dtype)
#save the df ready for the analysis
tripsclean = trips[["ride_id",
                   "starting_time",
                   "ending_time",
                   "start_station_name",
                   "start_station_id",
                   "end_station_name",
                   "end_station_id",
                   "member_casual",
                   "quarter",
                   "date",
                   "month",
                   "day",
                   "year",
                   "day_of_week",
                   "reverse",
                   "no_time",
                   "same_station",
                   "errors",
                   "trip_duration",
                   "short_trip",
                   "user_error"]]
#save it
tripsclean.to_csv("out_data/tripsclean.csv", index = False)
############################# PROCESS #############################                             
#### SUMMARY ANALYSIS
#travel time avg
mean_travel_time = trips["trip_duration"].mean()
print("Average trip duration:", mean_travel_time, "sec")
#travel time median
median_travel_time = trips["trip_duration"].median()
print("Median travel time:", median_travel_time, "sec")
#longest ride
longest_ride = tripsclean["trip_duration"].max()
print("The longest ride is of", longest_ride, "hour")
#print the top 50 longest ride
longest = trips.nlargest(100, 'trip_duration')
print(longest[["trip_duration", "starting_time", "ending_time", "member_casual"]])
#save largest
longest.to_csv("out_data/longestRides.csv", index = False)
#shortest ride
shortest_ride = tripsclean["trip_duration"].min()
print("The shortest ride is of", shortest_ride, "sec")
#day_of_week mode (most freq day of the week)
mode_day_of_week = tripsclean["day_of_week"].mode()[0]
print("The most frequent day of the week is:", mode_day_of_week)
#start_station mode
mode_start_station = tripsclean["start_station_name"].mode()[0]
print("The most used departure station is:", mode_start_station)
#end_station mode
mode_end_station = tripsclean["end_station_name"].mode()[0]
print("The most used arrival station is:", mode_end_station)
#compute the avg, max, min, and total travel_time based on memeber type
average_travel_time = tripsclean.groupby("member_casual")["trip_duration"].mean()
total_travel_time = tripsclean.groupby("member_casual")["trip_duration"].sum()
max_travel_time = tripsclean.groupby("member_casual")["trip_duration"].max()
min_travel_time = tripsclean.groupby("member_casual")["trip_duration"].min()
#print the results:
print("The average trip duration based on user is:", average_travel_time, "sec")
print("The total trip duration based on user is:", total_travel_time, "sec")
print("The longest trip based on user is:", max_travel_time, "sec")
print("The shortest trip based on user is:", min_travel_time, "sec")
#compute the avg, max, min and total travel time based on day of the week
average_travel_time_day = tripsclean.groupby("day_of_week")["trip_duration"].mean()
total_travel_time_day = tripsclean.groupby("day_of_week")["trip_duration"].sum()
max_travel_time_day = tripsclean.groupby("day_of_week")["trip_duration"].max()
min_travel_time_day = tripsclean.groupby("day_of_week")["trip_duration"].min()
#print the results
print("The average trip duration based on user is:", average_travel_time_day, "sec")
print("The total trip duration based on user is:", total_travel_time_day, "sec")
print("The longest trip based on user is:", max_travel_time_day, "sec")
print("The shortest trip based on user is:", min_travel_time_day, "sec")
#count how many trips are done each day of the week
trips_per_day_of_week = tripsclean['day_of_week'].value_counts()
print("Trips done per day of the week:\n", trips_per_day_of_week)
#format starting_time as time HH:MM:SS and name it as "time"
tripsclean.loc[:,"time"] = (pd.to_datetime(
    tripsclean["starting_time"]
    )
    .dt.strftime("%H:%M:%S"))
#Build rush hours ranges (7.00 - 9.30am; 5 - 8.30pm)
tripsclean.loc[:, "rush1"] = (pd.to_datetime(
    pd.Series(
        ["7:30:00"] * len(tripsclean)),
        format="%H:%M:%S")
        .dt.time
    )
tripsclean.loc[:, "rush2"] = (pd.to_datetime(
    pd.Series(
        ["9:00:00"] * len(tripsclean)),
        format="%H:%M:%S")
        .dt.time
    )
tripsclean.loc[:, "rush3"] = (pd.to_datetime(
    pd.Series(
        ["17:00:00"] * len(tripsclean)),
        format="%H:%M:%S")
        .dt.time
    )
tripsclean.loc[:, "rush4"] = (pd.to_datetime(
    pd.Series(
        ["20:00:00"] * len(tripsclean)),
        format="%H:%M:%S")
        .dt.time
    )
#check if a trip is within morning rush hour range
rush_morning = np.where((pd.to_datetime(tripsclean["time"], format="%H:%M:%S").dt.time >= tripsclean["rush1"]) &
                        (pd.to_datetime(tripsclean["time"], format="%H:%M:%S").dt.time <= tripsclean["rush2"]), "Yes", "No")
#check if a trip is within afternoon rush hour range
rush_afternoon = np.where((pd.to_datetime(tripsclean["time"], format="%H:%M:%S").dt.time >= tripsclean["rush3"]) &
                        (pd.to_datetime(tripsclean["time"], format="%H:%M:%S").dt.time <= tripsclean["rush4"]), "Yes", "No")
##add these two var to the df
tripsclean = tripsclean.assign(rush_morning=rush_morning) #method1
tripsclean["rush_afternoon"] = rush_afternoon #method2
#count rush hour trips
rush_trips = np.where((rush_morning == "Yes") | (rush_afternoon == "Yes"), "Yes", "No")
#add effective rush trips by getting rid of sundays rush trips
rush_trip_weekly = np.where((rush_trips == "Yes") & (tripsclean["day_of_week"] != "Sunday"), "Yes", "No")
print(pd.Series(rush_trip_weekly).value_counts())
#add these vars to the df
tripsclean = tripsclean.assign(rush_trips = rush_trips)
tripsclean = tripsclean.assign(rush_trip_weekly = rush_trip_weekly)
#count how many rush trips are done
print( "Number of weekly rush trips is:", tripsclean["rush_trip_weekly"].value_counts())
#count the rush trips x user type
print(tripsclean.groupby("member_casual")["rush_trip_weekly"].value_counts())
#summary stats for tripduration
tripsduration = tripsclean["trip_duration"].describe()
print(tripsduration)
#summary stats based on quarters
tripsquarter_count = tripsclean.groupby("quarter")["trip_duration"].count()
tripsquarter_total = tripsclean.groupby("quarter")["trip_duration"].sum()
print(tripsquarter_count)
print(tripsquarter_total)
#build dfs for tripduration stats based on users and day of the week
tripsmean = (tripsclean.groupby(
    ["member_casual", "day_of_week"]
    )
    ["trip_duration"].mean()
    .reset_index()
)
tripssum = (tripsclean.groupby(
    ["member_casual", "day_of_week"]
    )
    ["trip_duration"].sum()
    .reset_index()
)
tripsmax = (tripsclean.groupby(
    ["member_casual", "day_of_week"]
    )
    ["trip_duration"].max()
    .reset_index()
)
tripsmin = (tripsclean.groupby(
    ["member_casual", "day_of_week"]
    )
    ["trip_duration"].min()
    .reset_index()
)
tripscount = (tripsclean.groupby(
    ["member_casual", "day_of_week"]
    )
    ["trip_duration"].size()
    .reset_index(name="count")
)
rush_trip_summary = (tripsclean.groupby(
    ["member_casual", "day_of_week"]
    )
    ["rush_trip_weekly"].sum()
    .reset_index()
)
#weekday only analysis
daymean = tripsclean.groupby('day_of_week')['trip_duration'].mean().reset_index()
daysum = tripsclean.groupby('day_of_week')['trip_duration'].sum().reset_index()
daycount = tripsclean.groupby('day_of_week')['trip_duration'].size().reset_index(name='count')
#month analysis and member
monthmean = tripsclean.groupby(["member_casual", "month"])["trip_duration"].mean().reset_index()
monthsum = tripsclean.groupby(["month", "member_casual"])["trip_duration"].sum().reset_index()
monthcount = tripsclean.groupby(["month", "member_casual"])["trip_duration"].size().reset_index(name = "count")
#change the dfs from long to wide
monthcountwide = monthcount.pivot(index = "month", columns = "member_casual", values = "count" )
monthsumwide = monthsum.pivot(index = "month", columns = "member_casual", values = "trip_duration")
monthmeanwide = monthmean.pivot(index = "month", columns = "member_casual", values = "trip_duration")
#add the column "month_name"
month_name = ["January", "February", "March", "April", "May", "June","July","August","September","October","November", "December"]
monthcountwide["month_name"] = month_name
monthsumwide["month_name"] = month_name
monthmeanwide["month_name"] = month_name
#date analysis
datemean = tripsclean.groupby(["member_casual", "date"])["trip_duration"].mean().reset_index(name="mean")
datesum = tripsclean.groupby(["member_casual", "date"])["trip_duration"].sum().reset_index(name="sum")
datecount = tripsclean.groupby(["member_casual", "date"])["trip_duration"].size().reset_index(name = "count")
#merge them all to have them together
dateAnalysis = pd.concat([datemean, datesum, datecount], axis = 1, join = "inner")
#remove duplicate columns
dateAnalysis = dateAnalysis.T.drop_duplicates().T
#date df in wide shape
datecountwide = datecount.pivot(index = "date", columns = "member_casual", values = "count")
datemeanwide = datemean.pivot(index = "date", columns = "member_casual", values = "mean")
datesumwide = datesum.pivot(index = "date", columns = "member_casual", values = "sum")
datesumwide["total"] = datesumwide["casual"] + datesumwide["member"]
#count rush trips based on membership
tripsmemberrush = tripsclean.groupby("member_casual")["rush_trip_weekly"].sum()
#count rush trip based on day of thr week
tripsdayrush = tripsclean.groupby("day_of_week")["rush_trip_weekly"].sum()
### Station use
#count how many times each station has been used during the year
startStatCount = trips["start_station_name"].value_counts().reset_index().rename(columns={"index" : "station_name", "start_station_name" : "tot"})
endStatCount = trips["end_station_name"].value_counts().reset_index().rename(columns={"index": "station_name", "end_station_name": "tot"})
stationCount = pd.concat([trips["start_station_name"], trips["end_station_name"]]).value_counts().reset_index().rename(columns={"index": "station_name", 0: "tot"})
############################ LATITUTE AND LONGITUDE ANALYSIS ###################
#re-load q1 but under another name
q1lat = pd.read_csv("in_data/2020q1.csv")
### start station
#keep only lat/long-related cols
q1latStart = q1lat[["start_station_name", 
                  "start_station_id", 
                  "start_lat", 
                  "start_lng"]]
#group by station and create a df with that (as_index = False)
Startlat = q1latStart.groupby(["start_station_name", "start_lat", "start_lng"], as_index=False).size()
Startlat = Startlat[["start_station_name",
                   "start_lat",
                   "start_lng"]]
#check that everything is fine
print(Startlat.info())
#rename the col into startStatCount
startStatCount = startStatCount.rename(columns={"tot" : "start_station_name"})
#merge to get the lat/long for every station
latlongStart = pd.merge(Startlat, startStatCount, on = "start_station_name")
#check for NaN
latlongStart.isnull().sum()
### end station
#keep only lat/long-related cols
q1latEnd = q1lat[["end_station_name", 
                  "end_station_id", 
                  "end_lat", 
                  "end_lng"]]
#group by station and create a df with that (as_index = False)
Endlat = q1latEnd.groupby(["end_station_name", "end_lat", "end_lng"], as_index=False).size()
Endlat = Endlat[["end_station_name",
                   "end_lat",
                   "end_lng"]]
#check that everything is fine
print(Endlat.info())
#rename the col into startStatCount
endStatCount = endStatCount.rename(columns={"tot" : "end_station_name"})
#merge to get the lat/long for every station
latlongEnd = pd.merge(Endlat, endStatCount, on = "end_station_name")
#check for NaN
latlongEnd.isnull().sum()
######################## DATA EXPORT #############################################
#save all the dfs created
tripsquarter_count.to_csv('out_data/quartercount.csv', index=False)
tripsquarter_total.to_csv('out_data/quartertotal.csv', index=False)
tripsmean.to_csv('out_data/tripsmean.csv', index=False)
tripssum.to_csv('out_data/tripssum.csv', index=False)
tripsmax.to_csv('out_data/tripsmax.csv', index=False)
tripsmin.to_csv('out_data/tripsmin.csv', index=False)
rush_trip_summary.to_csv('out_data/rushtrip_total.csv', index=False)
daymean.to_csv('out_data/daymean.csv', index=False)
daysum.to_csv('out_data/daysum.csv', index=False)
daycount.to_csv('out_data/daycount.csv', index=False)
tripsmemberrush.to_csv('out_data/rushmembers.csv', index=False)
tripsdayrush.to_csv('out_data/rushday.csv', index=False)
stationCount.to_csv("out_data/stationCount.csv", index = False)
startStatCount.to_csv("out_data/startStatCount.csv", index = False)
endStatCount.to_csv("out_data/endStatCount.csv", index = False)
latlongStart.to_csv("out_data/latlongStart.csv", index = False)
latlongEnd.to_csv("out_data/latlongEnd.csv", index = False)
monthmeanwide.to_csv("out_data/monthmean.csv", index = True)
monthsumwide.to_csv("out_data/monthsum.csv", index = True)
monthcountwide.to_csv("out_data/monthcount.csv", index = True)
datemeanwide.to_csv("out_data/datemean.csv", index = True)
datecountwide.to_csv("out_data/datecount.csv", index = True)
datesumwide.to_csv("out_data/datesum.csv", index = True)
dateAnalysis.to_csv("out_data/dateAnalysis.csv", index = False)
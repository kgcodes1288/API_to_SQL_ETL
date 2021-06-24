import requests
import pandas as pd
from datetime import datetime
import json
from update_load import *

#helper function
def replacenan(df):
    df = df.where(pd.notnull(df), None)
    return df

create_new = True
update_monthly = not create_new

if create_new: #create new
    events = pd.DataFrame()
    year = 2017
    for month in range(12):
        month = month + 1
        if len(str(month)) == 1:
            month = '0' + str(month)
        else:
            month = str(month)
        url = r'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={0}-{1}-01&endtime={0}-{1}-31'.format(str(year),month)
        response = requests.get(url)
        data = response.json()
        temp_df = pd.DataFrame(data['features'])
        print("Length of the data for month , ", month, " is ", str(len(temp_df)))                       
        events = events.append(temp_df, ignore_index=True)
    print("Total length for year ", str(year), " is ", str(len(events))) 
else: #monthly batch update
    current_month = datetime.today().month
    current_year = datetime.today().year
    month_needed = current_month-1 #assuming the batch update takes place monthly
    if len(str(month_needed)) == 1:
        month = '0' + str(month_needed)
    else:
        month = str(month_needed)
    url = r'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={0}-{1}-01&endtime={0}-{1}-31'.format(str(current_year),month)
    response = requests.get(url)
    data = response.json()
    events = pd.DataFrame(data['features'])
    print("Length of the data for month , ", month, " is ", str(len(events)))                       
       
                        
#keys
#geometry,id,properties,type
for i, r in events.iterrows():
    #adding id to geometry
    for col in ['geometry','properties']:
        temp = r[col]
        temp['ids'] = r['id']
        events.at[i, col] = temp

geometry = pd.DataFrame(list(events['geometry']))
geometry['LAT'] = geometry['coordinates'].apply(lambda x: x[0])
geometry['LONG'] = geometry['coordinates'].apply(lambda x: x[1])
geometry['DEPTH'] = geometry['coordinates'].apply(lambda x: x[2])
geometry = geometry.drop(columns=['coordinates'])


properties = pd.DataFrame(list(events['properties']))
properties['event_time'] = properties['time'].apply(lambda x: datetime.fromtimestamp(x/1000))
properties['event_hour'] = properties['event_time'].apply(lambda x: x.hour)
properties['event_date'] = properties['event_time'].apply(lambda x: x.date())
properties['updated'] = properties['updated'].apply(lambda x: datetime.fromtimestamp(x/1000))
properties = properties.rename(columns={'type':'event_type'})

#all data creation

alldata = properties.merge(geometry, on='ids')

#master table

master = alldata[['ids','mag','cdi','felt','magType','alert','mmi','sig','tsunami','status']]
master = replacenan(master)
print("Working on Master")
populatemaster(master)

#creating datetime table

timetable = alldata[['ids','event_time', 'event_hour','event_date','updated']] #ids is the FK
timetable = replacenan(timetable)
print("Working on datetime")
populatedatetime(timetable)

#creating description table

description = alldata[['ids','title','event_type']]
description = replacenan(description)
print("Working on description")
populatedes(description)

#creating location table

location = alldata[['ids','LAT','LONG','DEPTH','place','tz','gap','rms','dmin']]
location = replacenan(location)
print("Working on location")
populateloc(location)


#product types table

producttypes = alldata[['ids','types']]
producttypes['types'] = producttypes['types'].apply(lambda x: x.split(','))
producttypes['types'] = producttypes['types'].apply(lambda li: [x for x in li if x != ''])
producttypes = producttypes.explode('types') #key ids, types
producttypes = replacenan(producttypes)
print("Working on product types")
populateprod(producttypes)
#network table

network = alldata[['ids','net','nst','sources']]
network['sources'] = network['sources'].apply(lambda li: [x for x in set(li.split(',')) if x != ''])
network = network.explode('sources')
network['Preferred'] = network.apply(lambda r: 'Yes' if r['net'] == r['sources'] else 'No', axis=1)
network = replacenan(network)
print("Working on network")
populatenet(network)

#url_table
url = alldata[['ids','url','detail']]
print("Working on urls")
populateurls(url)


#column descriptions
#mag, magnitude, decimal
#cdi, decimal maximum reported intensity
#place, string
#time, string
#updated, string
#tz timezone offset integer
#url string
#detail string
#felt integer,  The total number of felt reports
#gap decimal The largest azimuthal gap between azimuthally adjacent stations (in degrees).
#mmi max extimated intensity decimal [0-10]
#magtype Algorithm used to calculate magnitude
#net id of network contributor
#nst integer  total number of sismic stations used to determine the earthquake location
#place description of location, string
#rms root mean square, his parameter provides a measure of the fit of the observed arrival times to the predicted arrival times for this location
#sig [0-1000] how significant the event is
#sources string
#status string, automatic, reviewed, deleted
#tsunami 0,1
#event_type", string
#types A comma-separated list of product types associated to this event.
#title string
#event_hour
#dmin - horizontal distance from nearest station


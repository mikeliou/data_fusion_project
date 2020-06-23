from bs4 import BeautifulSoup
import json

contents = ''
with open("20190320_fleetmon_data.json", "r", encoding="utf-8") as myfile:
    contents = json.loads(myfile.read())

for obj in contents:
    soup_name = BeautifulSoup(obj["vessel_name"], 'html.parser')
    soup_seen_first = BeautifulSoup(obj["seen_first"], 'html.parser')
    soup_seen_last = BeautifulSoup(obj["seen_last"], 'html.parser')

    obj["time_in_port"] = BeautifulSoup(obj["time_in_port"], 'html.parser').get_text()
    obj["vessel_flag"] = BeautifulSoup(obj["vessel_flag"], 'html.parser').img['uk-tooltip']
    obj["vessel_name"] = soup_name.a.get_text()
    obj["vessel_type"] = soup_name.span.get_text()

    split_arr = obj["vessel_mmsi_callsign"].split('<br />')
    obj["vessel_mmsi"] = split_arr[0]
    obj["vessel_callsign"] = split_arr[1]

    try:
        obj["seen_first"] = soup_seen_first.find("span", {"class": "date"}).get_text() + ' ' + soup_seen_first.find("span", {"class": "time"}).get_text()
    except AttributeError:
        obj["seen_first"] = None

    try:
        obj["seen_last"] = soup_seen_last.find("span", {"class": "date"}).get_text() + ' ' + soup_seen_last.find("span", {"class": "time"}).get_text()
    except AttributeError:
        obj["seen_last"] = None

    del obj['vessel_vesselicon']
    del obj['dwt']
    del obj['vessel_mmsi_callsign']

with open('data.json', 'w') as outfile:
    json.dump(contents, outfile)


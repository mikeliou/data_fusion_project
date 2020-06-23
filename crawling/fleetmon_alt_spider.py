import requests
import time

url = "https://www.fleetmon.com/ports/singapore_sgsin_18387/"

querystring = {"sEcho":"19","iColumns":"10","sColumns":"vessel_flag%2Cvessel_name%2Cvessel_vesselicon%2Cvessel_imo%2Cvessel_mmsi_callsign%2Cvessellength_range%2Cdwt%2Cseen_first%2Cseen_last%2Ctime_in_port","iDisplayStart":"0","iDisplayLength":"50","mDataProp_0":"vessel_flag","mDataProp_1":"vessel_name","mDataProp_2":"vessel_vesselicon","mDataProp_3":"vessel_imo","mDataProp_4":"vessel_mmsi_callsign","mDataProp_5":"vessellength_range","mDataProp_6":"dwt","mDataProp_7":"seen_first","mDataProp_8":"seen_last","mDataProp_9":"time_in_port","sSearch":"","bRegex":"false","sSearch_0":"","bRegex_0":"false","bSearchable_0":"true","sSearch_1":"","bRegex_1":"false","bSearchable_1":"false","sSearch_2":"","bRegex_2":"false","bSearchable_2":"true","sSearch_3":"","bRegex_3":"false","bSearchable_3":"false","sSearch_4":"","bRegex_4":"false","bSearchable_4":"false","sSearch_5":"","bRegex_5":"false","bSearchable_5":"true","sSearch_6":"","bRegex_6":"false","bSearchable_6":"false","sSearch_7":"","bRegex_7":"false","bSearchable_7":"false","sSearch_8":"","bRegex_8":"false","bSearchable_8":"false","sSearch_9":"","bRegex_9":"false","bSearchable_9":"false","iSortCol_0":"7","sSortDir_0":"desc","iSortingCols":"1","bSortable_0":"true","bSortable_1":"true","bSortable_2":"false","bSortable_3":"true","bSortable_4":"true","bSortable_5":"false","bSortable_6":"true","bSortable_7":"true","bSortable_8":"true","bSortable_9":"false","table":"arr_hist_table","filter_by_daterange":"20/03/2019-20/03/2019","filter_vessel_type":"","filter_dwt_range":"","filter_in_myfleet":"0","filter_in_myfleet_tag":"","_":"1553333715857"}

payload = ""
headers = {
    'Host': "www.fleetmon.com",
    'Connection': "keep-alive",
    'Accept': "application/json, text/javascript, */*; q=0.01",
    'X-Requested-With': "XMLHttpRequest",
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
    'Referer': "https://www.fleetmon.com/ports/singapore_sgsin_18387/",
    'Accept-Encoding': "gzip, deflate, br",
    'Accept-Language': "en-US,en;q=0.9,el;q=0.8",
    'Cookie': "ajs_group_id=null; ajs_anonymous_id=%2260fadfc8-2bdb-488c-8fdd-0473812a5f86%22; _ga=GA1.2.508957030.1552769372; _hjIncludedInSample=1; ajs_user_id=%222169801%22; main-search-input-value=singapore; intercom-id-kwshk9to=64295504-cc13-4afd-902d-a3e185fe6a3f; fm_remember='1552773710_2169801:1h5HNG:HloZPwaTDUkBXH92zjhAsGpgChE'; csrftoken=tYCTXZvzbRi0ZBTfor99eP4a0BAQYOjv; cookieconsent_status=dismiss; fmc_session=85u14i3n1x2pus8qym8x68otw9ynvttz; _gid=GA1.2.972298058.1553333719; intercom-session-kwshk9to=UHpMeS8zNUtCbDVqdnQzcjAzbTV5WUJPemd0ZGRycVQ1OGV4UTlPNVpSNGZlQkRvWm1aOExENGw1UnArMzNLKy0tQm1pRlRxMnFiaS96c2RzQmlkaWVpQT09--17d2a27135ff2ead7066b3bddc2948a91471218e",
    'cache-control': "no-cache",
    'Postman-Token': "69d7d38d-8812-4489-91da-91ab7cba4941"
    }

for x in range(0, 4044, 50):
    time.sleep(2)
    querystring["iDisplayStart"] = str(x)
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

    with open(str(x) + "_fleetmon.txt", "a", encoding="utf-8") as myfile:
        myfile.write(response.text)

    print(x)
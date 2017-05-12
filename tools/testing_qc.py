
import requests
import pandas as pd

# url_test = 'https://ooinet-dev-01.oceanobservatories.org/api/m2m/12578/qcparameters/inv/CE04OSPS/SF01B/2A-CTDPFA107/'
# url_production = 'https://ooinet.oceanobservatories.org/api/m2m/12578/qcparameters/inv/'
#
# r_t = requests.get(url_test, verify=False)
# r_t.ok
#
# r_p = requests.get(url_production, verify=False)
# r_p.ok
# r_p.json()

def get_global_ranges(platform, node, sensor, variable, api_user=None, api_token=None):
    port = '12578'
    base_url = '{}/qcparameters/inv/{}/{}/{}/'.format(port, platform, node, sensor)
    url = 'https://ooinet-dev-01.oceanobservatories.org/api/m2m/{}'.format(base_url)
    if (api_user is None) or (api_token is None):
        r = requests.get(url, verify=False)
    else:
        r = requests.get(url, auth=(api_user, api_token), verify=False)

    if r.status_code is 200:
        if r.json(): # If r.json is not empty
            values = pd.io.json.json_normalize(r.json())
            t1 = values[values['qcParameterPK.streamParameter'] == variable]
            if not t1.empty:
                t2 = t1[t1['qcParameterPK.qcId'] == 'dataqc_globalrangetest_minmax']
                if not t2.empty:
                    local_min = float(t2[t2['qcParameterPK.parameter'] == 'dat_min'].iloc[0]['value'])
                    local_max = float(t2[t2['qcParameterPK.parameter'] == 'dat_max'].iloc[0]['value'])
                else:
                    local_min = None
                    local_max = None
            else:
                local_min = None
                local_max = None
        else:
            local_min = None
            local_max = None
    return [local_min, local_max]

test = get_global_ranges("CE04OSPS","SF01B","2A-CTDPFA107","corrected_dissolved_oxygen")
print test
#-------------------------------------------------------------------------------
# Name:         Universal Data Capture for ArcGIS REST Services
# Purpose:      Captures data from ArcGIS REST Services and with modification to
#               the code, can ouput to any desired format.
#
# Author:      John Spence
#
#
#
# Created:    18 May 2023
# Modified:
# Modification Purpose:
#
#
#-------------------------------------------------------------------------------


# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
#   Adjust the settings below to match your org. eMail functionality is not 
#   present currently, though obviously can be built in later.
#
# ------------------------------- Dependencies ---------------------------------
#
#
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Open Data Source
dataSource = r'https://services1.arcgis.com/EYzEZbDhXZjURPbP/arcgis/rest/services/Bellevue_Permits/FeatureServer/0'
dataFields = 'PERMITSTATUS,PERMITNUMBER,PERMITTYPE,PERMITTYPEDESCRIPTION,PERMITYEAR,PROJECTNAME,PROJECTDESCRIPTION,APPLICANT,CONTRACTOR,APPLIEDDATE,ISSUEDDATE,ZONING,LOTSIZE'
dataWKID = 4326 
dataFMT = r'json'
dataWhereC = '1=1' # Where clause statement... Example r'PERMITSTATUS=\'Closed\''. Set to 1=1 if you want to pull all records.

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

import datetime
import time
import base64
import urllib
import requests
import json
import sys
import os
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

#-------------------------------------------------------------------------------
#
#
#                                 Functions
#
#
#-------------------------------------------------------------------------------

def main():
#-------------------------------------------------------------------------------
# Name:        Function - main
# Purpose:  Starts the whole thing.
#-------------------------------------------------------------------------------

    starttime = startup()
    print ('Starup job @: {}'.format(starttime))
    payloadProcessing()
    stoptime = startup()
    print ('Finished job @: {}'.format(stoptime))


    return

def startup():
#-------------------------------------------------------------------------------
# Name:        Function - main
# Purpose:  Starts the whole thing.
#-------------------------------------------------------------------------------

    starttime = datetime.datetime.now()

    return (starttime)

def captureDS(resultOffset):
#-------------------------------------------------------------------------------
# Name:        Function - captureDS
# Purpose:  
#-------------------------------------------------------------------------------

    values = {'f': dataFMT,
                'where': dataWhereC,
                'outFields': dataFields,
                'outSR': dataWKID,
                'resultOffset': resultOffset
                'resultRecordCount': 2000              
                }

    headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}

    url = dataSource + r'/query'

    data = urllib.parse.urlencode(values).encode("utf-8")
    req = urllib.request.Request(url, data, headers)

    response = None
    attempt = 0
    while response is None:
        attempt += 1
        if attempt > 3:
            time.sleep (10)
            attempt = 0
        try:
            response = urllib.request.urlopen(req)
        except:
            pass

    the_page = response.read().decode(response.headers.get_content_charset())
    payload_json = json.loads(the_page)

    return (payload_json)

def captureDSCount():
#-------------------------------------------------------------------------------
# Name:        Function - captureDSCount
# Purpose:  
#-------------------------------------------------------------------------------

    values = {'f': dataFMT,
                'where': dataWhereC,
                'returnCountOnly': 'true'
                }

    headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}

    url = dataSource + r'/query'

    data = urllib.parse.urlencode(values).encode("utf-8")
    req = urllib.request.Request(url, data, headers)

    response = None
    attempt = 0
    while response is None:
        attempt += 1
        if attempt > 3:
            time.sleep (10)
            attempt = 0
        try:
            response = urllib.request.urlopen(req)
        except:
            pass

    the_page = response.read().decode(response.headers.get_content_charset())
    payload_json = json.loads(the_page)

    return (payload_json)


def payloadProcessing():
#-------------------------------------------------------------------------------
# Name:        Function - payloadProcessing
# Purpose:  
#-------------------------------------------------------------------------------

    payload = captureDSCount()
    toGo = int(payload['count']/2000)
    toGo += 1
    toDo = 0
    processedPayload = []

    while toDo < toGo:
        if toDo == 0:
            resultOffset = 0
        else:
            resultOffset = toDo * 2000

        payload = captureDS(resultOffset)

        for permits in payload['features']:
            pendPayload = []
            payloadFields = dataFields.split(sep=',')

            for fieldName in payloadFields:
                pendPayload.append('{}'.format(permits['attributes']['{}'.format(fieldName)]))
            processedPayload.append(pendPayload)

        toDo += 1

    records = 0
    for payload in processedPayload:
        records += 1
        #print (payload)  # Review output to make sure the filters worked right....
        #print ('\n')
    print ('{} records reviewed'.format(records))

    return()


#-------------------------------------------------------------------------------
#
#
#                                 MAIN SCRIPT
#
#
#-------------------------------------------------------------------------------

if __name__ == "__main__":
    main()

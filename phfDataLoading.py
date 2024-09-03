# ***********************************************************************
# *                                                                     * 
# *   PROGRAM NAME   : ODSParser WBFPHF                                 *
# *   PURPOSE        : This program will parse pipe delimited file      * 
# *                    containing Station Destination data and          *
# *                    transform into JSON                              *  
# *   AUTHOR         : Nayana Rathod                                    *
# *   LAST UPDATED   : 06Aug2024                                        *
# *   INPUT FILE     : WBFPHF.txt                                       *
# *   OUTPUT FILE    : It will load data in dynamodb                    *
# *                                                                     * 
# *********************************************************************** 
#
#from dynamodb_json import json_util as json
#
import json
import boto3
import os
import pysftp
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# SFTP server details

SFTP_HOST = 'your_sftp_host'
SFTP_PORT = 22
SFTP_USERNAME = 'your_sftp_username'
SFTP_PASSWORD = 'your_sftp_password'
SFTP_FILE_PATH = 'path/to/your/file/on/sftp'
LOCAL_FILE_PATH = '/tmp/tempfile'  # Lambda provides a /tmp directory

# S3 bucket details
S3_BUCKET_NAME = 'your_s3_bucket_name'
S3_FILE_KEY = 'path/in/s3/bucket'

# **********************************************************
strPayloadHistData = ""
PayloadHistId = ""
# **********************************************************
PayloadHistDeparture = {}
jPayloadHistData = ""
jPayloadHistDataList = []
# **********************************************************
# **********************************************************
flist = []
# **********************************************************
# **********************************************************
# PayloadHist Header Section
# **********************************************************
DayOfWeek = 0
NumOfLegs = 0
LegHistNotUpdatedInd = 0
FlightNum = 0
# **********************************************************
# PayloadHist Departure ID Section
# **********************************************************
DepartureStn = []
ArrivalStn = []
NumOfHistoricalWeeks = []
SchedEqpType = []
RevPsgrsFwd = []
RevPsgrsRear = []
PsgrAdjValue = []
KidCount = []
TaxiFuel = []
HvyBagPercent = []
BagPerPsgrRatio = []
FuelLoad = []
AirMailWgt = []
FirstClassMailWgt = []
FirstFreightWgt = []
FreightWgt = []
NumOfBags = []
CompanyMaterial = []
PayloadHistHeader = {}
PayloadHistDepartureItem = []
PayloadHistDeparture = {}
PayloadHistData = {}


# **********************************************************
# Initialize 
# **********************************************************
def doinit():
    # **********************************************************
    # PayloadHist Header Section
    # **********************************************************
    DayOfWeek = 0
    NumOfLegs = 0
    LegHistNotUpdatedInd = 0
    FlightNum = 0
    # **********************************************************
    # PayloadHist Departure ID Section
    # **********************************************************
    DepartureStn.clear()
    ArrivalStn.clear()
    NumOfHistoricalWeeks.clear()
    SchedEqpType.clear()
    RevPsgrsFwd.clear()
    RevPsgrsRear.clear()
    PsgrAdjValue.clear()
    KidCount.clear()
    TaxiFuel.clear()
    HvyBagPercent.clear()
    BagPerPsgrRatio.clear()
    FuelLoad.clear()
    AirMailWgt.clear()
    FirstClassMailWgt.clear()
    FirstFreightWgt.clear()
    FreightWgt.clear()
    NumOfBags.clear()
    CompanyMaterial.clear()
    PayloadHistHeader.clear()
    PayloadHistDeparture.clear()
    PayloadHistDepartureItem.clear()
    PayloadHistData.clear()


# **********************************************************
def parseLine():
    try:
        listlen = len(flist)
        print(f"Number of Fields:{listlen}")
        # **********************************************************
        # PayloadHist Header Section
        # **********************************************************
        x = 2
        DayOfWeek = int(flist[x])
        print(f"DayOfWeek:{DayOfWeek}")
        x = x + 1
        NumOfLegs = int(flist[x])
        nolgs = NumOfLegs
        x = x + 1
        LegHistNotUpdatedInd = int(flist[x])
        x = x + 1
        FlightNum = int(flist[x])
        print(FlightNum)
        x = x + 1
        # **********************************************************
        # PayloadHist Departure ID Section
        # **********************************************************
        # **********************************************************
        #  Collect DepartureStn in a list
        # **********************************************************
        stpos = x
        endpos = stpos + nolgs
        for x in range(stpos, endpos):
            DepartureStn.append(flist[x])
        print(DepartureStn
              )
        x = x + 1
        #print(f"DepartureStn:{DepartureStn}")
        # **********************************************************
        #  Collect ArrivalStn in a list
        # *********************************************************
        stpos = x
        endpos = stpos + nolgs
        for x in range(stpos, endpos):
            ArrivalStn.append(flist[x])
        print(ArrivalStn)
        x = x + 1
        #print(f"ArrivalStn:{ArrivalStn}")
        # **********************************************************
        #  Collect NumOfHistoricalWeeks in a list
        # **********************************************************
        stpos = x
        endpos = stpos + nolgs
        for x in range(stpos, endpos):
            NumOfHistoricalWeeks.append(int(flist[x]))
        x = x + 1
        #print(f"NumOfHistoricalWeeks:{NumOfHistoricalWeeks}")
        # **********************************************************
        #  Collect SchedEqpType in a list
        # **********************************************************
        stpos = x
        endpos = stpos + nolgs
        for x in range(stpos, endpos):
            SchedEqpType.append(flist[x])
        x = x + 1
        #print(f"SchedEqpType:{SchedEqpType}")
        # **********************************************************
        #  Collect RevPsgrsFwd in a list
        # **********************************************************
        stpos = x
        endpos = stpos + nolgs
        for x in range(stpos, endpos):
            RevPsgrsFwd.append(int(flist[x]))
        x = x + 1
        #print(f"RevPsgrsFwd:{RevPsgrsFwd}")
        # **********************************************************
        #  Collect RevPsgrsRear in a list
        # **********************************************************
        stpos = x
        endpos = stpos + nolgs
        for x in range(stpos, endpos):
            RevPsgrsRear.append(int(flist[x]))
        x = x + 1
        #print(f"RevPsgrsRear:{RevPsgrsRear}")
        # **********************************************************
        #  Collect PsgrAdjValue in a list
        # **********************************************************
        stpos = x
        endpos = stpos + nolgs
        for x in range(stpos, endpos):
            PsgrAdjValue.append(int(flist[x]))
        x = x + 1
        #print(f"PsgrAdjValue:{PsgrAdjValue}")
        # **********************************************************
        #  Collect KidCount in a list
        # **********************************************************
        stpos = x
        endpos = stpos + nolgs
        for x in range(stpos, endpos):
            KidCount.append(int(flist[x]))
        x = x + 1
        #print(f"KidCount:{KidCount}")
        # **********************************************************
        #  Collect TaxiFuel in a list
        # **********************************************************
        stpos = x
        endpos = stpos + nolgs
        for x in range(stpos, endpos):
            TaxiFuel.append(int(flist[x]))
        x = x + 1
        #print(f"TaxiFuel:{TaxiFuel}")
        # **********************************************************
        #  Collect HvyBagPercent in a list
        # **********************************************************
        stpos = x
        endpos = stpos + nolgs
        for x in range(stpos, endpos):
            HvyBagPercent.append(int(flist[x]))
        x = x + 1
        #print(f"HvyBagPercent:{HvyBagPercent}")
        # **********************************************************
        #  Collect BagPerPsgrRatio in a list
        # **********************************************************
        stpos = x
        endpos = stpos + nolgs
        for x in range(stpos, endpos):
            BagPerPsgrRatio.append(int(flist[x]))
        x = x + 1
        #print(f"BagPerPsgrRatio:{BagPerPsgrRatio}")
        # **********************************************************
        #  Collect FuelLoad in a list
        # **********************************************************
        stpos = x
        endpos = stpos + nolgs
        for x in range(stpos, endpos):
            FuelLoad.append(int(flist[x]))
        x = x + 1
        #print(f"FuelLoad:{FuelLoad}")
        # **********************************************************
        #  Collect AirMailWgt in a list
        # **********************************************************
        stpos = x
        endpos = stpos + (nolgs * 2)
        for x in range(stpos, endpos):
            AirMailWgt.append(int(flist[x]))
        #print(nolgs)
        Airmail = []
        for i in range((nolgs)):
            c1 = [AirMailWgt[i * 2]] + [AirMailWgt[i * 2 + 1]]
            #print(c1)
            Airmail.append(c1)
        x = x + 1
        #print(f"AirMailWgt:{AirMailWgt}")
        # **********************************************************
        #  Collect FirstClassMailWgt in a list
        # **********************************************************
        stpos = x
        endpos = stpos + (nolgs * 2)
        for x in range(stpos, endpos):
            FirstClassMailWgt.append(int(flist[x]))
        FirstClass = []
        for i in range((nolgs)):
            c2 = [FirstClassMailWgt[i * 2]] + [FirstClassMailWgt[i * 2 + 1]]
            #print(c2)
            FirstClass.append(c2)
        x = x + 1
        #print(f"FirstClassMailWgt:{FirstClassMailWgt}")
        # **********************************************************
        #  Collect FirstFreightWgt in a list
        # **********************************************************
        stpos = x
        endpos = stpos + (nolgs * 2)
        for x in range(stpos, endpos):
            FirstFreightWgt.append(int(flist[x]))
        FirstFright = []
        for i in range((nolgs)):
            c3 = [FirstFreightWgt[i * 2]] + [FirstFreightWgt[i * 2 + 1]]
            #print(c3)
            FirstFright.append(c3)
        x = x + 1
        #print(f"FirstFreightWgt:{FirstFreightWgt}")
        # **********************************************************
        #  Collect FreightWgt in a list
        # **********************************************************
        stpos = x
        endpos = stpos + (nolgs * 2)
        for x in range(stpos, endpos):
            FreightWgt.append(int(flist[x]))
        Fright = []
        for i in range((nolgs)):
            c4 = [FreightWgt[i * 2]] + [FreightWgt[i * 2 + 1]]
            #print(c4)
            Fright.append(c4)
        x = x + 1
        #print(f"FreightWgt:{FreightWgt}")
        # **********************************************************
        #  Collect NumOfBags in a list
        # **********************************************************
        stpos = x
        endpos = stpos + (nolgs * 2)
        for x in range(stpos, endpos):
            NumOfBags.append(int(flist[x]))
        Num = []
        for i in range((nolgs)):
            #print(i)
            c5 = [NumOfBags[i * 2]] + [NumOfBags[i * 2 + 1]]
            #print(c5)
            Num.append(c5)
        #print(NumOfBags)
        x = x + 1
        #print(f"NumOfBags:{NumOfBags}")
        # **********************************************************
        #  Collect CompanyMaterial in a list
        # **********************************************************
        stpos = x
        endpos = stpos + (nolgs * 2)
        CompanyMaterial = []
        for x in range(stpos, endpos):
            CompanyMaterial.append(int(flist[x]))
        Company = []
        for i in range((nolgs)):
            c6 = [CompanyMaterial[i * 2]] + [CompanyMaterial[i * 2 + 1]]
            #print(c6)
            Company.append(c6)

        #x = x + 1
        #PayloadHistDepartureItem={}
        for x in range(nolgs):
            PayloadHistDepartureItem.append({'DepartureStn': DepartureStn[x],
                                             'ArrivalStn': ArrivalStn[x],
                                             'NumOfHistoricalWeeks': NumOfHistoricalWeeks[x],
                                             'SchedEqpType': SchedEqpType[x], 'RevPsgrsFwd': RevPsgrsFwd[x],
                                             'RevPsgrsRear': RevPsgrsRear[x],
                                             'PsgrAdjValue': PsgrAdjValue[x], 'KidCount': KidCount[x],
                                             'TaxiFuel': TaxiFuel[x], 'HvyBagPercent': HvyBagPercent[x],
                                             'BagPerPsgrRatio': BagPerPsgrRatio[x], 'FuelLoad': FuelLoad[x],
                                             'AirMailWgt': Airmail[x], 'FirstClassMailWgt': FirstClass[x],
                                             'FirstFreightWgt': FirstFright[x], 'FreightWgt': Fright[x],
                                             'NumOfBags': Num[x], 'CompanyMaterial': Company[x]})
        #print(f"CompanyMaterial:{CompanyMaterial}")
        PayloadHistHeader.update(
            {'DayOfWeek': DayOfWeek, 'NumOfLegs': NumOfLegs, 'LegHistNotUpdatedInd': LegHistNotUpdatedInd,
             'FlightNum': FlightNum})
        PayloadHistDeparture.update({'PayloadHistDepartureItem': PayloadHistDepartureItem})
        PayloadHistData.update({'PayloadHistHeader': PayloadHistHeader, 'PayloadHistDeparture': PayloadHistDeparture})
        #print(PayloadHistData)
        jPayloadHistData = json.dumps(PayloadHistData)
        jPayloadHistDataList.append(jPayloadHistData)

    except Exception as e:
        print(f'Error in parsing line function: {e}')


def load_data(devices, dynamodb=None):
    try:
        dynamodb = boto3.resource('dynamodb')
        devices_table = dynamodb.Table('ccl-WBTransData')
        with devices_table.batch_writer() as writer:
            for device in devices:
                out = {}
                data = json.loads(device)
                out = data
                out['WBTransPK'] = 'PHF'
                out['WBTransSK'] = str(out.get('PayloadHistHeader').get('FlightNum')) + '#' + str(
                    out.get('PayloadHistHeader').get('DayOfWeek'))
                print(out)

                response = writer.put_item(Item=out)
                print(response)
    except Exception as e:
        print(f'Error in load_data, while loading data into dynamodb: {e}')

def download_from_sftp():
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Disable host key checking (optional)

    with pysftp.Connection(host=SFTP_HOST, username=SFTP_USERNAME, password=SFTP_PASSWORD, port=SFTP_PORT, cnopts=cnopts) as sftp:
        sftp.get(SFTP_FILE_PATH, LOCAL_FILE_PATH)
        print(f"Downloaded {SFTP_FILE_PATH} to {LOCAL_FILE_PATH}")

def upload_to_s3():
    s3 = boto3.client('s3')

    try:
        s3.upload_file(LOCAL_FILE_PATH, S3_BUCKET_NAME, S3_FILE_KEY)
        print(f"Uploaded {LOCAL_FILE_PATH} to s3://{S3_BUCKET_NAME}/{S3_FILE_KEY}")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")


# **********************************************************
# Main Program
# **********************************************************
#download_from_sftp()
#upload_to_s3()
PayloadHistcount = 0
s3_client = boto3.client('s3')
file_content = ""
try:
    # Get the file object from S3
    response = s3_client.get_object(Bucket="ccl-s3-qa", Key="ccl-image/Nayana/WBFPHF.txt")

    # Read the file content
    file_content = response['Body'].read().decode('utf-8')

    # Split the text using \r\n as the delimiter
    file_content = file_content.split('\r\n')

    # Print the resulting list
    print("List of lines:")
    print(file_content)

    # Process the file content as needed
    print(f'File content: {file_content}')

except (NoCredentialsError, PartialCredentialsError) as e:
    print(f'Credentials error: {e}')

except Exception as e:
    print(f'Error: {e}')

try:
    for linex in file_content:
        #print("*****************************************************************************")
        jStationConfig = ""
        PayloadHistcount = PayloadHistcount + 1
        doinit()
        if bool(linex) and not linex.isspace():
            flist = linex.split('|')
            parseLine()
    strPayloadHistData = str(jPayloadHistDataList)
    strPayloadHistData = strPayloadHistData.replace("'", "")
    load_data(jPayloadHistDataList)
    print(f"*****************************************************************************")
    print(f"WBFPHF Data loading completed")
    print(f"Number of WBFPHF Records loaded: {PayloadHistcount}")
    print(f"*****************************************************************************")
except Exception as e:
    print(f'Error while loading data into dynamodb: {e}')

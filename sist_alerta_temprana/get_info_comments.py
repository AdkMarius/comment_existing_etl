from datetime import datetime
import requests
import json
import pandas as pd
import logging
from logging_utils import setup_logging
from main import PATH

# Setup logging for the script
setup_logging()

# Get the current date
current_date = datetime.now().date()

# Define the path to the CSV file
csv_path = PATH + 'dataorg.csv'

# Load the CSV file into a pandas DataFrame
illnes = pd.read_csv(csv_path)

# Filter and get names of illnesses where dataelement is 'DailyDeath' (the first seven names in the files)
names_illnes = list(illnes.loc[illnes['dataelement'] == 'DailyDeath', 'name'])

# Filter and get UIDs based on different data elements
result1 = list(illnes.loc[illnes['dataelement'] == 'DailyDeath', 'uid'])
result2 = list(illnes.loc[illnes['dataelement'] == 'DailyConfirmedt', 'uid'])
result3 = list(illnes.loc[illnes['dataelement'] == 'DailyRecoveredt', 'uid'])
result4 = list(illnes.loc[illnes['dataelement'] == 'DailySolvedt', 'uid'])
result5 = list(illnes.loc[illnes['dataelement'] == 'ActiveCasest', 'uid'])
result6 = list(illnes.loc[illnes['dataelement'] == 'PreActiveCasest', 'uid'])
result7 = list(illnes.loc[illnes['dataelement'] == 'DeathsPercentaget', 'uid'])
result8 = list(illnes.loc[illnes['dataelement'] == 'RecoveredPercentaget', 'uid'])
result9 = list(illnes.loc[illnes['dataelement'] == 'aHSI', 'uid'])
resulta = list(illnes.loc[illnes['dataelement'] == 'dHSI', 'uid'])
resultb = list(illnes.loc[illnes['dataelement'] == 'hPORt', 'uid'])
resultc = list(illnes.loc[illnes['dataelement'] == 'icuPORr', 'uid'])
resultd = list(illnes.loc[illnes['dataelement'] == 'A', 'uid'])
resulte = list(illnes.loc[illnes['dataelement'] == 'PHC', 'uid'])
resultf = list(illnes.loc[illnes['dataelement'] == 'Bt', 'uid'])
resultg = list(illnes.loc[illnes['dataelement'] == 'It', 'uid'])
resulth = list(illnes.loc[illnes['dataelement'] == 'Solvedt', 'uid'])
resulti = list(illnes.loc[illnes['dataelement'] == 'Suspected', 'uid'])
resultcon = list(illnes.loc[illnes['dataelement'] == 'Confirmed', 'uid'])
resultj = list(illnes.loc[illnes['dataelement'] == 'Deaths', 'uid'])
resultk = list(illnes.loc[illnes['dataelement'] == 'Recovered', 'uid'])
resultl = list(illnes.loc[illnes['dataelement'] == 'Gross mortality rate', 'uid'])
resultm = list(illnes.loc[illnes['dataelement'] == 'Autopsy rate', 'uid'])
resultn = list(illnes.loc[illnes['dataelement'] == 'Newborn rate', 'uid'])
resulto = list(illnes.loc[illnes['dataelement'] == 'surgery', 'uid'])
resultp = list(illnes.loc[illnes['dataelement'] == 'surgical', 'uid'])
resultq = list(illnes.loc[illnes['dataelement'] == 'Cesarean rate', 'uid'])
resultr = list(illnes.loc[illnes['dataelement'] == 'Vaginal rate', 'uid'])
results = list(illnes.loc[illnes['dataelement'] == 'weekend surgical', 'uid'])
resultt = list(illnes.loc[illnes['dataelement'] == 'B', 'uid'])
resultu = list(illnes.loc[illnes['dataelement'] == 'Episodes surgery', 'uid'])
resultx = list(illnes.loc[illnes['dataelement'] == 'Turnover index', 'uid'])
resultz = list(illnes.loc[illnes['dataelement'] == 'Average stay', 'uid'])

# Function to log in and create a session with basic authentication
def login():
    session = requests.Session()
    session.auth = ('admin', 'district')
    return session

# Get Info about DataSets filtered by name
def getDataSetInfoByName(name, namedhis2, portdhis2):
    """
    :param name: name used to filter into data sets
    :param namedhis2: DHIS2 server name
    :param portdhis2: DHIS2 server port
    :return: This method returns the basic information about a specific dataset filtered by id
    """
    try:
        session = login()
        url = f"http://{namedhis2}:{portdhis2}/api/dataSets.json?"
        params = {
            "filter": f"name:like:{name}"
        }
        response = session.get(url, headers={'content-type': 'application/json'}, params=params)
        # Uncomment the following line to log the response
        # changeLog(f"[FETCHING DATA-SET INFO] >> {response.text}")
        return response.text
    except Exception as e:
        print(e)
        # Uncomment the following line to log unexpected exceptions
        # changeLog("Unexpected exception")

# Get Info about Organisation Units filtered by name
def getOrgUnitInfoByName(name, namedhis2, portdhis2):
    """
    :param name: name used to filter into OrgUnits
    :param namedhis2: DHIS2 server name
    :param portdhis2: DHIS2 server port
    :return: This method returns the basic information about a specific organisation unit filtered by id
    """
    try:
        session = login()
        url = f"http://{namedhis2}:{portdhis2}/api/organisationUnits.json?"
        params = {
            "filter": f"name:like:{name}"
        }
        response = session.get(url, headers={'content-type': 'application/json'}, params=params)
        # Uncomment the following line to log the response
        # changeLog(f"[FETCHING ORG-UNIT INFO] >> {response.text}")
        return response.text
    except Exception as e:
        print(e)

# Get Info about DataElements filtered by name
def getDataElementInfoByName(name, namedhis2, portdhis2):
    """
    :param name: name used to filter into Data Elements
    :param namedhis2: DHIS2 server name
    :param portdhis2: DHIS2 server port
    :return: This method returns the basic information about a specific data element filtered by id
    """
    try:
        session = login()
        url = f"http://{namedhis2}:{portdhis2}/dataElements.json?"
        params = {
            "filter": f"name:like:{name}"
        }
        response = session.get(url, headers={'content-type': 'application/json'}, params=params)
        # Uncomment the following line to log the response
        # changeLog(f"[FETCHING DATA-ELEMENT INFO] >> {response.text}")
        return response.text
    except Exception as e:
        print(e)
        # Uncomment the following line to log unexpected exceptions
        # changeLog("Unexpected exception")


# Function to add data values to a dataset
def addDataValue(DataSetName: str, OrgUnitName: str, namedhis2, portdhis2, DataValueSets):
    # Getting data set id for payload
    DataSetLoaded = json.loads(getDataSetInfoByName(DataSetName, namedhis2, portdhis2))
    DataSetId = DataSetLoaded['dataSets'][0]['id']

    # Getting orgUnit id for payload
    OrgUnitLoaded = json.loads(getOrgUnitInfoByName(OrgUnitName, namedhis2, portdhis2))
    OrgUnitId = OrgUnitLoaded['organisationUnits'][0]['id']

    # Logging for converting data element name to data element id
    # changeLog(f"[PROCESSING DATA] >> " + "Converting dataelement name to data element id")
    DataValueSets_loaded = json.loads(DataValueSets)
    if not DataValueSets_loaded:
        print('No data found')
    else:
        diccion = DataValueSets_loaded[0]

        payloads = []
        # Create payloads for each date and corresponding data values
        for fecha, sub_diccionario in diccion.items():
            sub_diccionario_lista = [sub_diccionario]
            payload = {
                "dataSet": DataSetId,
                "completeDate": f"{current_date.strftime('%Y-%m-%d')}",
                "period": fecha,
                "orgUnit": OrgUnitId,
                "dataValues": sub_diccionario_lista
            }
            payloads.append(payload)

        for payload in payloads:
            try:
                session = login()
                url = "http://" + namedhis2 + ':' + portdhis2 + "/api/dataValueSets"
                response = session.post(url, json=payload, headers={'content-type': 'application/json'})
                print(payload)
                data = response.json()
                http_status_code = data.get('httpStatusCode')
                status = data.get('status')
                import_count = data.get('response', {}).get('importCount')
                logging.info(f"{status} + {http_status_code} + {import_count}")
                print(f"status:{status}-http_status_code:{http_status_code}-import_count:{import_count}")
                # Uncomment to print the full response text
                # print(response.text)
            except Exception as e:
                print(e)


# Function to process and add data values
def valuedata(datasetname, Orgunits_name, dhis2_name, dhis2_port, Nueva_Columna):
    for i in Nueva_Columna:
        addDataValue(datasetname, Orgunits_name, dhis2_name, dhis2_port, json.dumps(i))


# Function to add multiple data elements to a dataset
def add_data(pacience):
    for elementos in pacience:
        user, password, DDBB_name, Hostname, Orgunits_code, Orgunits_name, ports, dhis2_name, dhis2_port, \
            Nueva_Columna, Nueva_Columna2, Nueva_Columna3, Nueva_Columna4, Nueva_Columna5, Nueva_Columna8, \
            Nueva_Columnag, Nueva_Columnas, Nueva_Columnat, Nueva_Columnau, Nueva_Columnaw = elementos

        datasetname1 = illnes['datasetName'][0]
        datasetname2 = illnes['datasetName'][100]
        datasetname3 = illnes['datasetName'][255]

        logging.info(f"starting add data")
        print(f"starting add data")

        # Process different sets of new columns for each dataset
        valuedata(datasetname1, Orgunits_name, dhis2_name, str(dhis2_port), Nueva_Columnag)
        valuedata(datasetname2, Orgunits_name, dhis2_name, str(dhis2_port), Nueva_Columna8)

        addDataValue(datasetname3, Orgunits_name, dhis2_name, str(dhis2_port), json.dumps(Nueva_Columna))
        addDataValue(datasetname3, Orgunits_name, dhis2_name, str(dhis2_port), json.dumps(Nueva_Columna2))
        addDataValue(datasetname3, Orgunits_name, dhis2_name, str(dhis2_port), json.dumps(Nueva_Columna3))
        addDataValue(datasetname3, Orgunits_name, dhis2_name, str(dhis2_port), json.dumps(Nueva_Columna4))
        addDataValue(datasetname3, Orgunits_name, dhis2_name, str(dhis2_port), json.dumps(Nueva_Columna5))
        addDataValue(datasetname3, Orgunits_name, dhis2_name, str(dhis2_port), json.dumps(Nueva_Columnas))
        addDataValue(datasetname3, Orgunits_name, dhis2_name, str(dhis2_port), json.dumps(Nueva_Columnat))
        addDataValue(datasetname3, Orgunits_name, dhis2_name, str(dhis2_port), json.dumps(Nueva_Columnau))
        # Uncomment to process additional columns if needed
        # addDataValue(datasetname3, Orgunits_name, dhis2_name, str(dhis2_port), json.dumps(Nueva_Columnaw))
        # addDataValue(datasetname3, Orgunits_name, dhis2_name, str(dhis2_port), json.dumps(Nueva_Columnay))
        # addDataValue(datasetname3, Orgunits_name, dhis2_name, str(dhis2_port), json.dumps(Nueva_Columnaz))

        logging.info(f"Process completed for {Orgunits_name}")

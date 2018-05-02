import requests
import json
import pandas
import numpy

flexioPaginationLength = 100


def get_resource(flexioURL, account, resourceName, auth, header="", fields=[]):
    requestURL = flexioURL + '/' + account + '/' + resourceName
    rangeFrom = 0
    dataset = pandas.DataFrame()  # TODO Definir structure de donnees (dataframe ?)

    dots = ['.   ', '..  ', '... ']
    doti = 0

    while True:
        # if verbose:
        # TODO Print \r
        range = '{}-{}'.format(rangeFrom, rangeFrom + flexioPaginationLength-1)
        print("Getting records {} from Flexio  {}".format(range, dots[doti]))
        doti = 0 if (doti == 2) else doti + 1

        req = requests.get(requestURL, headers={'Authorization': auth, 'range': range})
        #TODO Gerer header

        if req.status_code not in [200, 206]:
            # TODO Print \r
            print(req.text)
            # TODO Trouver le bon champ de req
            return None

        jason = str(req.text.replace('None', '"None"'))
        datasetnew = pandas.read_json(jason, orient='records')
        dataset = dataset.append(datasetnew, ignore_index=True)

        rangeFrom = rangeFrom + flexioPaginationLength
        if req.status_code == 200:
            break
        # if verbose:
        # TODO Print \r

    schema = get_resource_fields_types(
        flexioURL=flexioURL,
        account=account,
        resourceName=resourceName,
        auth=auth
    )

    # TODO Convertir les colonnes si besoin

    dataset = dataset.filter(schema['names'])

    if len(fields) != 0:
        dataset = dataset.filter(fields)

    return dataset


def get_resource_schema(flexioURL, account, resourceName, auth):
    requestURL = flexioURL + '/' + account + '/' + resourceName + '/schema'
    req = requests.get(requestURL, headers={'Authorization': auth})
    if req.status_code not in [200]:
        return None
        #TODO Print error message
    return json.loads(req.text)


def get_resource_fields_types(flexioURL, account, resourceName, auth):
    schema = get_resource_schema(
        flexioURL=flexioURL,
        account=account,
        resourceName=resourceName,
        auth=auth
    )

    types = pandas.DataFrame({'names': [], 'types': []})
    for i in range(len(schema['properties'])):
        types = types.append({'names': schema['properties'][i]['name'], 'types': schema['properties'][i]['data-type']}, ignore_index=True)
    return types


def post_resource(flexioURL, account, resourceName, auth, data, verbose=False):
    requestURL = flexioURL + '/' + account + '/' + resourceName
    dots = ['.   ', '..  ', '... ']
    doti = 0

    print(data)

    n = len(data)
    for entry in range(n):
        line = data.loc[[entry]]
        jason = line.to_json(orient='records')[1:-1]
        print(jason)
        req = requests.post(url=requestURL, data=jason, headers={'Authorization': auth, 'Content-type':'application/json'})
        if req.status_code not in [201]:
            print(req.text)
            return False

    return True


def get_record(flexioURL, account, resourceName, auth, recordID, fields=[], verbose=False):
    requestURL = flexioURL + '/' + account + '/' + resourceName + '/' + recordID

    req = requests.get(url=requestURL, headers={'Authorization': auth})
    if req.status_code not in [200]:
        return None

    print(req.text)
    jason = str(req.text)
    record = pandas.read_json(jason, orient='records')
    record = record.append(datasetnew, ignore_index=True)
    return record



def putRecord(flexioURL, account, resourceName, auth, recordID, data, verbose=False):
    print("#TODO")


def patchRecord(flexioURL, account, resourceName, auth, recordID, data, fields=[], verbose=False):
    print("#TODO")


def deleteRecord(flexioURL, account, resourceName, recordID, auth, verbose=False):
    print("#TODO")


def castStringToNum():
    print("#TODO")


def castDATETIMEToTime():
    print("#TODO")


def castTIMEToTime():
    print("#TODO")


def setFieldNames():
    print("#TODO")


def splitDataset():
    print("#TODO")


def cleanDataset():
    print("#TODO")


def saveDatasetToFile():
    print("#TODO")

import pandas as pd
import requests
import gc
import tqdm
import time
from datetime import datetime
import json
import logging
import os
import smtplib
import func_timeout
import schedule
from typing import Any, Callable

# disable SettingWithCopyWarning
pd.options.mode.chained_assignment = None

def sendemail(gmail_user, gmail_password, send_to):
    #body = get_data_from_link.err.__class__.__name__ + ' occured at ' + str(
    body = 'Error occured at ' + str(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # Error type and occurence time in the message
    email_text = """\
    From: %s,


    %s
    """ % (gmail_user, body)

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)  # Setting the server for Gmail (you may need to set different parameters for your mailbox)
        server.ehlo()
        server.login(gmail_user,
                     gmail_password)  # Authentication of gmail account, application key is required - to be set on google account
        server.sendmail(gmail_user, send_to, email_text)
        server.close()
        print('email sent')
    except:
        print('SOMETHING WENT TERRIBLY WRONG WHEN SENDING THE EMAIL! Have you provided parameters for sendmail function?')


def get_data_from_link(link: str) -> dict:
    """
    Geta data from API UM Warszawa based on a link
    Arguments:
        link: API request link
    Returns:
        A dictionary in JSON format, which will be used for creating dataframes
    """

    def make_json_dictionary() -> dict:
        """
        Output a JSON dictionary based on a link
        """
        requested_data = requests.get(link)
        return requested_data.json()

    def run_function(f: Callable, max_wait: int, default_value: Any):
        """
        Run make_json_dictionary function, if it takes longer the 'max_wait'
        seconds to complete, output 'default_value'
        Arguments:
            f: a callable function (in this case make_json_dictionary)
            max_wait: maximal number of seconds we want the f function to run for
        Return:
            Output of make_json_dictionary function or 'default_value'

        """
        try:
            return func_timeout.func_timeout(max_wait, make_json_dictionary)
        except func_timeout.FunctionTimedOut:
            pass
        return default_value

    for i in range(1, 6):
        try:
            json_dictionary = run_function(make_json_dictionary, 10, None)
        except AttributeError as err:
            logs.error('Attribute error occurred! ' + str(err))
            continue
        except (ConnectionError, TimeoutError) as err:
            logs.error('Connection error occurred! ' + str(err))
            continue
        except TimeoutError as err:
            logs.error('Timeout Error occurred! ' + str(err))
            continue
        except OSError as err:
            logs.error('OS error occurred! ' + str(err))
            continue
        except NotImplementedError as err:
            logs.error('NotImplementedError Error occurred! ' + str(err))
            continue
        except KeyError as err:
            logs.error('Key Error occurred! ' + str(err))
            continue
        except Exception:
            logs.error('Unknown exception accured. Sending email just to let you know.')
            sendemail(gmail_user, gmail_password, send_to) # provide your email credentials along with specific app password
        if (json_dictionary != None) and ('result' in json_dictionary) and (
                type(json_dictionary['result']) == type([])):
            break
        else:
            logs.error(
                f'Failed at generating proper json_dictionary object. Attempt {i} of 5. Waiting for 60 seconds...')
            time.sleep(60)

    if json_dictionary == None:
        logs.error(f"Serious lag on UM Warszawa API's end")
        logs.error('Ending script! Sending email just to let you know.')
        sendemail(gmail_user, gmail_password, send_to) # provide your email credentials along with specific app password
        exit()

    elif 'result' not in json_dictionary:
        logs.error(f'No result key in dictionary')
        logs.error('Script crashed! Sending email just to let you know.')
        sendemail(gmail_user, gmail_password, send_to) # provide your email credentials along with specific app password
        exit()

    elif type(json_dictionary['result']) != type([]):
        logs.error(f"Error from UM Warszawa API::: {json_dictionary['result']}")
        logs.error('Script crashed! Sending email just to let you know.')
        sendemail(gmail_user, gmail_password, send_to) # provide your email credentials along with specific app password
        exit()

    return json_dictionary


def make_stops_table(API_KEY: str) -> pd.DataFrame:
    """
    Make a DataFrame with basic stops data
    Arguments:
        API_KEY: api key from credentials.json
    Returns:
        Dataframe with data like geo coordinates of stops
    """
    # info about stops from current day
    stops_link = 'https://api.um.warszawa.pl/api/action/dbstore_get/?id=1c08a38c-ae09-46d2-8926-4f9d25cb0630&apikey=' \
                 + API_KEY

    # make a request for the API
    json_dictionary = get_data_from_link(stops_link)

    # create dataframe
    df = pd.json_normalize(json_dictionary['result'])

    # all values are in a format:
    # {'value': '01', 'key': 'slupek'},
    # {'value': 'Kijowska', 'key': 'nazwa_zespolu'},
    # {'value': '2201', 'key': 'id_ulicy'},
    # ...

    # get column names based on keys from first observation
    column_names = df['values'].apply(pd.Series).iloc[0].apply(lambda x: x.get('key')).tolist()

    # apply column names to dataframe
    df = df['values'].apply(pd.Series)
    df.columns = column_names

    # get values from dictionary and use them as values in dataframe
    for col in column_names:
        df[col] = df[col].apply(lambda x: x.get('value'))

    return df


def add_lines_to_stops_table(df: pd.DataFrame, API_KEY: str) -> pd.DataFrame:
    """
    Send a request to every stop about line numbers being used on that stop
    Arguments:f
        df: dataframe with stops informaction e.g. with 'zespół' and 'słupek'
        API_KEY: api key from credentials.json
    Returns:
        Original dataframe but with an extra column ('linie') containing every
        line for every stop
    """
    df['linie'] = None  # insert empty column

    # for every entry make a request about stop informations
    for index, row in tqdm.tqdm(df[['zespol', 'slupek']].iterrows(), total=df.shape[0]):

        stop_watch = time.time()
        zespol = row['zespol']
        slupek = row['slupek']

        link = 'https://api.um.warszawa.pl/api/action/dbtimetable_get/?id=88cd555f-6f31-43ca-9de4-66c479ad5942&busstopId=' \
               + zespol + '&busstopNr=' + slupek + '&apikey=' + API_KEY

        json_dictionary = get_data_from_link(link)

        # transform the information into line numbers
        lines = [elem.get('values')[0].get('value') for elem in json_dictionary['result']]

        # insert line numbers into the dataframe
        df.loc[index, 'linie'] = lines

    # leave only active stops and discard the rest
    df = df[df['linie'].map(lambda x: len(x) > 0)]

    return df


def make_timetables_for_lines(df: pd.DataFrame, API_KEY: str, only_trams: bool = False) -> pd.DataFrame:
    """
    Send a request to every line number on every stop about the timetable for
    that particulat line on that particular stop
    Arguments:
        df: DataFrame with stops data one line numbers (with the extra column 'linie)
        API_KEY: api key from credentials.json
        only_trams: do we want data only for trams or for all types of vehicles
    Return:
        Table with every timetable for every line in every stop
    """

    # set a vehicle type based on its number
    df[
        'typ'] = 'A'  # first assign 'A' to all types of vehicles

    df.loc[df['linie'].apply(lambda x: str(x[0])[0] == 'W'), 'typ'] = 'WKD'  # assign WKD label
    df.loc[df['linie'].apply(lambda x: str(x[0])[0] == 'R'), 'typ'] = 'R'  # assign KM train label
    df.loc[df['linie'].apply(lambda x: str(x[0])[0] == 'S'), 'typ'] = 'S'  # assign SKM train label
    df.loc[df['linie'].apply(lambda x: str(x[0])[0] == 'M'), 'typ'] = 'M'  # assign metro train label
    df = df.reset_index()

    # find the rows, that have 'A' and their length isn't greater then 2
    less_then_2_index = df[(df.typ == 'A') & (df.linie.apply(lambda x: len(x[0]) <= 2))].index.tolist()

    df.loc[less_then_2_index, 'typ'] = 'T'  # assign tram label

    # restrict dataframe to only trams in onlty_trams == True
    if only_trams:
        df = df[df['typ'] == 'T']

    lines_column = []; trasa_column = []; brygada_column = []

    # for every line number on every stop get the timetable
    for index, row in tqdm.tqdm(df.iterrows(), total=df.shape[0]):
        zespol = row['zespol']
        slupek = row['slupek']
        linie = row['linie']

        czas_list = []
        brygada_list = []
        trasa_list = []
        for linia in linie:
            link = 'https://api.um.warszawa.pl/api/action/dbtimetable_get/?id=e923fa0e-d96c-43f9-ae6e-60518c9f3238&busstopId=' \
                   + zespol + '&busstopNr=' + slupek + '&line=' + linia + '&apikey=' + API_KEY

            # make request to the API
            json_dictionary = get_data_from_link(link)

            # get timetable for line number 'linia'
            czas = tuple(d['value'] for d in
                         [json_dictionary['result'][n].get('values')[5] for n in range(len(json_dictionary['result']))])
            brygada = tuple(d['value'] for d in \
                [json_dictionary['result'][n].get('values')[2] for n in range(len(json_dictionary['result']))])
            trasa = tuple(d['value'] for d in  \
                [json_dictionary['result'][n].get('values')[4] for n in range(len(json_dictionary['result']))])

            # delete seconds from timetable
            czas = tuple([elem[:-3] if len(czas) >= 1 else elem for elem in czas])
            czas_list.append(czas)
            brygada_list.append(brygada)
            trasa_list.append(trasa)

        lines_column.append(dict(zip(linie, czas_list)))
        brygada_column.append(dict(zip(linie, brygada_list)))
        trasa_column.append(dict(zip(linie, trasa_list)))

    df['linie'] = lines_column
    df['brygada'] = brygada_column
    df['trasa'] = trasa_column
    return df


def init_logging(logs: logging.Logger, file_name: str) -> logging.Logger:
    """
    Log information to the console and file
    Arguments:
        logs: Default logger from logging module
        file_name: log filename
    Returns:
        Logs object with specified log format and name of log file
    """
    logs.setLevel(logging.DEBUG)

    logformat = logging.Formatter("%(asctime)s : %(levelname)s : %(message)s", datefmt='%y-%m-%d %H:%M:%S')

    # log to file
    file = logging.FileHandler(file_name)
    file.setLevel(logging.INFO)
    file.setFormatter(logformat)

    # log to console
    stream = logging.StreamHandler()
    stream.setLevel(logging.INFO)
    stream.setFormatter(logformat)

    logs.addHandler(stream)
    logs.addHandler(file)

    return logs


def load_api_key(credentials_file_name: str = 'credentials.json'):
    """
    Get API key from file
    Arguments:
        credentials_file_name: name of file where the API key is located
    Returns:
        API key needed for further requests
    """
    if os.path.exists(credentials_file_name):
        with open(credentials_file_name) as f:
            API_KEY = json.load(f)['API_KEY']
    else:
        init_logging.logs.error(f"No file {credentials_file_name} with proper API key")
        input('Press any key to end.')
        exit()

    return API_KEY


def run_script():
    if __name__ == '__main__':

        # current datetime
        run_script.now = datetime.now().strftime("%Y-%m-%d")
        '''
        gather info about all types of vehicles or just trams
        while True:
            only_trams = input('What to download? \n [0] - whole timetable; [1] - only trams ')
            if only_trams not in ['0', '1']:
                print('Enter proper value (0 or 1)!')
                continue
            else:
                only_trams = bool(int(only_trams))
                break
        '''
        only_trams = True

        # log to file and console
        global logs
        logs = logging.getLogger(__name__)
        logs = init_logging(logs, 'StopsLog.log')

        # get API key
        API_KEY = load_api_key()

        logs.info('Downloading basic stops information...')
        df = make_stops_table(API_KEY)

        logs.info('Downloading line numbers for stops...')
        df = add_lines_to_stops_table(df, API_KEY)

        logs.info('Downloading timetables for all lines...')
        df = make_timetables_for_lines(df, API_KEY, only_trams=only_trams)

        logs.info(f'Saving data to rozklady_{run_script.now}.pkl')
        try:
            df.to_pickle(f'rozklady_{run_script.now}.pkl', compression='zip')
        except Exception as err:
            logs.error(err)

        logs.info('Deleting unnecessary data from memory...')
        del df
        gc.collect()

        logs.info('Download completed. The script will restart at 10:00')

print('The script will start running every day at 10:00 ...')
schedule.every().day.at("13:52").do(run_script)

while True:
    schedule.run_pending()

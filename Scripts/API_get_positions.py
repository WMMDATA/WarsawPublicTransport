import pandas as pd
import requests
import time
import json
import os
import smtplib
import logging
from datetime import datetime

# Working version - saving to TXT files

def set_API(API_KEY, resource_id):
    vehicle_type = input('Insert "1" for buses or "2" for trams ') #API link parameter
    if vehicle_type == 1:
        set_API.prefix = 'buses_'
    else:
        set_API.prefix = 'trams_'
    set_API.target_time = datetime.strptime(input('Until when to collect data? (please input datetime in format: YYYY-MM-DD HH:MM:SS) - default: till the end of year 2023 ') or '2023-12-31 23:59:59', '%Y-%m-%d %H:%M:%S') #Datetime to which the while lopp will run
    print('https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id=%20' + resource_id \
    + '&apikey=' + API_KEY \
    + '&type=' + vehicle_type)
    set_API.link = 'https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id=%20' + resource_id \
    + '&apikey=' + API_KEY \
    + '&type=' + vehicle_type


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

    logformat = logging.Formatter("%(asctime)s : %(levelname)s : %(message)s",datefmt='%y-%m-%d %H:%M:%S')

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


def sendemail(gmail_user, gmail_password, sent_to):
    body = run_script.err.__class__.__name__ + ' occured at ' + str(run_script.current_time) #Error type and occurence time in the message
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


def run_script():
    API_KEY = load_api_key()
    set_API(API_KEY, resource_id = 'f2e5503e-927d-4ad3-9500-4ab9e55deb59')
    requested_data = requests.get(set_API.link)
    json_dictionary = requested_data.json()
    df = pd.json_normalize(json_dictionary['result'])


    current_time = datetime.strptime(df['Time'][0], '%Y-%m-%d %H:%M:%S')
    base_folder = input('Wskaż folder zapisu danych: ') or str(os.getcwd()) #By default it gets the project's directory
    logs = logging.getLogger(__name__)
    logs = init_logging(logs, 'PositionsLog.log')
    logs.info('Rozpoczęcie zbierania danych...')
    os.makedirs(os.path.join(base_folder, str(current_time.month) + '_' + str(current_time.year)), exist_ok = True) #Create a directory named 'MONTH_YEAR' in the set CWD
    os.chdir((os.path.join(base_folder, str(current_time.month) + '_' + str(current_time.year))))
    cwd = os.getcwd()

    while current_time < set_API.target_time:
        try:
            requested_data = requests.get(set_API.link)
            json_dictionary = requested_data.json()
            df = pd.json_normalize(json_dictionary['result'])
            current_time = datetime.now().replace(microsecond=0)
            file_name = 'trams_' + str(current_time.year) + '_' + str(current_time.month) + '_' + str(current_time.day) + '_' + str(current_time.hour) + '.txt'
            with open(os.path.join(cwd, file_name), 'a') as f:
                f.write(str(current_time) + '\n')
                json.dump(json_dictionary, f)
                f.write('\n\n')
                f.close()
                time.sleep(30)
                new_time = datetime.strptime(df['Time'].iloc[-1], '%Y-%m-%d %H:%M:%S')
                if new_time.day != current_time.day:
                    os.chdir(base_folder)
                    os.makedirs(os.path.join(base_folder, str(new_time.month) + '_' + str(new_time.year)), exist_ok = True)
                    os.chdir((os.path.join(base_folder, str(new_time.month) + '_' + str(new_time.year))))
                    cwd = os.getcwd()
        except AttributeError as err:
            logs.error('Attribute error occurred! ' + str(err))
        except (ConnectionError, TimeoutError) as err:
            logs.error('Connection error occurred! ' + str(err))
        except OSError as err:
            logs.error('OS error occurred! ' + str(err))
        except NotImplementedError as err:
            logs.error('NotImplementedError Error occurred! ' + str(err))
        except KeyError as err:
            logs.error('Key Error occurred! ' + str(err))
        except Exception:
            sendemail(gmail_user, gmail_password, send_to) # provide your email credentials along with specific app password
            continue

run_script()

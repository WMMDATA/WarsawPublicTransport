import pandas as pd
import requests
import time
import json
import os
import smtplib
import logging
from datetime import datetime

# Working version - saving to TXT files

def set_API(apikey, resource_id): # Generate your own API key here: https://api.um.warszawa.pl/# --> Hit 'logowanie' and then under 'Rejestracja konta' provide a login and password
    vehicle_type = input('Wpisz "1" dla autobusów lub "2" dla tramwajów ') #API link parameter
    if vehicle_type == 1:
        set_API.prefix = 'buses_'
    else: 
        set_API.prefix = 'trams_'
    set_API.target_time = datetime.strptime(input('Do kiedy zbierać dane? (w formacie: YYYY-MM-DD HH:MM:SS) - domyślnie do końca 2023 roku') or '2023-12-31 23:59:59', '%Y-%m-%d %H:%M:%S') #Datetime to which the while lopp will run
    set_API.link = 'https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id=%20' + resource_id \
    + '&apikey=' + apikey \
    + '&type=' + vehicle_type

    
def init_logging(logs: logging.Logger, file_name: str) -> logging.Logger:
    """
    Funkcja do logowania informacji jednocześnie do konsoli i do pliku

    Arguments:
        logs: Domyślny logger z modułu logging
        file_name: nazwa pliku, do którego mają być logowane dane
    
    Returns:
        Obiekt 'logs' z określonym formatem wpisów jakie mają być w nim umieszczane i
        wskazanym plikiem do zapisywania logów.
    """
    logs.setLevel(logging.DEBUG)
    
    logformat = logging.Formatter("%(asctime)s : %(levelname)s : %(message)s",datefmt='%y-%m-%d %H:%M:%S')

    # logowanie do pliku
    file = logging.FileHandler(file_name)
    file.setLevel(logging.INFO)
    file.setFormatter(logformat)
    
    # logowanie do konsoli
    stream = logging.StreamHandler()
    stream.setLevel(logging.INFO)
    stream.setFormatter(logformat)

    logs.addHandler(stream)    
    logs.addHandler(file)

    return logs
    
def sendemail(from_email, application_password, to_email): # The script is configured send emails from Gmail account - insert your parameters here.
    body = run_script.err.__class__.__name__ + ' occured at ' + str(run_script.current_time) #Error type and occurence time in the message
    email_text = """\
    From: %s,


    %s
    """ % (gmail_user, body)

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465) #Setting server to gmail
        server.ehlo()
        server.login(gmail_user, gmail_password) #Authentication of gmail account, application key is required - to be set on google account
        server.sendmail(gmail_user, sent_to, email_text)
        server.close()
        print('email sent')
    except:
        print('SOMETHING WENT TERRIBLY WRONG WHEN SENDING EMAIL!')

def run_script():
    set_API(apikey, 'f2e5503e-927d-4ad3-9500-4ab9e55deb59') 
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
        except Exception as err:
            sendemail(from_email, application_password, to_email) 
            continue

run_script()
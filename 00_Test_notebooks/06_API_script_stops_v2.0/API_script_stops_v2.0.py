import pandas as pd
import requests
import gc
import tqdm
from datetime import datetime
import json
import pickle
import logging
import os

# disable SettingWithCopyWarning
pd.options.mode.chained_assignment = None

def get_data_from_link(link: str) -> dict:
    '''
    Pobierz dane z API UM Warszawa na podstawie linku

    Arguments:
        link: link z zapytaniem do API
    
    Returns:
        Słownik w formacie JSON, będący podstawą do utworzenia tabel
    '''
    try:
        requested_data = requests.get(link)
        json_dictionary = requested_data.json()

    except AttributeError as err:
        logs.error('Attribute error occurred! ' + err)

    except (ConnectionError, TimeoutError) as err:
        logs.error('Connection error occurred! ' + err)

    except OSError as err:
        logs.error('OS error occurred! ' + err)

    except NotImplementedError as err:
        logs.error('NotImprelemnted error occurred! ' + err)

    except KeyError as err:
        logs.error('Key Error occurred! ' + err)

    return json_dictionary


def make_stops_table(API_KEY: str) -> pd.DataFrame:
    '''
    Utwórz tabelę z podstawowywmi danymi przystankowymi.

    Arguments:
        API_KEY: klucz api z pliku credentials.json

    Returns:
        Dataframe m.in. ze współrzędnymi geogranicznymi przystanków
    '''
    
    # informacje o przystankach z aktualnego dnia
    stops_link = 'https://api.um.warszawa.pl/api/action/dbstore_get/?id=1c08a38c-ae09-46d2-8926-4f9d25cb0630&apikey=' + API_KEY
    
    # zrób request do API
    json_dictionary = get_data_from_link(stops_link)

    # utwórz dataframe
    df = pd.json_normalize(json_dictionary['result'])

    # wszystkie dane są w formie:
    # {'value': '01', 'key': 'slupek'},
    # {'value': 'Kijowska', 'key': 'nazwa_zespolu'},
    # {'value': '2201', 'key': 'id_ulicy'},
    # ...

    # pobierz nazwy kolumn na podstawie kluczy z pierwszej obserwacji
    column_names = df['values'].apply(pd.Series).iloc[0].apply(lambda x: x.get('key')).tolist()

    # przypisz nazwy kolumn do tabeli
    df = df['values'].apply(pd.Series)
    df.columns = column_names

    # wydobądź values ze słownika i użyj ich jako wartości w dataframe
    for col in column_names:
        df[col] = df[col].apply(lambda x: x.get('value'))
    
    return df


def add_lines_to_stops_table(df: pd.DataFrame, API_KEY: str) -> pd.DataFrame:
    '''
    Zadaje zapytanie do każdego przystanku o numery linii, jakie są przez niego obsługiwane

    Arguments:
        df: tabela z zespołami i słupkami przystanków
        API_KEY: klucz api z pliku credentials.json
    
    Returns:
        Tabela z dodatkową kolumną 'linie', gdzie są wszystkie numery linii jakie są
        dostępne dla wszystkich przystanków
    '''
    df['linie'] = None # wstaw pustą kolumnę

    # dla każdego wpisu zrób zapytanie o informacje przystankowe
    for index, row in tqdm.tqdm(df[['zespol', 'slupek']].iterrows(), total = df.shape[0]):
        zespol = row['zespol']
        slupek = row['slupek']

        link = 'https://api.um.warszawa.pl/api/action/dbtimetable_get/?id=88cd555f-6f31-43ca-9de4-66c479ad5942&busstopId=' + zespol + '&busstopNr=' + slupek + '&apikey=' + API_KEY

        # zrób request do API
        json_dictionary = get_data_from_link(link)

        # zamień pobraną informację na numery linii
        lines = [elem.get('values')[0].get('value') for elem in json_dictionary['result']]

        # wpisz numery linii do tabeli 
        df.loc[index, 'linie'] = lines
    
    # weź pod uwagę tylko aktywne przystanki
    df = df[df['linie'].map(lambda x: len(x)>0)]

    return df


def make_timetables_for_lines(df: pd.DataFrame, API_KEY: str, only_trams:bool = False) -> pd.DataFrame:
    '''
    Zadaje zapytanie do każdego numeru linii jaki występuje na każdym
    przystanku o jego rozkład z danego przystanku

    Arguments:
        df: tabela z danymi przystankowymi oraz z numerami linii (z dodatkową kolumną 'linie')
        API_KEY: klucz api z pliku credentials.json
        only_trams: czy mają być zbierane rozkłady tylko dla tramwajów czy dla wszystkich typów pojazdów

    Return:
        Tabela ze wszystkimi rozkładami dla każdej linii każdego przystanku
    '''

    # określ typ pojazdu na podstawie jego numeru
    df['typ'] = 'A' # najpierw przypisz 'A' dla wszystkich pojazdów

    df.loc[df['linie'].apply(lambda x: str(x[0])[0] == 'W'), 'typ'] = 'WKD' # oznacz WKD
    df.loc[df['linie'].apply(lambda x: str(x[0])[0] == 'R'), 'typ'] = 'R' # oznacz pociągi KM
    df.loc[df['linie'].apply(lambda x: str(x[0])[0] == 'S'), 'typ'] = 'S' # oznacz pociągi SKM
    df.loc[df['linie'].apply(lambda x: str(x[0])[0] == 'M'), 'typ'] = 'M' # oznacz pociągi metra
    df = df.reset_index()

    # znajdź indeksy tych wierszy, które mają oznaczenie 'A' i ich długość jest nie większa niż 2
    less_then_2_index =  df[(df.typ == 'A') & (df.linie.apply(lambda x: len(x[0]) <=2))].index.tolist()

    df.loc[less_then_2_index,'typ'] = 'T' # oznacz tramwaje

    # czy ograniczamy się jedynie do tramwajów
    if only_trams:
        df = df[df['typ'] == 'T']

    lines_column = []
    
    # dla każdego numeru linii na każdym przystanku pobierz rozkłady jazdy
    for index, row in tqdm.tqdm(df.iterrows(), total = df.shape[0]):
        zespol = row['zespol']
        slupek = row['slupek']
        linie = row['linie']

        czas_list = []        
        for linia in linie:
            link = 'https://api.um.warszawa.pl/api/action/dbtimetable_get/?id=e923fa0e-d96c-43f9-ae6e-60518c9f3238&busstopId=' + zespol + '&busstopNr=' + slupek + '&line=' + linia + '&apikey=' + API_KEY

            # zrób request do API
            json_dictionary = get_data_from_link(link)

            # pobierz rozkład dla danej linii 
            czas = tuple(d['value'] for d in [json_dictionary['result'][n].get('values')[5] for n in range(len(json_dictionary['result']))])

            # usuń sekundy z czasów
            czas = tuple([elem[:-3] if len(czas) >= 1 else elem for elem in czas])
            czas_list.append(czas)
        lines_column.append(dict(zip(linie, czas_list)))
    
    df['linie']  = lines_column
    return df

def init_logging(logs: logging.Logger, file_name: str) -> logging.Logger:
    '''
    Funkcja do logowania informacji jednocześnie do konsoli i do pliku

    Arguments:
        logs: Domyślny logger z modułu logging
        file_name: nazwa pliku, do którego mają być logowane dane
    
    Returns:
        Obiekt 'logs' z określonym formatem wpisów jakie mają być w nim umieszczane i
        wskazanym plikiem do zapisywania logów.
    '''
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


def load_api_key(credentials_file_name: str = 'credentials.json'):
    '''
    Wczytaj klucz api z pliku

    Arguments:
        credentials_file_name: nazwa pliku json z kluczem API

    Returns:
        Klucz API potrzebny do składania zapytań do serwera UM Warszawa
    '''
    if os.path.exists(credentials_file_name):
        with open(credentials_file_name) as f:
            API_KEY = json.load(f)['API_KEY']
    else:
        logs.error(f"Brak poprawnego pliku {credentials_file_name} z kluczem API")
        input('Naciśnij dowolny klawisz, aby zakończyć.')
        exit()

    return API_KEY

if __name__ == '__main__':    

    # aktualna data i godzina
    now = datetime.now().strftime("%Y-%m-%d")

    # zbieramy informacje o wszystkich pojazdach albo tylko o tramwajach
    while True:
        only_trams = input('Co pobierać? \n [0] - cały rozkład; [1] - tylko tramwaje ')
        if only_trams not in ['0', '1']:
            continue
        else:
            only_trams = bool(int(only_trams))
            break

   # logowanie do pliku i do konsoli 
    logs = logging.getLogger(__name__)
    logs = init_logging(logs, 'script.log')
    
    # wczytaj klucz API
    API_KEY = load_api_key()

    logs.info('Pobieranie podstawowych informacji o przystankach...')
    df = make_stops_table(API_KEY)

    logs.info('Pobieranie linii dla przystanków...')
    df = add_lines_to_stops_table(df, API_KEY)

    logs.info('Pobieranie rozkładów dla wszystkich linii...')
    df = make_timetables_for_lines(df, API_KEY, only_trams = only_trams)
    
    logs.info(f'Zapisywanie rozkładów do pliku rozklady_{now}.pkl')
    try:
        df.to_pickle(f'rozklady_{now}.pkl', compression='zip')
    except Exception as err:
        logs.error(err)

    logs.info('Czyszczenie pamięci...')
    del df
    gc.collect()

    logs.info(f"{'Koniec':.^20}")
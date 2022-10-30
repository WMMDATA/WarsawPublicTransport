import pandas as pd
import requests
import gc
import tqdm
from datetime import datetime
import json
import pickle

# disable SettingWithCopyWarning
pd.options.mode.chained_assignment = None

def make_stops_table(API_KEY: str) -> pd.DataFrame:
    '''
    Utwórz tabelę z podstawowywmi danymi przystankowymi.

    Arguments:
        API_KEY: klucz api 

    Returns:
        Dataframe m.in. ze współrzędnymi geogranicznymi przystanków
    '''
    
    # informacje o przystankach z aktualnego dnia
    stops_link = 'https://api.um.warszawa.pl/api/action/dbstore_get/?id=1c08a38c-ae09-46d2-8926-4f9d25cb0630&apikey=' + API_KEY
    
    # zrób request do API
    requested_data = requests.get(stops_link)

    # zamień na json
    json_dictionary = requested_data.json()

    # utwórz dataframe
    df = pd.json_normalize(json_dictionary['result'])

    # wszystkie dane są w formie:
    # {'value': '01', 'key': 'slupek'},
    # {'value': 'Kijowska', 'key': 'nazwa_zespolu'},
    # {'value': '2201', 'key': 'id_ulicy'},
    # ...

    # pobierz nazwy kolumn na podstawie kluczy z pierwszej obserwacji
    column_names = df['values'].apply(pd.Series).iloc[0].apply(lambda x: x.get('key')).tolist()

    # przypisz nazwy kolumn
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
        API_KEY: klucz api
    
    Returns:
        Tabela z dodatkową kolumną 'linie', gdzie są wszystkie numery linii jakie są
        dostępne dla danego przystanku
    '''
    df['linie'] = None # wstaw pustą kolumns

    # dla każdego wpisu zrób zapytanie o informacje przystankowe
    for index, row in tqdm.tqdm(df[['zespol', 'slupek']].iterrows(), total = df.shape[0]):
        zespol = row['zespol']
        slupek = row['slupek']

        link = 'https://api.um.warszawa.pl/api/action/dbtimetable_get/?id=88cd555f-6f31-43ca-9de4-66c479ad5942&busstopId=' + zespol + '&busstopNr=' + slupek + '&apikey=' + API_KEY

        requested_data = requests.get(link)
        json_dictionary = requested_data.json()
        lines = [elem.get('values')[0].get('value') for elem in json_dictionary['result']]
        
        df.loc[index, 'linie'] = lines
    
    # weź pod uwagę tylko aktywne przystanki
    df = df[df['linie'].map(lambda x: len(x)>0)]
    return df


def make_timetables_for_lines(df: pd.DataFrame, API_KEY: str) -> pd.DataFrame:
    '''
    Zadaje zapytanie do każdego numeru linii jaki występuje na każdym
    przystanku o jego rozkład z danego przystanku

    Arguments:
        df: tabela z danymi przystankowymi oraz z numerami linii (kolumna 'linie')
    
    Return:
        Tabela ze wszystkimi brygadami, trasami oraz rozkładem dla każdej linii
        dla każdego przystanku
    '''

    # rozszerz tabelę tak, aby w każdym wieszu był numer linii
    df_full = df.explode('linie')

    # wstaw puste kolumny
    df_full['brygada'] = None
    df_full['trasa'] = None
    df_full['czas'] = None

    # dla każdego numeru linii na każdym przystanku pobierz rozkłady jazdy
    for index, row in tqdm.tqdm(df_full.iterrows(), total = df_full.shape[0]):
        zespol = row['zespol']
        slupek = row['slupek']
        linia = row['linie']
        
        link = 'https://api.um.warszawa.pl/api/action/dbtimetable_get/?id=e923fa0e-d96c-43f9-ae6e-60518c9f3238&busstopId=' + zespol + '&busstopNr=' + slupek + '&line=' + linia + '&apikey=' + API_KEY

        requested_data = requests.get(link)
        json_dictionary = requested_data.json()
        
        brygada = tuple(d['value'] for d in [json_dictionary['result'][n].get('values')[2] for n in range(len(json_dictionary['result']))])
        trasa = tuple(d['value'] for d in [json_dictionary['result'][n].get('values')[4] for n in range(len(json_dictionary['result']))])
        czas = tuple(d['value'] for d in [json_dictionary['result'][n].get('values')[5] for n in range(len(json_dictionary['result']))])
        
        row['brygada'] = brygada
        row['trasa'] = trasa
        row['czas'] = czas
    
    # zostawiamy tylko te kolumny, które są istotne do dalszej analizy
    df_full = df_full.drop(['nazwa_zespolu', 'id_ulicy', 'szer_geo', 'dlug_geo', 'kierunek', 'obowiazuje_od'],axis = 1)
    
    # pozbywamy się sekund w kolumnie 'czas'
    df_full['czas'] = df_full['czas'].apply(lambda x: tuple(x[elem][:-3] for elem in range(len(x))) if len(x) >=1 else x)

    # określamy typy pojazdów

    df_full['typ'] = 'A' # przypisz 'A' dla wszystkich typów pojazdów

    # df_full[df_full['linie'].str.contains('WKD', na=False)]['typ'] == 'WKD' # przypisz WKD

    df_full.loc[df_full['linie'].str.contains('WKD', na=False), 'typ'] = 'WKD' # oznacz WKD
    df_full.loc[df_full['linie'].apply(lambda x: str(x)[0] == 'R'), 'typ'] = 'R' # oznacz pociągi KM
    df_full.loc[df_full['linie'].apply(lambda x: str(x)[0] == 'S'), 'typ'] = 'S' # oznacz pociągi SKM
    df_full.loc[df_full['linie'].apply(lambda x: str(x)[0] == 'M'), 'typ'] = 'M' # oznacz pociągi metra
    df_full = df_full.reset_index()

    # znajdź indeksy tych wierszy, które mają oznaczenie 'A' i ich długość jest nie większa niż 2
    less_then_2_index =  df_full[(df_full.typ == 'A') & (df_full.linie.str.len()<=2)].index.tolist()

    df_full.loc[less_then_2_index,'typ'] = 'T' # oznacz tramwaje

    return df_full

def generate_all_vehicle_numbers(vehicle_type: str = None) -> list:
    '''
    Utwóz listę numerów wszystkich pojazdów

    Arguments:
        vehicle_type: typ pojazdu. Możliwe opcje:
            T - tramwaj
            A - autobus
            M - metro
            S - pociag SKM 
            R - pociąg KM 
            WKD - pociąg WKD 
            None - wszystkie typy pojazdów
        
    Returns:
        Lista unikalnyh numerów pojazdów
    '''
    if vehicle_type == None:
        return df_full[df_full['brygada'].apply(lambda x: len(x)>0)]['linie'].unique().tolist()
    else:
       return  df_full[(df_full['typ'] == vehicle_type) & (df_full['brygada'].apply(lambda x: len(x)>0))]['linie'].unique().tolist()


def make_timetables_dict(lines_list: list, df_full: pd.DataFrame) -> dict:
    '''
    Generuje trasy na podstawie pobranych informacji.

    Arguments:
        lines_list: lista unikalnych numerów linii
        df_full : pełna tabela z brygadami i czasami
    
    Returns:
        Słownik w następującej postaci:
        {'102': {'57_TP-OLS': 
                    [('05:02', '1231_07'),
                    ('05:03', '1232_04'),
                    ('05:04', '1231_02'),
                    ('05:06', '1001_01'),
                    ('05:07', '2001_04'),
                    ...
    '''
    lines_dict = {}
    for line in lines_list:
        df_test = df_full[df_full['linie'] == line]
        df_test['concat'] = None
        d = {}
        for index, row in tqdm.tqdm(df_test.iterrows(), total = df_test.shape[0]):
            df_test.at[index, 'concat'] = {a:x +'_'+ y for (a, x, y) in zip(df_test['czas'][index], df_test['brygada'][index], df_test['trasa'][index])}
            for k,v in df_test.at[index, 'concat'].items():
                d.setdefault(v, []).append((k, df_test.at[index, 'zespol']+'_'+df_test.at[index, 'slupek'])) 

        for bt in d:
            d[bt] = sorted(list(d.values())[0], key = lambda x: x[0])
        lines_dict[line] = d
    return lines_dict



if __name__ == '__main__':    
    # aktualna data i godzina
    now = datetime.now().strftime("%Y-%m-%d")

    # wczytaj klucz API
    with open('credentials.json') as f:
        API_KEY = json.load(f)['API_KEY']

    print('\n')
    print(f'{now}')
    print('='*30)
    print('Odczytywanie podstawowych informacji o przystankach...') 
    df = make_stops_table(API_KEY)

    print('Odczytywanie linii dla przystanków...')
    df = add_lines_to_stops_table(df, API_KEY)

    print(f'Zapis przystanków do pliku przystanki_{now}.pkl')
    df.to_pickle(f'przystanki_{now}.pkl', compression='zip')

    print('Pobieranie rozkładów dla wszystkich linii...')
    df_full = make_timetables_for_lines(df, API_KEY)
    
    print(f'Zapis rozkładów do pliku rozklady_{now}.pkl')
    df_full.to_pickle(f'rozklady_{now}.pkl', compression='zip')

    print('Czyszczenie pamięci...')
    del df
    del df_full
    gc.collect()

    print('Koniec.')
    print('='*30)

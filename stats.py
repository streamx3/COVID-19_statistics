#!/usr/bin/env python3

# In order for this program to work you need to:
# 1) install 'countryinfo' pip package to your environment
# 2) put this file to the clone folder of https://github.com/CSSEGISandData/COVID-19

import os
import sys
import csv
import json
import subprocess
from typing import Dict, List, Any
from enum import Enum

try:
    from countryinfo import CountryInfo
except ImportError:
    import pip

    print('Failed to import countrinfo. Trying to install')
    if hasattr(pip, 'main'):
        pip.main(['install', 'countryinfo'])
    else:
        pip._internal.main(['install', 'countryinfo'])
    try:
        from countryinfo import CountryInfo
    except ImportError:
        sys.stdout.write('Failed to install and import countryinfo!\nInstall it yourself!\n')
        sys.exit(-1)
    print('Successfully installed and imported countryinfo.')


class EAlign(Enum):
    default = 0
    left = 1
    right = 2


key_lat = 'lat'
key_lon = 'lon'
key_header = 'header'
key_series = 'series'
key_active = 'active'
key_country = 'country'
key_ratings = 'ratings'
key_province = 'province'
key_mainland = 'mainland'
key_mortality = 'Mort'
key_lethality = 'let'
key_population = 'population'
key_invalidate = 'invalidate'
key_territories = 'territories'
key_active_per_unknown = 'APU'  # unknown stands for population - confirmeds
key_active_per_population = 'APP'
key_confirmed_per_population = 'CPP'
key_daily_confirmed_per_population = 'daily_confirmed_per_population'

key_totals = 'totals'

key_deaths = 'deaths'
key_confirmed = 'confirmed'
key_recovered = 'recovered'
case_types = [key_deaths, key_confirmed, key_recovered]

folder_prefix = '.'
folder_parrent = '..'
folder_COVID_19 = 'COVID-19'
folder_data = 'csse_covid_19_data'
folder_timeseries = 'csse_covid_19_time_series'

file_ext_cache_json = '.cache.json'


if not os.path.exists(folder_data):
    folder_prefix = os.path.join(folder_parrent, folder_COVID_19)
    if os.path.exists(os.path.join(folder_prefix, folder_data)):
        # print('Using ..' + os.path.sep + 'COVID-19')
        pass
    else:
        sys.stderr.write('Could not find data\n')
        sys.exit(-1)

files = {key_deaths: os.path.join(folder_prefix, folder_data, folder_timeseries,
                                  'time_series_covid19_deaths_global.csv'),
         key_confirmed: os.path.join(folder_prefix, folder_data, folder_timeseries,
                                     'time_series_covid19_confirmed_global.csv'),
         key_recovered: os.path.join(folder_prefix, folder_data, folder_timeseries,
                                     'time_series_covid19_recovered_global.csv')}

#                   JHU database : CountryInfo
exceptional_names = {'Andorra': None,
                     'Bahamas': 'The Bahamas',
                     'Cabo Verde': 'Cape Verde',
                     'Congo (Brazzaville)': 'Republic of the Congo',
                     'Congo (Kinshasa)': 'Democratic Republic of the Congo',
                     "Cote d'Ivoire": 'Ivory Coast',
                     'Diamond Princess': None,
                     'Czechia': 'Czech Republic',
                     'Eswatini': 'Swaziland',
                     'Gambia': 'The Gambia',
                     'Holy See': None,  # Extremely slippery, due to Vatican special status
                     'Korea, South': 'South Korea',
                     'Montenegro': None,
                     'North Macedonia': 'Republic of Macedonia',
                     'Serbia': None,  # Actually exists in countryinfo! Probably a bug.
                     'Taiwan*': 'Taiwan',
                     'US': 'United States',
                     'Timor-Leste': None,
                     'West Bank and Gaza': None,
                     'Kosovo': None,
                     'Burma': None,  # Surprisingly is missing in a package
                     'MS Zaandam': None,
                     'Sao Tome and Principe': 'São Tomé and Príncipe'}

exceptional_populations = {
    'Andorra': 77543,  # http://citypopulation.de/en/andorra/
    # N/A since on ship is particially evacuated and moving 'Diamond Princess': 3711 # https://en.wikipedia.org/wiki/COVID-19_pandemic_on_Diamond_Princess
    'Montenegro': 622359,  # http://www.monstat.org/eng/page.php?id=234&pageid=48
    'Timor-Leste': 1183643,
    # http://www.statistics.gov.tl/category/publications/census-publications/
    'West Bank and Gaza': 2939418,
    # 2018 via CIA https://www.cia.gov/library/publications/the-world-factbook/geos/we.html
    'Kosovo': 1810463,  # https://countrymeters.info/en/Kosovo
    'Burma': 53582855,  # 2017 http://www.worldometers.info/world-population/myanmar-population/
    'Holy See': 825,  # https://www.vaticanstate.va/it/stato-governo/note-generali/popolazione.html
    'Serbia': 6963764  # http://www.stat.gov.rs/
    # Not using due to lack of information 'MS Zaandam': 1243 # https://www.theguardian.com/world/2020/mar/27/stranded-at-sea-cruise-ships-around-the-world-are-adrift-as-ports-turn-them-away
}


def get_git_revision_hash(folder: str) -> str:
    # TODO Test on Windows to see if UTF-8 fits
    retval = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=folder)
    return retval.decode('utf-8').replace('\n', '')


def enofile(fname: str):
    if fname in files:
        return 'ERROR: File "' + files[fname] + '" does not exist!'
    else:
        return 'ERROR: Unknown file is missing, LOL XD'


def is_country_name_valid(name: str) -> bool:
    rv = True
    try:
        c = CountryInfo(name)
        data = c.native_name()
    except KeyError:
        rv = False
    return rv


def row2dict(row: List, header: List, type: str = None) -> Dict[str, Any]:
    retval = {key_province: row[0],
              key_country: row[1],
              key_lat: row[2],
              key_lon: row[3]}
    len_row = len(row)
    len_hdr = len(header)
    len_min = min(len_hdr, len_row)
    if type is None:
        key_placeholder = key_series
    else:
        key_placeholder = type
    retval[key_placeholder] = {}
    for i in range(4, len_min, 1):
        retval[key_placeholder][header[i]] = row[i]
    return retval


def load_json(type: str, countries: Dict = None):
    # TODO rename
    if countries is None:
        countries = {}
    with open(files[type]) as fcsv:
        data_csv = csv.reader(fcsv, delimiter=',')
        header = next(data_csv)
        for row in data_csv:
            data = row2dict(row, header, type)
            country = data[key_country]
            province = data[key_province]
            del data[key_country]
            del data[key_province]

            if province == '':
                province = key_mainland
            if country not in countries:
                countries[country] = {}
                countries[country][key_territories] = {}
            if province in countries[country][key_territories]:
                countries[country][key_territories][province][type] = data[type]
            else:
                countries[country][key_territories][province] = data
    return countries


def countries2json() -> Dict[str, Any]:
    countries = None
    for case_type in case_types:
        countries = load_json(case_type, countries)

    n_territories = 0
    for country in countries:
        ter = countries[country][key_territories]
        n_territories += len(ter)

    print('Impored ' + str(len(countries)) + ' countries with ' + str(n_territories) + \
          ' territories from CSV.')
    # Processing imported data
    missing_countries = []
    no_mainlands = []
    i = 0
    for country in countries:
        # if key_mainland not in countries[country][key_territories]:  # WTF???
        #     no_mainlands.append(country)
        # Adding poplation itself
        def add_population(country, name=country):
            country_info = CountryInfo(name)
            countries[country][key_population] = country_info.population()

        if is_country_name_valid(country):
            add_population(country)
        else:
            if country in exceptional_names:
                ex_name = exceptional_names[country]
                if ex_name is not None and is_country_name_valid(ex_name):
                    add_population(country, ex_name)
                else:
                    if country in exceptional_populations:
                        countries[country][key_population] = exceptional_populations[country]
                    else:
                        missing_countries.append(country)

        # Merging totals
        countries[country][key_totals] = {key_deaths: {}, key_recovered: {}, key_confirmed: {}}
        if len(countries[country][key_territories]) > 1:
            for case_type in case_types:

                for territory in countries[country][key_territories]:
                    if case_type not in countries[country][key_territories][territory]:
                        continue
                    for date in countries[country][key_territories][territory][case_type]:
                        if date not in countries[country][key_totals][case_type]:
                            countries[country][key_totals][case_type][date] = \
                                int(countries[country][key_territories][territory][case_type][date])
                        else:
                            added = \
                                int(countries[country][key_territories][territory][case_type][date])
                            countries[country][key_totals][case_type][date] += added
        else:
            for case_type in case_types:
                countries[country][key_totals][case_type] = \
                    countries[country][key_territories][key_mainland][case_type]
            del countries[country][key_territories]

        # Writing progress
        i += 1
        sys.stdout.write('\rMerging cases: {0}/{1}'.format(i, len(countries)))
        sys.stdout.flush()

    print('\nImpossible to process: ' + str(len(missing_countries)))
    print(missing_countries)
    # print('No mainlands: ' + ', '.join(no_mainlands))
    return countries


def string_fit(string: str, size: int, align: EAlign = EAlign.left) -> str:
    strlen = len(string)
    delta = size - strlen
    if delta > 0:
        if align == EAlign.left:
            for i in range(delta):
                string += ' '
        else:
            for i in range(delta):
                string = ' ' + string
    elif delta < -1:
        string = string[:size]
    return string


def print_table(array: [[str]], header_align: EAlign = EAlign.left,
                data_align: EAlign = EAlign.right, header: bool = True,
                col2_align: EAlign = EAlign.default) -> [str]:
    rows = len(array)
    columns = 0
    sizes = []

    # Calculate MAX columns and max size for each column
    for row in array:
        if len(row) > columns:
            columns = len(row)

    for i in range(columns):
        sizes.append(0)

    for row in array:
        i = 0
        for cell in row:
            len_cell = len(cell)
            if sizes[i] < len_cell:
                sizes[i] = len_cell
            i += 1

    retval = []
    # Print table cells
    for row in array:
        i = 0
        rowbuf = ''
        for cell in row:
            if header and array[0] == row:
                rowbuf += string_fit(cell, sizes[i], align=header_align) + ' '
            else:
                if col2_align != EAlign.default and cell == row[1]:
                    rowbuf += string_fit(cell, sizes[i], align=col2_align) + ' '
                else:
                    rowbuf += string_fit(cell, sizes[i], align=data_align) + ' '
            i += 1
        print(rowbuf)
        # retval.append(rowbuf)


def calculate_ratings(data: Dict[str, Any], min_population: int = None, date: str = None) -> Dict:
    if date is None:
        date = list(data['US'][key_totals][key_deaths])[-1]
        print('Using data: ' + date + ' (latest downloaded)')
    else:
        print('Using data: ' + date)
    if min_population is not None and min_population > 1:
        str_limit = ''
        if 100 < min_population < 999:
            str_limit = str(min_population / 100) + ' hundred'
        elif 999 < min_population < 999999:
            str_limit = str(min_population / 1000) + ' thousand'
        elif 999999:
            str_limit = str(min_population / 1000000) + ' million'
        print('Ommiting countries with less than ' + str_limit + ' habitants')
    ratings = {}
    for country in data:
        if key_population not in data[country]:
            continue
        if min_population is not None:
            population = data[country][key_population]
            if population < min_population:
                continue
        # Mortality people per million. For precents -- remove 4 zeroes
        deaths_cases = data[country][key_totals][key_deaths][date]
        if type(deaths_cases) is str:
            deaths_cases = int(deaths_cases)
        mortality = deaths_cases / data[country][key_population] * 1000000
        mortality = round(mortality, 3)

        # Lethality, % of dead per known infected
        confirmed_cases = data[country][key_totals][key_confirmed][date]
        if type(confirmed_cases) is str:
            confirmed_cases = int(confirmed_cases)
        lethality = deaths_cases / confirmed_cases * 100
        lethality = round(lethality, 3)

        # Active_vs_unknown
        recovered_cases = data[country][key_totals][key_recovered][date]
        if type(recovered_cases) is str:
            recovered_cases = int(recovered_cases)
        active_cases = confirmed_cases - recovered_cases - deaths_cases
        unknown_cases = data[country][key_population] - confirmed_cases
        active_vs_unknown = active_cases / unknown_cases
        active_vs_unknown = round(active_vs_unknown, 3)

        # Active_per_population
        active_per_population = active_cases / population * 100
        active_per_population = round(active_per_population, 3)

        # Confirmed_per_population, %
        confirmed_per_population = confirmed_cases / data[country][key_population] * 100
        confirmed_per_population = round(confirmed_per_population, 3)

        # Daily_confirmed_per_population

        ratings[country] = {key_mortality: mortality,
                            key_lethality: lethality,
                            key_active_per_unknown: active_vs_unknown,
                            key_deaths: deaths_cases,
                            key_confirmed: confirmed_cases,
                            key_active: active_cases,
                            key_active_per_population: active_per_population,
                            key_confirmed_per_population: confirmed_per_population,
                            key_population: data[country][key_population],
                            key_deaths: data[country][key_totals][key_deaths][date]}
    return ratings


def print_topmost_20(ratings: Dict[str, Any]):
    if ratings is None:
        sys.stderr.write('Error: Invalid ratings passed!\n')
        sys.exit(-1)
    rating_keys = [key_mortality, key_lethality, key_active_per_population,
                   key_active_per_unknown, key_confirmed_per_population]
    tops = {}
    for rating_key in rating_keys:
        rating = {k: v for k, v in sorted(ratings.items(), key=lambda item: item[1][rating_key])}
        topmost_20 = list(rating)[-20:]
        topmost_20.reverse()
        tops[rating_key] = topmost_20  # Placing topmost_20.reverse() retults in using None as
        # value, since .reverse() has internal effect and returns nothing

    # Cond.Capicalize: if not 'ALL_UPPERCASE' then 'Capitalize'
    def ccapitalize(string: str):
        if string.upper() != string:
            string = string.capitalize()
        return string

    def print_rating(header: str, key: str, dimmension: str, col1: str,
                     col2: str = None, col3: str = None):
        array2d = []
        print('\n' + header + ':')
        table_header = ['N', key_country.capitalize(), col1.capitalize()]
        if col2:
            table_header.append(ccapitalize(col2))
        if col3:
            table_header.append(ccapitalize(col3))
        table_header.append(ccapitalize(key) + '[' + dimmension + ']')

        array2d.append(table_header)

        i = 1
        for country in tops[key]:
            n = ''
            if i < 10:
                n = ' '
            n += str(i) + ' '
            i += 1
            row = [n, country, str(ratings[country][col1])]
            if col2:
                row.append(str(ratings[country][col2]))
            if col3:
                row.append(str(ratings[country][col3]))
            row.append(str(ratings[country][key]))
            array2d.append(row)
        print_table(array2d, header_align=EAlign.left, data_align=EAlign.right,
                    header=True, col2_align=EAlign.left)

    # TODO Continue here to implement 2x2 tables layout
    arr_mortality = print_rating('MORTALITY', key_mortality, '/1M', key_population, key_deaths)
    arr_lethality = print_rating('KNOWN LETHALITY', key_lethality, '%', key_confirmed, key_deaths)
    arr_kn_pe_pop = print_rating('KNOWN ACTIVE PER POPULATION', key_active_per_population, '%',
                                 key_active, key_population)
    arr_conf_p_pop = print_rating('CONFIRMED PER POPULATION', key_confirmed_per_population, '%',
                 key_confirmed, key_population)


def get_cachefile_name(hash: str) -> str:
    if len(hash) != 40:
        sys.stderr.write('git hash invalid\n')
        return None
    name = hash + file_ext_cache_json
    return name


def load_cache_if_available(expected_cachefile: str) -> Dict:
    if expected_cachefile is None:
        return None

    if not os.path.exists(expected_cachefile) or not os.path.isfile(expected_cachefile):
        return None
    with open(expected_cachefile, 'r') as f:
        try:
            retval = json.load(f)
            print('Loaded from ' + expected_cachefile + ' based on `git rev-parse HEAD`')
        except json.decoder.JSONDecodeError:
            retval = None
    return retval


def invalidate_cache(valid_hash: str):
    # Filter function
    def func_filter_ending_matches(el):
        if type(el) is not str:
            return False
        return el.endswith(file_ext_cache_json)

    data = os.listdir('.')
    caches_iterator = filter(func_filter_ending_matches, data)
    caches = list(caches_iterator)

    for c in caches:
        if c != str(valid_hash + file_ext_cache_json):
            try:
                os.remove('.' + os.path.sep + c)
            except Exception as e:
                return
            print('Removed outdated cache ' + c)


def print_country_rating(country: str, data: Dict):
    remap = {
        key_mortality: 'Mortality [per 1M]',
        key_lethality: 'Lethality [%]',
        key_active_per_unknown: 'Active per unknown [%]',
        key_deaths: 'Deaths',
        key_confirmed: 'Confirmed',
        key_active: 'Active',
        key_active_per_population: 'Active per population [%]',
        key_confirmed_per_population: 'Confirmed per population [%]',
        key_population: 'Population'
    }
    cdata = data[key_ratings][country]
    print('[' + country + ']')
    for k in cdata:
        if type(cdata[k]) is not str:
            print(remap[k] + ': ' + str(cdata[k]))
        else:
            print(remap[k] + ': ' + cdata[k])


if __name__ == '__main__':
    print('[WUHAN FLU rating calculator]')
    # TODO handle launch from different directory
    # print(os.path.dirname(os.path.realpath(__file__)))
    hash = get_git_revision_hash(folder_prefix)
    cachefile = get_cachefile_name(hash)
    invalidate_cache(hash)
    data = load_cache_if_available(cachefile)
    # print(data)
    if data is None:
        data = countries2json()
        data = {key_series: data}
        data[key_ratings] = calculate_ratings(data[key_series], min_population=1000000)
        with open(cachefile, 'w') as f:
            print('Writing ' + cachefile)
            json.dump(data, f)

    if len(sys.argv) > 1:
        if sys.argv[1] == key_invalidate:
            invalidate_cache('')
            sys.exit(0)
        if is_country_name_valid(sys.argv[1]):
            print_country_rating(sys.argv[1], data)
            sys.exit(0)
    else:
        print_topmost_20(data[key_ratings])

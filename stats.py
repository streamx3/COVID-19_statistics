#!/usr/bin/env python3

import os
import sys
import csv
from typing import Dict, List, Any
from countryinfo import CountryInfo

# In order for this program to work you need to:
# 1) install 'countryinfo' pip package to your environment
# 2) put this file to the clone folder of https://github.com/CSSEGISandData/COVID-19


key_lat = 'lat'
key_lon = 'lon'
key_header = 'header'
key_series = 'series'
key_active = 'active'
key_country = 'country'
key_province = 'province'
key_mainland = 'mainland'
key_mortality = 'mortality'
key_lethality = 'lethality'
key_population = 'population'
key_territories = 'territories'
key_active_vs_unknown = 'active_vs_unknown'  # unknown stands for population - confirmeds
key_active_per_population = 'active_per_population'
key_confirmed_per_population = 'confirmed_vs_population'
key_daily_confirmed_per_population = 'daily_confirmed_per_population'

key_totals = 'totals'

key_deaths = 'deaths'
key_confirmed = 'confirmed'
key_recovered = 'recovered'
case_types = [key_deaths, key_confirmed, key_recovered]

folder_data = 'csse_covid_19_data'
folder_timeseries = 'csse_covid_19_time_series'

files = {key_deaths: os.path.join(folder_data, folder_timeseries, 'time_series_covid19_deaths_global.csv'),
         key_confirmed: os.path.join(folder_data, folder_timeseries, 'time_series_covid19_confirmed_global.csv'),
         key_recovered: os.path.join(folder_data, folder_timeseries, 'time_series_covid19_recovered_global.csv')}

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
                    'Burma': None, # Surprisingly is missing in a package
                    'MS Zaandam': None,
                    'Sao Tome and Principe': 'São Tomé and Príncipe'}


exceptional_populations = {
    'Andorra': 77543, # http://citypopulation.de/en/andorra/
    # N/A since on ship is particially evacuated and moving 'Diamond Princess': 3711 # https://en.wikipedia.org/wiki/COVID-19_pandemic_on_Diamond_Princess
    'Montenegro': 622359,  # http://www.monstat.org/eng/page.php?id=234&pageid=48
    'Timor-Leste': 1183643,  # http://www.statistics.gov.tl/category/publications/census-publications/
    'West Bank and Gaza': 2939418,  # 2018 via CIA https://www.cia.gov/library/publications/the-world-factbook/geos/we.html
    'Kosovo': 1810463,  # https://countrymeters.info/en/Kosovo
    'Burma': 53582855,  # 2017 http://www.worldometers.info/world-population/myanmar-population/
    'Holy See': 825,  # https://www.vaticanstate.va/it/stato-governo/note-generali/popolazione.html
    'Serbia': 6963764  # http://www.stat.gov.rs/
    # Not using due to lack of information 'MS Zaandam': 1243 # https://www.theguardian.com/world/2020/mar/27/stranded-at-sea-cruise-ships-around-the-world-are-adrift-as-ports-turn-them-away
}


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

    print('Impored ' + str(len(countries)) + ' countries with ' + str(n_territories) + ' territories from CSV.')
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
                            countries[country][key_totals][case_type][date] = int(countries[country][key_territories][territory][case_type][date])
                        else:
                            added = int(countries[country][key_territories][territory][case_type][date])
                            countries[country][key_totals][case_type][date] += added
        else:
            for case_type in case_types:
                countries[country][key_totals][case_type] = countries[country][key_territories][key_mainland][case_type]
            del countries[country][key_territories]

        # Writing progress
        i += 1
        sys.stdout.write('\rMerging cases: {0}/{1}'.format(i, len(countries)))
        sys.stdout.flush()

    print('\nImpossible to process: ' + str(len(missing_countries)))
    print(missing_countries)
    # print('No mainlands: ' + ', '.join(no_mainlands))
    return countries


def print_topmost_20(data: Dict[str, Any], min_population: int = None, date: str = None):
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

        # Lethality, % of dead per known infected
        confirmed_cases = data[country][key_totals][key_confirmed][date]
        if type(confirmed_cases) is str:
            confirmed_cases = int(confirmed_cases)
        lethality = deaths_cases / confirmed_cases * 100

        # Active_vs_unknown
        recovered_cases = data[country][key_totals][key_recovered][date]
        if type(recovered_cases) is str:
            recovered_cases = int(recovered_cases)
        active_cases = confirmed_cases - recovered_cases - deaths_cases
        unknown_cases = data[country][key_population] - confirmed_cases
        active_vs_unknown = active_cases / unknown_cases

        # Active_per_population
        active_per_population = active_cases / population * 100

        # Confirmed_per_population, %
        confirmed_per_population = confirmed_cases / data[country][key_population] * 100

        # Daily_confirmed_per_population

        ratings[country] = {key_mortality: mortality,
                           key_lethality: lethality,
                           key_active_vs_unknown: active_vs_unknown,
                           key_deaths: deaths_cases,
                           key_confirmed: confirmed_cases,
                           key_active: active_cases,
                           key_active_per_population: active_per_population,
                           key_confirmed_per_population: confirmed_per_population,
                           key_population: data[country][key_population],
                           key_deaths: data[country][key_totals][key_deaths][date]}

    rating_keys = [key_mortality, key_lethality, key_active_per_population, key_active_vs_unknown, key_confirmed_per_population]
    tops = {}
    for rating_key in rating_keys:
        rating = {k: v for k, v in sorted(ratings.items(), key=lambda item: item[1][rating_key])}
        topmost_20 = list(rating)[-20:]
        topmost_20.reverse()
        tops[rating_key] = topmost_20  # placing topmost_20.reverse() retults in using None as value,
        # since .reverse() has internal effect and returns nothing

    print('\nMORTALITY RATE:\nCountry,Population,Deaths,Mortality(per 1M)')
    for country in tops[key_mortality]:
        print(country + "," + str(ratings[country][key_population]) + ',' + str(ratings[country][key_deaths]) + ','
              + str(ratings[country][key_mortality]))

    print('\nKNOWN LETHALITY RATE:\nCountry,Confirmed,Deaths,%')
    for country in tops[key_lethality]:
        print(country + "," + str(ratings[country][key_confirmed]) + ',' + str(ratings[country][key_deaths]) + ','
              + str(ratings[country][key_lethality]))

    print('\nKnown active per population:\nCountry,Active,Population,%')
    for country in tops[key_active_per_population]:
        print(country + "," + str(ratings[country][key_active]) + ',' + str(ratings[country][key_population]) + ','
              + str(ratings[country][key_active_per_population]))

    print('\nConfirmed per population:\nCountry,Confirmed,Population,%')
    for country in tops[key_confirmed_per_population]:
        print(country + "," + str(ratings[country][key_confirmed]) + ',' + str(ratings[country][key_population]) + ','
              + str(ratings[country][key_confirmed_per_population]))


if __name__ == '__main__':
    # print(os.path.dirname(os.path.realpath(__file__)))
    print('[WUHAN FLU rating calculator]')
    data = countries2json()
    # print(data)
    print_topmost_20(data, min_population=1000000)

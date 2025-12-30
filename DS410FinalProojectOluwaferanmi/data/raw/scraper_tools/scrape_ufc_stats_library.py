# imports
from typing import List, Tuple
import pandas as pd
import numpy as np
import re
import requests
from bs4 import BeautifulSoup
import itertools
import string

# get soup
def get_soup(url: str) -> BeautifulSoup:
    # get page
    page = requests.get(url)
    # make soup
    soup = BeautifulSoup(page.content, 'html.parser')

    return soup

# parse event details
def parse_event_details(soup: BeautifulSoup) -> pd.DataFrame:
    # lists for data
    event_names = []
    event_urls = []
    event_dates = []
    event_locations = []

    # get names and urls
    for tag in soup.find_all('a', class_='b-link b-link_style_black'):
        event_names.append(tag.text.strip())
        event_urls.append(tag['href'])

    # get dates
    for tag in soup.find_all('span', class_='b-statistics__date'):
        event_dates.append(tag.text.strip())

    # get locations
    for tag in soup.find_all('td', class_='b-statistics__table-col b-statistics__table-col_style_big-top-padding'):
        event_locations.append(tag.text.strip())

    # remove first item
    # skip upcoming event
    event_dates = event_dates[1:]
    event_locations = event_locations[1:]

    # make df
    event_details_df = pd.DataFrame({
        'EVENT':event_names,
        'URL':event_urls,
        'DATE':event_dates,
        'LOCATION':event_locations
    })

    return event_details_df

# parse fight details
def parse_fight_details(soup: BeautifulSoup) -> pd.DataFrame:
    # list for urls
    fight_urls = []
    # get urls
    for tag in soup.find_all('tr', class_='b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click'):
        fight_urls.append(tag['data-link'])

    # list for fighters
    fighters_in_event = []
    # get fighters
    for tag in soup.find_all('a', class_='b-link b-link_style_black'):
        fighters_in_event.append(tag.text.strip())

    # list for fights
    fights_in_event = []
    # loop urls
    for url in fight_urls:
        # get soup
        soup_fight = get_soup(url)
        # list names
        fighters_names = []
        # get names
        for tag in soup_fight.find_all('a', class_='b-link b-fight-details__person-link'):
            fighters_names.append(tag.text.strip())
        # join names
        fights_in_event.append(' vs. '.join(fighters_names))
    
    # make df
    fight_details_df = pd.DataFrame({'BOUT':fights_in_event, 'URL':fight_urls})
    # add event col
    fight_details_df['EVENT'] = soup.find('h2', class_='b-content__title').text.strip()
    # fix cols
    fight_details_df = move_columns(fight_details_df, ['EVENT'], 'BOUT', 'before')

    return fight_details_df

# parse fight results
def parse_fight_results(soup: BeautifulSoup) -> List[str]:
    # list for results
    fight_results = []

    # get event name
    fight_results.append(soup.find('h2', class_='b-content__title').text)

    # get fighters
    for tag in soup.find_all('a', class_='b-link b-fight-details__person-link'):
        fight_results.append(tag.text.strip())

    # get w/l
    for tag in soup.find_all('div', class_='b-fight-details__person'):
        for i_text in tag.find_all('i'):
            fight_results.append(i_text.text)

    # get weight class
    fight_results.append(soup.find('div', class_='b-fight-details__fight-head').text)

    # get method
    fight_results.append(soup.find('i', class_='b-fight-details__text-item_first').text)

    # get other stuff
    remaining_results = soup.find_all('p', class_='b-fight-details__text')

    # get details
    for tag in remaining_results[0].find_all('i', class_='b-fight-details__text-item'):
        fight_results.append(tag.text.strip())

    # get more details
    fight_results.append(remaining_results[1].get_text())

    # clean text
    fight_results = [text.replace('\n', '').replace('  ', '') for text in fight_results]

    return fight_results

# organise fight results
def organise_fight_results(results_from_soup: List[str], fight_results_column_names: List[str]) -> pd.DataFrame:
    # clean list
    fight_results_clean = []
    # add event
    fight_results_clean.append(results_from_soup[0])
    # add fighters
    fight_results_clean.append(' vs. '.join(results_from_soup[1:3]))
    # add outcome
    fight_results_clean.append('/'.join(results_from_soup[3:5]))
    # fix labels
    fight_results_clean.extend([re.sub('^(.+?): ?', '', text) for text in results_from_soup[5:]])

    # make df
    fight_result_df = pd.DataFrame(columns=fight_results_column_names)
    # add to df
    fight_result_df.loc[len(fight_result_df)] = fight_results_clean

    return fight_result_df

# parse fight stats
def parse_fight_stats(soup: BeautifulSoup) -> Tuple[List[str], List[str]]:
    # lists for stats
    fighter_a_stats = []
    fighter_b_stats = []

    # find stat cols
    for tag in soup.find_all('td', class_='b-fight-details__table-col'):
        # loop tags
        for index, p_text in enumerate(tag.find_all('p')):
            # check index
            if index % 2 == 0:
                fighter_a_stats.append(p_text.text.strip())
            # else other fighter
            else:
                fighter_b_stats.append(p_text.text.strip())

    return fighter_a_stats, fighter_b_stats

# organise stats
def organise_fight_stats(stats_from_soup: List[str]) -> List[List[str]]:
    # list for stats
    fighter_stats_clean = []
    # group by name
    for name, stats in itertools.groupby(stats_from_soup, lambda x: x == stats_from_soup[0]):
        # new sublist
        if name:
            fighter_stats_clean.append([])
        # add stats
        fighter_stats_clean[-1].extend(stats)

    return fighter_stats_clean

# convert stats to df
def convert_fight_stats_to_df(clean_fighter_stats: List[List[str]], totals_column_names: List[str], significant_strikes_column_names: List[str]) -> pd.DataFrame:
    # make dfs
    totals_df = pd.DataFrame(columns=totals_column_names)
    significant_strikes_df = pd.DataFrame(columns=significant_strikes_column_names)

    # check empty
    if len(clean_fighter_stats) == 0:
        # add nans
        totals_df.loc[len(totals_df)] = [np.nan] * len(list(totals_df)) 
        significant_strikes_df.loc[len(significant_strikes_df)] = [np.nan] * len(list(significant_strikes_df))
    
    # if not empty
    else:
        # get rounds
        number_of_rounds = int((len(clean_fighter_stats) - 2) / 2)

        # make dfs
        totals_df = pd.DataFrame(columns=totals_column_names)
        significant_strikes_df = pd.DataFrame(columns=significant_strikes_column_names)

        # loop rounds
        for round in range(number_of_rounds):
            # add totals
            totals_df.loc[len(totals_df)] = ['Round '+str(round+1)] + clean_fighter_stats[round+1]
            # add sig strikes
            significant_strikes_df.loc[len(significant_strikes_df)] = ['Round '+str(round+1)] + clean_fighter_stats[round+1+int((len(clean_fighter_stats) / 2))]

    # merge dfs
    fighter_stats_df = totals_df.merge(significant_strikes_df, how='inner')

    return fighter_stats_df

# combine stats
def combine_fighter_stats_dfs(fighter_a_stats_df: pd.DataFrame, fighter_b_stats_df: pd.DataFrame, soup: BeautifulSoup) -> pd.DataFrame:
    # combine stats
    fight_stats = pd.concat([fighter_a_stats_df, fighter_b_stats_df])

    # get event
    fight_stats['EVENT'] = soup.find('h2', class_='b-content__title').text.strip()

    # list names
    fighters_names = []
    # get names
    for tag in soup.find_all('a', class_='b-link b-fight-details__person-link'):
        fighters_names.append(tag.text.strip())

    # get bout
    fight_stats['BOUT'] = ' vs. '.join(fighters_names)

    # fix cols
    fight_stats = move_columns(fight_stats, ['EVENT', 'BOUT'], 'ROUND', 'before')

    return fight_stats

# parse and organise results and stats
def parse_organise_fight_results_and_stats(soup: BeautifulSoup, url: str, fight_results_column_names: List[str], totals_column_names: List[str], significant_strikes_column_names: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # parse results

    # get results
    fight_results = parse_fight_results(soup)
    # add url 
    fight_results.append('URL:'+url)
    # clean results
    fight_results_df = organise_fight_results(fight_results, fight_results_column_names)

    # parse stats

    # get stats
    fighter_a_stats, fighter_b_stats = parse_fight_stats(soup)
    # clean stats
    fighter_a_stats_clean = organise_fight_stats(fighter_a_stats)
    fighter_b_stats_clean = organise_fight_stats(fighter_b_stats)
    # make dfs
    fighter_a_stats_df = convert_fight_stats_to_df(fighter_a_stats_clean, totals_column_names, significant_strikes_column_names)
    fighter_b_stats_df = convert_fight_stats_to_df(fighter_b_stats_clean, totals_column_names, significant_strikes_column_names)
    # combine all
    fight_stats_df = combine_fighter_stats_dfs(fighter_a_stats_df, fighter_b_stats_df, soup)

    return fight_results_df, fight_stats_df

# generate urls
def generate_alphabetical_urls() -> List[str]:
    # list for urls
    list_of_alphabetical_urls = []

    # loop alphabet
    for character in list(string.ascii_lowercase):
        list_of_alphabetical_urls.append('http://ufcstats.com/statistics/fighters?char='+character+'&page=all')
    
    return list_of_alphabetical_urls

# parse fighter details
def parse_fighter_details(soup: BeautifulSoup, fighter_details_column_names: List[str]) -> pd.DataFrame:
    # get name
    # list names
    fighter_names = []
    # loop names
    for tag in soup.find_all('a', class_='b-link b-link_style_black'):
        # add name
        fighter_names.append(tag.text.strip())

    # get url
    # list urls
    fighter_urls = []
    # loop urls
    for tag in soup.find_all('a', class_='b-link b-link_style_black'):
        # add url
        fighter_urls.append(tag['href'])

    # zip data
    fighter_details = list(zip(fighter_names[0::3], fighter_names[1::3], fighter_names[2::3], fighter_urls[0::3]))

    # make df
    fighter_details_df = pd.DataFrame(fighter_details, columns=fighter_details_column_names)
    
    return fighter_details_df

# parse fighter tape
def parse_fighter_tott(soup: BeautifulSoup) -> List[str]:
    # list for tape
    fighter_tott = []

    # get name
    fighter_name = soup.find('span', class_='b-content__title-highlight').text
    # add name
    fighter_tott.append('Fighter:'+fighter_name)

    # get tape
    tott = soup.find_all('ul', class_='b-list__box-list')[0]
    # loop tags
    for tag in tott.find_all('i'):
        # add text
        fighter_tott.append(tag.text.strip() + tag.next_sibling.strip())
    # clean text
    fighter_tott = [text.replace('\n', '').replace('  ', '') for text in fighter_tott]
    
    return fighter_tott

# organise fighter tape
def organise_fighter_tott(tott_from_soup: List[str], fighter_tott_column_names: List[str], url: str) -> pd.DataFrame:
    # fix labels
    fighter_tott_clean = [re.sub('^(.+?): ?', '', text) for text in tott_from_soup]
    # add url
    fighter_tott_clean.append(url)
    # make df
    fighter_tott_df = pd.DataFrame(columns=fighter_tott_column_names)
    # add details
    fighter_tott_df.loc[(len(fighter_tott_df))] = fighter_tott_clean

    return fighter_tott_df

# move columns
def move_columns(df: pd.DataFrame, cols_to_move: List[str], ref_col: str, place: str) -> pd.DataFrame:
    # get cols
    cols = df.columns.tolist()
    
    if place == 'after':
        seg1 = cols[:list(cols).index(ref_col) + 1]
        seg2 = cols_to_move
    if place == 'before':
        seg1 = cols[:list(cols).index(ref_col)]
        seg2 = cols_to_move + [ref_col]

    seg1 = [i for i in seg1 if i not in seg2]
    seg3 = [i for i in cols if i not in seg1 + seg2]

    return(df[seg1 + seg2 + seg3])

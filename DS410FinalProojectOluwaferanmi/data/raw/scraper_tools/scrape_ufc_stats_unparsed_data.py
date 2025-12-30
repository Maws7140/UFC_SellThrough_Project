# imports
import pandas as pd
from tqdm import tqdm

# import library
import scrape_ufc_stats_library as LIB

# import config
import yaml
config = yaml.safe_load(open('scrape_ufc_stats_config.yaml'))



### check unparsed events ###
print('### Checking for unparsed events... ###')
print('\n')

# read details
parsed_event_details_df = pd.read_csv(config['event_details_file_name'])
# read fight details
parsed_fight_details_df = pd.read_csv(config['fight_details_file_name'])

# list parsed events
list_of_events_with_event_details = list(parsed_event_details_df['EVENT'])
# list complete events
list_of_events_with_fight_details = list(parsed_fight_details_df['EVENT'].unique())

# get soup
soup = LIB.get_soup(config['completed_events_all_url'])
# parse events
updated_event_details_df = LIB.parse_event_details(soup)
# list all events
list_of_all_events = list(updated_event_details_df['EVENT'])

# find new events
list_of_new_events = [event for event in list_of_all_events 
                      if event not in list_of_events_with_event_details]

# find incomplete events
list_of_incomplete_events = [event for event in list_of_events_with_event_details 
                             if event not in list_of_events_with_fight_details]

# combine lists
list_of_unparsed_events = list_of_new_events + list_of_incomplete_events

# check unparsed
unparsed_events = False
# if empty
if not list_of_unparsed_events:
    print('### All available events have been fully parsed. ###')
    print('\n')
else:
    # set true
    unparsed_events = True
    # show unparsed
    print('### There are unparsed or incomplete events. ###')
    print('\n')
    if list_of_new_events:
        print(f'New events (not in event details): {list_of_new_events}')
        print('\n')
    if list_of_incomplete_events:
        print(f'Incomplete events (no fight details): {list_of_incomplete_events}')
        print('\n')
    # save file
    updated_event_details_df.to_csv(config['event_details_file_name'], index=False)



# parse missing events ###

if unparsed_events:
    # read files
    parsed_fight_results_df = pd.read_csv(config['fight_results_file_name'])
    parsed_fight_stats_df = pd.read_csv(config['fight_stats_file_name'])

    ### parse details ###
    print('### Parsing Fight Details... ###')
    print('\n')

    # list missing urls
    list_of_unparsed_events_urls = list(updated_event_details_df['URL'].loc[(updated_event_details_df['EVENT'].isin(list_of_unparsed_events))])

    # make df
    unparsed_fight_details_df = pd.DataFrame(columns=config['fight_details_column_names'])

    # loop events
    for url in tqdm(list_of_unparsed_events_urls):
        # get soup
        soup = LIB.get_soup(url)

        # parse links
        fight_details_df = LIB.parse_fight_details(soup)

        # concat details
        unparsed_fight_details_df = pd.concat([unparsed_fight_details_df, fight_details_df])

    # remove old data
    if list_of_incomplete_events:
        # clean old data
        parsed_fight_details_df = parsed_fight_details_df[~parsed_fight_details_df['EVENT'].isin(list_of_incomplete_events)]
        parsed_fight_results_df = parsed_fight_results_df[~parsed_fight_results_df['EVENT'].isin(list_of_incomplete_events)]
        parsed_fight_stats_df = parsed_fight_stats_df[~parsed_fight_stats_df['EVENT'].isin(list_of_incomplete_events)]

    # combine details
    parsed_fight_details_df = pd.concat([unparsed_fight_details_df, parsed_fight_details_df])

    # save details
    parsed_fight_details_df.to_csv(config['fight_details_file_name'], index=False)
    print(unparsed_fight_details_df)
    print('\n')

    ### parse results and stats
    print('### Parsing Fight Results and Fight Stats... ###')
    print('\n')

    # list urls
    list_of_unparsed_fight_details_urls = list(unparsed_fight_details_df['URL'])

    # make results df
    unparsed_fight_results_df = pd.DataFrame(columns=config['fight_results_column_names'])
    # make stats df
    unparsed_fight_stats_df = pd.DataFrame(columns=config['fight_stats_column_names'])

    # loop fights
    for url in tqdm(list_of_unparsed_fight_details_urls):
        # get soup
        soup = LIB.get_soup(url)

        # parse both
        fight_results_df, fight_stats_df = LIB.parse_organise_fight_results_and_stats(
            soup,
            url,
            config['fight_results_column_names'],
            config['totals_column_names'],
            config['significant_strikes_column_names']
            )

        # add results
        unparsed_fight_results_df = pd.concat([unparsed_fight_results_df, fight_results_df])
        # add stats
        unparsed_fight_stats_df = pd.concat([unparsed_fight_stats_df, fight_stats_df])

    # combine all
    parsed_fight_results_df = pd.concat([unparsed_fight_results_df, parsed_fight_results_df])
    parsed_fight_stats_df = pd.concat([unparsed_fight_stats_df, parsed_fight_stats_df])

    # save results
    parsed_fight_results_df.to_csv(config['fight_results_file_name'], index=False)
    # save stats
    parsed_fight_stats_df.to_csv(config['fight_stats_file_name'], index=False)
    print(unparsed_fight_results_df)
    print('\n')
    print(unparsed_fight_stats_df)
    print('\n')



### check unparsed fighters ###
print('### Checking for unparsed fighters... ###')
print('\n')

#read fighters
parsed_fighter_details_df = pd.read_csv(config['fighter_details_file_name'])
# list urls
list_of_parsed_urls = list(parsed_fighter_details_df['URL'])

# gen urls
list_of_alphabetical_urls = LIB.generate_alphabetical_urls()

# make df
all_fighter_details_df = pd.DataFrame()

# loop urls
for url in tqdm(list_of_alphabetical_urls):
    # get soup
    soup = LIB.get_soup(url)
    # parse fighter
    fighter_details_df = LIB.parse_fighter_details(soup, config['fighter_details_column_names'])
    # add to df
    all_fighter_details_df = pd.concat([all_fighter_details_df, fighter_details_df])

# get urls
unparsed_fighter_urls = list(all_fighter_details_df['URL'])

# list unparsed
list_of_unparsed_fighter_urls = [url for url in unparsed_fighter_urls if url not in list_of_parsed_urls]

#heck unparsed
unparsed_fighters = False
# if empty
if not list_of_unparsed_fighter_urls:
    print('### All available fighters have been parsed. ###')
    print('\n')
else:
    # set true
    unparsed_fighters = True
    # show unparsed
    print('### There are unparsed fighters. ###')
    print('\n')

    # save file
    all_fighter_details_df.to_csv(config['fighter_details_file_name'], index=False)
    print(list_of_unparsed_fighter_urls)
    print('\n')



# parse missing fighters 

if unparsed_fighters:
    print('### Parsing Fighter ToTT... ###')
    print('\n')
    # read files
    parsed_fighter_tott_df = pd.read_csv(config['fighter_tott_file_name'])

    # make df
    unparsed_fighter_tott_df = pd.DataFrame(columns=config['fighter_tott_column_names'])

    # loop urls
    for url in tqdm(list_of_unparsed_fighter_urls):
        # get soup
        soup = LIB.get_soup(url)
        # parse tape
        fighter_tott = LIB.parse_fighter_tott(soup)
        # organise tape
        fighter_tott_df = LIB.organise_fighter_tott(fighter_tott, config['fighter_tott_column_names'], url)
        # add fighter
        unparsed_fighter_tott_df = pd.concat([unparsed_fighter_tott_df, fighter_tott_df])

    # combine tape
    parsed_fighter_tott_df = pd.concat([parsed_fighter_tott_df, unparsed_fighter_tott_df])
    # save file
    parsed_fighter_tott_df.to_csv(config['fighter_tott_file_name'], index=False)
    print(unparsed_fighter_tott_df)
    print('\n')
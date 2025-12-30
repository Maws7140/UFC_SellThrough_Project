# Raw UFC Data

This folder contains the raw data I downloaded from UFCStats.com.

## Files

| File | What's in it | Rows |
|------|--------------|------|
| events.csv | All UFC events (date, location) | ~756 |
| fight_bouts.csv | List of fights per event | ~8,500 |
| fight_results.csv | Who won, how, what round | ~8,500 |
| fight_stats.csv | Round-by-round stats (strikes, takedowns) | ~40,000 |
| fighters.csv | Fighter names and nicknames | ~4,500 |
| fighter_attributes.csv | Height, weight, reach, stance | ~4,500 |

## Where I got this data

I downloaded it from [Greco1899/scrape_ufc_stats](https://github.com/Greco1899/scrape_ufc_stats) on GitHub. They scrape UFCStats.com daily and publish the CSVs.

## Important columns

### events.csv
- EVENT: Event name (like "UFC 300")
- DATE: When it happened
- LOCATION: City, state/country

### fight_results.csv
- BOUT: Fighter names (like "Jon Jones vs Stipe Miocic")
- OUTCOME: W/L
- METHOD: How it ended (KO/TKO, Submission, Decision)
- ROUND: What round it ended

### fight_stats.csv
This has the detailed stats for each round of each fight:
- SIG.STR.: Significant strikes landed/attempted
- TD: Takedowns landed/attempted
- CTRL: Control time

## Notes

- Some fighters have missing data for height/reach (shown as "--")
- Dates are in different formats - had to handle this in the ETL script
- Old events (like UFC 1) have less detailed stats

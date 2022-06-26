"""
Script outputting quartely aggregates for each counties (2020 borders).

Author: Jesper Kloster Ellingsen
"""
import pandas as pd


counties = ['Oslo County', 'Viken', 'Vestland', 'Nordland', 'Trøndelag', 'Innlandet',
 'Vestfold og Telemark', 'Møre og Romsdal', 'Agder', 'Rogaland',
 'Troms og Finnmark', 'unknown']
years = [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
final_dfs = {county: pd.DataFrame() for county in counties}
OUTPUT_PATH = '../datasets/county-aggregated/'

def resample_data(df, freq = "D"):
    res_median = df.resample(freq).median().rename(columns={"Ned": "median_down", "Opp": "median_up", "Delay (ping)":"median_delay"})
    res_mean = df.resample(freq).mean().rename(columns={"Ned": "mean_down", "Opp": "mean_up", "Delay (ping)":"mean_delay"})
    res_std = df.resample(freq).std().rename(columns={"Ned": "std_down", "Opp": "std_up", "Delay (ping)":"std_delay"})
    res_quant = df.resample(freq).quantile([0.25,0.75]).unstack().rename(columns={"Ned": "quantile_down", "Opp": "quantile_up", "Delay (ping)":"quantile_delay"})
    res_count = df['Ned'].resample(freq).count().rename("count")
    return res_median, res_mean, res_std, res_quant, res_count


def rename_counties(year, df):
    mapping_num={'00':'unknown',
               '01':'Viken', 
               '02':'Agder', 
               '04':'Viken', 
               '05':'Troms og Finnmark', 
               '06':'Innlandet',
               '07':'Vestland', 
               '08':'Møre og Romsdal',
               '09':'Nordland',
               '10':'Trøndelag', 
               '11':'Innlandet',
               '12':'Oslo County',
               '13':'Viken',
               '14':'Rogaland',
               '15':'Vestland',
               '16':'Trøndelag',
               '17':'Vestfold og Telemark',
               '18':'Troms og Finnmark',
               '19':'Agder',
               '20':'Vestfold og Telemark',
               'Akershus':'Viken', 
               'Aust-Agder':'Agder', 
               'Buskerud':'Viken', 
               'Finnmark':'Troms og Finnmark',
               'Finnmark Fylke':'Troms og Finnmark',
               'Hedmark':'Innlandet',
               'Hordaland Fylke':'Vestland', 
               'Hordaland':'Vestland', 
               'More og Romsdal fylke':'Møre og Romsdal',
               'Nordland Fylke':'Nordland',
               'Nord-Trondelag Fylke':'Trøndelag', 
               'Oppland':'Innlandet',
               'Østfold':'Viken',
               'Rogaland Fylke':'Rogaland',
               'Rogaland':'Rogaland',
               'Sogn og Fjordane Fylke':'Vestland',
               'Sogn og Fjordane':'Vestland',
               'Sor-Trondelag Fylke':'Trøndelag',
               'Telemark':'Vestfold og Telemark',
               'Troms Fylke':'Troms og Finnmark',
               'Troms':'Troms og Finnmark',
               'Vest-Agder Fylke':'Agder',
               'Vest-Agder':'Agder',
               'Vestfold':'Vestfold og Telemark'
                 
              }
    
    df.Fylke = df.Fylke.replace(mapping_num)
    return df

    
def import_agg_data(DATA_PATH):
    df = pd.read_csv(DATA_PATH, sep=";", header= None)
    df.columns = ["ID","Ned","Opp","Delay (ping)", "Tid", "ISP", "By", "Fylke", "Land", "Bredde", "Lengde", "test_id", "Isp_id", "prod_id","User platform", "nope", "ip_ver" ]
    df = df[df.Land == 'NO']
    df.drop(columns =[ "ID", "ISP", "By", "Land", "Bredde", "Lengde", "test_id", "Isp_id", "prod_id","User platform", "nope", "ip_ver" ], inplace=True)
    df["Tid"] = pd.to_datetime(df["Tid"])
    df.index = df["Tid"]
    return df


if __name__ == "__main__":
    for year in years:
        DATA_PATH = '../datasets/nettfart-'+ str(year) +'/nettfart-'+ str(year) + '.csv'
        print("Importing " +DATA_PATH+"...") 
        df = import_agg_data(DATA_PATH)
       
        print("renaming counties ") 
        df = rename_counties(year, df)
        
        print("aggregating...")

        for county in counties:
            if county == 'unknown':
                continue
            current_df = df[df.Fylke == county].copy()
            current_df.drop(columns =["Fylke"], inplace=True)
            res_median, res_mean, res_std, res_quant, res_count = resample_data(current_df, 'D')
            yagg_df = pd.concat([res_median,res_mean, res_std, res_quant, res_count], axis=1)
            #try:
            final_dfs[county] = pd.concat([final_dfs[county], yagg_df])
            #except:
            #    print(county)
        print("aggr. and added for " + str(year))

    print("writing dataframes to csv...")
    for county in counties:
        final_dfs[county].to_csv(OUTPUT_PATH + county.replace(" ", "_") + '_day_aggr.csv')
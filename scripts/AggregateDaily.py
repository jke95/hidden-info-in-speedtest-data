import pandas as pd

"""
Script for aggregating Nettfart speedtest data daily and outputting an aggregte file with median, mean, std and count (num of measuremnts each day). 
"""
years = [2012,2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]

final_df = pd.DataFrame()
for year in years:
    DATA_PATH = '../datasets/nettfart-'+ str(year) +'/nettfart-'+ str(year) + '.csv'
    df = pd.read_csv(DATA_PATH, sep=";", header= None)
    df.columns = ["ID","Ned","Opp","Delay (ping)", "Tid", "ISP", "By", "Fylke", "Land", "Bredde", "Lengde", "test_id", "Isp_id", "prod_id","User platform", "nope", "ip_ver" ]
    df = df[df.Land == 'NO']
    df.drop(columns =[ "ID", "ISP", "By", "Fylke", "Land", "Bredde", "Lengde", "test_id", "Isp_id", "prod_id","User platform", "nope", "ip_ver" ], inplace=True)
    df["Tid"] = pd.to_datetime(df["Tid"])
    df.index = df["Tid"]
    
    print("filtering and converting...")
    #converting from Kbit/s to Mbit/s 
    df[['Ned', 'Opp']] = df[['Ned', 'Opp']]/1000
    #removing measurments over 1000 Mbit/s
    df = df[df['Ned'] <= 1000]
    
    daily_median = df.resample("D").median().rename(columns={"Ned": "median_down", "Opp": "median_up", "Delay (ping)":"median_delay"})
    daily_mean = df.resample("D").mean().rename(columns={"Ned": "mean_down", "Opp": "mean_up", "Delay (ping)":"mean_delay"})
    daily_std = df.resample("D").std().rename(columns={"Ned": "std_down", "Opp": "std_up", "Delay (ping)":"std_delay"})
    daily_quant = df.resample("D").quantile([0.25,0.75]).unstack().rename(columns={"Ned": "quantile_down", "Opp": "quantile_up", "Delay (ping)":"quantile_delay"})

    daily_count = df['Ned'].resample("D").count().rename("count")
    
    yagg_df = pd.concat([daily_median,daily_mean, daily_std, daily_quant ,daily_count], axis=1)
    final_df = pd.concat([final_df, yagg_df])
    print("aggr. and added for " + str(year) )
    

final_df.to_csv('../datasets/Nettfart_daily_web.csv')

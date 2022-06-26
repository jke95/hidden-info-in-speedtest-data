import pandas as pd
import numpy as np

"""
Script for cutting Nettfart speedtest data int bins of download speeds (corresponding to national statistics).
"""
years = [2017, 2018, 2019, 2020]

final_df = pd.DataFrame()
for year in years:
    DATA_PATH = '../datasets/nettfart-'+ str(year) +'/nettfart-'+ str(year) + '.csv'
    df = pd.read_csv(DATA_PATH, sep=";", header= None)
    df.columns = ["ID","Ned","Opp","Delay (ping)", "Tid", "ISP", "By", "Fylke", "Land", "Bredde", "Lengde", "test_id", "Isp_id", "prod_id","User platform", "nope", "ip_ver" ]
    df = df[df.Land == 'NO']
    df.drop(columns =[ "ID", "ISP", "By", "Fylke", "Land", "Bredde", "Lengde", "test_id", "Isp_id", "prod_id","User platform", "nope", "ip_ver" ], inplace=True)
    df["Tid"] = pd.to_datetime(df["Tid"])
    df.index = df["Tid"]
    df['Ned'] = df['Ned']/1000 
    #definig thresholds for bins
    bins = [0,10, 30, 100, 250, 500, 1000, np.inf]
    #cut all measurements into bisn for each year. 
    groups = df.groupby([pd.Grouper(freq='Y'), pd.cut(df['Ned'], bins)])
    bin_counts = groups.size().unstack()
    
 
    counts = df['Ned'].resample("Y").count().rename("tot_count")
    
    yagg_df = pd.concat([bin_counts ,counts], axis=1)
    final_df = pd.concat([final_df, yagg_df])
    print("aggr. and added for " + str(year) )
    
    
    

final_df.to_csv('../datasets/web_bin_count_down.csv')

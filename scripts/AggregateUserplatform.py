import pandas as pd
import httpagentparser

"""
Script for aggregating extracting the user OS and browser name from http agent
"""
years = [2012,2016, 2020]

final_df = pd.DataFrame()
for year in years:
    DATA_PATH = '../datasets/nettfart-'+ str(year) +'/nettfart-'+ str(year) + '.csv'
    df = pd.read_csv(DATA_PATH, sep=";", header= None)
    df.columns = ["ID","Ned","Opp","Delay (ping)", "Tid", "ISP", "By", "Fylke", "Land", "Bredde", "Lengde", "test_id", "Isp_id", "prod_id","User platform", "nope", "ip_ver" ]
    df = df[df.Land == 'NO']
    df.drop(columns =[ "ISP", "By", "Fylke", "Land", "Bredde", "Lengde", "test_id", "Isp_id", "prod_id", "nope", "ip_ver" ], inplace=True)
    df["Tid"] = pd.to_datetime(df["Tid"])
    df.index = df["Tid"]
    
    print("parisng user agent...")
    df['User platform'] =df['User platform'].apply(lambda s: httpagentparser.simple_detect_tuple(s))
    
    #extract OS and broser from user platform information. 
    df['OS'] = df['User platform'].apply(lambda s: s[0])
    df['OS_ver'] = df['User platform'].apply(lambda s: s[1])
    df['Browser'] = df['User platform'].apply(lambda s: s[2])
    df['Unknown'] = df['User platform'].apply(lambda s: s[3])
    
    print("Counting...")
    os_count = df.groupby("OS")['ID'].count()
    os_ver_count = df.groupby("OS_ver")['ID'].count()
    browser = df.groupby("Browser")['ID'].count()
    unknown = df.groupby("Unknown")['ID'].count()

    
    
    agg_df = pd.concat([os_count,os_ver_count, browser, unknown], axis=1)
    agg_df['year'] = year
    
    final_df = pd.concat([final_df, agg_df])
    print("aggr. and added for " + str(year) )
    
    #groups = df.groupby(['username', pd.cut(df.nedlas, bins)])
    

final_df.to_csv('../datasets/web_user_platforms.csv')
import pandas as pd
import logging, util
import os, re
import datetime as dtime

ratesFolder = util.get_rates_dir()
logging.info("Raw File Directory -------->"+ ratesFolder)

curr_week = util.get_week()
curr_year = util.get_year()

TR_rates_df = pd.read_excel(os.path.join(ratesFolder, "toll_roadsfastrack.xlsx"))
TR_rates_df["RATE SOURCE"] = "toll_roadsfastrack"

ITOLL_df = pd.read_excel(os.path.join(ratesFolder, "itolls.xlsx"), dtype=str)
ITOLL_df["RATE SOURCE"] = "itolls"

PA_OH_df = pd.read_excel(os.path.join(ratesFolder, "paoh Toll Rates.xlsx"), dtype=str)
PA_OH_df["RATE SOURCE"] = "paoh Toll Rates"

TIMELESS_Agency_Rate_DF = pd.read_excel(os.path.join(ratesFolder, "Timeless Agencies Toll Rates.xlsx"), dtype=str)
TIMELESS_Agency_Rate_DF["RATE SOURCE"] = "Timeless Agencies Toll Rates"

fixed_Agency_Rate_DF = pd.read_excel(os.path.join(ratesFolder, "agency fixed rates.xlsx"), dtype=str)
fixed_Agency_Rate_DF["RATE SOURCE"] = "agency fixed rates"

SouthBay_DF = pd.read_excel(os.path.join(ratesFolder, "South Bay Expressway Toll Schedule.xlsx"), dtype=str)
SouthBay_DF["RATE SOURCE"] = "South Bay Expressway Toll Schedule"

PANYNJ_DF = pd.read_excel(os.path.join(ratesFolder, "PANYNJ.xlsx"), dtype=str)
PANYNJ_DF["RATE SOURCE"] = "PANYNJ"

CFX_DF = pd.read_excel(os.path.join(ratesFolder, "cfxpidrates.xlsx"), dtype=str)
CFX_DF["RATE SOURCE"] = "cfxpidrates"

KTA_DF = pd.read_excel(os.path.join(ratesFolder, "KTA.xlsx"), dtype=str)
KTA_DF["RATE SOURCE"] = "KTA"

timeAgenciesDf = pd.read_excel(os.path.join(ratesFolder, "timeAgencies.xlsx"), dtype=str)
timeAgenciesDf["RATE SOURCE"] = "timeAgencies"

timeOnlyAgenciesDf = pd.read_excel(os.path.join(ratesFolder, "timeOnlyAgency.xlsx"), dtype=str)
timeOnlyAgenciesDf["RATE SOURCE"] = "timeOnlyAgency"

ptcRAtes = pd.read_excel(os.path.join(ratesFolder, "PTC.xlsx"), dtype=str)
ptcRAtes["RATE SOURCE"] = "PTC"

NJTA_DF = pd.read_excel(os.path.join(ratesFolder, "NJTA.xlsx"), dtype=str)
NJTA_DF["RATE SOURCE"] = "NJTA"

NTTA_DF = pd.read_excel(os.path.join(ratesFolder, "NTTA Rates.xlsx"), dtype=str)
NTTA_DF["RATE SOURCE"] = "NTTA Rates"

LanesperAg_DF = pd.read_excel(os.path.join( ratesFolder, "AgencyExitLanes.xlsx"))
LanesperAg_DF["RATE SOURCE"] = "AgencyExitLanes"

CFX_DF = CFX_DF[CFX_DF["PID"] != "-"]

OTCIRAtesDf = pd.read_excel(os.path.join(ratesFolder, "OHIO_TURNPIKE.xlsx"), dtype=str)
OTCIRAtesDf["RATE SOURCE"] = "OHIO_TURNPIKE"

FTE_DF = pd.read_excel(os.path.join(ratesFolder, "SR 821 (fte).xlsx"), dtype=str)
FTE_DF["RATE SOURCE"] = "SR 821 (fte)"

def time_is_between(time, time_range):
    if time_range[1] < time_range[0]:
        return time >= time_range[0] or time <= time_range[1]
    return time_range[0] <= time <= time_range[1]


def processHigherRates(final_df = pd.DataFrame()):
    logging.info("=========================FILLING HIGH RATES=================================")

    if final_df.shape[0] == 0:
        final_df = pd.read_excel(f'{util.get_output_folder_dir()}/WEEK {curr_week} ({curr_year}) TOLLS ELECTRONIC Before Rates.xlsx')
    final_df["HIGH RATES"] = ""
    logging.info("Processing rates per file")
# Processing ILTOLL/ILLINOIS
    logging.info("ILTOLL RATES")
    iltoll_df = final_df[final_df["AGENCY"].astype(str).str.contains("ILTOLL", regex=False) | final_df["AGENCY"].astype(str).str.contains("Illinois State Toll Highway Authority", case=False, regex=False) | final_df["AGENCY"].astype(str).str.contains("ILLINOIS TOLLWAY", regex=False) | final_df["AGENCY"].astype(str).str.contains("ILLINOIS STATE TOLL HIGHWAY AUTHORITY", regex=False)]
    for i_i, i_row in iltoll_df.iterrows():
        lane = None
        Exit_Location = i_row["EXIT LOCATION"].replace(".","")
        lane = Exit_Location
        pl = ["PL","Pl","pl"]
        if "-" in Exit_Location and any(Exit_Location.startswith(item) for item in pl):
            lane = Exit_Location.split("-")[1].strip()
        if "-" in Exit_Location and Exit_Location[0].isdigit():
            lane = Exit_Location.split("-")[0].strip()        
        lookupdf = ITOLL_df[ITOLL_df['Plaza No'] == lane]
        if lookupdf.shape[0] != 0:
            t_time = str(i_row["EXIT DATE/TIME"]).split(" ")[1]
            hrtime = ["06:00:00", "22:00:00"]
            for l_i, l_row in lookupdf.iterrows():
                CLASS = str(i_row['CLASS'])
                class_column = None
                if time_is_between(t_time, hrtime):
                    if CLASS == "-" or int(CLASS.strip().lstrip('0')) >= 6:
                        class_column = 'd_axle_5.tag' 
                    else:
                        class_column = 'd_axle_' + CLASS.strip().lstrip('0') + '.tag'             
                else:
                    if CLASS == "-" or int(CLASS.strip().lstrip('0')) >= 6:
                        class_column = 'n_axle_5.tag' 
                    else:
                        class_column = 'n_axle_' + CLASS.strip().lstrip('0') + '.tag'  
                final_df.loc[((final_df["AGENCY"] == "ILTOLL") | (final_df["AGENCY"] == "Illinois State Toll Highway Authority") | (final_df["AGENCY"] == "ILLINOIS TOLLWAY") | (final_df["AGENCY"] == "ILLINOIS STATE TOLL HIGHWAY AUTHORITY")) & (final_df["EXIT DATE/TIME"] == i_row["EXIT DATE/TIME"]) & (final_df["EXIT LOCATION"] == i_row["EXIT LOCATION"]) & (final_df["AMOUNT"] == i_row["AMOUNT"]) & (final_df["CLASS"] == i_row["CLASS"]),["HIGH RATES"]] = lookupdf[class_column].iloc[0]
                final_df.loc[((final_df["AGENCY"] == "ILTOLL") | (final_df["AGENCY"] == "Illinois State Toll Highway Authority") | (final_df["AGENCY"] == "ILLINOIS TOLLWAY") | (final_df["AGENCY"] == "ILLINOIS STATE TOLL HIGHWAY AUTHORITY")) & (final_df["EXIT DATE/TIME"] == i_row["EXIT DATE/TIME"]) & (final_df["EXIT LOCATION"] == i_row["EXIT LOCATION"]) & (final_df["AMOUNT"] == i_row["AMOUNT"]) & (final_df["CLASS"] == i_row["CLASS"]),["RATE SOURCE"]] = lookupdf["RATE SOURCE"].iloc[0]
                  
    datatoexcel = pd.ExcelWriter(f'{util.get_output_folder_dir()}/WEEK {curr_week} ({curr_year}) TOLLS ELECTRONIC.xlsx', datetime_format='m/dd/yyyy h:mm:ss AM/PM')
    final_df.to_excel(datatoexcel, index=False)
    datatoexcel._save()

    return final_df

processHigherRates()




       
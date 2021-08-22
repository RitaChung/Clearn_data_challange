import os
import re
import pandas as pd
import sqlite3
import numpy as np
from collections import Counter
import operator

path = os.getcwd()
db_path = os.path.join(path,'xeneta.db')
ocean_freight_rates_path = os.path.join(path,'data_science_test_1.xlsx')

con = sqlite3.connect(db_path)
ports = pd.read_sql_query("select * from ports", con)
con.close()

xl = pd.ExcelFile(ocean_freight_rates_path)
init = xl.parse('Ocean Freight Rates')

# get information
info = init.iloc[:4,0:2]
info = info.columns.to_frame().T.append(info, ignore_index=True)
info.columns = ["inform","value"]
customer = info[info['inform']=='Customer Name']['value'].values[0]
carrier = info[info['inform']=='Carrier']['value'].values[0]
contract_num = info[info['inform']=='Contract Number']['value'].values[0]

rates = init[5:].reset_index(drop=True)
headers = rates.iloc[0]
rate = pd.DataFrame(rates.values[1:], columns=headers)

# makeup
rate["Receipt"].loc[617] = "Beijiao, China"
rate["Delivery"].loc[617] = "Male, Maldives"
rate["Delivery"].loc[592] = "Kolkata, India"
rate["40HDRY"].loc[6444] = "72.5 MYR"
rate["40DRY"].loc[6417] = "72.5 MYR"


# get unique shipment
sub = rate.loc[(rate["Service Mode"] == "CY/CY")&(rate["Commodity Name"] == "FAK")&(rate["Rate Basis"] == "PER_CONTAINER")].reset_index(drop=True)
sub["Origin Port (name)"] = np.where(sub["Load Port"].isna(),sub["Receipt"],sub["Load Port"])
sub["Destination Port (name)"] = np.where(sub["Discharge Port"].isna(),sub["Delivery"],sub["Discharge Port"])

target = sub[["Effective Date","Expiry Date","Origin Port (name)","Destination Port (name)","Commodity Name"]].drop_duplicates().reset_index(drop=True)


# calculate "THC Used"
sub["has_ohc"] = np.where(sub["Charge"] == "OHC",1,0)
sub["has_dhc"] = np.where(sub["Charge"] == "DHC",1,0)
sub_g = sub.groupby(["Effective Date","Expiry Date","Origin Port (name)","Destination Port (name)","Commodity Name"])\
    .agg({"has_ohc":"sum","has_dhc":"sum"}).reset_index()


def THC_USED(ohc, dhc):
    if (ohc == 1) & (dhc == 0):
        return "OTHC"
    elif (dhc == 1) & (ohc == 0):
        return "DTHC"
    elif (ohc == 1) & (dhc == 1):
        return "BOTH"
    else:
        return "NONE"


sub_g["THC Used"] = sub_g.apply(lambda x: THC_USED(x["has_ohc"], x["has_dhc"]), axis=1)
thc_used = sub_g[['Effective Date', 'Expiry Date', 'Origin Port (name)','Destination Port (name)', 'Commodity Name','THC Used']]
target = target.merge(thc_used, how='left')

# separate amount and current, create new columns by different charge


def split_amount_and_currency(rate):
    rate = re.sub("\s+", " ", str(rate).strip())
    if len(rate.split(" ")) == 2:
        amount, curr = rate.split(" ")
        if amount == "0":
            amount = np.nan
        else:
            amount = float(amount)
    elif len(rate.split(" ")) == 1:
        if rate.isnumeric():
            curr = np.nan
            if rate == "0":
                amount = np.nan
            else:
                amount = float(rate)
        else:
            amount = np.nan
            curr = rate
    else:
        amount, curr = np.nan, np.nan
    return amount, curr


def inform_transform(DRY20, DRY40, HDRY40):
    amount_dry20, curr_dry20 = split_amount_and_currency(DRY20)
    amount_dry40, curr_dry40 = split_amount_and_currency(DRY40)
    amount_hdry40, curr_hdry40 = split_amount_and_currency(HDRY40)

    curr_dict = dict(Counter([curr_dry20, curr_dry40, curr_hdry40]))
    stand_curr = max(curr_dict.items(), key=operator.itemgetter(1))[0]
    return stand_curr, amount_dry20, amount_dry40, amount_hdry40

sub['currency'], sub['20DC'], sub['40DC'], sub['40HC'] = sub.apply(lambda x: inform_transform(x["20DRY"], x["40DRY"], x["40HDRY"]), result_type='expand', axis=1).transpose().values


def generate_charge_columns(tab, charge):
    temp = tab[tab["Charge"] == charge][['Effective Date', 'Expiry Date', 'Origin Port (name)','Destination Port (name)',
                                         'Commodity Name','currency','20DC','40DC','40HC']].reset_index(drop=True)
    remain_columns = ['Effective Date', 'Expiry Date', 'Origin Port (name)', 'Destination Port (name)', 'Commodity Name']
    rename_columns = ['currency','20DC','40DC','40HC']
    new_name_list = [charge + ', ' + i for i in rename_columns]
    new_name_list = remain_columns + new_name_list
    temp.columns = new_name_list
    return temp


charge_list = list(sub["Charge"].unique())
for charge in charge_list:
    charge_tab = generate_charge_columns(sub, charge)
    target = target.merge(charge_tab, how='left')


# port_name mapping
target = target.merge(ports, how='left', left_on="Origin Port (name)", right_on="port_name").drop('port_name', axis=1)\
    .rename(columns={"port_code":"Origin Port (code)"})
target = target.merge(ports, how='left', left_on="Destination Port (name)", right_on="port_name").drop('port_name', axis=1)\
    .rename(columns={"port_code":"Destination Port (code)"})

# get customer info
target['Customer'] = customer
target['Carrier'] = carrier
target['Contract Number'] = contract_num

# save csv
target.rename(columns={"Effective Date":"Rate - Valid from","Expiry Date":"Rate - Valid to","Commodity Name":"Commodity"}, inplace=True)
target[['Rate - Valid from', 'Rate - Valid to', 'Origin Port (name)', 'Origin Port (code)', 'Destination Port (name)',
        'Destination Port (code)', 'Customer', 'Carrier', 'Contract Number', 'Commodity', 'THC Used', 'BAS, currency',
        'BAS, 20DC', 'BAS, 40DC', 'BAS, 40HC', 'CFD, currency', 'CFD, 20DC', 'CFD, 40DC', 'CFD, 40HC', 'CFO, currency',
        'CFO, 20DC', 'CFO, 40DC', 'CFO, 40HC', 'DHC, currency', 'DHC, 20DC', 'DHC, 40DC', 'DHC, 40HC', 'ERS, currency',
        'ERS, 20DC', 'ERS, 40DC', 'ERS, 40HC', 'EXP, currency', 'EXP, 20DC', 'EXP, 40DC', 'EXP, 40HC', 'IMP, currency',
        'IMP, 20DC', 'IMP, 40DC', 'IMP, 40HC', 'LSS, currency', 'LSS, 20DC', 'LSS, 40DC', 'LSS, 40HC', 'OHC, currency',
        'OHC, 20DC', 'OHC, 40DC', 'OHC, 40HC', 'PSS, currency', 'PSS, 20DC', 'PSS, 40DC', 'PSS, 40HC', 'RHI, currency',
        'RHI, 20DC', 'RHI, 40DC', 'RHI, 40HC', 'EBS, currency', 'EBS, 20DC', 'EBS, 40DC', 'EBS, 40HC', 'PAE, currency',
        'PAE, 20DC', 'PAE, 40DC', 'PAE, 40HC', 'SBF, currency', 'SBF, 20DC', 'SBF, 40DC', 'SBF, 40HC']]\
    .to_excel("output.xlsx", index=False)


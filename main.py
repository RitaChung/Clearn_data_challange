import os
import re
import pandas as pd
import sqlite3
import numpy as np
from collections import Counter
import operator


class shipment:
    def __init__(self, record, ports):
        self.ports = ports
        self.customer, self.carrier, self.contract_number = self.parse_customer_information(record)
        self.clear_record = self.parse_record(record)
        self.target = self.get_target_shipment()
        self.THC_Used = self.calculated_THC_Used()
        self.report = self.get_report()

    def parse_customer_information(self, record):
        info = record.iloc[:4, 0:2]
        info = info.columns.to_frame().T.append(info, ignore_index=True)
        info.columns = ["inform", "value"]
        customer = info[info['inform'] == 'Customer Name']['value'].values[0]
        carrier = info[info['inform'] == 'Carrier']['value'].values[0]
        contract_num = info[info['inform'] == 'Contract Number']['value'].values[0]
        return customer, carrier, contract_num

    def parse_record(self, record):
        rates = record[5:].reset_index(drop=True)
        headers = rates.iloc[0]
        rate = pd.DataFrame(rates.values[1:], columns=headers)

        # makeup
        rate["Receipt"].loc[617] = "Beijiao, China"
        rate["Delivery"].loc[617] = "Male, Maldives"
        rate["Delivery"].loc[592] = "Kolkata, India"
        rate["40HDRY"].loc[6444] = "72.5 MYR"
        rate["40DRY"].loc[6417] = "72.5 MYR"

        # filter by condition
        rate = rate.loc[(rate["Service Mode"] == "CY/CY") & (rate["Commodity Name"] == "FAK") & (rate["Rate Basis"] == "PER_CONTAINER")]\
            .reset_index(drop=True)

        # create extra columns
        rate["Origin Port (name)"] = np.where(rate["Load Port"].isna(), rate["Receipt"], rate["Load Port"])
        rate["Destination Port (name)"] = np.where(rate["Discharge Port"].isna(), rate["Delivery"], rate["Discharge Port"])
        rate["has_ohc"] = np.where(rate["Charge"] == "OHC", 1, 0)
        rate["has_dhc"] = np.where(rate["Charge"] == "DHC", 1, 0)
        return rate

    def get_target_shipment(self):
        target = self.clear_record[["Effective Date", "Expiry Date", "Origin Port (name)", "Destination Port (name)",
                      "Commodity Name"]].drop_duplicates().reset_index(drop=True)
        return target

    def get_THC_used(self, ohc, dhc):
        if (ohc == 1) & (dhc == 0):
            return "OTHC"
        elif (dhc == 1) & (ohc == 0):
            return "DTHC"
        elif (ohc == 1) & (dhc == 1):
            return "BOTH"
        else:
            return "NONE"

    def calculated_THC_Used(self):
        temp = self.clear_record.groupby(["Effective Date","Expiry Date","Origin Port (name)","Destination Port (name)","Commodity Name"])\
            .agg({"has_ohc":"sum","has_dhc":"sum"}).reset_index()
        temp["THC Used"] = temp.apply(lambda x: self.get_THC_used(x["has_ohc"], x["has_dhc"]), axis=1)
        return temp[["Effective Date","Expiry Date","Origin Port (name)","Destination Port (name)","Commodity Name","THC Used"]]

    def split_amount_and_currency(self, rate):
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

    def inform_transform(self, DRY20, DRY40, HDRY40):
        amount_dry20, curr_dry20 = self.split_amount_and_currency(DRY20)
        amount_dry40, curr_dry40 = self.split_amount_and_currency(DRY40)
        amount_hdry40, curr_hdry40 = self.split_amount_and_currency(HDRY40)

        curr_dict = dict(Counter([curr_dry20, curr_dry40, curr_hdry40]))
        stand_curr = max(curr_dict.items(), key=operator.itemgetter(1))[0]
        return stand_curr, amount_dry20, amount_dry40, amount_hdry40

    def generate_charge_columns(self, tab, charge):
        temp = tab[tab["Charge"] == charge][
            ['Effective Date', 'Expiry Date', 'Origin Port (name)', 'Destination Port (name)',
             'Commodity Name', 'currency', '20DC', '40DC', '40HC']].reset_index(drop=True)
        remain_columns = ['Effective Date', 'Expiry Date', 'Origin Port (name)', 'Destination Port (name)',
                          'Commodity Name']
        rename_columns = ['currency', '20DC', '40DC', '40HC']
        new_name_list = [charge + ', ' + i for i in rename_columns]
        new_name_list = remain_columns + new_name_list
        temp.columns = new_name_list
        return temp

    def get_report(self):
        # merge thc_used
        aim = self.target.merge(self.THC_Used, how='left')

        # merge rate by charge
        median = self.clear_record
        median['currency'], median['20DC'], median['40DC'], median['40HC'] = median.apply(
            lambda x: self.inform_transform(x["20DRY"], x["40DRY"], x["40HDRY"]), result_type='expand',
            axis=1).transpose().values

        charge_list = list(median["Charge"].unique())
        for charge in charge_list:
            charge_tab = self.generate_charge_columns(median, charge)
            aim = aim.merge(charge_tab, how='left')

        # merge port_name mapping
        aim = aim.merge(self.ports, how='left', left_on="Origin Port (name)", right_on="port_name").drop('port_name', axis=1)\
            .rename(columns={"port_code": "Origin Port (code)"})
        aim = aim.merge(self.ports, how='left', left_on="Destination Port (name)", right_on="port_name").drop('port_name', axis=1)\
            .rename(columns={"port_code": "Destination Port (code)"})

        # merge customer info
        aim['Customer'] = self.customer
        aim['Carrier'] = self.carrier
        aim['Contract Number'] = self.contract_number
        aim.rename(columns={"Effective Date": "Rate - Valid from", "Expiry Date": "Rate - Valid to",
                            "Commodity Name": "Commodity"}, inplace=True)
        return aim[['Rate - Valid from', 'Rate - Valid to', 'Origin Port (name)', 'Origin Port (code)',
                    'Destination Port (name)', 'Destination Port (code)', 'Customer', 'Carrier', 'Contract Number',
                    'Commodity', 'THC Used', 'BAS, currency', 'BAS, 20DC', 'BAS, 40DC', 'BAS, 40HC', 'CFD, currency',
                    'CFD, 20DC', 'CFD, 40DC', 'CFD, 40HC', 'CFO, currency', 'CFO, 20DC', 'CFO, 40DC', 'CFO, 40HC',
                    'DHC, currency', 'DHC, 20DC', 'DHC, 40DC', 'DHC, 40HC', 'ERS, currency', 'ERS, 20DC', 'ERS, 40DC',
                    'ERS, 40HC', 'EXP, currency', 'EXP, 20DC', 'EXP, 40DC', 'EXP, 40HC', 'IMP, currency', 'IMP, 20DC',
                    'IMP, 40DC', 'IMP, 40HC', 'LSS, currency', 'LSS, 20DC', 'LSS, 40DC', 'LSS, 40HC', 'OHC, currency',
                    'OHC, 20DC', 'OHC, 40DC', 'OHC, 40HC', 'PSS, currency', 'PSS, 20DC', 'PSS, 40DC', 'PSS, 40HC',
                    'RHI, currency', 'RHI, 20DC', 'RHI, 40DC', 'RHI, 40HC', 'EBS, currency', 'EBS, 20DC', 'EBS, 40DC',
                    'EBS, 40HC', 'PAE, currency', 'PAE, 20DC', 'PAE, 40DC', 'PAE, 40HC', 'SBF, currency', 'SBF, 20DC',
                    'SBF, 40DC', 'SBF, 40HC']]


# loading data
path = os.getcwd()
db_path = os.path.join(path,'xeneta.db')
ocean_freight_rates_path = os.path.join(path,'data_science_test_1.xlsx')

con = sqlite3.connect(db_path)
ports = pd.read_sql_query("select * from ports", con)
con.close()

xl = pd.ExcelFile(ocean_freight_rates_path)
init = xl.parse('Ocean Freight Rates')

# get the final report
customer = shipment(init, ports)
report = customer.report
report.to_excel("output.xlsx", index=False)

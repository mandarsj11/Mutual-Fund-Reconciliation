import pandas as pd
import urllib, json #data input from url and reading json
from datetime import datetime, date, timedelta
import dateutil.parser # 'Data time' format to 'Date'
from pandas.io.json import json_normalize
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup #for web scrapping
import requests #get url
import scipy.optimize #xirr calculation

MF_Spreadsheet = pd.read_excel('MF_Consolidated.xlsx', sheet_name=None)
# Stage 1: get MF scheme info from mfapi
MF_scheme_input = MF_Spreadsheet['MF_Static']
consolidated_nav = pd.DataFrame()
if (dateutil.parser.parse(str(MF_scheme_input.min()['date'])).date()) <= (date.today() - timedelta(1)):
    for items, source in zip(MF_scheme_input['reference'], MF_scheme_input['nav_source']):
        if source == 'mfapi':
            response = urllib.request.urlopen('https://api.mfapi.in/mf/'+ str(items))
            summary = json.loads(response.read())
            summary['data'][0]['date'] = pd.to_datetime(summary['data'][0]['date'],dayfirst=True) #convert 01-07-2019 to 2019-07-01 first number is date
            # this assignment converts 'Date' format to 'date time' automatically (2019-07-01 00:00:00). - need to investigate the issue
            
            scheme_category = summary['meta']['scheme_category']
            dash_location = scheme_category.index('-')
            scheme_category = scheme_category[dash_location + 1:]        
                   
            MF_scheme_input.loc[MF_scheme_input['reference']==items,['date']] = summary['data'][0]['date']
            MF_scheme_input.loc[MF_scheme_input['reference']==items,['latest_NAV']] = pd.to_numeric(summary['data'][0]['nav'])
            MF_scheme_input.loc[MF_scheme_input['reference']==items,['scheme_category']] = scheme_category
            
            #collecting nav for all schemes from inception untill today
            data = json_normalize(summary['data'])
            meta = json_normalize(summary['meta'])
            no_of_dates = len(summary['data']) #no of elements in a jason
            count = 0
            single_mf_data = pd.DataFrame()
            while count < no_of_dates:
                single_mf_data = single_mf_data.append({
                                                        'reference':meta['scheme_code'][0],
                                                        'MF_Scheme':meta['scheme_name'][0],
                                                        'date':data['date'][count],
                                                        'latest_NAV':pd.to_numeric(data['nav'][count])
                                                        },ignore_index=True)
                count = count + 1
            single_mf_data['date']=pd.to_datetime(single_mf_data['date'],dayfirst=True) #convert string to Date format with dayfirst=tru: 12-06-2019 --> 2019-06-12 ie 12 is considered a Day and not Month
            #single_mf_data['latest_NAV']=pd.to_numeric(single_mf_data['latest_NAV']) #convert string to numeric format
            single_mf_data = single_mf_data.sort_values(by='date') #sort joined table by date from first to last date - affects daily return calculation otherwise
            consolidated_nav = single_mf_data.append(consolidated_nav, ignore_index=True) #https://pythonprogramming.net/concatenate-append-data-analysis-python-pandas-tutorial/
        elif source == 'etweb':
            web_data = requests.get(items)
            #web_content = web_data.content
            #html = urllib.request.urlopen(url, context=ctx).read()
            soup = BeautifulSoup(web_data.content, 'html.parser')
            web_nav_date = soup.find("div", {"class": "today_info"})
            web_nav_date = web_nav_date.text
            web_nav_date = web_nav_date[6:16] 
            web_nav = soup.find("div", {"class": "spot_value semibold flt"})
            MF_scheme_input.loc[MF_scheme_input['reference']==items,['date']] = pd.to_datetime(web_nav_date,dayfirst=True)
            MF_scheme_input.loc[MF_scheme_input['reference']==items,['latest_NAV']] = pd.to_numeric(web_nav.text)        
else:
    consolidated_nav = MF_Spreadsheet['Consolidated_NAV']
# Stage 2: transcation level calculations
    
# Stage 2.1: Update MF stansactions
MF_Data = MF_Spreadsheet['MF_Statement']
no_of_NT_dates = len(MF_scheme_input['next_transaction_date'])
count = 0
next_tx_date = pd.DataFrame()
MF_statement_new = pd.DataFrame() #just for trial to be merged with MF_Data
while count < no_of_NT_dates:
    next_date = MF_scheme_input['next_transaction_date'][count]
    day_of_month = MF_scheme_input['sip_schedule'][count]
    if MF_scheme_input['next_transaction_date'][count] != 'na': #skip sip terminated schemes
        if MF_scheme_input['reference'][count] != '-': # statement can not be generated if NAV not available
            # loop for next rolling date start
            month = 1
            while dateutil.parser.parse(str(next_date)).date() < date.today():
 
                #loop for non trading day adjustment start
                next_nav_df = pd.DataFrame()                
                while next_nav_df.empty and dateutil.parser.parse(str(next_date)).date() < date.today():        
                    next_nav_df = consolidated_nav.loc[(consolidated_nav['reference']==MF_scheme_input['reference'][count]) & 
                                                       (consolidated_nav['date']==pd.Timestamp(next_date).normalize()),['latest_NAV']] # to normalize 2019-02-15 to 2019-01-15 00:00:00
                    
                    if not next_nav_df.empty:
                        data_check =  MF_Data.loc[(MF_Data['MF_Scheme'] == MF_scheme_input['MF_Scheme'][count]) & 
                                                  (MF_Data['Transaction_Date'] == pd.Timestamp(next_date).normalize())] # to normalize 2019-02-15 to 2019-01-15 00:00:00
                        if data_check.empty:
                            MF_Data = MF_Data.append({
                                                      'entry_type':'Automatic',
                                                      'MF_Scheme':MF_scheme_input['MF_Scheme'][count],
                                                      'Transaction_Date':pd.Timestamp(next_date).normalize(), # to normalize 2019-02-15 to 2019-01-15 00:00:00
                                                      'Amount(Rs)':MF_scheme_input['sip_contribution'][count],
                                                      'NAV':next_nav_df.iat[0,0],
                                                      'Units':MF_scheme_input['sip_contribution'][count]/next_nav_df.iat[0,0],
                                                      },ignore_index=True)
                    next_date = next_date + timedelta(1)
                #loop for non trading day adjustment end
                next_date = (dateutil.parser.parse(str(MF_scheme_input['next_transaction_date'][count])).date()) + relativedelta(months=month, day=day_of_month) # to calculate next month's sip date
                month = month + 1
                #print('next sip date for ', MF_scheme_input['MF_Scheme'][count],' : ', next_date)
           # loop for next rolling date end         
    count = count + 1
    
# Stage 2.2: Calculate MF Transaction related info
first_td = MF_Data.groupby('MF_Scheme').min()[['Transaction_Date']].reset_index()
first_td = first_td.rename(columns={'Transaction_Date':'First_Transaction_Date'})

last_td = MF_Data.groupby('MF_Scheme').max()[['Transaction_Date']].reset_index()
last_td = last_td.rename(columns={'Transaction_Date':'Last_Transaction_Date'})

no_of_sip = MF_Data.groupby('MF_Scheme').count()[['Transaction_Date']].round(0).reset_index()
no_of_sip = no_of_sip.rename(columns={'Transaction_Date':'SIP_Transaction_count'})

mf_transaction_info = pd.merge(first_td,last_td, how = 'outer', on ='MF_Scheme')
mf_transaction_info = pd.merge(mf_transaction_info,no_of_sip, how = 'outer', on ='MF_Scheme')

# Stage 2.2: Calculate MF return analysis
sum_amount_units = MF_Data.groupby('MF_Scheme').sum()[['Amount(Rs)','Units']].round(2)
sum_amount_units = sum_amount_units.rename(columns={'Amount(Rs)':'Investment','Units':'Total_Units'})

min_nav = MF_Data.groupby('MF_Scheme').min()[['NAV']].round(2)
min_nav = min_nav.rename(columns={'NAV':'MIN_NAV'})

max_nav = MF_Data.groupby('MF_Scheme').max()[['NAV']].round(2)
max_nav = max_nav.rename(columns={'NAV':'MAX_NAV'})

mf_return_info = pd.merge(MF_scheme_input,sum_amount_units, how ='outer', on='MF_Scheme')
mf_return_info = pd.merge(mf_return_info,min_nav, how ='outer', on='MF_Scheme')
mf_return_info = pd.merge(mf_return_info,max_nav, how ='outer', on='MF_Scheme')
mf_return_info['AVG_NAV'] =  round(mf_return_info['Investment']/mf_return_info['Total_Units'],2)
mf_return_info['Market_Value'] =  round(mf_return_info['Total_Units']*mf_return_info['latest_NAV'],2)
mf_return_info.drop(['reference','sip_contribution','sip_schedule','next_transaction_date','nav_source'], axis=1, inplace=True)

# Stage 2.3: Calculate Xirr for cash flows
def xnpv(rate, values, dates):
    if rate <= -1.0:
        return float('inf')
    d0 = dates[0]    # or min(dates)
    return sum([ vi / (1.0 + rate)**((di - d0).days / 365.0) for vi, di in zip(values, dates)])

def xirr(values, dates):
    try:
        return scipy.optimize.newton(lambda r: xnpv(r, values, dates), 0.0)
    except RuntimeError:    # Failed to converge?
        return scipy.optimize.brentq(lambda r: xnpv(r, values, dates), -1.0, 1e10)
#reference for above two functions - https://stackoverflow.com/questions/8919718/financial-python-library-that-has-xirr-and-xnpv-function
xirr_output = pd.DataFrame()
for cf_items, cf_date, cf_amount in zip(mf_return_info['MF_Scheme'],mf_return_info['date'],mf_return_info['Market_Value']):
    cash_flows = MF_Data.loc[MF_Data['MF_Scheme'] == cf_items]
    cash_flows['xirr_value'] = cash_flows['Amount(Rs)'] * (-1)
    cash_flows = cash_flows.append({
                                    'entry_type':'xirr',
                                    'MF_Scheme':cf_items,
                                    'Transaction_Date':cf_date,
                                    'Amount(Rs)':cf_amount,
                                    'NAV':'na',
                                    'Units':'na',
                                    'xirr_value': cf_amount,
                                    },ignore_index=True)
    #print('MF_Scheme',cf_items, xirr(cash_flows['xirr_value'], cash_flows['Transaction_Date']))
    xirr_output = xirr_output.append({'MF_Scheme':cf_items,
                                      'XIRR':xirr(cash_flows['xirr_value'], cash_flows['Transaction_Date']),
                                      },ignore_index=True)
mf_return_info = pd.merge(mf_return_info,xirr_output, how ='outer', on='MF_Scheme')

# Stage 2.4: Adding row to calculate total 'Investment' & total 'Market_Value'
col_total = mf_return_info[['Investment','Market_Value']].sum()
row_total = pd.DataFrame(data=col_total).T
row_total=row_total.reindex(columns=mf_return_info.columns)
mf_return_info = mf_return_info.append(row_total,ignore_index=True)
mf_return_info = mf_return_info.rename(index={16:"Total"})

# Stage 3: Writing output into original Spreadsheet
MF_Spreadsheet['MF_Static'] = MF_scheme_input
MF_Spreadsheet['MF_Statement'] = MF_Data
MF_Spreadsheet['Dashboard'] = mf_return_info
MF_Spreadsheet['MF_Details'] = mf_transaction_info
MF_Spreadsheet['Consolidated_NAV'] =consolidated_nav
writer_orig = pd.ExcelWriter('MF_Consolidated.xlsx', engine='xlsxwriter',datetime_format='yyyy-mmm-dd',date_format='yyyy-mmm-dd') #engine='openpyxl' - for csv file, mode='a'
for ws_name, df_sheet in MF_Spreadsheet.items(): # ws_name = tab name & df_sheet = tab containt
    df_sheet.to_excel(writer_orig, index = False, sheet_name=ws_name)
    workbook = writer_orig.book
    worksheet = writer_orig.sheets[ws_name]
    if ws_name == 'Dashboard':
        num_format = workbook.add_format({'align': 'right', 'num_format': '#,##0.00'})
        percent_format = workbook.add_format({'num_format': '0.00%'}) 
        worksheet.set_column('F:K', None, num_format)
        worksheet.set_column('L:L', None, percent_format) 
    for i, col in enumerate(df_sheet.columns): #excel autoadjust col width - https://stackoverflow.com/questions/17326973/is-there-a-way-to-auto-adjust-excel-column-widths-with-pandas-excelwriter
        column_len = df_sheet[col].astype(str).str.len().max()
        column_len = max(column_len, len(col)) + 2
        worksheet.set_column(i, i, column_len)
    worksheet.freeze_panes(1,2)
    if ws_name == 'MF_Static':
        worksheet.set_column('D:D',10) 
writer_orig.save()

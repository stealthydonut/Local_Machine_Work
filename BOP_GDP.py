import ast
import itertools
import numpy as np
import pandas as pd
import requests
import datetime
from pandas.compat import StringIO
import StringIO
import datetime as dt
import ast
import itertools
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO as stio
else:
    from io import StringIO as stio

####################################    
#Get the credentials to acquire data    
####################################
#Sourced from the following site https://github.com/mortada/fredapi
from fredapi import Fred
fred = Fred(api_key='4af3776273f66474d57345df390d74b6')
    
bop = []
bopg = []
bopgs = []
bop_gold = pd.DataFrame()
bopgs_gold = pd.DataFrame()

base3 = "https://api.census.gov/data/timeseries/eits/ftd?get=cell_value,data_type_code,time_slot_id,category_code,seasonally_adj&time="
year_list = ['2013','2014','2015','2016','2017']

#########################        
#BOP - Services and Goods        
#########################        
for i in year_list:
    year2=''.join(i) 
    url=base3
    url2=url+year2
    r = requests.get(url2, headers={'User-agent': 'your bot 0.1'})
    if r.text:
        r = ast.literal_eval(r.text)
        df = pd.DataFrame(r[2:], columns=r[0])
        bop.append(df)
    else:
        rejects2.append(int(i))

bop= pd.concat(bop).reset_index().drop('index', axis=1)        
bop.columns=['VALUE','TYPE','TIME_ID','CAT','FLAG','YEAR']        
typelist=['BOPG','BOPGS']
typelist2=['EXP','IMP','BAL']

for i in typelist:
    TYP2=''.join(i)
    test=bop[bop['CAT']==TYP2]
    for b in typelist2:
        TYP=''.join(b)
        testx=pd.DataFrame(test[test['TYPE']==TYP])
        testx['EXP']=0
        testx['IMP']=0
        testx['BAL']=0
        testx['{}'.format(b)]=testx['VALUE']    
        if bop_gold.empty:
            bop_gold = pd.DataFrame(testx)
        else:
            bop_gold = bop_gold.append(pd.DataFrame(testx))

##################
#Develop variables            
##################           
bopg=bop_gold[bop_gold['CAT']=='BOPG']
bopg['bopg_exp']=pd.to_numeric(bopg['EXP'], errors='coerce')
bopg['bopg_imp']=pd.to_numeric(bopg['IMP'], errors='coerce')
bopg['bopg_bal']=pd.to_numeric(bopg['BAL'], errors='coerce')
bopg.__delitem__('VALUE')
bopg.__delitem__('TYPE')
bopg.__delitem__('TIME_ID')
bopg.__delitem__('CAT')
bopg.__delitem__('FLAG')
bopg.__delitem__('EXP')
bopg.__delitem__('IMP')
bopg.__delitem__('BAL')
bopg_exp=bopg[['YEAR','bopg_exp']]
bopg_imp=bopg[['YEAR','bopg_imp']]
bopg_bal=bopg[['YEAR','bopg_bal']]
bopg_exp1=bopg_exp[bopg_exp['bopg_exp']>0]
bopg_imp1=bopg_imp[bopg_imp['bopg_imp']>0]
bopg_bal1=bopg_bal[bopg_bal['bopg_bal']<0]
bopg1=pd.merge(bopg_exp1, bopg_imp1, left_on=('YEAR'), right_on=('YEAR'))
bopg2=pd.merge(bopg1, bopg_bal1, left_on=('YEAR'), right_on=('YEAR'))
bopg_gold=bopg2.drop_duplicates(['YEAR'], keep='last')
bopgs=bop_gold[bop_gold['CAT']=='BOPGS']
bopgs['bopgs_exp']=pd.to_numeric(bopgs['EXP'], errors='coerce')
bopgs['bopgs_imp']=pd.to_numeric(bopgs['IMP'], errors='coerce')
bopgs['bopgs_bal']=pd.to_numeric(bopgs['BAL'], errors='coerce')
bopgs.__delitem__('VALUE')
bopgs.__delitem__('TYPE')
bopgs.__delitem__('TIME_ID')
bopgs.__delitem__('CAT')
bopgs.__delitem__('FLAG')
bopgs.__delitem__('EXP')
bopgs.__delitem__('IMP')
bopgs.__delitem__('BAL')
bopgs_exp=bopgs[['YEAR','bopgs_exp']]
bopgs_imp=bopgs[['YEAR','bopgs_imp']]
bopgs_bal=bopgs[['YEAR','bopgs_bal']]
bopgs_exp1=bopgs_exp[bopgs_exp['bopgs_exp']>0]
bopgs_imp1=bopgs_imp[bopgs_imp['bopgs_imp']>0]
bopgs_bal1=bopgs_bal[bopgs_bal['bopgs_bal']<0]
bopgs1=pd.merge(bopgs_exp1, bopgs_imp1, left_on=('YEAR'), right_on=('YEAR'))
bopgs2=pd.merge(bopgs1, bopgs_bal1, left_on=('YEAR'), right_on=('YEAR'))
bopgs_gold=bopgs2.drop_duplicates(['YEAR'], keep='last')
#Get the balance of payments
bopfile=pd.merge(bopg_gold, bopgs_gold, left_on=('YEAR'), right_on=('YEAR'))
#Generate Date
bopfile['day']='01'
bopfile['year2'] = bopfile['YEAR'].astype(str)
bopfile['year3'] = bopfile['year2'].str[:4]
bopfile['month'] = bopfile['year2'].str[5:]
bopfile['slash']='/'
bopfile['period']=bopfile['month']+bopfile['slash']+bopfile['day']+bopfile['slash']+bopfile['year3']
bopfile['ind']=pd.to_datetime(bopfile['period'], errors='coerce')
bopfile.__delitem__('day')
bopfile.__delitem__('year2')
bopfile.__delitem__('year3')
bopfile.__delitem__('slash')
bopfile.__delitem__('month')
bopfile.__delitem__('period')
#########################################
#########################################
#Read in the historical bop CSV file
#########################################
#########################################
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('macrofiles')
# Then do other things...
blob = bucket.get_blob('historial_bop.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
details=pd.read_csv(inMemoryFile, low_memory=False)
details['ind']=pd.to_datetime(details['YEAR'], errors='coerce')
details.__delitem__('YEAR')
######################################################
#Append the historical file and the new file together#
######################################################
bigdata = bopfile.append(details, ignore_index=True)
bop_gold_gs=bigdata.drop_duplicates(['ind'], keep='last')
bop_gold_gs['cc']='US'

#############################################
#############################################
#Get the GDP - Note it is quarterly#
#############################################
#############################################
from fredapi import Fred
fred = Fred(api_key='4af3776273f66474d57345df390d74b6')
gdp = fred.get_series_all_releases('GDP') #GDP quarterly
#Need to get the last updated record
gdp2=gdp[gdp['value'].notnull()]
gdp3=gdp2.sort_values(['date','realtime_start'], ascending=[False, True])
gdp4=gdp3.drop_duplicates(['date'], keep='last')
gdp4['GDP value'] = pd.to_numeric(gdp4['value'], errors='coerce')
#gdp4['GDP value mm']= gdp4['GDP value']*1000000
gdp4['ind']=pd.to_datetime(gdp4['date'], errors='coerce')
gdp4['cc']='US'
gdp4['CTYNAME']='United States'
gdp4['year'] = gdp4['ind'].dt.strftime("%Y")
gdp4['month'] = gdp4['ind'].dt.strftime("%m")
gdp4['month2'] = pd.to_numeric(gdp4['month'], errors='coerce')
gdp4['monthnum']=pd.to_numeric(gdp4['month2'], errors='coerce')
gdp4['fl1'] = np.where(gdp4['monthnum']<4, 'Q1', 'no')
gdp4['fl2'] = np.where((gdp4['monthnum']>3) & (gdp4['monthnum']<7), 'Q2', 'no')
gdp4['fl3'] = np.where((gdp4['monthnum']>6) & (gdp4['monthnum']<10), 'Q3', 'no')
gdp4['fl4'] = np.where(gdp4['monthnum']>9, 'Q4', 'no')
q1=gdp4[gdp4['fl1']=='Q1']
q1['merge']=q1['year'].map(str)+" "+q1['fl1']
q2=gdp4[gdp4['fl2']=='Q2']
q2['merge']=q2['year'].map(str)+" "+q2['fl2']
q3=gdp4[gdp4['fl3']=='Q3']
q3['merge']=q3['year'].map(str)+" "+q3['fl3']
q4=gdp4[gdp4['fl4']=='Q4']
q4['merge']=q4['year'].map(str)+" "+q4['fl4']

gold1 = q1.append(q2, ignore_index=True)
gold2 = q3.append(gold1, ignore_index=True)
gdp_gold = q4.append(gold2, ignore_index=True)
gdp_gold.__delitem__('date')
gdp_gold.__delitem__('realtime_start')
gdp_gold.__delitem__('value')
gdp_gold.__delitem__('year')
gdp_gold.__delitem__('month')
gdp_gold.__delitem__('month2')
gdp_gold.__delitem__('monthnum')
gdp_gold.__delitem__('fl1')
gdp_gold.__delitem__('fl2')
gdp_gold.__delitem__('fl3')
gdp_gold.__delitem__('fl4')
gdp_gold.__delitem__('ind')

#Make a special dataset for the united states
usdataset=bop_gold_gs[bop_gold_gs['cc']=='US']
usdataset['CTYNAME']='United States'
usdataset['year'] = usdataset['ind'].dt.strftime("%Y")
usdataset['month'] = usdataset['ind'].dt.strftime("%m")
usdataset['month2'] = pd.to_numeric(usdataset['month'], errors='coerce')
usdataset['monthnum']=pd.to_numeric(usdataset['month2'], errors='coerce')
usdataset['fl1'] = np.where(usdataset['monthnum']<4, 'Q1', 'no')
usdataset['fl2'] = np.where((usdataset['monthnum']>3) & (usdataset['monthnum']<7), 'Q2', 'no')
usdataset['fl3'] = np.where((usdataset['monthnum']>6) & (usdataset['monthnum']<10), 'Q3', 'no')
usdataset['fl4'] = np.where(usdataset['monthnum']>9, 'Q4', 'no')
q1=usdataset[usdataset['fl1']=='Q1']
q1['merge']=q1['year'].map(str)+" "+q1['fl1']
q2=usdataset[usdataset['fl2']=='Q2']
q2['merge']=q2['year'].map(str)+" "+q2['fl2']
q3=usdataset[usdataset['fl3']=='Q3']
q3['merge']=q3['year'].map(str)+" "+q3['fl3']
q4=usdataset[usdataset['fl4']=='Q4']
q4['merge']=q4['year'].map(str)+" "+q4['fl4']
gold1 = q1.append(q2, ignore_index=True)
gold2 = q3.append(gold1, ignore_index=True)
bop_gold = q4.append(gold2, ignore_index=True)
bop_gold.__delitem__('YEAR')
bop_gold.__delitem__('year')
bop_gold.__delitem__('month')
bop_gold.__delitem__('month2')
bop_gold.__delitem__('monthnum')
bop_gold.__delitem__('fl1')
bop_gold.__delitem__('fl2')
bop_gold.__delitem__('fl3')
bop_gold.__delitem__('fl4')
bop_gold.__delitem__('ind')
#################################################
#Group all the quarters together into one dataset
#################################################
bop_gold_merge= bop_gold.groupby(['cc','CTYNAME','merge'], as_index=False)['bopg_bal','bopg_exp','bopg_imp','bopgs_bal','bopgs_exp','bopgs_imp'].sum()
############################
#join GDP data to bopgs bopg
############################
bopgdp=pd.merge(gdp_gold, bop_gold_merge,how='outer',  left_on=['cc','merge'], right_on=['cc','merge'])
bopgdp.__delitem__('CTYNAME_y')
############################
#Analytical Measures on File
############################
#Balance of Services
bopgdp['bos_bal']=bopgdp['bopgs_bal']-bopgdp['bopg_bal']
bopgdp['bos_exp']=bopgdp['bopgs_exp']-bopgdp['bopg_exp']
bopgdp['bos_imp']=bopgdp['bopgs_imp']-bopgdp['bopg_imp']
#Balance of Goods and Balance of Services Import and Export Differences
bopgdp['bos_diff_imp/exp']=bopgdp['bos_exp']-bopgdp['bos_imp']
bopgdp['bopg_diff_imp/exp']=bopgdp['bopg_exp']-bopgdp['bopg_imp']
#GDP is in billions and BOP is in millions
bopgdp['GDP_mil']=bopgdp['GDP value']*1000
bopgdp['bos_diff/gdp']=bopgdp['bos_diff_imp/exp']/bopgdp['GDP_mil']
bopgdp['bopg_diff/gdp']=bopgdp['bopg_diff_imp/exp']/bopgdp['GDP_mil']
#source http://fofoa.blogspot.ca/search?q=mmt
#There are three definitions private sector, government sector, and foreign sector
#foreign sector = private sector + government sector (these are all of the imports and exports
#the foreign sector in deficit is a net currency exporter (i.e. - it exports currency to get free goods)
#the foreign sector is the amount of US paper our foreign trading partners are taking in each year
#When they take in dollars, those show up as a current account surplus on their sheet and a current account deficit on ours
#the government sector is always negative because the US gets stuff for free because it exports currency
#the private sector are the services that are sold to foreign countries
bopgdp['private sector']=bopgdp['bos_bal']/bopgdp['GDP_mil']
bopgdp['government sector']=bopgdp['bopg_bal']/bopgdp['GDP_mil']

##################################
#Put the dataset back into storage
##################################
from google.cloud import storage
client = storage.Client()
bucket2 = client.get_bucket('macrofiles')
df_out = pd.DataFrame(bopgdp)
df_out.to_csv('bopgdp.csv', index=False)
blob2 = bucket2.blob('bopgdp.csv')
blob2.upload_from_filename('bopgdp.csv')







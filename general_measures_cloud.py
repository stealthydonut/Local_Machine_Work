#this program creates a daily file that tracks commodities and different measures that can be analyzed from a reserve persepctive

import quandl
import urllib
import pandas as pd
import quandl
import urllib
import pandas as pd
import numpy as np
import StringIO
import datetime
import requests
import sys
from pandas.compat import StringIO
if sys.version_info[0] < 3: 
    from StringIO import StringIO as stio
else:
    from io import StringIO as stio
    
#import matplotlib
#import matplotlib.pyplot as plt

#Sourced from the following site https://github.com/mortada/fredapi

#########
#GLD Data
#########

dls = "http://www.spdrgoldshares.com/assets/dynamic/GLD/GLD_US_archive_EN.csv"
r = requests.get(dls)
daily_prices = pd.read_csv(StringIO(r.text), skiprows=6)


##########
#FRED Data
##########
from fredapi import Fred
fred = Fred(api_key='4af3776273f66474d57345df390d74b6')
#djia = fred.get_series_all_releases('DJIA')
treas10 = fred.get_series_all_releases('DGS10') #10-Year Treasury Constant Maturity Rate 
libor3 = fred.get_series_all_releases('USD3MTD156N') #libor
fedassets = fred.get_series_all_releases('WALCL')# All Federal Reserve Banks: Total Assets (WALCL) #Federal reserve balance sheet
libor12 = fred.get_series_all_releases('USD12MD156N')# 12 month libor
fedbudget = fred.get_series_all_releases('MTSDS133FMS')# Budget Surplus/Deficity
############
#Quandl Data
#############
quandl.ApiConfig.api_key = 'BVno6pBYgcEvZJ6uctTr'
####################
#Get the Quandl Data
###################
ism = quandl.get("ISM/NONMAN_INVSENT")
ism2 = quandl.get("ISM/MAN_PMI") #another ISM
gold = quandl.get("LBMA/GOLD")
silver = quandl.get("LBMA/SILVER")
copper = quandl.get ("COM/COPPER")  #copper
corn = quandl.get ("TFGRAIN/CORN") #corn
soybean = quandl.get ("TFGRAIN/SOYBEANS") #soybean
oil = quandl.get("OPEC/ORB")
uranium = quandl.get("ODA/PURAN_USD")
ustax = quandl.get("FMSTREAS/MTS")
shiller = quandl.get("MULTPL/SHILLER_PE_RATIO_MONTH")
paladium = quandl.get("LPPM/PALL")
platinum = quandl.get("LPPM/PLAT")
balticdryindex = quandl.get("LLOYDS/BDI")
balticcapesizeindex = quandl.get("LLOYDS/BCI") #>150k DWT
balticsupramexindex = quandl.get("LLOYDS/BSI") #50-60k DWT
balticpanamaxindex = quandl.get("LLOYDS/BPI") #65-80k DWT
trade_Weighted_Index = quandl.get("FRED/TWEXBPA")
fed_funds_rate = quandl.get("FED/RIFSPFF_N_M")
fxusdcad = quandl.get("FRED/DEXCAUS")
fxusdyuan = quandl.get("FRED/DEXCHUS")
fxusdjap = quandl.get("FRED/DEXJPUS")
fxusdind = quandl.get("FRED/DEXINUS")
fxusdbra = quandl.get("FRED/DEXBZUS")
fxusdsko = quandl.get("FRED/DEXKOUS")
fxusdaud = quandl.get("FRED/DEXUSAL")
fxusdmex = quandl.get("FRED/DEXMXUS")
fxusdche = quandl.get("FRED/DEXSZUS")
fxusdeur = quandl.get("FED/RXI_US_N_B_EU")
cobalt = quandl.get("LME/PR_CO")
molybdenum = quandl.get("LME/PR_MO")
zinc = quandl.get("LME/PR_ZI")
tin = quandl.get("LME/PR_TN")
aluminum = quandl.get("LME/PR_AL")
nickel = quandl.get("LME/PR_NI")
copper = quandl.get("LME/PR_CU")


######################
#Clean up Column Names
######################
gold.columns=['Gold USD (AM)','Gold USD (PM)','Gold GBP (AM)','Gold GBP (PM)','Gold EURO (AM)','Gold EURO (PM)']
silver.columns=['Silver USD','Silver GBP','Silver EURO']
copper.columns=['Copper USD']
corn.columns=['Cash Price Corn','Basis Corn','Fall Price Corn','Fall Basis Corn']
soybean.columns=['Cash Price Soybean','Basis Soybean','Fall Price Soybean','Fall Basis Soybean']
oil.columns=['Oil USD']
shiller.columns=['Shiller Value']
ustax.columns=['US Receipts','US Outlays','US Deficit/Surplus (-)','US Borrowing from the Public','USReduction of Operating Cash','US By Other Means']
ism.columns=['ISM % Too High','ISM % About Right','ISM % Too Low','ISM Diffusion Index']
uranium.columns=['Uranium Value']
platinum.columns=['Platinum USD (AM)','Platinum USD (PM)','Platinum GBP (AM)','Platinum GBP (PM)','Platinum EURO (AM)','Platinum EURO (PM)']
paladium.columns=['Paladium USD (AM)','Paladium USD (PM)','Paladium GBP (AM)','Paladium GBP (PM)','Paladium EURO (AM)','Paladium EURO (PM)']
balticdryindex.columns=['balticdryindex Index']
balticcapesizeindex.columns=['balticcapesizeindex Index']
balticsupramexindex.columns=['balticsupramexindex Index']
balticpanamaxindex.columns=['balticpanamaxindex Index']
trade_Weighted_Index.columns=['trade_wighted_index']
fed_funds_rate.columns=['fed_funds_rate']
fxusdcad.columns=['cad/usd']
fxusdyuan.columns=['yuan/usd']
fxusdjap.columns=['jap/usd']
fxusdind.columns=['ind/usd']
fxusdbra.columns=['bra/usd']
fxusdsko.columns=['sko/usd']
fxusdaud.columns=['aud/usd']
fxusdmex.columns=['mex/usd']
fxusdche.columns=['che/usd']
fxusdeur.columns=['eur/usd']
cobaltgold=cobalt[['Cash Buyer','3-months Buyer']]
molybdenumgold=molybdenum[['Cash Buyer','3-months Buyer']]
zincgold=zinc[['Cash Buyer','3-months Buyer']]
tingold=tin[['Cash Buyer','3-months Buyer']]
aluminumgold=aluminum[['Cash Buyer','3-months Buyer']]
nickelgold=nickel[['Cash Buyer','3-months Buyer']]
coppergold=copper[['Cash Buyer','3-months Buyer']]
cobaltgold.columns=['cobalt price','cobalt 3mth price']
molybdenumgold.columns=['molybdenum','molybdenum 3mth price']
zincgold.columns=['zinc','zinc 3mth price']
tingold.columns=['tin','tin 3mth price']
aluminumgold.columns=['aluminum','aluminum 3mth price']
nickelgold.columns=['nickel','nickel 3mth price']
coppergold.columns=['copper','copper 3mth price']
#######################
#Clean up the FRED data
#######################
libor3['libor3mth']=pd.to_numeric(libor3['value'], errors='coerce')
libor3['ind']=pd.to_datetime(libor3['date'], errors='coerce')
libor12['libor12mth']=pd.to_numeric(libor12['value'], errors='coerce')
libor12['ind']=pd.to_datetime(libor12['date'], errors='coerce')
treas10['treas10mth']=pd.to_numeric(treas10['value'], errors='coerce')
treas10['ind']=pd.to_datetime(treas10['date'], errors='coerce')
fedassets['fedassets']=pd.to_numeric(fedassets['value'], errors='coerce')
fedassets['ind']=pd.to_datetime(fedassets['date'], errors='coerce')
fedbudget['fedbudget']=pd.to_numeric(fedbudget['value'], errors='coerce')
fedbudget['ind']=pd.to_datetime(fedbudget['date'], errors='coerce')

libor3x=libor3[['libor3mth','ind']]
libor12x=libor12[['libor12mth','ind']]
treas10x=treas10[['treas10mth','ind']]
fedassetsx=fedassets[['fedassets','ind']]
fedbudgetx=fedbudget[['fedbudget','ind']]
######################
#Clean up the GLD data
######################
daily_prices2=daily_prices
daily_prices2['ind']=pd.to_datetime(daily_prices2['Date'], errors='coerce')
daily_prices2['GLD Closex'] = daily_prices2.loc[daily_prices2[' GLD Close'].index, ' GLD Close'].map(lambda x: str(x).replace('HOLIDAY',''))
daily_prices2['GLD Close']=pd.to_numeric(daily_prices2['GLD Closex'], errors='coerce')
daily_prices2['LBMA Gold Pricex'] = daily_prices2.loc[daily_prices2[' LBMA Gold Price'].index, ' LBMA Gold Price'].map(lambda x: str(x).replace('HOLIDAY',''))
daily_prices2['LBMA Gold Pricex'] = daily_prices2.loc[daily_prices2['LBMA Gold Pricex'].index, 'LBMA Gold Pricex'].map(lambda x: str(x).replace('$',''))
daily_prices2['LBMA Gold Price']=pd.to_numeric(daily_prices2['LBMA Gold Pricex'], errors='coerce')
daily_prices2['NAV per GLD in Goldx'] = daily_prices2.loc[daily_prices2[' NAV per GLD in Gold'].index, ' NAV per GLD in Gold'].map(lambda x: str(x).replace('HOLIDAY',''))
daily_prices2['NAV per GLD in Gold']=pd.to_numeric(daily_prices2['NAV per GLD in Goldx'], errors='coerce')
daily_prices2['NAV/sharex'] = daily_prices2.loc[daily_prices2[' NAV/share at 10.30 a.m. NYT'].index, ' NAV/share at 10.30 a.m. NYT'].map(lambda x: str(x).replace('HOLIDAY',''))
daily_prices2['NAV/share']=pd.to_numeric(daily_prices2['NAV/sharex'], errors='coerce')
daily_prices2['Indicative Price of GLDx'] = daily_prices2.loc[daily_prices2[' Indicative Price of GLD at 4.15 p.m. NYT'].index, ' Indicative Price of GLD at 4.15 p.m. NYT'].map(lambda x: str(x).replace('HOLIDAY',''))
daily_prices2['Indicative Price of GLD']=pd.to_numeric(daily_prices2['Indicative Price of GLDx'], errors='coerce')
daily_prices2['Mid point of bid/ask spreadx'] = daily_prices2.loc[daily_prices2[' Mid point of bid/ask spread at 4.15 p.m. NYT#'].index, ' Mid point of bid/ask spread at 4.15 p.m. NYT#'].map(lambda x: str(x).replace('HOLIDAY',''))
daily_prices2['Mid point of bid/ask spreadx'] = daily_prices2.loc[daily_prices2['Mid point of bid/ask spreadx'].index, 'Mid point of bid/ask spreadx'].map(lambda x: str(x).replace('$',''))
daily_prices2['Mid point of bid/ask spread']=pd.to_numeric(daily_prices2['Mid point of bid/ask spreadx'], errors='coerce')
daily_prices2['Premium/Discount of GLD mid point v Indicative Value of GLDx'] = daily_prices2.loc[daily_prices2[' Premium/Discount of GLD mid point v Indicative Value of GLD at 4.15 p.m. NYT'].index, ' Premium/Discount of GLD mid point v Indicative Value of GLD at 4.15 p.m. NYT'].map(lambda x: str(x).replace('HOLIDAY',''))
daily_prices2['Premium/Discount of GLD mid point v Indicative Value of GLDx'] = daily_prices2.loc[daily_prices2['Premium/Discount of GLD mid point v Indicative Value of GLDx'].index, 'Premium/Discount of GLD mid point v Indicative Value of GLDx'].map(lambda x: str(x).replace('%',''))
daily_prices2['Premium/Discount of GLD mid point v Indicative Value of GLD']=pd.to_numeric(daily_prices2['Premium/Discount of GLD mid point v Indicative Value of GLDx'], errors='coerce')
daily_prices2['Daily Share Volumex'] = daily_prices2.loc[daily_prices2[' Daily Share Volume'].index, ' Daily Share Volume'].map(lambda x: str(x).replace('HOLIDAY',''))
daily_prices2['Daily Share Volume']=pd.to_numeric(daily_prices2['Daily Share Volumex'], errors='coerce')
daily_prices2['Total Net Asset Value Ounces in the Trustx'] = daily_prices2.loc[daily_prices2[' Total Net Asset Value Ounces in the Trust as at 4.15 p.m. NYT'].index, ' Total Net Asset Value Ounces in the Trust as at 4.15 p.m. NYT'].map(lambda x: str(x).replace('HOLIDAY',''))
daily_prices2['Total Net Asset Value Ounces in the Trust']=pd.to_numeric(daily_prices2['Total Net Asset Value Ounces in the Trustx'], errors='coerce')
daily_prices2['Total Net Asset Value Tonnes in the Trustx'] = daily_prices2.loc[daily_prices2[' Total Net Asset Value Tonnes in the Trust as at 4.15 p.m. NYT'].index, ' Total Net Asset Value Tonnes in the Trust as at 4.15 p.m. NYT'].map(lambda x: str(x).replace('HOLIDAY',''))
daily_prices2['Total Net Asset Value Tonnes in the Trust']=pd.to_numeric(daily_prices2['Total Net Asset Value Tonnes in the Trustx'], errors='coerce')
daily_prices2['Total Net Asset Value in the Trustx'] = daily_prices2.loc[daily_prices2[' Total Net Asset Value in the Trust'].index, ' Total Net Asset Value in the Trust'].map(lambda x: str(x).replace('HOLIDAY',''))
daily_prices2['Total Net Asset Value in the Trust']=pd.to_numeric(daily_prices2['Total Net Asset Value in the Trustx'], errors='coerce')
dfgld = daily_prices2[['ind','GLD Close','LBMA Gold Price','NAV per GLD in Gold','NAV/share','Indicative Price of GLD','Mid point of bid/ask spread',\
'Premium/Discount of GLD mid point v Indicative Value of GLD','Daily Share Volume','Total Net Asset Value Ounces in the Trust','Total Net Asset Value Tonnes in the Trust',\
'Total Net Asset Value in the Trust']]


#################
#Index Generation
#################
gold['ind']=gold.index
silver['ind']=silver.index
oil['ind']=oil.index
copper['ind']=copper.index
paladium['ind']=paladium.index
platinum['ind']=platinum.index 
corn['ind']=corn.index 
soybean['ind']=soybean.index 
balticdryindex['ind']=balticdryindex.index
balticcapesizeindex['ind']=balticcapesizeindex.index
balticsupramexindex['ind']=balticsupramexindex.index
balticpanamaxindex['ind']=balticpanamaxindex.index
fxusdcad['ind']=fxusdcad.index
fxusdyuan['ind']=fxusdyuan.index
fxusdjap['ind']=fxusdjap.index
fxusdind['ind']=fxusdind.index
fxusdbra['ind']=fxusdbra.index
fxusdsko['ind']=fxusdsko.index
fxusdaud['ind']=fxusdaud.index
fxusdmex['ind']=fxusdmex.index
fxusdche['ind']=fxusdche.index
fxusdeur['ind']=fxusdeur.index
cobaltgold['ind']=cobaltgold.index
molybdenumgold['ind']=molybdenumgold.index
zincgold['ind']=zincgold.index
tingold['ind']=tingold.index
aluminumgold['ind']=aluminumgold.index
nickelgold['ind']=nickelgold.index
coppergold['ind']=coppergold.index
###########################
#Merge daily files together
###########################
df=gold.merge(silver, on='ind', how='outer')
df1=df.merge(paladium, on='ind', how='outer')
df2=df1.merge(platinum, on='ind', how='outer')
df3=df2.merge(fxusdcad, on='ind', how='outer')
df4=df3.merge(fxusdyuan, on='ind', how='outer')
df5=df4.merge(fxusdjap, on='ind', how='outer')
df6=df5.merge(fxusdind, on='ind', how='outer')
df7=df6.merge(fxusdbra, on='ind', how='outer')
df8=df7.merge(fxusdsko, on='ind', how='outer')
df9=df8.merge(fxusdaud, on='ind', how='outer')
df10=df9.merge(fxusdmex, on='ind', how='outer')
df11=df10.merge(fxusdche, on='ind', how='outer')
df12=df11.merge(fxusdeur, on='ind', how='outer')
df13=df12.merge(libor3x, on='ind', how='outer')
df14=df13.merge(libor12x, on='ind', how='outer')
#df15=df14.merge(fedassets, on='ind', how='outer')
df16=df14.merge(treas10x, on='ind', how='outer')
df17=df16.merge(dfgld, on='ind', how='outer')
df18=df17.merge(copper, on='ind', how='outer') 
df19=df18.merge(balticdryindex, on='ind', how='outer') 
df20=df19.merge(balticcapesizeindex, on='ind', how='outer') 
df21=df20.merge(balticsupramexindex, on='ind', how='outer') 
df22=df21.merge(balticpanamaxindex, on='ind', how='outer') 
df23=df22.merge(corn, on='ind', how='outer') 
df24=df23.merge(soybean, on='ind', how='outer')
df25=df24.merge(cobaltgold, on='ind', how='outer')
df26=df25.merge(molybdenumgold, on='ind', how='outer')
df27=df26.merge(zincgold, on='ind', how='outer')
df28=df27.merge(tingold, on='ind', how='outer')
df29=df28.merge(aluminumgold, on='ind', how='outer')
df30=df29.merge(nickelgold, on='ind', how='outer')
df31=df30.merge(coppergold, on='ind', how='outer')
daily_filex=df31.merge(oil, on='ind', how='outer')
###########################
#Generate a daily timestamp
###########################
daily_filex['daily date']=daily_filex['ind']
############################
#Get rid of date duplicates#
############################
daily_file=daily_filex.drop_duplicates(['ind'], keep='last')

##################################
#Put the dataset back into storage
##################################
from google.cloud import storage
client = storage.Client()
bucket2 = client.get_bucket('macrofiles')
df_out = pd.DataFrame(daily_file)
df_out.to_csv('daily_file.csv', index=False)
blob2 = bucket2.blob('daily_file.csv')
blob2.upload_from_filename('daily_file.csv')


#################################
#Merge the monthly files together
#################################
ism['ind'] = ism.index
ism['monthyear'] = ism['ind'].dt.strftime("%Y,%m")
ism2['ind'] = ism2.index
ism2['monthyear'] = ism2['ind'].dt.strftime("%Y,%m")
uranium['ind'] = uranium.index
uranium['monthyear'] = uranium['ind'].dt.strftime("%Y,%m")
ustax['ind'] = ustax.index
ustax['monthyear'] = ustax['ind'].dt.strftime("%Y,%m")
shiller['ind'] = shiller.index
shiller['monthyear'] = shiller['ind'].dt.strftime("%Y,%m")
#balticdryindex['ind'] = balticdryindex.index
#balticdryindex['monthyear'] = balticdryindex['ind'].dt.strftime("%Y,%m")
balticcapesizeindex['ind'] = balticcapesizeindex.index
balticcapesizeindex['monthyear'] = balticcapesizeindex['ind'].dt.strftime("%Y,%m")
balticsupramexindex['ind'] = balticsupramexindex.index
balticsupramexindex['monthyear'] = balticsupramexindex['ind'].dt.strftime("%Y,%m")
balticpanamaxindex['ind'] = balticpanamaxindex.index
balticpanamaxindex['monthyear'] = balticpanamaxindex['ind'].dt.strftime("%Y,%m")
trade_Weighted_Index['ind'] = trade_Weighted_Index.index
trade_Weighted_Index['monthyear'] = trade_Weighted_Index['ind'].dt.strftime("%Y,%m")
fed_funds_rate['ind'] = fed_funds_rate.index
fed_funds_rate['monthyear'] = fed_funds_rate['ind'].dt.strftime("%Y,%m")
#####################################
#Clean up files to join to daily file
#####################################
ismx = ism
ismx.__delitem__('ind')
uraniumx = uranium
uraniumx.__delitem__('ind')
ustaxx = ustax
ustaxx.__delitem__('ind')
shillerx = shiller
shillerx.__delitem__('ind')
trade_Weighted_Indexx = trade_Weighted_Index
trade_Weighted_Indexx.__delitem__('ind')
fed_funds_ratex = fed_funds_rate
fed_funds_ratex.__delitem__('ind')


#################################################
#Create daycnts so the averages can be calculated
#################################################
daily_file['Gold daycnt'] = np.where(daily_file['Gold USD (AM)']>0, 1, 0)
daily_file['Silver daycnt'] = np.where(daily_file['Silver USD']>0, 1, 0)
daily_file['Paladium daycnt'] = np.where(daily_file['Paladium USD (AM)']>0, 1, 0)
daily_file['Platinum daycnt'] = np.where(daily_file['Platinum USD (AM)']>0, 1, 0)
daily_file['Oil daycnt'] = np.where(daily_file['Oil USD']>0, 1, 0)
daily_file['Corn daycnt'] = np.where(daily_file['Cash Price Corn']>0, 1, 0)
daily_file['Soybean daycnt'] = np.where(daily_file['Cash Price Soybean']>0, 1, 0)
daily_file['Copper daycnt'] = np.where(daily_file['Copper USD']>0, 1, 0)
daily_file['libor3mth daycnt'] = np.where(daily_file['libor3mth']>0, 1, 0)
daily_file['libor12mth daycnt'] = np.where(daily_file['libor12mth']>0, 1, 0)
daily_file['fedassets daycnt'] = np.where(daily_file['fedassets']>0, 1, 0)
daily_file['treas10mth daycnt'] = np.where(daily_file['treas10mth']>0, 1, 0)
daily_file['balticdryindex Index daycnt'] = np.where(daily_file['balticdryindex Index']>0, 1, 0)
daily_file['balticcapesizeindex Index daycnt'] = np.where(daily_file['balticcapesizeindex Index']>0, 1, 0)
daily_file['balticsupramexindex Index daycnt'] = np.where(daily_file['balticsupramexindex Index']>0, 1, 0)
daily_file['balticpanamaxindex Index daycnt'] = np.where(daily_file['balticpanamaxindex Index']>0, 1, 0)
daily_file['Total Net Asset Value Tonnes in the Trust daycnt'] = np.where(daily_file['Total Net Asset Value Tonnes in the Trust']>0, 1, 0)
daily_file['Daily Share Volume daycnt'] = np.where(daily_file['Daily Share Volume']>0, 1, 0)
daily_file['daycnt'] = 1

dailymth = daily_file.groupby(['monthyear'], as_index=False)['daycnt','Gold daycnt','Silver daycnt','Oil daycnt','Copper daycnt','Paladium daycnt','Platinum daycnt','Daily Share Volume daycnt',\
'libor3mth daycnt','libor12mth daycnt','treas10mth daycnt','libor3mth','libor12mth','treas10mth','balticdryindex Index daycnt','Soybean daycnt','Copper daycnt',\
'balticcapesizeindex Index daycnt','balticsupramexindex Index daycnt','balticpanamaxindex Index daycnt','balticcapesizeindex Index daycnt','balticsupramexindex Index daycnt','balticpanamaxindex Index daycnt','Total Net Asset Value Tonnes in the Trust daycnt',\
'Gold USD (AM)','Gold USD (PM)','Gold GBP (AM)','Gold GBP (PM)','Gold EURO (AM)','Gold EURO (PM)','Copper USD','Silver USD','Silver GBP','Silver EURO','Oil USD','Total Net Asset Value Tonnes in the Trust','balticdryindex Index','Platinum USD (AM)','Paladium USD (AM)','Daily Share Volume',\
                                                           'Cash Price Corn','Cash Price Soybean'].sum()


################################
#Merge the monthly data together
################################
mf=ustax.merge(dailymth, on='monthyear', how='outer')
mf1=mf.merge(ism, on='monthyear', how='outer')
mf2=mf1.merge(shiller, on='monthyear', how='outer')
mf3=mf2.merge(trade_Weighted_Index, on='monthyear', how='outer')
mf4=mf3.merge(fed_funds_rate, on='monthyear', how='outer')
monthly_file=mf4.merge(uranium, on='monthyear', how='outer')

#######################
#Monthly File Measures#
#######################
monthly_file['gold price']=monthly_file['Gold USD (AM)']/monthly_file['Gold daycnt']
monthly_file['silver price']=monthly_file['Silver USD']/monthly_file['Silver daycnt']
monthly_file['oil price']=monthly_file['Oil USD']/monthly_file['Oil daycnt']
monthly_file['copper price']=monthly_file['Copper USD']/monthly_file['Copper daycnt']
monthly_file['paladium price']=monthly_file['Paladium USD (AM)']/monthly_file['Paladium daycnt']
monthly_file['platinum price']=monthly_file['Platinum USD (AM)']/monthly_file['Platinum daycnt']
monthly_file['gld daily share volume']=monthly_file['Daily Share Volume']/monthly_file['Daily Share Volume daycnt']
monthly_file['libor3mth value']=monthly_file['libor3mth']/monthly_file['libor3mth daycnt']
monthly_file['libor12mth value']=monthly_file['libor12mth']/monthly_file['libor12mth daycnt']
monthly_file['treas10mth value']=monthly_file['treas10mth']/monthly_file['treas10mth daycnt']
monthly_file['balticdryindex Index']=monthly_file['balticdryindex Index']/monthly_file['balticdryindex Index daycnt']
#monthly_file['balticcapesizeindex Index value']=monthly_file['balticcapesizeindex Index']/monthly_file['balticcapesizeindex Index daycnt']
#monthly_file['balticsupramexindex Index value']=monthly_file['balticsupramexindex Index']/monthly_file['balticsupramexindex Index daycnt']
#monthly_file['balticpanamaxindex Index value']=monthly_file['libor3mth']/monthly_file['balticpanamaxindex Index daycnt']
monthly_file['Total Net Asset Value Tonnes in the Trust value']=monthly_file['Total Net Asset Value Tonnes in the Trust']/monthly_file['Total Net Asset Value Tonnes in the Trust daycnt']


monthly_file['gold oil ratio']=monthly_file['gold price']/monthly_file['oil price']
monthly_file['gold silver ratio']=monthly_file['gold price']/monthly_file['silver price']
monthly_file['gold copper ratio']=monthly_file['gold price']/monthly_file['copper price']

monthly_file['ma6 US Receipts'] = monthly_file['US Receipts'].rolling(window=6).mean()
monthly_file['ma6 ISM Diffusion Index'] = monthly_file['ISM Diffusion Index'].rolling(window=6).mean()

############################
#Get rid of date duplicates#
############################
monthly_filex=monthly_file.drop_duplicates(['monthyear'], keep='last')
######################
#Create date variable#
######################
monthly_filex['day']='01'
monthly_filex['year'] = monthly_filex['monthyear'].str[:4]
monthly_filex['month'] = monthly_filex['monthyear'].str[5:7]
monthly_filex['slash']='/'
monthly_filex['period']=monthly_filex['month']+monthly_filex['slash']+monthly_filex['day']+monthly_filex['slash']+monthly_filex['year']
monthly_filex['ind']=pd.to_datetime(monthly_filex['period'], errors='coerce')
##################################
#Put the dataset back into storage
##################################
from google.cloud import storage
client = storage.Client()
bucket2 = client.get_bucket('macrofiles')
df_out = pd.DataFrame(monthly_filex)
df_out.to_csv('monthly_file.csv', index=False)
blob2 = bucket2.blob('monthly_file.csv')
blob2.upload_from_filename('monthly_file.csv')




############################
#Build measures on the files
############################
monthly_file['ma6 US Receipts'] = monthly_file['US Receipts'].rolling(window=6).mean()
monthly_file['ma6 ISM Diffusion Index'] = monthly_file['ISM Diffusion Index'].rolling(window=6).mean()



####################
#Daily File Measures
####################

#Generate the percentage change for any variable on the file
varlist=('cobalt','molybdenumgold','zinc','tin','alumininum','nickel','copper')
for i in varlist:
    part=''.join(i)
    time126='_lag126' #6 month
    time252='_lag252' #1 year
    time504='_lag504' #2 year
    time756='_lag756' #3 year
    time6mth='_6mth'
    time1yr='_1yr'
    time2yr='_2yr'
    time3yr='_3yr'
    #Generate lag variable names
    text126=part+time126
    text252=part+time252
    text504=part+time504
    text756=part+time756
    #Generate variable titles
    title6mth=part+time6mth
    title1yr=part+time1yr
    title2yr=part+time2yr    
    title3yr=part+time3yr
    
    daily_file[text126] = daily_file[part].shift(126)
    daily_file[text252] = daily_file[part].shift(252)
    daily_file[text504] = daily_file[part].shift(504)
    daily_file[text756] = daily_file[part].shift(756)
    daily_file[title6mth] = 1-(daily_file[part]/daily_file[text126])
    daily_file[title1yr]  = 1-(daily_file[part]/daily_file[text252])
    daily_file[title2yr]  = 1-(daily_file[part]/daily_file[text504])
    daily_file[title3yr]  = 1-(daily_file[part]/daily_file[text756])    
    #delete variable names
    daily_file.__delitem__(text126)
    daily_file.__delitem__(text252)
    daily_file.__delitem__(text504)
    daily_file.__delitem__(text756)    
    print coppergold


daily_file['Gold Silver Ratio']=daily_file['Gold USD (PM)']/daily_file['Silver USD']
daily_file['Gold Oil Ratio']=daily_file['Gold USD (PM)']/daily_file['Oil USD']
daily_file['Silver Oil Ratio']=daily_file['Silver USD']/daily_file['Oil USD']
daily_file['Gold_CAD']=daily_file['Gold USD (PM)']*daily_file['cad/usd']
daily_file['Gold_YUAN']=daily_file['Gold USD (PM)']*daily_file['yuan/usd']
daily_file['Gold_JAP']=daily_file['Gold USD (PM)']*daily_file['jap/usd']
daily_file['Gold_IND']=daily_file['Gold USD (PM)']*daily_file['ind/usd']
daily_file['Gold_BRA']=daily_file['Gold USD (PM)']*daily_file['bra/usd']
daily_file['Gold_SKO']=daily_file['Gold USD (PM)']*daily_file['sko/usd']
daily_file['Gold_AUD']=daily_file['Gold USD (PM)']*daily_file['aud/usd']
daily_file['Gold_MEX']=daily_file['Gold USD (PM)']*daily_file['mex/usd']
daily_file['Gold_CHE']=daily_file['Gold USD (PM)']*daily_file['che/usd']
daily_file['Gold_EUR']=daily_file['Gold USD (PM)']*daily_file['eur/usd']
daily_file['Gold_Total']=daily_file['Gold_CAD']+daily_file['Gold_YUAN']+daily_file['Gold_JAP']+daily_file['Gold_IND']+daily_file['Gold_BRA']+daily_file['Gold_SKO']+daily_file['Gold_AUD']+daily_file['Gold_MEX']+daily_file['Gold_CHE']+daily_file['Gold_EUR']

#Develop the reserve file
res_ru['Country']='RU'
res_ru['ind']=res_ru.index
res_jp['Country']='JP'
res_jp['ind']=res_jp.index
res_ca['Country']='CA'
res_ca['ind']=res_ca.index
bigdata = res_ru.append(res_jp, ignore_index=True)


ism2.to_excel('C:\Python27\commodity_file.xls', index=False)   
daily_file.to_excel('C:\Users\davking\Documents\My Tableau Repository\Datasources\commodity_file.xls', index=False)     
monthly_file.to_excel('C:\Users\davking\Documents\My Tableau Repository\Datasources\monthly_file.xls', index=False)     
   

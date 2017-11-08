import ast
import itertools
import numpy as np
import pandas as pd
import requests
import datetime

from pandas.compat import StringIO
#Sourced from the following site https://github.com/mortada/fredapi
from fredapi import Fred
fred = Fred(api_key='4af3776273f66474d57345df390d74b6')
import StringIO
import datetime as dt
import ast
import itertools
#import matplotlib
#import matplotlib.pyplot as plt
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO as stio
else:
    from io import StringIO as stio

base = "https://api.census.gov/data/timeseries/intltrade/exports/hs?get=CTY_CODE,CTY_NAME,ALL_VAL_MO,ALL_VAL_YR&time="
base2 = "https://api.census.gov/data/timeseries/intltrade/imports/enduse?get=CTY_CODE,CTY_NAME,GEN_VAL_MO,GEN_VAL_YR&time="
base3 = "https://api.census.gov/data/timeseries/eits/ftd?get=cell_value,data_type_code,time_slot_id,category_code,seasonally_adj&time="
year_list = ['2013','2014','2015','2016','2017']
month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']

exports = []
rejects = []
imports = []
rejects2 = []
bop = []
bopg = []
bopgs = []
bop_gold = pd.DataFrame()
bopgs_gold = pd.DataFrame()

for year, month in itertools.product(year_list, month_list):
    url = '%s%s-%s' % (base, year, month)
    r = requests.get(url, headers={'User-agent': 'your bot 0.1'})
    if r.text:
        r = ast.literal_eval(r.text)
        df = pd.DataFrame(r[2:], columns=r[0])
        exports.append(df)
    else:
        rejects.append((int(year), int(month)))

for year, month in itertools.product(year_list, month_list):
    url = '%s%s-%s' % (base2, year, month)
    r = requests.get(url, headers={'User-agent': 'your bot 0.1'})
    if r.text:
        r = ast.literal_eval(r.text)
        df = pd.DataFrame(r[2:], columns=r[0])
        imports.append(df)
    else:
        rejects2.append((int(year), int(month)))


exports = pd.concat(exports).reset_index().drop('index', axis=1)
exports.columns=['CTY_CODE','CTY_NAME','EXPORT MTH','EXPORT YR','time']
exports['EXPORT MTH2']=pd.to_numeric(exports['EXPORT MTH'], errors='coerce')
exports['EXPORT YR2']=pd.to_numeric(exports['EXPORT YR'], errors='coerce')
exports['fred_key']=pd.to_numeric(exports['CTY_CODE'], errors='coerce')
exports['time2'] = exports['time'].astype(str)
exports['CTY_NAME2'] = exports['CTY_NAME'].astype(str)
exports.__delitem__('time')
exports.__delitem__('CTY_NAME')
exports.__delitem__('CTY_CODE')
exports.__delitem__('EXPORT MTH')
exports.__delitem__('EXPORT YR')
imports = pd.concat(imports).reset_index().drop('index', axis=1)
imports.columns=['CTY_CODE','CTY_NAME','GEN_VAL_MO','GEN_VAL_YR','time']
imports['GEN_VAL_YR2']=pd.to_numeric(imports['GEN_VAL_YR'], errors='coerce')
imports['GEN_VAL_MO2']=pd.to_numeric(imports['GEN_VAL_MO'], errors='coerce')
imports['fred_key']=pd.to_numeric(imports['CTY_CODE'], errors='coerce')
imports['time2'] = imports['time'].astype(str)
imports['CTY_NAME2'] = imports['CTY_NAME'].astype(str)
imports.__delitem__('time')
imports.__delitem__('CTY_NAME')
imports.__delitem__('CTY_CODE')
imports.__delitem__('GEN_VAL_YR')
imports.__delitem__('GEN_VAL_MO')
imexdata=pd.merge(imports, exports, left_on=('fred_key','CTY_NAME2','time2'), right_on=('fred_key','CTY_NAME2','time2'))
imexdata_gold=imexdata[imexdata['fred_key']>0]
imexdata_gold['date2']=pd.to_datetime(imexdata_gold['time2'], errors='coerce')
imexdata_gold['monthyear'] = imexdata_gold['date2'].dt.strftime("%Y,%m")

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


boplist=[('CA','Canada','1220','NAFTA'),
('MX','Mexico','2010'),
('GT','Guatemala','2050'),
('BZ','Belize','2080','South America'),
('SV','El Salvador','2110'),
('HN','Honduras','2150'),
('NI','Nicaragua','2190'),
('CR','Costa Rica','2230'),
('PA','Panama','2250'),
('BS','Bahamas','2360'),
('CU','Cuba','2390'),
('JM','Jamaica','2410'),
('TC','Turks and Caicos Islands','2430'),
('KY','Cayman Islands','2440'),
('HT','Haiti','2450'),
('DO','Dominican Republic','2470'),
('AI','Anguilla','2481'),
('VG','British Virgin Islands','2482'),
('KN','Saint Kitts and Nevis','2483'),
('AG','Antigua and Barbuda','2484'),
('DM','Dominica','2486'),
('VC','Saint Vincent and the Grenadines','2488'),
('GD','Grenada','2489'),
('BB','Barbados','2720'),
('TT','Trinidad and Tobago','2740'),
('AW','Aruba','2779'),
('MQ','Martinique','2839'),
('CO','Colombia','3010','South America'),
('VE','Venezuela','3070'),
('GY','Guyana','3120'),
('SR','Suriname','3150'),
('EC','Ecuador','3310'),
('PE','Peru','3330','South America'),
('BO','Bolivia','3350','South America'),
('CL','Chile','3370','South America'),
('BR','Brazil','3510','South America'),
('PY','Paraguay','3530'),
('UY','Uruguay','3550'),
('AR','Argentina','3570','South America'),
('IS','Iceland','4000','EU'),
('SE','Sweden','4010','EU'),
('NO','Norway','4039','EU'),
('FI','Finland','4050','EU'),
('DK','Denmark','4099','EU'),
('GB','United Kingdom of Great Britain and Northern Ireland','4120','EU'),
('IE','Ireland','4190','EU'),
('NL','Netherlands','4210','EU'),
('BE','Belgium','4231'),
('LU','Luxembourg','4239','EU'),
('AD','Andorra','4271'),
('MC','Monaco','4272','EU'),
('FR','France','4279','EU'),
('DE','Germany','4280','EU'),
('AT','Austria','4330','EU'),
('CZ','Czech Republic','4351','EU'),
('SK','Slovakia','4359','EU'),
('HU','Hungary','4370','EU'),
('LI','Liechtenstein','4411','EU'),
('CH','Switzerland','4419','EU'),
('EW','Estonia','4470'),
('LV','Latvia','4490','EU'),
('LT','Lithuania','4510','EU'),
('PL','Poland','4550','EU'),
('RU','Russia','4621','Russia'),
('BY','Belarus','4622'),
('UA','Ukraine','4623'),
('AM','Armenia','4631','EU'),
('AZ','Azerbaijan','4632'),
('GE','Georgia','4633'),
('KZ','Kazakhstan','4634','Russia'),
('KG','Kyrgyz Republic','4635'),
('MD','Moldova','4641'),
('TJ','Tajikistan','4642','Russia'),
('TM','Turkmenistan','4643','Russia'),
('UZ','Uzbekistan','4644','Russia'),
('ES','Spain','4700','EU'),
('PT','Portugal','4710','EU'),
('MT','Malta','4730'),
('VA','Holy See','4752'),
('IT','Italy','4759','EU'),
('HR','Croatia','4791','EU'),
('SI','Slovenia','4792'),
('BA','Bosnia and Herzegovina','4793','EU'),
('MK','Macedonia','4794'),
('RS','Serbia','4801','EU'),
('ME','Montenegro','4804','EU'),
('AL','Albania','4810'),
('GR','Greece','4840','EU'),
('RO','Romania','4850','EU'),
('BG','Bulgaria','4870','EU'),
('TR','Turkey','4890','Russia'),
('CY','Cyprus','4910','EU'),
('SY','Syria','5020'),
('LB','Lebanon','5040'),
('IQ','Iraq','5050','Oil'),
('IR','Iran  Islamic Republic of','5070','Oil'),
('IL','Israel','5081'),
('JO','Jordan','5110'),
('KW','Kuwait','5130','Oil'),
('SA','Saudi Arabia','5170','Oil'),
('QA','Qatar','5180','Oil'),
('AE','United Arab Emirates','5200','Oil'),
('YE','Yemen','5210'),
('OM','Oman','5230'),
('BH','Bahrain','5250'),
('AF','Afghanistan','5310'),
('IN','India','5330','ASIA EAST'),
('PK','Pakistan','5350'),
('NP','Nepal','5360'),
('BD','Bangladesh','5380'),
('LK','Sri Lanka','5420'),
('MM','Myanmar','5460'),
('TH','Thailand','5490'),
('VN','Viet Nam','5520'),
('LA','Laos','5530'),
('KH','Cambodia','5550'),
('MY','Malaysia','5570'),
('SG','Singapore','5590'),
('ID','Indonesia','5600','Oil'),
('TL','Timor-Leste','5601'),
('PH','Philippines','5650'),
('BT','Bhutan','5682'),
('MV','Maldives','5683'),
('CN','China','5700','ASIA WEST'),
('MN','Mongolia','5740'),
('KP','Korea','5790'),
('KR','Korea  Republic of','5800','ASIA EAST'),
('HK','Hong Kong','5820'),
('TW','Taiwan','5830'),
('JP','Japan','5880','ASIA EAST'),
('AU','Australia','6021','ASIA EAST'),
('PG','Papua New Guinea','6040'),
('NZ','New Zealand','6141'),
('WS','Samoa','6150'),
('SB','Solomon Islands','6223'),
('VU','Vanuatu','6224'),
('TV','Tuvalu','6227'),
('MH','Marshall Islands','6810'),
('FM','Micronesia  Federated States of','6820'),
('PW','Palau','6830'),
('NR','Nauru','6862'),
('FJ','Fiji','6863'),
('TO','Tonga','6864'),
('MA','Morocco','7140'),
('DZ','Algeria','7210'),
('TN','Tunisia','7230'),
('LY','Libya','7250','Oil'),
('EG','Egypt','7290'),
('SD','Sudan','7321'),
('SS','South Sudan','7323'),
('MR','Mauritania','7410'),
('CM','Cameroon','7420'),
('SN','Senegal','7440'),
('ML','Mali','7450'),
('GN','Guinea','7460'),
('SL','Sierra Leone','7470'),
('CI','Côte dIvoire','7480'),
('GH','Ghana','7490'),
('GM','Gambia','7500'),
('NE','Niger','7510'),
('TG','Togo','7520'),
('NG','Nigeria','7530','Oil'),
('CF','Central African Rep.','7540'),
('GA','Gabon','7550'),
('TD','Chad','7560'),
('BF','Burkina Faso','7600'),
('BJ','Benin','7610'),
('AO','Angola','7620'),
('CD','Congo','7630'),
('GW','Guinea-Bissau','7642'),
('CV','Cabo Verde','7643'),
('LR','Liberia','7650'),
('BI','Burundi','7670'),
('RW','Rwanda','7690'),
('SO','Somalia','7700'),
('ER','Eritrea','7741'),
('ET','Ethiopia','7749'),
('DJ','Djibouti','7770'),
('UG','Uganda','7780'),
('KE','Kenya','7790'),
('SC','Seychelles','7800'),
('TZ','Tanzania','7830'),
('MU','Mauritius','7850'),
('MZ','Mozambique','7870'),
('MG','Madagascar','7880'),
('KM','Comoros','7890'),
('ZA','South Africa','7910','Gold'),
('NA','Namibia','7920'),
('BW','Botswana','7930'),
('ZM','Zambia','7940'),
('SZ','Swaziland','7950'),
('ZW','Zimbabwe','7960'),
('MW','Malawi','7970'),
('LS','Lesotho','7990'),
('EZ','ECB','','EU'),
('GI','Gibraltar','','EU'),
('IM','Isle of Man','','EU'),
('US','United States','','US'),
('AQ','Antarctica'),
('AS','American Samoa'),
('AX','Åland Islands'),
('BL','Saint Barthélemy'),
('BM','Bermuda'),
('BN','Brunei Darussalam'),
('BQ','Bonaire Sint Eustatius and Saba'),
('BV','Bouvet Island'),
('BX','BIS3'),
('CC','Cocos Islands'),
('CK','Cook Islands'),
('CW','Curaçao'),
('CX','CEMAC'),
('EH','Western Sahara'),
('FK','Falkland Islands Malvinas'),
('FO','Faroe Islands'),
('GF','French Guiana'),
('GG','Guernsey'),
('GL','Greenland'),
('GP','Guadeloupe'),
('GQ','Equatorial Guinea'),
('GS','South Georgia and the South Sandwich Islands'),
('GU','Guam'),
('HM','Heard Island and McDonald Islands'),
('IO','British Indian Ocean Territory'),
('IZ','IMF'),
('JE','Jersey'),
('KI','Kiribati'),
('LC','Saint Lucia'),
('MF','Saint Martin'),
('MO','Macao'),
('MP','Northern Mariana Islands'),
('MS','Montserrat'),
('NC','New Caledonia'),
('NF','Norfolk Island'),
('NU','Niue'),
('PF','French Polynesia'),
('PM','Saint Pierre and Miquelon'),
('PN','Pitcairn'),
('PR','Puerto Rico'),
('PS','Palestine'),
('RE','Réunion'),
('SH','Saint Helena'),
('SJ','Svalbard and Jan Mayen'),
('SM','San Marino'),
('ST','Sao Tome and Principe'),
('SX','Sint Maarten'),
('TF','French Southern Territories'),
('TK','Tokelau'),
('UM','United States Minor Outlying Islands'),
('VI','US Virgin Islands'),
('WF','Wallis and Futuna'),
('WZ','WAEMU'),
('YT','Mayotte'),
]
labels=['cc', 'country', 'fred','segment']
cc_list = pd.DataFrame.from_records(boplist, columns=labels)

cc_list['joinkey']=pd.to_numeric(cc_list['cc'], errors='coerce')
cc_list['fred_key']=pd.to_numeric(cc_list['fred'], errors='coerce')
#######################################
#Get the reserve data for all countries
#######################################

reserve = pd.DataFrame()

for i in boplist:    
    try:
        part1='TRESEG'
        part2=''.join(i[0]) 
        part3='M052N'
        value=part1+part2+part3
        data2 = fred.get_series_all_releases(value)
        data2['amount']=pd.to_numeric(data2['value'], errors='coerce')
        data2['date2']=pd.to_datetime(data2['date'], errors='coerce')
        data2['cc']=part2
        data2['source']=value   
        data2['fredkey']=''.join(i[2])    
        reserve = reserve.append(data2, ignore_index=False)
    except:
        print i



reservex=reserve.sort_values(['date2','cc'], ascending=[True, True])
reservex['reserve_amt_mm']=reservex['amount']/100000000
reservex2=reservex[reservex['reserve_amt_mm'].notnull()]
reservex3=reservex2.drop_duplicates(['date2','cc'], keep='last')    
reservex3['monthyear'] = reservex3['date2'].dt.strftime("%Y,%m")
reservex3['year'] = reservex3['date2'].dt.strftime("%Y")
reservex3.__delitem__('date')
reservex3.__delitem__('realtime_start')
reservex3.__delitem__('value')
reservex3.__delitem__('amount')
reservex3.__delitem__('source')
reservex3.__delitem__('year')
reservex3.__delitem__('date2')
              
#######################################
#Get the SDR data for all countries
#######################################

sdr = pd.DataFrame()

for i in boplist:
    try:
        part1='TRESEG'
        part2=''.join(i[0]) 
        part3='M194N'
        value=part1+part2+part3
        data2 = fred.get_series_all_releases(value)
        data2['amount']=pd.to_numeric(data2['value'], errors='coerce')
        data2['date2']=pd.to_datetime(data2['date'], errors='coerce')
        data2['cc']=part2
        data2['source']=value
        data2['fredkey']=''.join(i[2])  
        sdr = sdr.append(data2, ignore_index=False)
    except:
        print i


sdrx=sdr.sort_values(['date2','cc'], ascending=[True, True])
sdrx['sdr_amt_mm']=sdrx['amount']/100000000
sdrx2=sdrx[sdrx['sdr_amt_mm'].notnull()]
sdrx3=sdrx2.drop_duplicates(['date2','cc'], keep='last')           
sdrx3['monthyear'] = sdrx3['date2'].dt.strftime("%Y,%m")
sdrx3['year'] = sdrx3['date2'].dt.strftime("%Y")
sdrx3.__delitem__('date')
sdrx3.__delitem__('realtime_start')
sdrx3.__delitem__('value')
sdrx3.__delitem__('amount')
sdrx3.__delitem__('source')
sdrx3.__delitem__('year')
sdrx3.__delitem__('date2')


ressdr=pd.merge(reservex3, sdrx3, left_on=('fredkey','monthyear','cc'), right_on=('fredkey','monthyear','cc'))
ressdr['fred_key']=pd.to_numeric(ressdr['fredkey'], errors='coerce')


#######################################
#Get the historical imports and exports
#######################################


#############################################################################
#Make two strip list, one strip list with the alpha, and one with the numeric
#############################################################################

#Read in the historical trade CSV file
#########################################
#Get the import, export, reserve sdr file
#########################################
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('historicalfiles')
# Then do other things...
blob = bucket.get_blob('US_Import_Export.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
details=pd.read_csv(inMemoryFile, low_memory=False)


#details = pd.read_csv('C:/Users/davking/Downloads/US_Import_Export.csv')


month_list=[('JAN','01'),('FEB','02'),('MAR','03'),('APR','04'),('MAY','05'),('JUN','06'),('JUL','07'),('AUG','08'),('SEP','09'),('OCT','10'),('NOV','11'),('DEC','12')]
labels = ['montha', 'monthn']

monthlist = pd.DataFrame.from_records(month_list, columns=labels)
#Get the month list in alpha
df1=monthlist['montha']
monthlista=df1.values.T.tolist()
monthlista = [x.strip(' ') for x in monthlista]   

historical = pd.DataFrame()

for i in monthlista:
    str1 = ''.join([i])
    value = 'I'+str1
    value2 = 'E'+str1
    details['month']=str1    
    details['IMPORT MTH']=details[value]
    details['EXPORT MTH']=details[value2]
    vardataset = details[['year','CTY_CODE','CTYNAME','month','IMPORT MTH','EXPORT MTH']]
    historical = historical.append(vardataset, ignore_index=False)


histdata=pd.merge(historical, monthlist, left_on='month', right_on='montha')
histdata['day']='01'
histdata['year2'] = histdata['year'].astype(str)
histdata['year3'] = histdata['year2'].str[:4]
histdata['slash']='/'
histdata["p1"] = histdata["monthn"].map(str) + histdata["slash"]
histdata["p2"] = histdata["p1"].map(str) + histdata["day"]
histdata["p3"] = histdata["p2"].map(str) + histdata["slash"]
histdata["period"] = histdata["p3"].map(str) + histdata["year3"]
histdata['ind']=pd.to_datetime(histdata['period'], errors='coerce')
histdata['monthyear'] = histdata['ind'].dt.strftime("%Y,%m")
histdata['fred_key']=histdata['CTY_CODE']
histdata2 = histdata[['year','fred_key','CTYNAME','IMPORT MTH','EXPORT MTH','period','monthyear']]   
############################################
#Clean up the import and export current data to join with historical
############################################
imexdata_gold['IMPORT MTH']=imexdata_gold['GEN_VAL_MO2']
imexdata_gold['EXPORT MTH']=imexdata_gold['EXPORT MTH2']
imexdata_gold['CTYNAME']=imexdata_gold['CTY_NAME2']
imexdata_gold['day']='01'
imexdata_gold['month'] = imexdata_gold['date2'].dt.strftime("%m")
imexdata_gold['month2'] = imexdata_gold['month'].astype(str)
imexdata_gold['year'] = imexdata_gold['date2'].dt.strftime("%Y")
imexdata_gold['year2'] = imexdata_gold['year'].astype(str)
imexdata_gold['slash']='/'
imexdata_gold["p1"] = imexdata_gold["month2"].map(str) + imexdata_gold["slash"]
imexdata_gold["p2"] = imexdata_gold["p1"].map(str) + imexdata_gold["day"]
imexdata_gold["p3"] = imexdata_gold["p2"].map(str) + imexdata_gold["slash"]
imexdata_gold["period"] = imexdata_gold["p3"].map(str) + imexdata_gold["year2"]
imexdata_gold['ind']=pd.to_datetime(imexdata_gold['period'], errors='coerce')
imexdata_gold['monthyear'] = imexdata_gold['ind'].dt.strftime("%Y,%m")
imexdata_gold2 = imexdata_gold[['year','fred_key','CTYNAME','IMPORT MTH','EXPORT MTH','period','monthyear','date2']]   
####################################################
#Merge the historical and the current files together
####################################################
imexdata_merge = histdata2.append(imexdata_gold2, ignore_index=True)
imexdata_merge2=imexdata_merge.drop_duplicates(['fred_key','monthyear'], keep='last')
imexdata_ressdr=pd.merge(ressdr, imexdata_merge2, how='outer', left_on=('fred_key','monthyear'), right_on=('fred_key','monthyear'))
###################################################################################
#Merge the balance of payments goods and services file with the imports and exports
###################################################################################
#For US there is no date because date is from reserve data and US has no US reserves
#Generate Date
imexdata_ressdr['day']='01'
imexdata_ressdr['year2'] = imexdata_ressdr['monthyear'].astype(str)
imexdata_ressdr['year3'] = imexdata_ressdr['year2'].str[:4]
imexdata_ressdr['month'] = imexdata_ressdr['year2'].str[5:]
imexdata_ressdr['slash']='/'
imexdata_ressdr['period']=imexdata_ressdr['month']+imexdata_ressdr['slash']+imexdata_ressdr['day']+imexdata_ressdr['slash']+imexdata_ressdr['year3']
imexdata_ressdr['ind']=pd.to_datetime(imexdata_ressdr['period'], errors='coerce')
imexdata_ressdr.__delitem__('day')
imexdata_ressdr.__delitem__('year2')
imexdata_ressdr.__delitem__('year3')
imexdata_ressdr.__delitem__('slash')
imexdata_ressdr.__delitem__('month')
imexdata_ressdr.__delitem__('period')
imexdata_ressdr_bop=pd.merge(imexdata_ressdr, bop_gold_gs, how='outer', left_on=('ind','cc'), right_on=('ind','cc'))

############################################
#Begin to develop the gold file
############################################
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('historicalfiles')
# Then do other things...
blob = bucket.get_blob('gold.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
gold_df=pd.read_csv(inMemoryFile, low_memory=False)

#gold_df = pd.read_csv("C:/Users/davking/Desktop/Python/gold.csv")

colist=[['Q1 2000','3/31/2000'],['Q2 2000','6/30/2000'],['Q3 2000','9/29/2000'],['Q4 2000','12/29/2000'],['Q1 2001','3/30/2001'],['Q2 2001','6/29/2001'],\
['Q3 2001','9/28/2001'],['Q4 2001','12/31/2001'],['Q1 2002','3/29/2002'],['Q2 2002','6/28/2002'],['Q3 2002','9/30/2002'],['Q4 2002','12/31/2002'],\
['Q1 2003','3/29/2003'],['Q2 2003','6/28/2003'],['Q3 2003','9/28/2003'],['Q4 2003','12/31/2003'],['Q1 2004','3/29/2004'],['Q2 2004','6/28/2004'],['Q3 2004','9/28/2004'],\
['Q4 2004','12/31/2004'],['Q1 2005','3/29/2005'],['Q2 2005','6/28/2005'],['Q3 2005','9/28/2005'],['Q4 2005','12/31/2005'],['Q1 2006','3/29/2006'],\
['Q2 2006','6/28/2006'],['Q3 2006','9/28/2006'],['Q4 2006','12/31/2006'],['Q1 2007','3/29/2007'],['Q2 2007','6/28/2007'],['Q3 2007','9/28/2007'],['Q4 2007','12/31/2007'],['Q1 2008','3/29/2008'],\
['Q2 2008','6/28/2008'],['Q3 2008','9/28/2008'],['Q4 2008','12/31/2008'],['Q1 2009','3/29/2009'],['Q2 2009','6/28/2009'],['Q3 2009','9/28/2009'],['Q4 2009','12/31/2009'],\
['Q1 2010','3/29/2010'],['Q2 2010','6/28/2010'],['Q3 2010','9/28/2010'],['Q4 2010','12/31/2010'],['Q1 2011','3/29/2011'],['Q2 2011','6/28/2011'],['Q3 2011','9/28/2011'],\
['Q4 2011','12/31/2011'],['Q1 2012','3/29/2012'],['Q2 2012','6/28/2012'],['Q3 2012','9/28/2012'],['Q4 2012','12/31/2012'],['Q1 2013','3/29/2013'],['Q2 2013','6/28/2013'],\
['Q3 2013','9/28/2013'],['Q4 2013','12/31/2013'],['Q1 2014','3/29/2014'],['Q2 2014','6/28/2014'],['Q3 2014','9/28/2014'],['Q4 2014','12/31/2014'],['Q1 2015','3/29/2015'],\
['Q2 2015','6/28/2015'],['Q3 2015','9/28/2015'],['Q4 2015','12/31/2015'],['Q1 2016','3/29/2016'],['Q2 2016','6/28/2016'],['Q3 2016','9/28/2016'],['Q4 2016','12/31/2016'],\
['Q1 2017','3/29/2017'],['Q2 2017','6/28/2017']]


bigdata = pd.DataFrame()

for i in colist:
    ticker=''.join(i[0])   
    df = gold_df[['Unnamed: 0',ticker]]
    df.columns=['countryx','gold amount']
    df['quarter']=''.join(i[0])
    df['date']=''.join(i[1])
    df['Gold Tonnex'] = df.loc[df['gold amount'].index, 'gold amount'].map(lambda x: x.replace('-','0'))
    df['Gold Tonne'] = df.loc[df['Gold Tonnex'].index, 'Gold Tonnex'].map(lambda x: str(x).replace(',',''))
    df['country2'] = df.loc[df['countryx'].index, 'countryx'].map(lambda x: x.replace(')',''))
    df['country']=df['country2'].str.upper()
    bigdata = bigdata.append(df, ignore_index=False)

labels=['cc','country2','Fred Key','cc group']
country_list = pd.DataFrame.from_records(boplist, columns=labels)
country_list['country']=country_list['country2'].str.upper()
bigdata2=pd.merge(country_list, bigdata, left_on='country', right_on='country')

imexdata_ressdr_bop['month'] = imexdata_ressdr_bop['ind'].dt.strftime("%m")
imexdata_ressdr_bop['month2'] = pd.to_numeric(imexdata_ressdr_bop['month'], errors='coerce')
imexdata_ressdr_bop['monthnum']=pd.to_numeric(imexdata_ressdr_bop['month2'], errors='coerce')
imexdata_ressdr_bop['fl1'] = np.where(imexdata_ressdr_bop['monthnum']<4, 'Q1', 'no')
imexdata_ressdr_bop['fl2'] = np.where((imexdata_ressdr_bop['monthnum']>3) & (imexdata_ressdr_bop['monthnum']<7), 'Q2', 'no')
imexdata_ressdr_bop['fl3'] = np.where((imexdata_ressdr_bop['monthnum']>6) & (imexdata_ressdr_bop['monthnum']<10), 'Q3', 'no')
imexdata_ressdr_bop['fl4'] = np.where(imexdata_ressdr_bop['monthnum']>9, 'Q4', 'no')
q1=imexdata_ressdr_bop[imexdata_ressdr_bop['fl1']=='Q1']
q1['merge']=q1['fl1']+" "+q1['year'].map(str)
q2=imexdata_ressdr_bop[imexdata_ressdr_bop['fl2']=='Q2']
q2['merge']=q2['fl2']+" "+q2['year'].map(str)
q3=imexdata_ressdr_bop[imexdata_ressdr_bop['fl3']=='Q3']
q3['merge']=q3['fl3']+" "+q3['year'].map(str)
q4=imexdata_ressdr_bop[imexdata_ressdr_bop['fl4']=='Q4']
q4['merge']=q4['fl4']+" "+q4['year'].map(str)

gold1 = q1.append(q2, ignore_index=True)
gold2 = q3.append(gold1, ignore_index=True)
imexdata_ressdrgold = q4.append(gold2, ignore_index=True)

del imexdata_ressdrgold['fl1']
del imexdata_ressdrgold['fl2']
del imexdata_ressdrgold['fl3']
del imexdata_ressdrgold['fl4']
del imexdata_ressdrgold['monthnum']
del imexdata_ressdrgold['month2']
del imexdata_ressdrgold['month']

imexdata_ressdrgold['cnt']=1
imexdata_ressdrgold2= imexdata_ressdrgold.groupby(['cc','fredkey','CTYNAME','merge'], as_index=False)['cnt','reserve_amt_mm','sdr_amt_mm','IMPORT MTH','EXPORT MTH','bopg_bal','bopg_exp','bopg_imp','bopgs_bal','bopgs_exp','bopgs_imp'].sum()
quarter_data=pd.merge(imexdata_ressdrgold2, bigdata2,how='left',  left_on=['cc','merge'], right_on=['cc','quarter'])

########################
#Build measures to graph
########################
imexdata_ressdr['import_amt_mm']=imexdata_ressdr['IMPORT MTH']/100000000   
imexdata_ressdr['export_amt_mm']=imexdata_ressdr['EXPORT MTH']/100000000  
imexdata_ressdr['trade_balances']=imexdata_ressdr['import_amt_mm']-imexdata_ressdr['export_amt_mm']
imexdata_ressdr['balance of reserve']=imexdata_ressdr['trade_balances']/imexdata_ressdr['reserve_amt_mm']
imexdata_ressdr['im_ex_ratio']=imexdata_ressdr['import_amt_mm']/imexdata_ressdr['export_amt_mm'] 
quarter_data['import_amt_mm']=quarter_data['IMPORT MTH']/100000000   
quarter_data['export_amt_mm']=quarter_data['EXPORT MTH']/100000000   
quarter_data['qreserve_amt_mm']=quarter_data['reserve_amt_mm']/quarter_data['cnt']
quarter_data['qsdr_amt_mm']=quarter_data['sdr_amt_mm']/quarter_data['cnt']
quarter_data['qimport_amt_mm']=quarter_data['import_amt_mm']/quarter_data['cnt']
quarter_data['qexport_amt_mm']=quarter_data['export_amt_mm']/quarter_data['cnt']
quarter_data['qbopg_bal']=quarter_data['bopg_bal']/quarter_data['cnt']
quarter_data['qbopg_exp']=quarter_data['bopg_exp']/quarter_data['cnt']
quarter_data['qbopg_imp']=quarter_data['bopg_imp']/quarter_data['cnt']
quarter_data['qbopgs_bal']=quarter_data['bopgs_bal']/quarter_data['cnt']
quarter_data['qbopgs_exp']=quarter_data['bopgs_exp']/quarter_data['cnt']
quarter_data['qbopgs_imp']=quarter_data['bopgs_imp']/quarter_data['cnt']

test=imexdata_ressdrgold[imexdata_ressdrgold['cc']=='US']

#####################
#Build Graph Measures#
######################
quarter_data['y'] = quarter_data['merge'].str[3:]
quarter_data['q'] = quarter_data['merge'].str[:2]
quarter_data['space'] = '  '
quarter_data['date_var'] = quarter_data['y']+quarter_data['space']+quarter_data['q']
del quarter_data['space']
del quarter_data['q']
del quarter_data['y']
##################################
#Put the dataset back into storage
##################################
from google.cloud import storage
client = storage.Client()
bucket2 = client.get_bucket('macrofiles')
df_out = pd.DataFrame(imexdata_ressdr_bop)
df_out.to_csv('imexdata_ressdr.csv', index=False)
blob2 = bucket2.blob('imexdata_ressdr.csv')
blob2.upload_from_filename('imexdata_ressdr.csv')

##################################
#Put the dataset back into storage
##################################
from google.cloud import storage
client = storage.Client()
bucket2 = client.get_bucket('macrofiles')
df_out = pd.DataFrame(quarter_data)
df_out.to_csv('quarter_data.csv', index=False)
blob2 = bucket2.blob('quarter_data.csv')
blob2.upload_from_filename('quarter_data.csv')


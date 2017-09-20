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
import matplotlib
import matplotlib.pyplot as plt
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO as stio
else:
    from io import StringIO as stio

base = "https://api.census.gov/data/timeseries/intltrade/exports/hs?get=CTY_CODE,CTY_NAME,ALL_VAL_MO,ALL_VAL_YR&time="
base2 = "https://api.census.gov/data/timeseries/intltrade/imports/enduse?get=CTY_CODE,CTY_NAME,GEN_VAL_MO,GEN_VAL_YR&time="
year_list = ['2013','2014','2015','2016','2017']
month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']

exports = []
rejects = []
imports = []
rejects2 = []

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
('SA','Saudi Arabia2','5170','Oil'),
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
('CN','China1','5700','ASIA WEST'),
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
details = pd.read_csv('C:/Users/davking/Downloads/US_Import_Export.csv')


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
########################
#Build measures to graph
########################
imexdata_ressdr['import_amt_mm']=imexdata_ressdr['IMPORT MTH']/100000000   
imexdata_ressdr['export_amt_mm']=imexdata_ressdr['EXPORT MTH']/100000000  
imexdata_ressdr['trade_balances']=imexdata_ressdr['import_amt_mm']-imexdata_ressdr['export_amt_mm']
imexdata_ressdr['balance of reserve']=imexdata_ressdr['trade_balances']/imexdata_ressdr['reserve_amt_mm']
imexdata_ressdr['im_ex_ratio']=imexdata_ressdr['import_amt_mm']/imexdata_ressdr['export_amt_mm']  
###############################################
#Only include data of the big reserve countries
############################################### 
rankvar=imexdata_ressdr[imexdata_ressdr['monthyear']=='2017,07']
rankvar['Ranked'] = rankvar['reserve_amt_mm'].rank(ascending=1)
rankvar2=rankvar[rankvar['Ranked'].notnull()]
rankvar2=rankvar2[['Ranked','cc']]
rankvar2['all']=1
rankvar2['All_Rank'] = rankvar2.groupby('all')['Ranked'].rank(ascending=False)
all2=pd.merge(rankvar2, imexdata_ressdr, how='outer', left_on='cc', right_on='cc')

#Build a country list 
cclist_for=all2[['cc']]
cclist_for2=cclist_for.drop_duplicates(['cc'], keep='last')
df1=cclist_for2['cc']
df2=df1.values.T.tolist()

graphdata = pd.DataFrame()
dateplot = []
dateplot2 = []

#Get the axis values for each 

all2['year'] = all2['date2'].dt.strftime("%Y")
all2['month'] = all2['date2'].dt.strftime("%m")
all2['day'] = all2['date2'].dt.strftime("%d")


for i in df2:
    str1 = ''.join([i])
    fileset=all2[all2['cc']==str1]    
    fileset['ma6 import_amt_mm'] = fileset['import_amt_mm'].rolling(window=6).mean()
    fileset['ma6 export_amt_mm'] = fileset['export_amt_mm'].rolling(window=6).mean()
    fileset['ma6 trade_balances'] = fileset['trade_balances'].rolling(window=6).mean()
    fileset[['trade_balances']] = fileset[['trade_balances']].apply(pd.to_numeric)
    fileset[['ma6 import_amt_mm']] = fileset[['ma6 import_amt_mm']].apply(pd.to_numeric)
    fileset[['ma6 export_amt_mm']] = fileset[['ma6 export_amt_mm']].apply(pd.to_numeric)
    test3=pd.DataFrame(fileset[fileset['date2'].notnull()])
    test4=test3.drop_duplicates(['date2'], keep='last')
    fig = plt.figure(figsize=(20,15))
    #As soon as graph1 is initialized, everything below the block is included until another graph is initialized
    graph1 = fig.add_subplot(411)
    graph1.tick_params('y', colors='b')
    graph1.plot(test4['date2'],test4['ma6 import_amt_mm'],'b-', linewidth=6.0, label='ma6 import_amt_mm')
    graph1.plot(test4['date2'],test4['ma6 export_amt_mm'],'r-', linewidth=6.0, label='ma6 export_amt_mm')   
    plt.title(str1)
    graph1.legend(loc='best')





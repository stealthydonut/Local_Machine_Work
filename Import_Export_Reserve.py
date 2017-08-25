import pandas as pd
import requests
from pandas.compat import StringIO
#Sourced from the following site https://github.com/mortada/fredapi
from fredapi import Fred
fred = Fred(api_key='4af3776273f66474d57345df390d74b6')

year_list = '2013','2014','2015','2016','2017'
month_list = '01','02','03','04','05','06','07','08','09','10','11','12'

#############################################
#Get the total exports from the United States
#############################################

exports = pd.DataFrame()

for i in year_list:
    for s in month_list:
        try:
            link="https://api.census.gov/data/timeseries/intltrade/exports/hs?get=CTY_CODE,CTY_NAME,ALL_VAL_MO,ALL_VAL_YR&time="
            str1 = ''.join([i])
            txt = '-'
            str2 = ''.join([s])
            total_link=link+str1+txt+str2
            r = requests.get(total_link)
            df = pd.read_csv(StringIO(r.text))
            ##################### change starts here #####################
            ##################### since it is a dataframe itself, so the method to create a dataframe from a list won't work ########################
            # Drop the total sales line
            df.drop(df.index[0])
            # Rename Column name
            df.columns=['CTY_CODE','CTY_NAME','EXPORT MTH','EXPORT YR','time','UN']
            # Change the ["1234" to 1234
            df['CTY_CODE']=df['CTY_CODE'].str[2:-1]
            # Change the 2017-01] to 2017-01
            df['time']=df['time'].str[:-1]
            ##################### change ends here #####################            
            exports = exports.append(df, ignore_index=False)
        except:
            print i
            print s
            
exports.__delitem__('UN')
#############################################
#Get the total imports from the United States
#############################################

imports = pd.DataFrame()

for i in year_list:
    for s in month_list:
        try:
            link="https://api.census.gov/data/timeseries/intltrade/imports/enduse?get=CTY_CODE,CTY_NAME,GEN_VAL_MO,GEN_VAL_YR&time="
            str1 = ''.join([i])
            txt = '-'
            str2 = ''.join([s])
            total_link=link+str1+txt+str2
            r = requests.get(total_link)
            df = pd.read_csv(StringIO(r.text))
            ##################### change starts here #####################
            ##################### since it is a dataframe itself, so the method to create a dataframe from a list won't work ########################
            # Drop the total sales line
            df.drop(df.index[0])
            # Rename Column name
            df.columns=['CTY_CODE','CTY_NAME','IMPORT MTH','IMPORT YR','time','UN']
            # Change the ["1234" to 1234
            df['CTY_CODE']=df['CTY_CODE'].str[2:-1]
            # Change the 2017-01] to 2017-01
            df['time']=df['time'].str[:-1]
            ##################### change ends here #####################            
            imports = imports.append(df, ignore_index=False)
        except:
            print i
            print s

imports.__delitem__('UN')
######################################
#Join the imports and exports together
###################################### 
imexdata=pd.merge(imports, exports, left_on=('CTY_CODE','CTY_NAME','time'), right_on=('CTY_CODE','CTY_NAME','time'))
imexdata['fred_key']=pd.to_numeric(imexdata['CTY_CODE'], errors='coerce')
imexdata_gold=imexdata[imexdata['fred_key']>0]


#Generate a country list


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

##################################################
#Join the imports and reserves by country together
##################################################

imexdata_withcc=pd.merge(cc_list, imexdata, left_on='fred_key', right_on='fred_key')




print cc_list.dtypes

##################################################################
#Generate a total by country to align with the imports and exports
#################################################################

allgroup = ressdr.groupby(['monthyear',], as_index=False)['sdr_amt_mm','reserve_amt_mm'].sum() 




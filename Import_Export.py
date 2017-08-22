#https://www.census.gov/data/developers/data-sets/international-trade.html

import pandas as pd

#Generate the list required to build the files

#The country list has the two digit country code which is required to join all the data from all sources together

boplist = [('Afghanistan','5310','AF'),('Albania','4810','AL'),('Algeria','7210','DZ'),('Andorra','4271','AD'),('Angola','7620','AO'),('Anguilla','2481','AI'),('Antigua and Barbuda','2484','AG'),('Argentina','3570','AR'),\
('Armenia','4631','AM'),('Aruba','2779','AW'),('Australia','6021','AU'),('Austria','4330','AT'),('Azerbaijan','4632','AZ'),('Bahamas','2360','BS'),\
('Bahrain','5250','BH'),('Bangladesh','5380','BD'),('Barbados','2720','BB'),('Belarus','4622','BY'),('Belgium','4231','BE'),\
('Belize','2080','BZ'),('Benin','7610','BJ'),('Bhutan','5682','BT'),('Bolivia','3350','BO'),('Bosnia','4793','BA'),('Botswana','7930','BW'),('Brazil','3510','BR'),('British Virgin Islands','2482','VG'),\
('Bulgaria','4870','BG'),('Burkina Faso','7600','BF'),('Burma','5460','MM'),('Burundi','7670','BI'),('Cambodia','5550','KH'),('Cameroon','7420','CM'),('Canada','1220','CA'),\
('Cape Verde','7643','CV'),('Cayman Islands','2440','CY'),('Central African Republic','7540','CF'),('Chad','7560','TD'),('Chile','3370','CL'),('China','5700','CN'),('Colombia','3010','CO'),('Comoros','7890','KM'),\
('Congo','7630','CD'),('Costa Rica','2230','CR'),('Cote dIvoire','7480','CI'),('Croatia','4791','HR'),('Cuba','2390','CU'),('Cyprus','4910','CY'),('Czech Republic','4351','CZ'),\
('Denmark','4099','DK'),('Dijbouti','7770','DJ'),('Dominca','2486','DM'),('Domincan Republic','2470','DO'),('Ecuador','3310','EC'),('Egypt','7290','EG'),('El Salvador','2110','ES'),('Equatorial','7380','Country'),\
('Eritrea','7741','ER'),('Estonia','4470','ES'),('Ethiopia','7749','ET'),('Fiji','6863','FJ'),('Finland','4050','FI'),('France','4279','FR'),('Gabon','7550','GA'),('Gambia','7500','GM'),('Georgia','4633','GE'),('Germany','4280','DE'),\
('Ghana','7490','GH'),('Greece','4840','GR'),('Grenada','2489','GD'),('Guatemala','2050','GT'),('Guinea  ','7460','GN'),('Guinea-Bissau','7642','GW'),('Guyana','3120','GY'),('Haiti','2450','HT'),('Honduras','2150','HN'),\
('Hong Kong','5820','HK'),('Hungary','4370','HU'),('Iceland','4000','IS'),('India','5330','IN'),('Indonesia','5600','ID'),('Iran','5070','IR'),('Iraq','5050','IQ'),\
('Ireland','4190','IE'),('Israel','5081','IL'),('Italy','4759','IT'),('Jamaica','2410','JM'),('Japan','5880','JP'),('Jordan','5110','JO'),('Kazakhastan','4634','KZ'),('Kenya','7790','KE'),('Kyrgyzstan','4635','KG'),('Laos','5530','LA'),\
('Latvia','4490','LV'),('Lebanon','5040','LB'),('Lechtenstein','4411','LI'),('Lesotho','7990','LS'),('Liberia','7650','LR'),('Libya','7250','LY'),('Lithuania','4510','LT'),('Luxembourg','4239','LU'),\
('Macedonia','4794','MK'),('Madagascar','7880','MG'),('Malawi','7970','MW'),('Malaysia','5570','MY'),('Maldives','5683','MV'),('Mali','7450','ML'),('Malta','4730','MT'),('Marshall Islands','6810','MH'),('Martinique','2839','MQ'),('Mauritania','7410','MR'),\
('Mauritius','7850','MU'),('Mexico','2010','MX'),('Micronesia','6820','FM'),('Moldova','4641','MD'),('Monaco','4272','MC'),('Mongolia','5740','MN'),\
('Montenegro','4804','ME'),('Morocco','7140','MA'),('Mozambique','7870','MZ'),('Namibia','7920','NA'),('Nauru','6862','NR'),('Nepal','5360','NP'),('Netherlands','4210','NL'),('New Zealand','6141','NZ'),('Nicaragua','2190','NI'),\
('Niger','7510','NE'),('Nigeria','7530','NG'),('North Korea','5790','KP'),('Norway','4039','NO'),('Oman','5230','OM'),('Pakistan','5350','PK'),('Palau','6830','PW'),('Panama','2250','PA'),('Papau New Guinea','6040','PG'),('Paraguay','3530','PY'),\
('Peru','3330','PE'),('Philippines','5650','PH'),('Poland','4550','PL'),('Portugal','4710','PT'),('Qatar','5180','QA'),('Romania','4850','RO'),('Russia','4621','RU'),\
('Rwanda','7690','RW'),('Saint Kitts','2483','KN'),('Saint Vincent','2488','VC'),('Samoa','6150','WS'),('San Tome','7644','Country'),\
('Saudi Arabia','5170','SA'),('Senegal','7440','SN'),('Serbia','4801','RS'),('Seychelles','7800','SC'),('Sierra Leone','7470','SL'),('Singapore','5590','SG'),\
('Slovakia','4359','SK'),('Slovenia','4792','SI'),('Solomon Islands','6223','SB'),('Somalia','7700','SO'),('South Africa','7910','ZA'),('South Korea','5800','KR'),\
('South Sudan','7323','SS'),('Spain','4700','ES'),('Sri Lanka','5420','LK'),('Sudan','7321','SD'),('Suriname','3150','SR'),\
('Swaziland','7950','SZ'),('Sweden','4010','SE'),('Switzerland','4419','CH'),('Syria','5020','SY'),('Taiwan','5830','TW'),('Tajikistan','4642','TJ'),\
('Tanzania','7830',''),('Thailand','5490','TH'),('Timor-Leste','5601','TL'),('Togo','7520','TG'),('Tonga','6864','TO'),('Trinidad','2740','TT'),\
('Tunisia','7230','TN'),('Turkey','4890','TR'),('Turkmenistan','4643','TM'),('Turks and Caicos','2430','TC'),('Tuvalu','6227','TV'),('Uganda','7780','UG'),\
('Ukraine','4623','UA'),('United Arab Emirates','5200','AE'),('United Kingdom','4120','GB'),('Uruguay','3550','UY'),('Uzbekistan','4644','UZ'),('Vanuatu','6224','VU'),\
('Vatican City','4752','VA'),('Venezuela','3070','VE'),('Vietnam','5520','VN'),('Yemen','5210','YE'),('Zambia','7940','ZM'),('Zimbabwe','7960','ZW'),\
('Kosovo','4803','XK'),('Kuwait','5130','KW')]
labels=['country', 'use_cc', 'cc']
df = pd.DataFrame.from_records(boplist, columns=labels)

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
            df = pd.read_csv(total_link)
            exports = exports.append(df, ignore_index=False)
        except:
            print i
            print s

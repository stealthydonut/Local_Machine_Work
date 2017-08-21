

import urllib
import pandas as pd
#sudo -H pip install six==1.10.0
#sudo -H pip install html5lib==1.0b8
#sudo pip install beautifulsoup4
import html5lib
#import beautifulsoup4
import datetime
import StringIO


dls = "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download"
urllib.urlretrieve(dls, "test.xls")
data1 = pd.read_csv('test.xls')
data1['Index']='nasdaq'

dls = "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download"
urllib.urlretrieve(dls, "test.xls")
data2 = pd.read_csv('test.xls')
data2['Index']='nyse'

dls = "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download"
urllib.urlretrieve(dls, "test.xls")
data3 = pd.read_csv('test.xls')
data3['Index']='amex'

###################################
#Set the data on top of one another
###################################
set1 = data1.append(data2, ignore_index=True)
ticker = data3.append(set1, ignore_index=True) 

###############################################
#Create a timestamp to insert on a master table
###############################################

from datetime import datetime, timedelta
insert = datetime.today()

year = datetime.today().year
month = datetime.today().month
day = datetime.today().day 

stamp=str(month)+'/'+str(day)+'/'+str(year)

ticker['insert date'] = stamp
      
##################################
#Clean up the market captilization 
##################################
ticker['char']=ticker['MarketCap'].str[-1:]
ticker['var'] = ticker.loc[ticker['MarketCap'].index, 'MarketCap'].map(lambda x: x.replace('$','').replace('.','').replace('B','').replace('M','').replace('n/a',''))
tickerB = ticker[ticker['char'] == 'B']
tickerB['zero']= '000000000'
tickerB['Mkt Cap']=tickerB['var']+tickerB['zero']
tickerB['Mkt Cap']=pd.to_numeric(tickerB['Mkt Cap'], errors='coerce')
tickerB['LastSale']=pd.to_numeric(tickerB['LastSale'], errors='coerce')
tickerB['Shares']=tickerB['Mkt Cap']/tickerB['LastSale']
tickerB['Shares'] = tickerB['Shares'].round(0).astype(int)
tickerM = ticker[ticker['char'] == 'M']
tickerM['zero']= '000000'
tickerM['Mkt Cap']=tickerM['var']+tickerM['zero']
tickerM['Mkt Cap']=pd.to_numeric(tickerM['Mkt Cap'], errors='coerce')
tickerM['LastSale']=pd.to_numeric(tickerM['LastSale'], errors='coerce')
tickerM['Shares']=tickerM['Mkt Cap']/tickerM['LastSale']
tickerM['Shares'] = tickerM['Shares'].round(0).astype(int)
tickera = ticker[ticker['char'] == 'a']

file1 = tickerM.append(tickerB, ignore_index=True)
file2 = tickera.append(file1, ignore_index=True)

##################################
#Begin to create the google ticker
##################################

#Replace the tickers with the google symbol
file2['symbol2'] = file2.loc[file2['Symbol'].index, 'Symbol'].map(lambda x: x.replace('^','-').replace('.WS',''))
#Get the correct google symbol
tickeramex = file2[file2['Index'] == 'amex']
tickeramex['exchange']='NYSEMKT:'
tickeramex['google symbol']=tickeramex['exchange']+tickeramex['symbol2']
tickernas = file2[file2['Index'] == 'nasdaq']
tickernas['exchange']='NASDAQ:'
tickernas['google symbol']=tickernas['exchange']+tickernas['symbol2']
tickernyse = file2[file2['Index'] == 'nyse']
tickernyse['exchange']='NYSEMKT:'
tickernyse['google symbol']=tickernyse['exchange']+tickernyse['symbol2']

file3 = tickernas.append(tickernyse, ignore_index=True)
ticker_gold = tickeramex.append(file3, ignore_index=True)

df1=df['Symbol']
df2=df1.values.T.tolist()
#strip out leading and trailing 0's
df2 = [x.strip(' ') for x in df2]   

myfile = ''
bigdata = pd.DataFrame()

myfile = ''
for i in df2:
    try:#Develop the text string that can get all the data
        start="http://finance.yahoo.com/d/quotes.csv?s="
        #date,Float Shares,Day's Low,Day's High,Open,Previous Close,Change,Volume,Name,Ticker,52 Low, 52 High,Dividend Share, Volume
        #end="&f=d1f6ghopc1vns"
        #date,Float ,Name,Ticker
        end="&f=d1f6s7oc1pghnsjkdk5j6rv"
        str1 = ''.join([i])
        text2=start+str1+end    
        #Get the data from the yahoo api
        link=text2
        f = urllib.urlopen(link)
        myfile += f.readline()
    except:
        print i

        
TESTDATA=stio(myfile)

daily_prices = pd.read_csv(TESTDATA, sep=",", names=['date','Float Shares','Short Ratio','Open','Change','Previous Close','Low','High','Name','Ticker','52 Low','52 High','Dividend','Per change 52 H','Per change 52 L','PE Ratio','Volume'])
daily_prices['Div Yield']=(daily_prices['Dividend']/daily_prices['Previous Close'])*100
daily_prices['Mkt Cap']=daily_prices['Previous Close']*daily_prices['Float Shares'] 
daily_prices['Vol Amt']=daily_prices['52 High']-daily_prices['52 Low']
daily_prices['Vol %']=daily_prices['Vol Amt']/daily_prices['Previous Close']

bigdata=pd.merge(daily_prices, df, left_on='Ticker', right_on='Symbol')


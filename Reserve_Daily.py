import StringIO
import pandas as pd

#########################################
#Get the import, export, reserve sdr file
#########################################
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('macrofiles')
# Then do other things...
blob = bucket.get_blob('import_export_res_sdr.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
imexsdr=pd.read_csv(inMemoryFile, low_memory=False)

bucket = client.get_bucket('macrofiles')
# Then do other things...
blob = bucket.get_blob('daily_monthly_file.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
dailyvar=pd.read_csv(inMemoryFile, low_memory=False)

bucket = client.get_bucket('macrofiles')
# Then do other things...
blob = bucket.get_blob('monthly_file.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
monthlyvar=pd.read_csv(inMemoryFile, low_memory=False)

#####################################################################
#Aggregate the data at the monthly level to join with the daily files
#####################################################################
allgroup = imexsdr.groupby(['monthyear'], as_index=False, sort=False)['sdr_amt_mm','reserve_amt_mm','export_amt_mm','import_amt_mm'].sum() 




goldfile=pd.merge(allgroup, monthlyvar, how='inner', left_on='monthyear', right_on='monthyear')


##################################
#Put the dataset back into storage
##################################
from google.cloud import storage
client = storage.Client()
bucket2 = client.get_bucket('macrofiles')
df_out = pd.DataFrame(goldfile)
df_out.to_csv('goldfile.csv', index=False)
blob2 = bucket2.blob('goldfile.csv')
blob2.upload_from_filename('goldfile.csv')

monthly_file['ma6 US Receipts'] = monthly_file['US Receipts'].rolling(window=6).mean()
monthly_file['ma6 ISM Diffusion Index'] = monthly_file['ISM Diffusion Index'].rolling(window=6).mean()




daily_file['Gold Silver Ratio']=daily_file['Gold USD (PM)']/daily_file['Silver USD']
daily_file['Gold Oil Ratio']=daily_file['Gold USD (PM)']/daily_file['Oil USD']
daily_file['Silver Oil Ratio']=daily_file['Silver USD']/daily_file['Oil USD']



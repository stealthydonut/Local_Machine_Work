############################
#Get the detail file
############################
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('macrofiles')
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

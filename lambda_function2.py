import boto3
import os
import json
import csv

s3_client = boto3.client("s3")
s3 = boto3.resource('s3') 
def lambda_handler(event, context):
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name'] #getting bucket
        key = record['s3']['object']['key']   #getting object
    target_bucket_name = os.environ['TARGET_BUCKET_NAME'] # target bucket
    original = s3_client.get_object(
        Bucket=bucket,
        Key=key)
        
    #Download file in temp memory
    file = s3_client.download_file(bucket, key, '/tmp/' + key) # downloading file
    res = os.path.isfile('/tmp/' + key)
    
    #checking file is present or not
    if res:
        initial = []
        lastname = []
        initial_lastname = []
        
        with open('/tmp/'+key,'r') as read_obj:
            csv_reader = csv.reader(read_obj)
            
            for line in csv_reader:
                  for item in line:
                      initial.append(item[0])
                      item = item.split()
                      lastname.append(item[1])
            r = len(lastname)
            for i in range(r):
                initial_lastname.append(initial[i]+lastname[i])
                
                
            
    else:
        print("file does not exists")
        return "Failure"
    listToStr = ','.join([str(elem) for elem in initial_lastname])
    response = str(listToStr)
    encoded_data= response.encode()
    byte_array = bytearray(encoded_data).decode("utf-8")  #converting to bytes
    
    object = s3.Object(target_bucket_name,key)  #putting on next bucketS
    result = object.put(Body=byte_array)

    res = result.get('ResponseMetadata')
    
    if res.get('HTTPStatusCode') == 200:
        print('File Uploaded Successfully')
    else:
        print('File Not Uploaded')

    return "Success"    
# -*- coding: utf-8 -*-
"""
@author: ntm20
"""

import boto3
s3_resource = boto3.resource('s3')

import uuid
def create_bucket_name(bucket_prefix):
 # The generated bucket name must be between 3 and 63 chars long
 return ''.join([bucket_prefix, str(uuid.uuid4())])

#Buckets Creation Function
def create_bucket(bucket_prefix, s3_connection):
 session = boto3.session.Session()
 current_region = session.region_name
 bucket_name = create_bucket_name(bucket_prefix)
 bucket_response = s3_connection.create_bucket(
 Bucket=bucket_name,
 CreateBucketConfiguration={
 'LocationConstraint': current_region})
 print(bucket_name, current_region)
 return bucket_name, bucket_response

#create first bucket name nitzan1
first_bucket_name, first_response = create_bucket(
    bucket_prefix='nitzan1', s3_connection=s3_resource.meta.client)

#create first bucket name nitzan2
second_bucket_name, second_response = create_bucket(
    bucket_prefix='nitzan2', s3_connection=s3_resource)

#create file function
def create_temp_file(size, file_name, file_content):
 random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
 with open(random_file_name, 'w') as f:
     f.write(str(file_content) * size)
 return random_file_name

#create file name firstfile.txt
first_file_name = create_temp_file(300, 'firstfile.txt', 'f') 


first_bucket = s3_resource.Bucket(name=first_bucket_name)
first_object = s3_resource.Object(
 bucket_name=first_bucket_name, key=first_file_name)

#uploading filertfile to nitzan1
s3_resource.Object(first_bucket_name, first_file_name).upload_file(
 Filename=first_file_name)

#download firstfile from nitzan1
s3_resource.Object(first_bucket_name, first_file_name).download_file(
 f'/tmp/{first_file_name}') 

#copy files betwween buckets function
def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
 copy_source = {
 'Bucket': bucket_from_name,
 'Key': file_name
 }
 s3_resource.Object(bucket_to_name, file_name).copy(copy_source)
copy_to_bucket(first_bucket_name, second_bucket_name, first_file_name)

#copy firstfile to nitzan2
s3_resource.Object(second_bucket_name, first_file_name).delete()

#create second file name secondfile.txt in nitzan1
second_file_name = create_temp_file(400, 'secondfile.txt', 's')
second_object = s3_resource.Object(first_bucket.name, second_file_name)
second_object.upload_file(second_file_name, ExtraArgs={
 'ACL': 'public-read'})

second_object_acl = second_object.Acl()

second_object_acl.grants

#make secondfile.txt private
response = second_object_acl.put(ACL='private')
second_object_acl.grants

#create third file name thirdfile.txt in nitzan1
third_file_name = create_temp_file(300, 'thirdfile.txt', 't')
third_object = s3_resource.Object(first_bucket_name, third_file_name)
third_object.upload_file(third_file_name, ExtraArgs={
 'ServerSideEncryption': 'AES256'})

#print third file algorithem
print(third_file_name, "algorithm:", third_object.server_side_encryption)

#change thirdfile.txt storageclass and upload it again
third_object.upload_file(third_file_name, ExtraArgs={
 'ServerSideEncryption': 'AES256',
 'StorageClass': 'STANDARD_IA'})

#print third file new object class
third_object.reload()
print("third file object class:", third_object.storage_class)

#enable bucket version function
def enable_bucket_versioning(bucket_name):
 bkt_versioning = s3_resource.BucketVersioning(bucket_name)
 bkt_versioning.enable()
 print("enable_bucket_versioning=", bkt_versioning.status)

enable_bucket_versioning(first_bucket_name)

#create two new version of the first file
s3_resource.Object(first_bucket_name, first_file_name).upload_file(
 first_file_name)
s3_resource.Object(first_bucket_name, first_file_name).upload_file(
 third_file_name)

#reupload second file and it will create a new version of it
s3_resource.Object(first_bucket_name, second_file_name).upload_file(
 second_file_name)

#print first file version
print(first_file_name, "version:", s3_resource.Object(first_bucket_name, first_file_name).version_id)

#print all buckets names
for bucket in s3_resource.buckets.all():
    print(bucket.name)

#print all files names in nitzan1 bucket
for obj in first_bucket.objects.all():
    print(obj.key)

#print all files in nitzan1 objets summery
for obj in first_bucket.objects.all():
    subsrc = obj.Object()
    print(obj.key, obj.storage_class, obj.last_modified,
          subsrc.version_id, subsrc.metadata)

#delete buckets function
def delete_all_objects(bucket_name):
 res = []
 bucket=s3_resource.Bucket(bucket_name)
 for obj_version in bucket.object_versions.all():
     res.append({'Key': obj_version.object_key,
                 'VersionId': obj_version.id})
 print(res)
 bucket.delete_objects(Delete={'Objects': res})
 
#delete all files in nitzan1 bucket
delete_all_objects(first_bucket_name)

#upload a file to nitzan2 bucket and delete it
s3_resource.Object(second_bucket_name, first_file_name).upload_file(first_file_name)
delete_all_objects(second_bucket_name)

#delete nitzan1 bucket
s3_resource.Bucket(first_bucket_name).delete()
#delete nitzan2 bucket
s3_resource.Bucket(second_bucket_name).delete()


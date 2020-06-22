from io import BytesIO
import zipfile
import boto3


def unzip_csvs():
    '''Iterate over relevant zipped files, unzip, and upload to private s3'''
    s3 = boto3.resource('s3')
    source = s3.Bucket('tripdata')

    for obj in source.objects.all():
        key = obj.key

        if not key.startswith('201307-201402') and key.endswith('.zip'):
            buffer = BytesIO(obj.get()['Body'].read())
            zipped = zipfile.ZipFile(buffer)

            for name in zipped.namelist():

                if not name.startswith('_') and name.endswith('.csv'):
                    s3.meta.client.upload_fileobj(
                        zipped.open(name),
                        Bucket = 'jlang-20b-de-ny',
                        Key = 'citibike/' + name
                    )

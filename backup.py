import boto3
import os
import json
import pyexifinfo
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

def main():
    glacier = boto3.resource('glacier')
    dyno = boto3.resource('dynamodb')
    backup_table = dyno.Table(os.environ.get('DYNAMODB_TABLE'))

    # source = Path('/Users/kienpham/Pictures/Bishop')
    source = Path('/Users/kienpham/Desktop/raw')
    if not source.is_dir():
        exit('not valid dir')

    vault = glacier.Vault(
        os.environ.get('AWS_ACCOUNT_ID'),
        os.environ.get('AWS_VAULT_NAME'))

    # print(source.parts)
    # print(source.name)
    # exit()
    for each_file in source.iterdir():
        if each_file.suffix.lower() != '.txt': #.cr2
            print('skipping non-raw file ', each_file)
            continue

        print('processing raw file ', each_file)
        file_path = source.joinpath(each_file)

        exif = pyexifinfo.get_json(str(file_path))[0]
        # exit(exif)
        # exif_summary = {
        #     'width': exif['EXIF:ImageWidth'],
        #     'height': exif['EXIF:ImageHeight'],
        #     'iso': exif['EXIF:ISO'],
        #     'focal': exif['EXIF:FocalLength'],
        #     'model': exif['EXIF:Model'],
        #     'lens': exif['Composite:LensID'],
        #     'flash': exif['EXIF:Flash'],
        #     'exif_version': exif['EXIF:ExifVersion'],
        #     'serial': exif['MakerNotes:SerialNumber'],
        # }
        archive = vault.upload_archive(
            archiveDescription=json.dumps(exif),
            body=file_path.open('rb')
        )

        if archive.id:
            res = backup_table.put_item(
                Item={
                    'archive_id': archive.id,
                    'datetime': datetime.today().isoformat(),
                    'exif': json.dumps(exif)
                },
            )
            print(res)

    # print(vault.creation_date)



if __name__ == '__main__':
    main()

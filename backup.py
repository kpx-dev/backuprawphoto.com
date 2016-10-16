import boto3
import os
import json
import pyexifinfo
import magic
import hashlib
from datetime import datetime
from pathlib import Path
from botocore.client import Config
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
boto_config = Config(connect_timeout=9999120, read_timeout=9999120,
                     region_name='us-east-1')


def main():
    glacier = boto3.resource('glacier', config=boto_config)
    dyno = boto3.resource('dynamodb', config=boto_config)
    backup_table = dyno.Table(os.environ.get('DYNAMODB_TABLE'))

    source = Path('/Users/kienpham/Pictures/Bishop')
    # source = Path('/Users/kienpham/Desktop/raw')
    if not source.is_dir():
        exit('not valid dir')

    vault = glacier.Vault(
        os.environ.get('AWS_ACCOUNT_ID'),
        os.environ.get('AWS_VAULT_NAME'))

    mime = magic.Magic(mime=True)

    for each_file in source.iterdir():
        file_path = source.joinpath(each_file)
        mime_type = mime.from_file(str(file_path))
        if not mime_type.startswith('image'):
        # if each_file.suffix.lower() != '.jpg': #.cr2
            print('skipping non-image file ', each_file)
            continue

        print('processing file ', each_file)

        exif = pyexifinfo.get_json(str(file_path))[0]
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
        with file_path.open('rb') as f:
            file_content = f.read()
            md5_checksum = hashlib.md5(file_content).hexdigest()

            archive = vault.upload_archive(
                archiveDescription=str(file_path),
                body=file_content
            )
            # print(archive)

        if archive.id:
            res = backup_table.put_item(
                Item={
                    'file_checksum': md5_checksum,
                    'datetime': datetime.today().isoformat(),
                    'archive_id': archive.id,
                    'vault_name': vault.name,
                    'exif': json.dumps(exif),
                    # 'exif_summary': exif_summary,
                    'file_path': str(file_path),
                    'file_name': str(each_file),
                },
            )
            print(archive.id)

if __name__ == '__main__':
    main()

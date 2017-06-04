import boto3
import os
import json
import pyexifinfo
import magic
import hashlib
import time
import redis
import argparse
from datetime import datetime
from pathlib import Path
from botocore.client import Config
from decimal import Decimal
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

redis_client = redis.StrictRedis(host=os.environ.get('REDIS_HOST'),
                                 port=int(os.environ.get('REDIS_PORT')),
                                 db=0)

boto_config = Config(connect_timeout=1000, read_timeout=1000)
RAW_MIMES = ['image/x-canon-cr2']

class Backup:
    pass

def main(directory):
    glacier = boto3.resource('glacier', config=boto_config)
    dyno = boto3.resource('dynamodb')
    backup_table = dyno.Table(os.environ.get('DYNAMODB_TABLE'))

    source = Path(directory)
    if not source.is_dir():
        exit('not valid dir')

    vault = glacier.Vault(
        account_id=os.environ.get('AWS_ACCOUNT_ID'),
        name=os.environ.get('AWS_VAULT_NAME'))

    mime = magic.Magic(mime=True)

    for each_file in source.iterdir():
        file_path = source.joinpath(each_file)
        mime_type = mime.from_file(str(file_path))

        # if not mime_type.startswith('image'):
        if mime_type not in RAW_MIMES and 'raw' not in mime_type:
            print('Skipping non-raw file ', each_file)
            continue

        print('Processing file ', each_file)

        with file_path.open('rb') as f:
            file_content = f.read()
            file_checksum = hashlib.sha512(file_content).hexdigest()
            redis_key = 'backuprawphoto_{}_{}'.format(file_checksum, str(file_path))

        redis_res = redis_client.get(redis_key)
        if redis_res:
            print('File already backed up ', file_path)
            continue

        archive = vault.upload_archive(
            archiveDescription=str(file_path),
            body=file_content
        )

        exif = pyexifinfo.get_json(str(file_path))[0]

        if archive.id:
            iso = int(exif['EXIF:ISO']) if 'EXIF:ISO' in exif else 0
            width = int(exif['EXIF:ImageWidth']) if 'EXIF:ImageWidth' in exif else 0
            flash = True if 'EXIF:Flash' in exif['EXIF:Flash'] and 'on' in exif['EXIF:Flash'].lower() else False
            camera_serial = str(exif['MakerNotes:SerialNumber']) if 'MakerNotes:SerialNumber' in exif else 'unknown'
            aperture = str(exif['EXIF:ApertureValue']) if 'EXIF:ApertureValue' in exif else 'unknown'
            focal_length = str(exif['EXIF:FocalLength']) if 'EXIF:FocalLength' in exif else 'unknown'
            camera_firmware = str(exif['MakerNotes:FirmwareVersion']) if 'MakerNotes:FirmwareVersion' in exif else 'unknown'
            shooting_mode = str(exif['Composite:ShootingMode']) if 'Composite:ShootingMode' in exif else 'unknown'
            max_focal_length = str(exif['MakerNotes:MaxFocalLength']) if 'MakerNotes:MaxFocalLength' in exif else 'unknown'
            lens_type = str(exif['MakerNotes:LensType']) if 'MakerNotes:LensType' in exif else 'unkown'
            height = int(exif['MakerNotes:OriginalImageHeight']) if 'MakerNotes:OriginalImageHeight' in exif else 0
            shutter_speed = str(exif['Composite:ShutterSpeed']) if 'Composite:ShutterSpeed' in exif else 'unknown'
            white_balance = str(exif['EXIF:WhiteBalance']) if 'EXIF:WhiteBalance' in exif else 'unknown'
            megapixels = str(exif['Composite:Megapixels']) if 'Composite:Megapixels' in exif else 0
            created_datetime = str(exif['EXIF:CreateDate']) if 'EXIF:CreateDate' in exif else 'unknown'
            quality = str(exif['MakerNotes:Quality']) if 'MakerNotes:Quality' in exif else 'unknown'
            file_type = str(exif['File:FileType']) if 'File:FileType' in exif else 'unknown'
            continuous_drive = str(exif['MakerNotes:ContinuousDrive']) if 'MakerNotes:ContinuousDrive' in exif else 'unknown'
            file_size = str(exif['File:FileSize']) if 'File:FileSize' in exif else 'unknown'
            orientation = str(exif['EXIF:Orientation']) if 'EXIF:Orientation' in exif else 'unknown'
            camera_manufacture = str(exif['EXIF:Make']) if 'EXIF:Make' in exif else 'unknown'
            shutter_speed = str(exif['EXIF:ShutterSpeedValue']) if 'EXIF:ShutterSpeedValue' in exif else 'unknown'
            self_timer = True if 'MakerNotes:SelfTimer' in exif and 'on' in exif['MakerNotes:SelfTimer'].lower() else False

            res = backup_table.put_item(
                Item={
                    'file_checksum': file_checksum,
                    'file_path': str(file_path),
                    'file_name': str(each_file),
                    'backup_datetime': datetime.today().isoformat(),
                    'archive_id': archive.id,
                    'vault_name': vault.name,
                    'exif': json.dumps(exif),
                    'iso': iso,
                    'width': width,
                    'flash': flash,
                    'camera_serial': camera_serial,
                    'aperture': aperture,
                    'focal_length': focal_length,
                    'camera_firmware': camera_firmware,
                    'mime': mime_type,
                    'shooting_mode': shooting_mode,
                    'max_focal_length': max_focal_length,
                    'lens_type': lens_type,
                    'height': height,
                    'shutter_speed': shutter_speed,
                    'white_balance': white_balance,
                    'megapixels': megapixels,
                    'created_datetime': created_datetime,
                    'quality': quality,
                    'file_type': file_type,
                    'continuous_drive': continuous_drive,
                    'file_size': file_size,
                    'orientation': orientation,
                    'camera_manufacture': camera_manufacture,
                    'shutter_speed': shutter_speed,
                    'self_timer': self_timer
                },
            )

            if 'ResponseMetadata' in res and 'RequestId' in res['ResponseMetadata']:
                redis_res = redis_client.set(redis_key, True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("directory")
    args = parser.parse_args()
    main(directory=args.directory)

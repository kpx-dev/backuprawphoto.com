import boto3
import os
import json
import pyexifinfo
import magic
import hashlib
from datetime import datetime
from pathlib import Path
from botocore.client import Config
from decimal import Decimal
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
boto_config = Config(connect_timeout=3600, read_timeout=3600,
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

        res = backup_table.get_item(
            Key={
                'file_checksum': md5_checksum,
                'file_path': str(file_path)
            },
            AttributesToGet=['file_checksum']
        )
        if 'Item' in res:
            print('File already backed up ', file_path)
            continue

        archive = vault.upload_archive(
            archiveDescription=str(file_path),
            body=file_content
        )

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
                    'file_checksum': md5_checksum,
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
            print(archive.id)

if __name__ == '__main__':
    main()

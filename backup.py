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
from botocore.exceptions import ClientError
from time import sleep
from zenlog import log

load_dotenv(find_dotenv())
redis_client = redis.StrictRedis(host=os.environ.get('REDIS_HOST'), port=int(os.environ.get('REDIS_PORT')), db=0)
boto_config = Config(connect_timeout=120, read_timeout=120)


class Backup:
    def __init__(self, file_path):
        self.file_path = file_path
        self.RAW_MIMES = ['image/x-canon-cr2']

        self.backup_table = boto3.resource('dynamodb').Table(os.environ.get('DYNAMODB_TABLE'))
        self.vault = boto3.resource('glacier', config=boto_config).Vault(account_id=os.environ.get('AWS_ACCOUNT_ID'), name=os.environ.get('AWS_VAULT_NAME'))

        self.mime = magic.Magic(mime=True)
        self.redis_prefix = 'backuprawphoto'


    def get_file_exif(self):
        exif = pyexifinfo.get_json(str(self.file_path))[0]
        unknown = ' '
        payload = {}

        try:
            payload = {
                'iso': int(exif['EXIF:ISO']) if 'EXIF:ISO' in exif else 0,
                'width': int(exif['EXIF:ImageWidth']) if 'EXIF:ImageWidth' in exif else 0,
                'flash': True if 'EXIF:Flash' in exif['EXIF:Flash'] and 'on' in exif['EXIF:Flash'].lower() else False,
                'camera_serial': str(exif['MakerNotes:SerialNumber']) if 'MakerNotes:SerialNumber' in exif else unknown,
                'aperture': str(exif['EXIF:ApertureValue']) if 'EXIF:ApertureValue' in exif else unknown,
                'focal_length': str(exif['EXIF:FocalLength']) if 'EXIF:FocalLength' in exif else unknown,
                'camera_firmware': str(exif['MakerNotes:FirmwareVersion']) if 'MakerNotes:FirmwareVersion' in exif else unknown,
                'shooting_mode': str(exif['Composite:ShootingMode']) if 'Composite:ShootingMode' in exif else unknown,
                'max_focal_length': str(exif['MakerNotes:MaxFocalLength']) if 'MakerNotes:MaxFocalLength' in exif else unknown,
                'lens_type': str(exif['MakerNotes:LensType']) if 'MakerNotes:LensType' in exif else unknown,
                'height': int(exif['MakerNotes:OriginalImageHeight']) if 'MakerNotes:OriginalImageHeight' in exif else 0,
                'shutter_speed': str(exif['Composite:ShutterSpeed']) if 'Composite:ShutterSpeed' in exif else unknown,
                'white_balance': str(exif['EXIF:WhiteBalance']) if 'EXIF:WhiteBalance' in exif else unknown,
                'megapixels': str(exif['Composite:Megapixels']) if 'Composite:Megapixels' in exif else 0,
                'created_datetime': str(exif['EXIF:CreateDate']) if 'EXIF:CreateDate' in exif else unknown,
                'quality': str(exif['MakerNotes:Quality']) if 'MakerNotes:Quality' in exif else unknown,
                'file_type': str(exif['File:FileType']) if 'File:FileType' in exif else unknown,
                'continuous_drive': str(exif['MakerNotes:ContinuousDrive']) if 'MakerNotes:ContinuousDrive' in exif else unknown,
                'file_size': str(exif['File:FileSize']) if 'File:FileSize' in exif else unknown,
                'orientation': str(exif['EXIF:Orientation']) if 'EXIF:Orientation' in exif else unknown,
                'camera_manufacture': str(exif['EXIF:Make']) if 'EXIF:Make' in exif else unknown,
                'shutter_speed': str(exif['EXIF:ShutterSpeedValue']) if 'EXIF:ShutterSpeedValue' in exif else unknown,
                'self_timer': True if 'MakerNotes:SelfTimer' in exif and 'on' in exif['MakerNotes:SelfTimer'].lower() else False
            }
        except Exception as e:
            log.error(e)

        return payload

    def get_file_stats(self):
        file_stats = {}
        try:
            stats = self.file_path.stat()
            file_stats['st_mode'] = stats.st_mode
            file_stats['st_ino'] = stats.st_ino
            file_stats['st_dev'] = stats.st_dev
            file_stats['st_nlink'] = stats.st_nlink
            file_stats['st_uid'] = stats.st_uid
            file_stats['st_gid'] = stats.st_gid
            file_stats['st_size'] = stats.st_size
            file_stats['st_atime'] = stats.st_atime
            file_stats['st_mtime'] = stats.st_mtime
        except Exception as e:
            log.error(e)

        return file_stats

    def get_file_checksum(self):
        payload = {}
        with self.file_path.open('rb') as f:
            file_content = f.read()
            file_checksum = hashlib.sha512(file_content).hexdigest()

            payload = {
                'content': file_content,
                'checksum': file_checksum
            }
        return payload

    def upload_archive(self, content, description):
        try:
            archive = self.vault.upload_archive(archiveDescription=description, body=content)

            return archive
        except ClientError as e:
            log.error(e)
            # if e.response['Error']['Code'] == 'RequestTimeoutException':
                # log.warn('Failed to retry, giving up this file: {}'.format(file_path))
                # log.warn('Timed out, sleeping for {} seconds'.format(sleep_time))
                # sleep(sleep_time)
                # sleep_time += 1

    def backup(self):
        log.info('Backup file: {}'.format(self.file_path))

        mime_type = self.mime.from_file(str(self.file_path))
        if mime_type not in self.RAW_MIMES and 'raw' not in mime_type:
            log.info('Skipping non-raw file {}'.format(self.file_path))
            return

        file_checksum = self.get_file_checksum()
        redis_key = '{}|{}|{}'.format(self.redis_prefix, file_checksum['checksum'], self.file_path)

        redis_res = redis_client.get(redis_key)
        if redis_res:
            log.info('File already backed up {}'.format(self.file_path))
            return

        archive = self.upload_archive(content=file_checksum['content'], description=str(file_path))

        if archive.id:
            file_exif = self.get_file_exif()
            file_stats = self.get_file_stats()
            base_item = {
                'file_path': '{}_{}'.format(vault_name, str(self.file_path)),
                'created_at': datetime.today().isoformat(),
                'file_checksum': file_checksum['checksum'],
                'file_name': str(self.file_path),
                'archive_id': archive.id,
                'vault_name': vault.name,
                'exif': json.dumps(file_exif)
            }
            item = {**base_item, **file_exif}
            res = backup_table.put_item(Item=item)

            if 'ResponseMetadata' in res and 'RequestId' in res['ResponseMetadata']:
                redis_res = redis_client.set(redis_key, True)

def handler(context, event):
    backup = Backup(file_path=event['file_path'])
    backup.backup()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("directory")
    args = parser.parse_args()

    source_dir = Path(args.directory)
    if not source_dir.is_dir():
        raise ValueError('{} is not a valid directory'.format(source_dir))

    event = {}
    for file_path in source_dir.iterdir():
        log.info('Sending file to handler: {} --------------------------------------------------'.format(file_path))
        event['file_path'] = file_path.resolve()
        handler(context={}, event=event)

    log.info('Done!')
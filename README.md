# backuprawphoto.com
Backup your raw photos to AWS Glacier.

It costs less than $5 / month to store 1TB photos (https://calculator.s3.amazonaws.com/index.html)

# Installation

    pip install -r requirements.txt

# Usage
Rename the `env-sample` file to `.env` and update its keys with your info.

    python backup.py ~/Pictures/Wedding1

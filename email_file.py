
import os
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
import zipfile

# Write all .eml files directly into a zip archive
zip_path = "emails_spatil.zip"
with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
    for i in range(0, 1000000):
        msg = EmailMessage()
        msg['Subject'] = f"Helloooo have a good day:) {i}"
        msg['From'] = "s@patil.com"
        msg['To'] = "a@w.com"
        yesterday_dt = datetime.now(timezone.utc) - timedelta(days=1)
        msg['Date'] = format_datetime(yesterday_dt)
        msg.set_content(f"Hello,\n\nThis is the content of dummy email number {i}")

        eml_filename = f"email_{i}.eml"
        zipf.writestr(eml_filename, bytes(msg))

print("Done!!!!!!")
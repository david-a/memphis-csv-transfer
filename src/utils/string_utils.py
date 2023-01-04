import hashlib
from datetime import datetime

def generate_hash(length=6):
    return hashlib.md5(str(datetime.now()).encode('utf-8')).hexdigest()[:length]
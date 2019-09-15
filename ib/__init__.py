from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

_gauth = GoogleAuth()
_gauth.CommandLineAuth()

drive = GoogleDrive(_gauth)
stock_index = {e['title']: e for e in drive.ListFile(
    {'q': "'1-7-Qgh_apBna6mO9-OFFQsebtOdAGN1z' in parents and trashed=false"}).GetList()}

from dotenv import load_dotenv
import os

load_dotenv()

DATA_DIR = os.getenv("DATA_DIR")
FILE_PATH=os.getenv("FILE_PATH")                             

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "database": os.getenv("DB_NAME")
}

base_url="https://worldpostalcode.com/"
countries_table ="Countries_Urls"
regions_urls_table="regions1"
sub_regions_table = "sub_regions"
sub_sub_regions_table = "sub_sub_regions"
pincodes_tab="pincodes_table"
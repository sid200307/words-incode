from validation import Store
import requests
from lxml import html
from urllib.parse import urljoin
from db import fetch_urls,update_q
from config import countries_table,regions_urls_table,sub_regions_table,sub_sub_regions_table


session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

# getiing urls for countries table
def get_countries_urls(url: str):
    try:
        county=[]
        res = session.get(url, timeout=10)
        tree=html.fromstring(res.text)
        for country in tree.xpath('//div[@class="regions"]//a'):
         name=country.xpath('string(./text())').strip()
         with open(f"Country_pages/{name}.html.gz","w") as f:
            f.write(res.text)
         temp={
              "Country":name,
              "Url":"https://worldpostalcode.com"+country.xpath('string(./@href)'),
              "Status":"pending"
         }     
         county.append(temp)
        
    except Exception as e:
        print("Error Happened", e)
    print(county)   
    return county 


#regions
def regions_urls(fetch_table, fetch_col, url_col, output_col):
    data = []
    try:
        for name, url in fetch_urls(fetch_table, fetch_col, url_col):

            # only process URLs ending with "/"
            if not url.endswith("/"):
                continue

            res = session.get(url, timeout=10)
            tree = html.fromstring(res.text)
            regions = tree.xpath('//div[@class="regions"]//a')

            if not regions:
                data.append({
                    output_col: name,   # ✅ FIXED
                    "Url": url,
                    "Status": "pending"
                })
            else:
                for region in regions:
                    region_name = region.text_content().strip().replace("→", "-")
                    region_href = region.get("href")
                    region_url = urljoin(url, region_href)

                    data.append({
                        output_col: region_name,   # 
                        "Url": region_url,
                        "Status": "pending"
                    })

            # update status in source table
            update_q(fetch_table, url_col, url)

    except Exception as e:
        print("Error", e)

    print(f"Rows found: {len(data)}")
    return data

#parser to get 
def parser(url:str):
    try:
        res = session.get(url, timeout=10)
        tree = html.fromstring(res.text)
        data = []
        for pin in tree.xpath('//div[@class="container"]'):
            data.append({
                "Region": pin.xpath('string(./div[@class="place"]/text())'),
                "Pincode": pin.xpath('string(.//span/text())')
            })
        return data
    except Exception as e:
        print(f"Error parsing {url}: {e}")
        return []

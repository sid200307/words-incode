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
def regions_urls():
    data = []
    try:
        for country, url in fetch_urls(countries_table, "Country", "Url"):
            res = session.get(url, timeout=10)
            tree = html.fromstring(res.text)
            regions = tree.xpath('//div[@class="regions"]//a')

            # no sub-regions found — leaf page, store country url as is
            if not regions:
                data.append({
                    "Region": country,
                    "Url": url,
                    "Status": "pending"
                })
            else:
                # sub-regions found — store each region with full url
                for region in regions:
                    region_name = region.text_content().strip()
                    region_href = region.get("href")
                    region_url = urljoin(url, region_href)
                    data.append({
                        "Region": region_name,
                        "Url": region_url,
                        "Status": "pending"
                    })

            # mark country as responded after processing
            update_q(countries_table, "Url", url)

    except Exception as e:
        print("Error", e)

    print(f"Regions found: {len(data)}")
    return data

def sub_regions_urls():
    data = []
    try:
        for region, url in fetch_urls(regions_urls_table, "Region", "Url"):
            res = session.get(url, timeout=10)
            tree = html.fromstring(res.text)
            sub_regions = tree.xpath('//div[@class="regions"]//a')

            if not sub_regions:
                data.append({
                    "Sub_Region": region,
                    "Url": url,
                    "Status": "pending"
                })
            else:
                for sub_region in sub_regions:
                    sub_region_name = sub_region.text_content().strip()
                    sub_region_href = sub_region.get("href")
                    sub_region_url = urljoin(url, sub_region_href)
                    data.append({
                        "Sub_Region": sub_region_name,
                        "Url": sub_region_url,
                        "Status": "pending"
                    })

            update_q(regions_urls_table, "Url", url)

    except Exception as e:
        print("Error", e)

    print(f"Sub Regions found: {len(data)}")
    return data


def sub_sub_regions_urls():
    data = []
    try:
        for sub_region, url in fetch_urls(sub_regions_table, "Sub_Region", "Url"):

            # if url doesn't end with / — it's a leaf page, no need to request
            if not url.endswith("/"):
                data.append({
                    "Sub_Sub_Region": sub_region,
                    "Url": url,
                    "Status": "pending"
                })
                update_q(sub_regions_table, "Url", url)
                continue

            # url ends with / — may have sub regions, make request
            res = session.get(url, timeout=10)
            tree = html.fromstring(res.text)
            sub_sub_regions = tree.xpath('//div[@class="regions"]//a')

            if not sub_sub_regions:
                data.append({
                    "Sub_Sub_Region": sub_region,
                    "Url": url,
                    "Status": "pending"
                })
            else:
                for sub_sub_region in sub_sub_regions:
                    sub_sub_region_name = sub_sub_region.text_content().strip()
                    sub_sub_region_href = sub_sub_region.get("href")
                    sub_sub_region_url = urljoin(url, sub_sub_region_href)
                    data.append({
                        "Sub_Sub_Region": sub_sub_region_name,
                        "Url": sub_sub_region_url,
                        "Status": "pending"
                    })

            update_q(sub_regions_table, "Url", url)

    except Exception as e:
        print("Error", e)

    print(f"Sub Sub Regions found: {len(data)}")
    return data

def parser(url):
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
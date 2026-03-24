from concurrent.futures import ThreadPoolExecutor , as_completed

from config import *
from utils import read_json
from parser import *
from db import *
import time



        

def main():
    st=time.time()
   
    con = make_connection()
    cursor = con.cursor()
    # create_table(cursor,countries_table)

       # get all country urls from base url
    # country_urls=get_countries_urls(base_url)
  
    # insert_into_db(cursor, con, country_urls,countries_table)
    
    # create_table(cursor,regions_urls_table,regions_urls_table)
    # with ThreadPoolExecutor(max_workers=15) as executor:
    #     future = executor.submit(regions_urls)
    #     reg_urls = future.result()
    # insert_into_db(cursor,con,reg_urls,regions_urls_table)

    #sub regions

    create_table(cursor,regions_urls_table,regions_urls_table)
    # with ThreadPoolExecutor(max_workers=15) as executor:
    #     future = executor.submit(sub_regions_urls)
    #     sub_reg_urls = future.result()
    # insert_into_db(cursor,con,sub_reg_urls,sub_regions_table)

    #Sub SUb Regions
    
    # with ThreadPoolExecutor(max_workers=20) as executor:
    #     future = executor.submit(sub_sub_regions_urls)
    #     sub_reg_urls = future.result()
    # insert_into_db(cursor,con,sub_reg_urls,sub_sub_regions_table)

    all_data = []
    urls = fetch_urls(sub_sub_regions_table, "Sub_Sub_Region", "Url")

    with ThreadPoolExecutor(max_workers=80) as tpe:  # 50 threads for 42k urls
        futures = {tpe.submit(parser, url): (region, url)
                   for region, url in urls}
        for future in as_completed(futures):
            region, url = futures[future]
            try:
                result = future.result()
                if result:
                    all_data.extend(result)
                    update_q(sub_sub_regions_table, "Url", url)
            except Exception as e:
                print(f"Error {url}: {e}")

        # insert in batches as results come in
        if len(all_data) >= 100:
            insert_into_db(cursor, con, all_data, pincodes_tab)
            all_data.clear()

    # insert remaining
    if all_data:
        insert_into_db(cursor, con, all_data, pincodes_tab)
    #commiting db changes
    con.commit()

    #closing db connetions
    cursor.close()
    con.close()
    print(time.time()-st)


if __name__ == "__main__":
    main()

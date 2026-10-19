import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ======================================
# CONFIGURATION
# ======================================
OUTPUT_FILE = "3newtimken_data.xlsx"

# Scrapes 15 pages simultaneously! 
CONCURRENCY = 5 

urls = [
"https://cad.timken.com/item/seals/oil-seals-inch/450523",
"https://cad.timken.com/item/seals/oil-seals-inch/40140s",
"https://cad.timken.com/item/seals/oil-seals-inch/450474",
"https://cad.timken.com/item/seals/oil-seals-inch/8835s",
"https://cad.timken.com/item/seals/oil-seals-inch/450412",
"https://cad.timken.com/item/seals/oil-seals-inch/450593",
"https://cad.timken.com/item/seals/oil-seals-inch/40230S",
"https://cad.timken.com/item/seals/oil-seals-inch/450588",
"https://cad.timken.com/item/seals/oil-seals-inch/450776",
"https://cad.timken.com/item/seals/oil-seals-inch/450492",
"https://cad.timken.com/item/seals/oil-seals-inch/450400",
"https://cad.timken.com/item/seals/oil-seals-inch/450604",
"https://cad.timken.com/item/seals/oil-seals-inch/450472",
"https://cad.timken.com/item/seals/oil-seals-inch/450446",
"https://cad.timken.com/item/seals/oil-seals-inch/450639",
"https://cad.timken.com/item/seals/oil-seals-inch/416738",
"https://cad.timken.com/item/seals/oil-seals-inch/482310",
"https://cad.timken.com/item/seals/oil-seals-inch/450911n",
"https://cad.timken.com/item/seals/oil-seals-inch/412725",
"https://cad.timken.com/item/seals/oil-seals-inch/415001",
"https://cad.timken.com/item/seals/oil-seals-inch/415041",
"https://cad.timken.com/item/seals/oil-seals-inch/455536",
"https://cad.timken.com/item/seals/oil-seals-inch/442109",
"https://cad.timken.com/item/seals/oil-seals-inch/450765",
"https://cad.timken.com/item/seals/oil-seals-inch/471276",
"https://cad.timken.com/item/seals/oil-seals-inch/415150",
"https://cad.timken.com/item/seals/oil-seals-inch/470390",
"https://cad.timken.com/item/seals/oil-seals-inch/416380",
"https://cad.timken.com/item/seals/oil-seals-inch/471137",
"https://cad.timken.com/item/seals/oil-seals-inch/456105",
"https://cad.timken.com/item/seals/oil-seals-inch/471344",
"https://cad.timken.com/item/seals/oil-seals-inch/471529",
"https://cad.timken.com/item/seals/oil-seals-inch/471684",
"https://cad.timken.com/item/seals/oil-seals-inch/471648",
"https://cad.timken.com/item/seals/oil-seals-inch/471651",
"https://cad.timken.com/item/seals/oil-seals-inch/471730",
"https://cad.timken.com/item/seals/oil-seals-inch/343099",
"https://cad.timken.com/item/seals/oil-seals-inch/340849",
"https://cad.timken.com/item/seals/oil-seals-inch/343101",
"https://cad.timken.com/item/seals/oil-seals-inch/343104",
"https://cad.timken.com/item/seals/oil-seals-inch/342517",
"https://cad.timken.com/item/seals/oil-seals-inch/471281",
"https://cad.timken.com/item/seals/oil-seals-inch/343185",
"https://cad.timken.com/item/seals/oil-seals-inch/342805",
"https://cad.timken.com/item/seals/oil-seals-inch/411253",
"https://cad.timken.com/item/seals/oil-seals-inch/411245",
"https://cad.timken.com/item/seals/oil-seals-inch/410524",
"https://cad.timken.com/item/seals/oil-seals-inch/410825",
"https://cad.timken.com/item/seals/oil-seals-inch/470989",
"https://cad.timken.com/item/seals/oil-seals-inch/410694",
"https://cad.timken.com/item/seals/oil-seals-inch/352541",
"https://cad.timken.com/item/seals/oil-seals-inch/417223",
"https://cad.timken.com/item/seals/oil-seals-inch/417258",
"https://cad.timken.com/item/seals/oil-seals-inch/411376",
"https://cad.timken.com/item/seals/oil-seals-inch/412943",
"https://cad.timken.com/item/seals/oil-seals-inch/416557",
"https://cad.timken.com/item/seals/oil-seals-inch/417252",
"https://cad.timken.com/item/seals/oil-seals-inch/417238",
"https://cad.timken.com/item/seals/oil-seals-inch/417480",
"https://cad.timken.com/item/seals/oil-seals-inch/417481",
"https://cad.timken.com/item/seals/oil-seals-inch/417273",
"https://cad.timken.com/item/seals/oil-seals-inch/417298",
"https://cad.timken.com/item/seals/oil-seals-inch/417491",
"https://cad.timken.com/item/seals/oil-seals-inch/417485",
"https://cad.timken.com/item/seals/oil-seals-inch/471354",
"https://cad.timken.com/item/seals/oil-seals-inch/417486",
"https://cad.timken.com/item/seals/oil-seals-inch/417488",
"https://cad.timken.com/item/seals/oil-seals-inch/417474",
"https://cad.timken.com/item/seals/oil-seals-inch/471504",
"https://cad.timken.com/item/seals/oil-seals-inch/471141",
"https://cad.timken.com/item/seals/oil-seals-inch/417501",
"https://cad.timken.com/item/seals/oil-seals-inch/417492",
"https://cad.timken.com/item/seals/oil-seals-inch/471407",
"https://cad.timken.com/item/seals/oil-seals-inch/471391",
"https://cad.timken.com/item/seals/oil-seals-inch/471535",
"https://cad.timken.com/item/seals/oil-seals-inch/471551",
"https://cad.timken.com/item/seals/oil-seals-inch/471579",
"https://cad.timken.com/item/seals/oil-seals-inch/471558",


    # Paste your remaining Timken URLs here...
]
# ======================================

def scrape_timken(url):
    row = {"URL": url, "Title": ""}
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # timeout=15 ensures it never gets stuck on a bad connection
            resp = requests.get(url, headers=headers, timeout=15)
            
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # 1. Extract Title
                title_tag = soup.find('h1')
                if title_tag:
                    row["Title"] = title_tag.text.strip()
                
                # 2. Extract Image
                img_tag = soup.find('img', id='plp-item-img')
                if img_tag and img_tag.get('src'):
                    src = img_tag.get('src')
                    # Make relative URLs absolute
                    row["Image_URL"] = f"https://cad.timken.com{src}" if src.startswith('/') else src
                
                # 3. Extract Specs from Table
                trs = soup.find_all('tr', itemprop='additionalProperty')
                for tr in trs:
                    name_td = tr.find('td', class_='plp-table-name')
                    val_td = tr.find('td', class_='plp-table-value')
                    
                    if name_td and val_td:
                        key = name_td.text.strip()
                        
                        # Many specs have two values (Inches and MM). This grabs both!
                        val_spans = val_td.find_all('span', itemprop='value')
                        if val_spans:
                            # Join multiple values with a slash (e.g., "3.625 in / 92.075 mm")
                            val = " / ".join([s.text.strip() for s in val_spans if s.text.strip()])
                        else:
                            # Fallback to plain text, but delete the hidden 'N/A' span first
                            na_span = val_td.find('span', {'data-measure': 'na'})
                            if na_span:
                                na_span.decompose()
                            val = val_td.text.strip()
                            
                        if key:
                            row[key] = val
                
                safe_title = str(row['Title'][:35]).encode('ascii', 'ignore').decode('ascii')
                print(f"[SUCCESS] {safe_title}... | Specs Found: {len(row)-2}")
                return row
                
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"[FAILED] {url.split('/')[-1]}: {str(e)[:50]}")
                row["Title"] = "FAILED"
                return row
        
        # Short delay before retrying a failed URL
        time.sleep(1)
        
    return row

def main():
    print(f" Starting ULTRA-FAST Timken Scraper for {len(urls)} URLs...")
    all_results = []
    
    # Run requests concurrently for maximum speed
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        future_to_url = {executor.submit(scrape_timken, url): url for url in urls}
        for future in as_completed(future_to_url):
            all_results.append(future.result())
            
    # Convert to Excel and sort dynamically found specs alphabetically
    df = pd.DataFrame(all_results)
    
    priority_cols = ["URL", "Title", "Image_URL"]
    existing_priority = [c for c in priority_cols if c in df.columns]
    dynamic_cols = [c for c in df.columns if c not in priority_cols]
    
    df = df[existing_priority + sorted(dynamic_cols)]
    df.to_excel(OUTPUT_FILE, index=False)
    
    print(f"\n ALL COMPLETE! Saved {len(all_results)} records to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
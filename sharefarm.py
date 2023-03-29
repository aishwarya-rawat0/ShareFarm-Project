
#importing necessary libraries
import psycopg2
from psycopg2 import sql
import requests
from bs4 import BeautifulSoup
import pandas as pd
from numpy import nan
import lxml



# This code extracts data from a given URL, cleans and transforms it to a SQL-compatible format,

def Extract_Data(URL):
    page = requests.get(URL)
    base_url = "https://www.marketnews.usda.gov/mnp/fv-report-top-filters"

    soup = BeautifulSoup(page.content, "html.parser")

    for element in soup.find_all("a"):
        if "Excel" in element.text:
            excel_element = element['href']

    page = requests.get(f"{base_url}{excel_element}")
    table = pd.read_html(page.text)
    return table[0]

def Clean_Transform_To_Sql_Format(df):
    df['Low Price'] = df['Low Price'].astype(str)
    df['High Price'] = df['High Price'].astype(str)
    df["low-highprice"] = df[["Low Price", "High Price"]].apply("-".join, axis=1)
    new_df = df[["City Name", "Package", "Date", "low-highprice", "Origin", "Commodity Name"]]
    new_df = new_df.rename(columns={"City Name": "Location", "Commodity Name": "Commodity"})
    return new_df



# Connect to PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="shareFarm",
    user="postgres",
    password="Password"
)

cur = conn.cursor()
table_name = "products.data"

input_data = {
    "onion_potatoes": "https://www.marketnews.usda.gov/mnp/fv-nav-byCom?navClass=ONPO&navType=byComm",
    "Vegetables": "https://www.marketnews.usda.gov/mnp/fv-nav-byCom?navClass=VEGETABLES&navType=byComm",
    "Fruits": "https://www.marketnews.usda.gov/mnp/fv-nav-byCom?navClass=FRUITS&navType=byComm",
    "herbs": "https://www.marketnews.usda.gov/mnp/fv-nav-byCom?navClass=HERBS&navType=byComm",
    "nuts": "https://www.marketnews.usda.gov/mnp/fv-nav-byCom?navClass=NUTS&navType=byComm",
    "ornamentals": "https://www.marketnews.usda.gov/mnp/fv-nav-byCom?navClass=ORNAMENTALS&navType=byComm"
  
}

base_url="https://www.marketnews.usda.gov/mnp/"
links_to_scrap = {}

for key, value in input_data.items():
    page = requests.get(value)
    soup = BeautifulSoup(page.content, "html.parser")
    links_to_scrap[key] = []
    
    # Loop over all 'a' elements in the parsed HTML content
    main_content_div = soup.find("div", {"class": "rptNavList"})
    if main_content_div:
        for link in main_content_div.find_all("a", href=True):
            if "fv-nav-report?" in link["href"]:
                links_to_scrap[key].append(link["href"])
    else:
        print("Main content div not found for key:", key)
#printing the urls
href=[]

for value in links_to_scrap.values():
    for item in value:
        href.append(item)

for index,item in enumerate(href):
    item = item.replace(" ","")
 # Combine the base URL and the 'href' attribute to form the complete URL of the report.    
    url=base_url+item
#     print(url)
    
    updated_url = url.replace("mnp/fv-nav-report?", "mnp/fv-report-top-filters?") + "&repType=termPriceDaily&type=termPrice"
    print(updated_url)
  #check the dataframe and continue  
    try:
        data = Extract_Data(updated_url)
        dataframe = Clean_Transform_To_Sql_Format(data)
    except:
        print("No dataframe found...")
        continue

    columns = list(dataframe.columns)
    fields = []
    for field in columns:
        field = field.replace("-", "_").lower()
        fields.append(field)
#     print(fields)

    for index, row in dataframe.iterrows():
        values = tuple(row)
        print("\nValues:", values)
        try:
            query = """INSERT INTO """ + table_name + """(""" + str(fields[0]) + "," + str(fields[1]) + "," + str(fields[2]) + "," + str(fields[3]) + "," + str(fields[4]) + "," + str(fields[5]) + """) VALUES""" + str(values).replace(",  nan,", ", 'nan',") + """;"""
            if list(values)[3] == 'nan-nan':
                print("Nan values occured...")
            else:
                print('Complete values...')
                try:
                    cur.execute(query)
                except:
                    cur.execute("ROLLBACK")
                    cur.execute(query)
                conn.commit()
        except:
            print("Skipping the URL...")
            pass
        
cur.close()
conn.close()







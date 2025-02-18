import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import subprocess
import shutil
import tarfile
import xml.etree.ElementTree as ET
import re
import gzip
import geopandas as gpd
from shapely.wkt import loads

def create_folder(folder_name):
    """建立資料夾"""
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return os.path.abspath(folder_name)

def delete_folders(deletelist):
    """
    刪除資料夾
    deletelist(list):需要為皆為路徑的list
    """
    for folder_name in deletelist: 
        if os.path.exists(folder_name): # 檢查資料夾是否存在
            shutil.rmtree(folder_name) # 刪除資料夾及其內容
        else:
            print(f"資料夾 '{folder_name}' 不存在。")

def getdatelist(time1, time2):
    '''
    建立日期清單
    time1、time2(str):為%Y-%M-%D格式的日期字串
    '''
    if time1 > time2:
        starttime = time2
        endtime = time1
    else:
        starttime = time1
        endtime = time2

    date_range = pd.date_range(start=starttime, end=endtime)
    datelist = [d.strftime("%Y%m%d") for d in date_range]
    return datelist

def read_xml(xml_file_path):
    """
    讀取並解析 XML 檔案。

    Args:
        xml_file_path (str): XML 檔案路徑。

    Returns:
        ElementTree.Element: XML 文件的根節點。
        None: 如果解析失敗。
    """
    # try:
    #     tree = ET.parse(xml_file_path)
    #     root = tree.getroot()
    #     return root
    try:
        with open(xml_file_path, 'r', encoding='utf-8') as f:  # 指定編碼
            xml_content = f.read()
        return xml_content
    except FileNotFoundError:
        print(f"檔案未找到：{xml_file_path}")
        return None
    except ET.ParseError as e:
        print(f"解析 XML 檔案時發生錯誤：{e}")
        return None
    
def findfiles(filefolderpath, filetype='.csv'):
    """
    尋找指定路徑下指定類型的檔案，並返回檔案路徑列表。

    Args:
        filefolderpath (str): 指定的檔案路徑。
        filetype (str, optional): 要尋找的檔案類型，預設為 '.csv'。

    Returns:
        list: 包含所有符合條件的檔案路徑的列表。
    """

    filelist = []  # 建立一個空列表來儲存檔案路徑

    # 使用 os.walk 遍歷資料夾及其子資料夾
    for root, _, files in os.walk(filefolderpath):
        for file in files:
            if file.endswith(filetype):  # 檢查檔案是否以指定類型結尾
                file_path = os.path.join(root, file)  # 建立完整的檔案路徑
                filelist.append(file_path)  # 將檔案路徑添加到列表中

    return filelist

def extract_gz(gz_file, extract_path):
    try:
        # 確保目標資料夾存在
        os.makedirs(extract_path, exist_ok=True)
        # 解壓縮後的檔案路徑（移除 .gz 副檔名）
        output_file = os.path.join(extract_path, os.path.basename(gz_file).replace('.gz', ''))
        with gzip.open(gz_file, 'rb') as f_in:
            with open(output_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"解壓縮成功：{output_file}")
    except Exception as e:
        print(f"解壓縮 {gz_file} 失敗：{e}")

def downloadsection(datelist):
    date = datelist[0]
    downloadfolder = create_folder(os.path.join(os.getcwd(),'..', 'Input','靜態資料','xml','temp'))

    downloadurldict = {"SectionShape":f"https://tisvcloud.freeway.gov.tw/history/motc20/Section/{date}/SectionShape_0000.xml.gz", 
                    "SectionLink":f"https://tisvcloud.freeway.gov.tw/history/motc20/Section/{date}/SectionLink_0000.xml.gz",
                    "Section":f"https://tisvcloud.freeway.gov.tw/history/motc20/Section/{date}/Section_0000.xml.gz"}

    for key, downloadurl in downloadurldict.items():
        filename = os.path.basename(downloadurl)
        destfile = os.path.join(downloadfolder, filename)

        response = requests.get(downloadurl)
        if response.status_code == 200:
            with open(destfile, 'wb') as file:
                file.write(response.content)

        outputfolder = os.path.abspath(os.path.join(downloadfolder, '..'))

        extract_gz(destfile, outputfolder)

    delete_folders([downloadfolder])
    return outputfolder

def parse_section_shape(xmlcontent):
    # 解析 XML
    root = ET.fromstring(xmlcontent)
    ns = {'ptx': 'http://ptx.transportdata.tw/standard/schema/TIX'}

    # 提取 SectionShape 資料
    section_data = []
    for section in root.findall(".//ptx:SectionShape", ns):
        section_id = section.find("ptx:SectionID", ns).text
        geometry_wkt = section.find("ptx:Geometry", ns).text
        geometry = loads(geometry_wkt)  # 轉換為 Shapely LineString

        section_data.append({"SectionID": section_id, "geometry": geometry})

    # 建立 GeoDataFrame
    gdf = gpd.GeoDataFrame(section_data, geometry="geometry", crs="EPSG:4326")
    return gdf

def parse_section_link(xmlcontent):
    # 解析 XML
    namespace = {"ns": "http://ptx.transportdata.tw/standard/schema/TIX"}
    root = ET.fromstring(xmlcontent)

    # 取得所有 SectionLink
    data = []
    for section in root.find("ns:SectionLinks", namespace).findall("ns:SectionLink", namespace):
        section_id = section.find("ns:SectionID", namespace).text
        for link in section.find("ns:LinkIDs", namespace).findall("ns:LinkID", namespace):
            data.append({"SectionID": section_id, "LinkID": link.text})

    # 轉換為 DataFrame
    df = pd.DataFrame(data)
    return df

def parse_section(xmlcontent):
    # 解析 XML
    namespace = {"ns": "http://ptx.transportdata.tw/standard/schema/TIX"}
    root = ET.fromstring(xmlcontent)

    # 取得所有 Section
    data = []
    for section in root.find("ns:Sections", namespace).findall("ns:Section", namespace):
        data.append({
            "SectionID": section.find("ns:SectionID", namespace).text,
            "SubAuthorityCode": section.find("ns:SubAuthorityCode", namespace).text,
            "SectionName": section.find("ns:SectionName", namespace).text,
            "RoadID": section.find("ns:RoadID", namespace).text,
            "RoadName": section.find("ns:RoadName", namespace).text,
            "RoadClass": section.find("ns:RoadClass", namespace).text,
            "RoadDirection": section.find("ns:RoadDirection", namespace).text,
            "Start": section.find("ns:RoadSection/ns:Start", namespace).text,
            "End": section.find("ns:RoadSection/ns:End", namespace).text,
            "SectionLength": float(section.find("ns:SectionLength", namespace).text),
            "StartKM": section.find("ns:SectionMile/ns:StartKM", namespace).text,
            "EndKM": section.find("ns:SectionMile/ns:EndKM", namespace).text,
            "SpeedLimit": int(section.find("ns:SpeedLimit", namespace).text),
        })

    # 轉換為 DataFrame
    df = pd.DataFrame(data)
    return df

def get_section(datelist):
    sectionfolder = downloadsection(datelist = datelist)
    filelist = findfiles(sectionfolder, filetype='.xml')

    shpfilefolder = create_folder(os.path.join(sectionfolder, '..', 'shp'))
    dataframefolder = create_folder(os.path.join(sectionfolder,'..','Table'))

    for file in filelist:
        filename = os.path.basename(file)
        if filename == "SectionShape_0000.xml":
            SectionShape = read_xml(file)
            SectionShape = parse_section_shape(SectionShape)
            SectionShape.to_file(os.path.join(shpfilefolder, 'SectionShape.shp'))
        elif filename == 'SectionLink_0000.xml':
            SectionLink = read_xml(file)
            SectionLink = parse_section_link(SectionLink)
            SectionLink.to_excel(os.path.join(dataframefolder, 'SectionLink.xlsx'), index = False)
        elif filename == 'Section_0000.xml':
            Section = read_xml(file)
            Section = parse_section(Section)
            Section.to_excel(os.path.join(dataframefolder, 'Section.xlsx'), index = False)
    return SectionShape, SectionLink, Section

def download_etag(etagurl, etagdownloadpath):
    """
    下載指定網址的 XML 檔案到指定位置。

    Args:
        etagurl (str): 要下載的 XML 檔案網址。
        etagdownloadpath (str): 檔案下載後的儲存路徑（包含檔案名稱）。
    """

    try:
        response = requests.get(etagurl, stream=True)
        response.raise_for_status()  # 檢查 HTTP 狀態碼，如有錯誤則拋出異常

        with open(etagdownloadpath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

    except requests.exceptions.RequestException as e:
        print(f"下載時發生錯誤：{e}")
    except Exception as e:
        print(f"發生錯誤：{e}")

def etag_xml_to_dataframe(xml_content):
    """
    將 XML 內容轉換為 Pandas DataFrame。

    Args:
        xml_content (str): XML 內容字串。

    Returns:
        pandas.DataFrame: 轉換後的 DataFrame。
        None: 如果解析失敗。
    """
    try:
        root = ET.fromstring(xml_content)  # 從字串解析 XML

        data = []
        for etag in root.findall('.//{http://traffic.transportdata.tw/standard/traffic/schema/}ETag'):
            etag_data = {}
            for element in etag:
                tag_name = element.tag.split('}')[-1]  # 去除命名空間
                if tag_name == 'RoadSection':  # 處理 RoadSection
                    for section_element in element:
                        etag_data[section_element.tag] = section_element.text
                else:
                    etag_data[tag_name] = element.text
            data.append(etag_data)

        df = pd.DataFrame(data)
        df.columns = ['ETagGantryID','LinkID', 'LocationType', 'PositionLon', 'PositionLat', 'RoadID', 'RoadName', 'RoadClass', 'RoadDirection', 'Start','End', 'LocationMile']
        return df

    except ET.ParseError as e:
        print(f"解析 XML 內容時發生錯誤：{e}")
        return None
    except Exception as e:
        print(f"發生錯誤：{e}")
        return None

def etag_getdf():
    etagfolder = create_folder(os.path.join(os.getcwd(),'..','Input','靜態資料', 'xml'))
    etagurl = 'https://tisvcloud.freeway.gov.tw/history/motc20/ETag.xml'
    etagdownloadpath = os.path.join(etagfolder, 'ETag.xml')
    download_etag(etagurl=etagurl, etagdownloadpath=etagdownloadpath)
    etagxml = read_xml(etagdownloadpath)
    etag = etag_xml_to_dataframe(etagxml)
    etagtablefolder = create_folder(os.path.join(etagfolder, '..', 'Table'))
    etag.to_excel(os.path.join(etagtablefolder,'Etag.xlsx'), index = False, sheet_name='ETag')
    return etag

def convert_km_format(value):
    # 轉換函式：將 "290K+100" 轉為 290100
    if pd.isna(value):
        return 0  # 處理 NaN
    km, m = value.split("K+")
    return int(km) * 1000 + int(m)

def main():
    # SectionShape, SectionLink, Section = get_section()
    endtime = datetime.today().strftime('%Y-%m-%d')
    starttime = (datetime.today() - timedelta(days=10)).strftime('%Y-%m-%d')
    datelist = getdatelist(endtime,starttime)
    SectionShape, SectionLink, Section = get_section(datelist=datelist)
    etag = etag_getdf()

    # Section 資料與ETag門架資料合併
    newetag = pd.merge(etag, SectionLink, on = 'LinkID', how = 'left')
    filtercolumns = list(set(Section.columns) - set(newetag.columns))
    filtercolumns.append('SectionID')
    newetag = pd.merge(newetag,Section[filtercolumns], on = 'SectionID', how = 'left').drop_duplicates().reset_index(drop = True)
    # newetag = newetag.drop_duplicates(subset=['ETagGantryID']).reset_index(drop = True)

    # ETag 資料再進行一次處理
    # (1) 重新排序
    reindex_columns = ['ETagGantryID', 'SectionID', 'SectionName', 'LinkID', 'LocationType', 'PositionLon', 'PositionLat', 'RoadID', 'RoadName', 'RoadClass', 'RoadDirection', 'Start', 'End', 'StartKM', 'EndKM', 'LocationMile','SpeedLimit','SectionLength','SubAuthorityCode']
    newetag = newetag.reindex(columns = reindex_columns)
    newetag['SpeedLimit'] = newetag['SpeedLimit'].fillna(90) # 宜蘭多處沒有標示速率

    # (2) 計算門架與交流道里程
    newetag[['StartKM', 'EndKM', 'LocationMile']] = newetag[['StartKM', 'EndKM', 'LocationMile']].fillna("0K+000")
    columns_to_convert = ['StartKM', 'EndKM', 'LocationMile']
    new_columns = ['StartKMReformat', 'EndKMReformat', 'ETagKMReformat']
    for old_col, new_col in zip(columns_to_convert, new_columns):
        newetag[new_col] = newetag[old_col].astype(str).apply(convert_km_format)
    
    # (3) 計算與上下游關係的里程距離
    newetag['UpstreamDistance'] = (newetag['StartKMReformat'] - newetag['ETagKMReformat']).abs()
    newetag['DownstreamDistance'] = (newetag['EndKMReformat'] - newetag['ETagKMReformat']).abs()

    # (4) 最後繼續依照方向性跟緯度、里程重新排序，把重複的刪除
    newetag_S = newetag[newetag['RoadDirection'] == 'S']
    newetag_S = newetag_S.sort_values(['RoadName', 'StartKMReformat', 'ETagKMReformat','EndKMReformat'], ascending=[True, True, True , True]).reset_index(drop = True)
    newetag_S = newetag_S.drop_duplicates(subset=['ETagGantryID']).reset_index(drop = True)

    newetag_N = newetag[newetag['RoadDirection'] == 'N']
    newetag_N = newetag_N.sort_values(['RoadName', 'EndKMReformat', 'ETagKMReformat','StartKMReformat'], ascending=[True, False, False, False]).reset_index(drop = True)
    newetag_N = newetag_N.drop_duplicates(subset=['ETagGantryID']).reset_index(drop = True)
    newetag = pd.concat([newetag_N, newetag_S])
    

    outputfolder = create_folder(os.path.join(os.getcwd(),'..','Input', "靜態資料","Table"))
    newetag.to_excel(os.path.join(outputfolder, "ETag整併資料.xlsx"), index = False)

if __name__ == '__main__':
    main()
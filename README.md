# Freeway
下載高公局資料，包含處理M03A、M056A、M06A、M08A等等。

## 安裝步驟  
因本專案的資料涉及個資無法提供，但欲使用相關的路線資料，可以至TDX官網進行下載路線資料，以及函文請示相關資料。  

需要確保目錄架構為：  
project_name/  
├── Code/  
│   ├── PreFreeway.py # 爬取ETag門架資料，若需要執行M05A、M06A必須執行  
│   └── Freeway.ipynb # 營運資料、票證資料的比對與計算  
├── Input/  
│   ├──   
│   ├── ETag匝道選擇.xlsx # 手動篩選M06A，需要篩選的交流道和門架  
│   └── ETC_M03A_202304_容量.xlsx # 目前計算出的高速公路各路段容量  
├── Input/  
│   └── Freeway.twb # 匯入output資料可以產生關連性資料，可以把資料匯入進行道路服務水準的估算  
├── requirements.txt # 用於安裝依賴包的列表  
└── README.md # 項目的說明文件  


## 執行須知
先執行PreFreeway.py，再執行Freeway.py。
兩個程式碼需要手動進行修改的部分如下
  
其中Freeway.py需要注意手動修改的內容為何。  
需要修正時間區間
* starttime
* endtime
  
若需要進行道路服務水準的估算，需要同時處理M03A、M05A；M06A則為計算交流道的進出量，需要先手動至 ETag匝道選擇.xlsx 檔案進行挑選門架ID和進出交流道。

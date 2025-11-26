import json
import duckdb
import solara
import leafmap.maplibregl as leafmap
import pandas as pd

# --- 1. DuckDB 連線設定 (保持不變) ---
con = duckdb.connect()
con.install_extension("httpfs")
con.install_extension("spatial")
con.load_extension("httpfs")
con.load_extension("spatial")

# 資料來源 URL
DATA_URL = 'https://data.gishub.org/duckdb/cities.csv'

# --- 2. 獲取所有國家列表 (用於下拉選單) ---
# 提前從檔案中讀取所有不重複的國家名稱
countrys_df = con.sql(f"SELECT DISTINCT country FROM '{DATA_URL}' ORDER BY country").df()
    # 轉換為 Python 列表
ALL_COUNTRYS = countrys_df['country'].tolist()

# 設定預設國家
DEFAULT_COUNTRY = "USA" if "USA" in ALL_COUNTRYS else ALL_COUNTRYS[0]

# --- 3. Solara 組件定義 ---

@solara.component
def Page():
    # 3.1. 定義響應式狀態 (使用 solara.use_reactive)
    # country: 儲存使用者當前選擇的國家
    country, set_country = solara.use_state(DEFAULT_COUNTRY)

    # 3.2. 根據選擇的國家篩選資料 (使用 solara.use_memo)
    # 當 country 改變時，這個區塊會自動重新計算
    def get_filtered_data(selected_country):
        print(f"Filtering data for: {selected_country}")
        try:
            # SQL 查詢：篩選國家並將經緯度轉換為 WKT 點幾何
            sql_select_wkt = f"""
                SELECT name, population, ST_Point(longitude, latitude) AS geometry
                FROM '{DATA_URL}'
                WHERE country = '{selected_country}'
                ORDER BY population DESC
            """
            city_df = con.sql(sql_select_wkt).df()
            
            # 使用 leafmap.df_to_gdf 轉換為 GeoDataFrame
            # 注意：這裡將 WKT 欄位名修正為 SQL 中 AS 的 'geometry'
            # 由於 leafmap/maplibregl 傾向於 WGS84 (EPSG:4326)，可以省略 src_crs/dst_crs 參數
            # 但如果數據源有特定 CRS，則應加上。這裡假設 DuckDB 的 ST_Point 輸出的點是 WGS84。
            gdf = leafmap.df_to_gdf(
                city_df,
                geometry="geometry", # 使用 SQL 輸出的欄位名
                # src_crs="EPSG:4326", # 假設 WKT 已經是 4326
                # dst_crs="EPSG:4326"
            )
            return gdf
        except Exception as e:
            print(f"Error running DuckDB query: {e}")
            return pd.DataFrame() # 返回空 DataFrame

    # 使用 use_memo 確保只有在 `country` 改變時才重新執行資料篩選
    gdf = solara.use_memo(lambda: get_filtered_data(country), dependencies=[country])

    # 3.3. 下拉式選單 (Select)
    # 當使用者選擇一個新國家時，set_country 會更新 country 狀態
    solara.Select(
        label="選擇國家", 
        value=country, 
        values=ALL_COUNTRYS,
        on_value_change=set_country # 將選擇的值傳給 set_country
    )

    # 3.4. Leafmap 地圖組件
    # 地圖組件會隨著 gdf 的更新而重新渲染
    m = leafmap.Map(
        style="dark-matter", 
        center=(0, 0), # 預設中心點，之後會自動縮放到資料範圍
        zoom=2
    )
    m.add_basemap("Esri.WorldImagery")

    # 3.5. 在地圖上添加篩選後的資料
    if not gdf.empty:
        # 將 Leafmap 的 add_data 邏輯移到這裡
        m.add_data(
             gdf,
             layer_type="circle",
             fill_color="#FFD700",
             radius=6,
             stroke_color="#FFFFFF",
             name=f"{country} Cities" # 顯示當前國家名稱
        )
        # 讓地圖自動縮放到點的範圍 (fit_bounds)
        m.zoom_to_data(gdf) 
    else:
        # 處理沒有數據的情況
        solara.Warning(f"**沒有找到 {country} 的城市數據。** 請嘗試選擇其他國家。")
    
    # 3.6. 返回 Solara 渲染的元素
    return m.to_solara()

# 備註：您不需要手動轉換為 GeoJSON，leafmap.Map.add_data 可以直接接受 GeoDataFrame。

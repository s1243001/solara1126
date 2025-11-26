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
ALL_COUNTRYS = countrys_df['country'].tolist()

# 設定預設國家
DEFAULT_COUNTRY = "USA" if "USA" in ALL_COUNTRYS else ALL_COUNTRYS[0]

# --- 3. Solara 組件定義 ---

@solara.component
def Page():
    # 3.1. 定義響應式狀態
    country, set_country = solara.use_state(DEFAULT_COUNTRY)

    # 3.2. 根據選擇的國家篩選資料 (使用 solara.use_memo)
    def get_filtered_data(selected_country):
        print(f"Filtering data for: {selected_country}")
        try:
            # 註：這裡使用標準的 'longitude'。如果數據無法加載，請嘗試替換為 'longtitude'
            sql_select_wkt = f"""
                SELECT name, population, ST_Point(longitude, latitude) AS geometry
                FROM '{DATA_URL}'
                WHERE country = '{selected_country}'
                ORDER BY population DESC
            """
            city_df = con.sql(sql_select_wkt).df()
            
            # 使用 leafmap.df_to_gdf 轉換為 GeoDataFrame
            gdf = leafmap.df_to_gdf(
                city_df,
                geometry="geometry",
            )
            return gdf
        except Exception as e:
            print(f"Error running DuckDB query: {e}")
            return pd.DataFrame()

    # 使用 use_memo 確保只有在 `country` 改變時才重新執行資料篩選
    gdf = solara.use_memo(lambda: get_filtered_data(country), dependencies=[country])

    # 3.3. 下拉式選單 (Select) - 確保狀態正確綁定
    select_widget = solara.Select(
        label="選擇國家",
        value=(country, set_country),  # 確保狀態能夠更新
        values=ALL_COUNTRYS,
    )
    
    # 3.4. Leafmap 地圖組件 (每次渲染時都創建一個新的實例)
    m = leafmap.Map(
        style="dark-matter",
        center=(0, 0),
        zoom=2
    )
    m.add_basemap("Esri.WorldImagery")

    # 3.5. 在地圖上添加篩選後的資料
    if not gdf.empty:
        m.add_data(
             gdf,
             layer_type="circle",
             fill_color="#FFD700",
             radius=6,
             stroke_color="#FFFFFF",
             name=f"{country} Cities"
        )
        m.zoom_to_data(gdf)
        
        # *** 修正 key 傳遞：將 key 應用到 solara.Div 容器上，強制地圖組件重新創建 ***
        map_widget = solara.Div(
            solara.VBox(
                [m.to_solara()], 
            ),
            key=country # 將 key 傳遞給 Div
        )
    else:
        # 如果沒有數據，顯示警告訊息
        warning_widget = solara.Warning(f"**沒有找到 {country} 的城市數據。** 請嘗試選擇其他國家。")
        
        # *** 修正 key 傳遞：使用 solara.Div 作為 key 容器 ***
        map_content = solara.VBox(
             [warning_widget, m.to_solara()],
            )
        map_widget = solara.Div(
            map_content,
            key=f"no-data-{country}" # 將 key 傳遞給 Div
        )
    
    # 3.6. 返回 Solara 渲染的元素：使用 solara.Column 垂直堆疊
    return solara.Column(
        [
            select_widget, 
            solara.Markdown("---"), 
            map_widget
        ],
        align="center",
        style={"width": "100%", "maxWidth": "1200px"}
    )
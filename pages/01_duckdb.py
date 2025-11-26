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
countrys_df = con.sql(f"SELECT DISTINCT country FROM '{DATA_URL}' ORDER BY country").df()
ALL_COUNTRYS = countrys_df['country'].tolist()
DEFAULT_COUNTRY = "USA" if "USA" in ALL_COUNTRYS else ALL_COUNTRYS[0]

# --- 3. Solara 組件定義 ---

@solara.component
def Page():
    # 3.1. 定義響應式狀態
    country, set_country = solara.use_state(DEFAULT_COUNTRY)

    # 3.2. 根據選擇的國家篩選資料 
    def get_filtered_data(selected_country):
        print(f"Filtering data for: {selected_country}")
        try:
            sql_select_wkt = f"""
                SELECT name, population, ST_Point(longtitude, latitude) AS geometry
                FROM '{DATA_URL}'
                WHERE country = '{selected_country}'
                ORDER BY population DESC
            """
            city_df = con.sql(sql_select_wkt).df()
            
            gdf = leafmap.df_to_gdf(
                city_df,
                geometry="geometry",
            )
            return gdf
        except Exception as e:
            print(f"Error running DuckDB query: {e}")
            return pd.DataFrame()

    gdf = solara.use_memo(lambda: get_filtered_data(country), dependencies=[country])

    # 3.3. 下拉式選單 (Select) - 修正狀態綁定
    select_widget = solara.Select(
        label="選擇國家",
        value=(country, set_country), # <--- 修正點
        values=ALL_COUNTRYS,
    )
    
    # 3.4. Leafmap 地圖組件
    m = leafmap.Map(
        style="dark-matter",
        center=[0,0],
        zoom=2
        )
    m.add_basemap("Esri.WorldImagery")

    # 3.5. 在地圖上添加篩選後的資料
    
    m.add_data(
             gdf,
             layer_type="circle",
             fill_color="#FFD700",
             radius=6,
             stroke_color="#FFFFFF",
             name=f"{country} Cities"
        )
    m.zoom_to_data(gdf)
    map_widget = m.to_solara()
       
    # 3.6. 返回 Solara 渲染的元素：使用 solara.Column 垂直堆疊
    return solara.Column(
        [
            select_widget, 
            solara.Markdown("---"), 
            map_widget
        ],
    )
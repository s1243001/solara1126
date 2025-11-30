import duckdb
import solara
import pandas as pd
# 引入 Plotly Express 取代 leafmap
import plotly.express as px
import plotly.graph_objects as go

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
            # 獲取城市數據，包含經度和緯度 (Plotly 需要單獨的欄位)
            sql_select = f"""
                SELECT name, population, longitude, latitude, country
                FROM '{DATA_URL}'
                WHERE country = '{selected_country}'
                ORDER BY population DESC
            """
            city_df = con.sql(sql_select).df()
            return city_df
        except Exception as e:
            print(f"Error running DuckDB query: {e}")
            return pd.DataFrame()

    # 使用 use_memo 確保只有在 `country` 改變時才重新執行資料篩選
    df = solara.use_memo(lambda: get_filtered_data(country), dependencies=[country])

    # 3.3. 下拉式選單 (Select) - 確保狀態正確綁定
    select_widget = solara.Select(
        label="選擇國家",
        value=(country, set_country),  # 確保狀態能夠更新
        values=ALL_COUNTRYS,
    )
    
    # 3.4. Plotly 地圖繪製邏輯
    
    if not df.empty:
        # 使用 Plotly Express 創建地圖
        fig = px.scatter_geo(
            df, 
            lat='latitude', 
            lon='longitude',
            hover_name='name',
            size='population', 
            color='population',
            color_continuous_scale=px.colors.sequential.Sunset,
            projection="natural earth",
            title=f"{country} 主要城市分佈",
            height=600,
        )
        
        # 設置地圖佈局
        fig.update_geos(
            scope=country.lower() if country.lower() != 'usa' else 'north america', # 嘗試縮放到國家範圍
            visible=False,
            showcountries=True,
            countrycolor="Black"
        )
        fig.update_layout(
            margin={"r":0,"t":50,"l":0,"b":0},
            coloraxis_showscale=False
        )
        
        # 使用 solara.FigurePlotly 渲染 Plotly 圖表
        map_widget = solara.FigurePlotly(fig, style={"height": "70vh", "width": "100%"})
        
    else:
        # 如果沒有數據，顯示警告訊息
        warning_widget = solara.Warning(f"**沒有找到 {country} 的城市數據。** 請嘗試選擇其他國家。")
        
        # 創建一個空的 Plotly 圖表作為替代（避免渲染錯誤）
        fig_empty = go.Figure()
        fig_empty.update_layout(
            title="請選擇一個國家",
            height=600
        )
        
        map_widget = solara.Column(
            [
                warning_widget,
                solara.FigurePlotly(fig_empty, style={"height": "70vh", "width": "100%"})
            ]
        )
    
    # 3.5. 返回 Solara 渲染的元素：使用 solara.Column 垂直堆疊
    return solara.Column(
        [
            select_widget, 
            solara.Markdown("---"), 
            map_widget
        ],
        align="center",
        style={"width": "100%", "maxWidth": "1200px"}
    )
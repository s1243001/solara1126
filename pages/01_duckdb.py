import duckdb
import solara
import pandas as pd
# 引入 Plotly Express
import plotly.express as px
import plotly.graph_objects as go

# ----------------------------------------------------------------------
# 1. DuckDB 連線設定與全局變數
# ----------------------------------------------------------------------
con = duckdb.connect()
con.install_extension("httpfs")
con.install_extension("spatial")
con.load_extension("httpfs")
con.load_extension("spatial")

# 資料來源 URL
DATA_URL = 'https://data.gishub.org/duckdb/cities.csv'

# 提前獲取所有國家列表
countrys_df = con.sql(f"SELECT DISTINCT country FROM '{DATA_URL}' ORDER BY country").df()
ALL_COUNTRYS = countrys_df['country'].tolist()

# 設定預設國家
DEFAULT_COUNTRY = "USA" if "USA" in ALL_COUNTRYS else (ALL_COUNTRYS[0] if ALL_COUNTRYS else "")

# ----------------------------------------------------------------------
# 2. 全局狀態管理 (使用 solara.reactive 模仿同學的結構)
# ----------------------------------------------------------------------
all_countries = solara.reactive(ALL_COUNTRYS)
selected_country = solara.reactive(DEFAULT_COUNTRY) 
data_df = solara.reactive(pd.DataFrame())

# ----------------------------------------------------------------------
# 3. 數據處理副作用
# ----------------------------------------------------------------------
def load_filtered_data():
    """當 selected_country 變數改變時，重新執行 DuckDB 查詢並更新 data_df。"""
    country_name = selected_country.value
    if not country_name:
        return
        
    print(f"Querying data for: {country_name}")
    try:
        sql_query = f"""
        SELECT name, country, population, latitude, longitude
        FROM '{DATA_URL}'
        WHERE country = '{country_name}'
        ORDER BY population DESC
        LIMIT 20;
        """
        # 使用現有的全局連接，避免重複初始化
        df_result = con.sql(sql_query).df()
        data_df.set(df_result)
    except Exception as e:
        print(f"Error executing query: {e}")
        data_df.set(pd.DataFrame())


# ----------------------------------------------------------------------
# 4. 模組化繪圖組件
# ----------------------------------------------------------------------
@solara.component
def CityMapPlotly(df: pd.DataFrame, country: str):
    """
    使用 Plotly Express 創建城市分佈地圖。
    """
    if df.empty:
        warning_widget = solara.Warning(f"**沒有找到 {country} 的城市數據。** 請嘗試選擇其他國家。")
        fig_empty = go.Figure()
        fig_empty.update_layout(title="請選擇一個國家或數據載入中")
        
        return solara.Div(
            [warning_widget, solara.FigurePlotly(fig_empty)],
            style={"height": "70vh", "width": "100%"}
        )

    # 使用 Plotly Express 創建地圖
    fig = px.scatter_geo(
        df, 
        lat='latitude', 
        lon='longitude',
        hover_name='name',
        size='population', 
        color='population',
        color_continuous_scale=px.colors.sequential.Sunset,
        # 移除 projection="natural earth"，避免特定投影引起的問題
        title=f"{country} 主要城市分佈",
    )
    
    # 設置地圖佈局
    # 修正：'scope' 只能是 ['africa', 'asia', 'europe', 'north america', 'oceania', 'south america', 'usa', 'world'] 之一
    map_scope = 'usa' if country == 'USA' else 'world'

    fig.update_geos(
        scope=map_scope,
        showland=True,           # 確保陸地顯示
        landcolor="lightgray",   # 設定陸地顏色
        visible=True,            # 確保地理子圖可見
        showcountries=True,
        countrycolor="Black",
        showsubunits=True,       # 顯示次級單位邊界（如州/省），有助於渲染
        subunitcolor="darkgray", # 設定次級單位邊界顏色
        oceancolor="lightblue"   # 設定海洋顏色，讓地圖看起來更完整
    )
    
    # 針對非美國國家進行更明確的自動居中設定，以確保視圖集中在數據點上
    if country != 'USA' and not df.empty:
        # 計算數據的中心點
        center_lat = df['latitude'].mean()
        center_lon = df['longitude'].mean()
        
        fig.update_layout(
            geo=dict(
                # 設置地圖居中
                center=dict(lat=center_lat, lon=center_lon),
                # 調整縮放級別 (1.0 是預設世界視圖，更高的值會放大)
                # 這裡暫時不設定 zoom，讓 Plotly 決定最佳縮放，但如果數據點非常集中，可能需要手動設定 zoom
            )
        )


    fig.update_layout(
        margin={"r":0,"t":50,"l":0,"b":0},
        coloraxis_showscale=False
    )
    
    plotly_figure = solara.FigurePlotly(fig)
    
    # 將 FigurePlotly 包裹在 Div 中來控制尺寸
    return solara.Div([plotly_figure], style={"height": "70vh", "width": "100%"})


# ----------------------------------------------------------------------
# 5. 頁面佈局組件
# ----------------------------------------------------------------------
@solara.component
def Page():
    # 設置依賴項：在 selected_country 改變時，調用 load_filtered_data 函數
    solara.use_effect(load_filtered_data, dependencies=[selected_country.value])
    
    solara.Title("城市地理人口分析 (DuckDB + Solara + Plotly)")

    with solara.Column(
        align="center",
        style={"width": "100%", "maxWidth": "1200px"}
    ):
        # 國家選擇器
        solara.Select(
            label="選擇國家",
            value=selected_country,  # 直接綁定 reactive 變數
            values=all_countries.value,
        )
        
        solara.Markdown("---") 

        # 根據數據狀態渲染地圖
        if selected_country.value and not data_df.value.empty:
            country_code = selected_country.value
            df = data_df.value
            
            # 渲染獨立的地圖組件
            CityMapPlotly(df=df, country=country_code)

            # 額外添加數據表格和人口分佈長條圖 (參考同學的程式碼結構)
            solara.Markdown(f"### 📋 數據表格 (前 {len(df)} 大城市)")
            solara.DataFrame(df)
            
            fig_bar = px.bar(
                df, 
                x="name",                           
                y="population",                     
                color="population",                 
                title=f"{country_code} 城市人口",
                labels={"name": "城市名稱", "population": "人口數"},
                height=400 
            )
            fig_bar.update_layout(xaxis_tickangle=-45)
            solara.FigurePlotly(fig_bar)

        elif selected_country.value:
            solara.Info(f"正在載入 {selected_country.value} 的數據...")
        else:
            solara.Info("正在載入國家清單...")
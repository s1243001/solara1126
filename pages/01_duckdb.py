import json
import duckdb
import solara
import ipywidgets as widgets
import leafmap.maplibregl as leafmap
import matplotlib.pyplot as plt

countrys = ["Kiwi", "Banana", "Apple"]
country = solara.reactive("USA")


con = duckdb.connect()
con.install_extension("httpfs")
con.install_extension("spatial")
con.load_extension("httpfs")
con.load_extension("spatial")

sql_select_wkt ="
      SELECT name, population, ST_Point(longtitude, latitude) AS geomentry
      FROM 'https://data.gishub.org/duckdb/cities.csv'
      WHERE country = 'USA'
      ORDER BY population DESC
      ;
      "
city_df = con.sql(sql_select_wkt).df()
gdf = leafmap.df_to_gdf(
    city_df,
    geometry="geometry", # 指定 WKT 欄位名稱
    src_crs="EPSG:26918",
    dst_crs="EPSG:4326"
)

@solara.component
def Page():
  solara.Select(label="國家", value=country, values=countrys)
  m = leafmap.Map(style="dark-matter")
  m.add_basemap("Esri.WorldImagery")
  m.add_data(
     gdf,
     layer_type="circle",
     fill_color="#FFD700",
     radius=6,
     stroke_color="#FFFFFF",
      name="city"
   )
  return m

    

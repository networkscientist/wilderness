from typing import Any
import rasterio
import pyproj
from rasterio.transform import rowcol
import pandas as pd
from pathlib import Path

data: pd.DataFrame = pd.read_excel("1_Dataset.ods")


def create_value_series(
    df: pd.DataFrame, lyr_name: str, lat_name: str, lon_name: str
) -> pd.Series:
    data_new: pd.DataFrame = df.filter([lat_name, lon_name]).rename(
        columns={lat_name: "lat", lon_name: "lon"}
    )
    layer_ds, transformer, src_transformed = load_tif_to_rio(lyr_name)
    temp_list: list = create_value_list_from_lonlat(
        data_new, layer_ds, src_transformed, transformer
    )
    return pd.Series(temp_list).rename(lyr_name)


def create_value_list_from_lonlat(
    df: pd.DataFrame, layer_ds, src_transformed, transformer: pyproj.Transformer
) -> list[int]:
    temp_list: list[int] = []
    for lon, lat in zip(df.lon.values, df.lat.values):
        x, y = transformer.transform(lon, lat)  # reproject to raster CRS
        row, col = rowcol(src_transformed, x, y)
        temp_list.append(layer_ds[row, col])
    return temp_list


def load_tif_to_rio(lyr_name: str) -> tuple[Any, pyproj.Transformer, Any]:
    with rasterio.open(
        Path(Path.home(), f"Downloads/GIS Daten/{lyr_name}/{lyr_name}.tif"), "r"
    ) as src:
        layer = src.read(1, masked=True)
        transformer: pyproj.Transformer = pyproj.Transformer.from_crs(
            "EPSG:4326", src.crs.to_string(), always_xy=True
        )
        src_transformed = src.transform
    return layer, transformer, src_transformed


wildnis_column_list = [
    "wildnis",
    "wildnis_remoteness",
    "wildnis_humanimpact",
    "wildnis_naturalness",
    "wildnis_curvature",
]
new_cols = [
    create_value_series(
        data,
        layer_name,
        lat_name="CoordinatesLatitude",
        lon_name="CoordinatesLongitude",
    )
    for layer_name in wildnis_column_list
]
pd.concat([data.iloc[:, 0:8].convert_dtypes(), *new_cols], axis=1).to_excel(
    "outfile.xlsx"
)

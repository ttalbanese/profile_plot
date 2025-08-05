from typing import Tuple, Union
import pandas as pd
import geopandas as gpd

def interpolate_points(
    gdf: gpd.GeoDataFrame, distance: Union[float, int], is_transect: bool
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    gdf = gdf.loc[gdf.length > distance]

    new_points = gdf.interpolate(distance)

    new_gdf = gpd.GeoDataFrame(
        geometry=new_points,
        data={
            "unique_id": gdf["unique_id"],
            "distance": distance,
            "is_transect": is_transect,
        },
        crs=gdf.crs,
    )

    return gdf, new_gdf

def get_segment_points(segments: gpd.GeoDataFrame, segment_length: int) -> gpd.GeoDataFrame:
    segments.set_geometry(segments.force_2d(), inplace=True)
    working_segments = segments.copy()
    segment_points = []
    for segment_distance in range(
        segment_length, int(segments.length.max()) + 1, segment_length
    ):
        working_segments, new_gdf = interpolate_points(
            working_segments, segment_distance - 1, False
        )
        segment_points.append(new_gdf)

        working_segments, new_gdf = interpolate_points(
            working_segments, segment_distance, True
        )
        segment_points.append(new_gdf)

    return pd.concat(segment_points, ignore_index=True)


def get_transects(
):
    transect_points = r"data\shapefiles\centerline_transects.shp"

    return gpd.read_file(transect_points)[['Milepost', 'geometry']]

def get_mileposts(segments: gpd.GeoDataFrame, transects: gpd.GeoDataFrame):

    segments.sjoin_nearest(transects, max_distance=1)
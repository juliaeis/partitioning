from oggm import workflow, cfg, tasks, graphics
from oggm.workflow import execute_entity_task
import salem
import geopandas as gpd
import pickle
import matplotlib.pyplot as plt
import os
import numpy as np
from shapely.geometry import mapping,shape


def _check_altitude_rage(gpd_obj):
    nokeep = ((gpd_obj['Perc_Alt_R'] < 0.1) | (gpd_obj['Alt_Range'] < 100))
    gpd_obj['keep'] = ~nokeep
    print('We keep {} divides out of {} '
          'after filtering.'.format(np.sum(gpd_obj['keep']),
                                    len(gpd_obj)))
    if np.sum(gpd_obj['keep']) == 1:
        # Nothing to do! The divide should be ignored
        return gpd_obj
    while not gpd_obj['keep'].all():
        geom = gpd_obj.loc[~gpd_obj['keep']].iloc[0]
        gpd_obj = gpd_obj.drop(geom.name)
        gpd_obj, bool = _merge_sliver(gpd_obj, geom.geometry)
    return gpd_obj


def _merge_sliver(gpd_obj, polygon):
    """merge sliver polygon to the glacier with the longest shared boundary.
    If polygon does not touch the glaciers, False will be returned.

    Parameters
    ----------
    gpd_obj : gpd.GeoDataFrame
        contains the geometry of each glacier
    polygon : shapely.geometry.Polygon instance
        sliver polygon, which should be merged

    Returns
    -------
    new gpd.GeoDataFrame,
    bool
    """
    intersection_array = gpd_obj.intersection(polygon.boundary).length
    if np.max(intersection_array) != 0:
        max_b = np.argmax(intersection_array)
        geom = gpd_obj.loc[max_b, 'geometry'].simplify(0.001).union(polygon.buffer(0))
        if geom.type is not 'Polygon':
            geom = geom.buffer(0.01).buffer(-0.01)
        gpd_obj.set_value(max_b, 'geometry', geom)
        merged = True
    # sliver does not touch glacier at the moment. Try again in the end
    else:
        merged = False
    return [gpd_obj, merged]

file ='/home/juliaeis/Dokumente/OGGM/work_dir/OGGM_ref_glaciers/per_glacier/RGI50-04/RGI50-04.05774/glaciers_range.shp'
glaciers = gpd.read_file(file)

glaciers = _check_altitude_rage(glaciers)
print(glaciers.type)

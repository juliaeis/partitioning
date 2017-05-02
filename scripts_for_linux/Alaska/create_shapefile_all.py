import os
import shutil
import geopandas as gpd
from functools import partial
import fiona
from shapely.geometry import mapping, shape
from shapely.ops import transform
import pyproj
from pyproj import Proj
import matplotlib.pyplot as plt
import salem
from oggm import workflow
import oggm.cfg as cfg
from partitioning import dividing_glaciers
import pickle


def run_divides(gdir1):
    # delete folders including divide_01
    for fol in os.listdir(gdir1.dir):
        if fol.startswith('divide'):
            shutil.rmtree(gdir1.dir + '/' + fol)
    os.makedirs(gdir1.dir + '/divide_01')
    to_copy = [gdir1.get_filepath('outlines', div_id=0),
               gdir1.dir + '/outlines.shx', gdir1.dir + '/outlines.dbf']
    for file in to_copy:
        shutil.copy(file, gdir.dir + '/divide_01')
    dividing_glaciers(gdir.get_filepath('dem', div_id=0),
                      gdir.get_filepath('outlines', div_id=0))


if __name__ == '__main__':
    cfg.initialize()
    base_dir = '/home/juliaeis/Dokumente/OGGM/work_dir/Alaska'
    cfg.PATHS['working_dir'] = base_dir
    cfg.PATHS['topo_dir'] = '/home/juliaeis/Dokumente/OGGM/input_data/topo'
    cfg.PARAMS['divides_gdf'] = gpd.GeoDataFrame()
    cfg.PARAMS['use_multiprocessing'] = False
    RGI_FILE = os.path.join(base_dir, '01_rgi50_Alaska.shp')
    ALL_DIVIDES = os.path.join(base_dir, 'Alaska_all_divides.shp')

    # set dem to 40 meters
    cfg.PARAMS['d1'] = 40
    cfg.PARAMS['dmax'] = 40

    RUN_DIVIDES = False

    rgidf = salem.read_shapefile(RGI_FILE, cached=True)
    gdirs = workflow.init_glacier_regions(rgidf, reset=False)
    all_divides = gpd.GeoDataFrame(crs=rgidf.crs)
    divided = []
    failed = []
    for gdir in gdirs:
        if gdir.n_divides is not 1:
            divided.append(gdir.rgi_id)
            '''
            for n in gdir.divide_ids:
                co_dir = gdir.get_filepath('outlines', div_id=n)
                div_co = gpd.read_file(co_dir).to_crs(rgidf.crs)
                all_divides = all_divides.append(div_co, ignore_index=True)
            '''
        elif len(os.listdir(gdir.dir)) > 15:
            failed.append(gdir.rgi_id)
    #all_divides.to_file(ALL_DIVIDES)
    pickle.dump(divided, open(os.path.join(base_dir, 'divided.pkl'), "wb"))
    pickle.dump(failed, open(os.path.join(base_dir, 'failed.pkl'), "wb"))
    print len(divided)
    print len(failed)


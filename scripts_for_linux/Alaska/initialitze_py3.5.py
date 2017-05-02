from oggm import workflow,cfg
import salem
import geopandas as gpd
import pickle
import os
import shutil

if __name__ == '__main__':
    cfg.initialize()
    base_dir = '/home/juliaeis/Dokumente/OGGM/work_dir'
    base_dir2 = '/home/juliaeis/Dokumente/OGGM/work_dir/Alaska'
    cfg.PATHS['working_dir'] = base_dir
    cfg.PATHS['topo_dir'] = '/home/juliaeis/Dokumente/OGGM/input_data/topo'
    cfg.PARAMS['divides_gdf'] = gpd.GeoDataFrame()
    RGI_FILE = os.path.join(base_dir2, '01_rgi50_Alaska.shp')
    cfg.PARAMS['use_multiprocessing'] = True
    # set dem to 40 meters
    cfg.PARAMS['grid_dx_method'] = 'fixed'
    cfg.PARAMS['fixed_dx'] = 40
    cfg.PARAMS['border'] = 10
    rgidf = salem.read_shapefile(RGI_FILE, cached=True)
    gdirs = workflow.init_glacier_regions(rgidf)
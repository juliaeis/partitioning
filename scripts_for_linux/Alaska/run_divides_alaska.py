from oggm import workflow,cfg
import salem
import geopandas as gpd
import pickle
import os
from shapely.geometry import Polygon


if __name__ == '__main__':

    RUN_DIVIDES = True
    CREATE_SHAPEFILE_ALL = False

    cfg.initialize()
    base_dir = '/home/juliaeis/Dokumente/OGGM/work_dir'
    RGI_FILE = os.path.join(base_dir, 'rgi', '01_rgi50_Alaska.shp')
    cfg.PATHS['working_dir'] = os.path.join(base_dir, 'divides')
    cfg.PATHS['topo_dir'] = '/home/juliaeis/Dokumente/OGGM/input_data/topo'
    cfg.PARAMS['divides_gdf'] = gpd.GeoDataFrame()

    cfg.PARAMS['use_multiprocessing'] = False
    # set dem to 40 meters
    cfg.PARAMS['grid_dx_method'] = 'fixed'
    cfg.PARAMS['fixed_dx'] = 40

    cfg.PARAMS['border'] = 10
    # no topo in CentralEurope
    # no_t = os.path.join(base_dir, 'rgi', 'no_topo_CentralEurope.pkl')
    # no_topo = pickle.load(open(no_t, 'rb'))
    no_topo = []

    rgidf = salem.read_shapefile(RGI_FILE, cached=True)

    indices = [(i not in no_topo) for i in rgidf.RGIId]
    gdirs = workflow.init_glacier_regions(rgidf[indices], reset=False)

    all_divides = gpd.GeoDataFrame(crs=rgidf.crs)
    failed = []
    div = 0

    for gdir in gdirs:

        input_shp = gdir.get_filepath('outlines', div_id=0)
        input_dem = gdir.get_filepath('dem', div_id=0)

        if RUN_DIVIDES:
            print(gdir.rgi_id)
            try:
                python = '/home/juliaeis/miniconda3/envs/test_pygeopro_env/bin/python'
                script = '/home/juliaeis/Documents/LiClipseWorkspace/partitioning-fork/scripts_for_linux/run_divides.py'
                os.system(python+' ' + script + ' ' + input_shp + ' ' + input_dem
                          + ' > /dev/null')
            except:
                failed.append(gdir.rgi_id)

        if CREATE_SHAPEFILE_ALL:
            if gdir.n_divides is not 1:
                div += 1
                for n in gdir.divide_ids:
                    co_dir = gdir.get_filepath('outlines', div_id=n)
                    div_co = gpd.read_file(co_dir).to_crs(rgidf.crs)
                    all_divides = all_divides.append(div_co, ignore_index=True)

    if CREATE_SHAPEFILE_ALL:
        all_shp = os.path.join(base_dir, 'divides', 'Alaska_divides.shp')
        all_divides.to_file(all_shp)
        print(div)
    fail = os.path.join(cfg.PATHS['working_dir'], 'failed.pkl')
    pickle.dump(failed, open(fail, 'wb'))


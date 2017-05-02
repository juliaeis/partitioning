from oggm import workflow, cfg, tasks, graphics
from oggm.workflow import execute_entity_task
import salem
import geopandas as gpd
import pickle
import matplotlib.pyplot as plt
import os


if __name__ == '__main__':

    RUN_DIVIDES = False
    CREATE_SHAPEFILE_ALL = False
    CREATE_SHAPEFILE_REGION = False

    cfg.initialize()
    # check the paths
    base_dir = '/home/juliaeis/Dokumente/OGGM/work_dir/OGGM_ref_glaciers'
    rgi_file = os.path.join(base_dir, 'RGI_list_ref_glaciers.shp')
    cfg.PATHS['topo_dir'] = '/home/juliaeis/Dokumente/OGGM/input_data/topo'
    cfg.PATHS['working_dir'] = base_dir
    #
    cfg.PARAMS['divides_gdf'] = gpd.GeoDataFrame()
    cfg.PARAMS['use_multiprocessing'] = False
    # set dem to 40 meters
    cfg.PARAMS['grid_dx_method'] = 'fixed'
    cfg.PARAMS['fixed_dx'] = 40

    cfg.PARAMS['border'] = 10

    file = os.path.join(cfg.PATHS['working_dir'], 'failed.pkl')
    not_working = pickle.load(open(file, 'rb'))[1]
    #print(not_working)
    #rgi = ['RGI50-03.01466']

    rgidf = salem.read_shapefile(rgi_file, cached=True)
    indices = [(i in not_working) for i in rgidf.RGIId]

    #gdirs = workflow.init_glacier_regions(rgidf, reset=False)
    gdirs = workflow.init_glacier_regions(rgidf[indices], reset=False)

    all_divides = gpd.GeoDataFrame(crs=rgidf.crs)
    failed = []
    div = 0
    regions = []
    for gdir in gdirs:
        input_shp = gdir.get_filepath('outlines', div_id=0)
        input_dem = gdir.get_filepath('dem', div_id=0)
        if gdir.rgi_region not in regions:
            regions.append(gdir.rgi_region)
        if RUN_DIVIDES:
            print(gdir.rgi_id)
            #try:
            python = '/home/juliaeis/miniconda3/envs/test_pygeopro_env/bin/python'
            script = '/home/juliaeis/Documents/LiClipseWorkspace/partitioning-fork/scripts_for_linux/run_divides.py'
            os.system(python+' ' + script + ' ' + input_shp + ' ' + input_dem )#+ ' > /dev/null')
            #except:
            #    failed.append(gdir.rgi_id)

        if CREATE_SHAPEFILE_ALL:
            if gdir.n_divides is not 1:
                div += 1
                for n in gdir.divide_ids:
                    co_dir = gdir.get_filepath('outlines', div_id=n)
                    div_co = gpd.read_file(co_dir).to_crs(rgidf.crs)
                    all_divides = all_divides.append(div_co, ignore_index=True)

    if CREATE_SHAPEFILE_ALL:
        all_shp = os.path.join(base_dir, 'divides_ref_glaciers.shp')
        all_divides.to_file(all_shp)
        print(div)
    if CREATE_SHAPEFILE_REGION:
        for region in regions:
            div = 0
            region_divides = gpd.GeoDataFrame(crs=rgidf.crs)
            for gdir in gdirs:
                if (gdir.rgi_region == region) and (gdir.n_divides is not 1):
                    div += 1
                    for n in gdir.divide_ids:
                        co_dir = gdir.get_filepath('outlines', div_id=n)
                        div_co = gpd.read_file(co_dir).to_crs(rgidf.crs)
                        region_divides = region_divides.append(div_co,
                                                         ignore_index=True)
                elif (gdir.rgi_region == region) and (len(os.listdir(gdir.dir)) not in [11, 16]):
                    failed.append(gdir.rgi_id)
            if len(region_divides) != 0:
                region_shp = os.path.join(base_dir, 'per_glacier', 'RGI50-'+region, 'divides_'+region+'.shp')
                region_divides.to_file(region_shp)

    fail = os.path.join(cfg.PATHS['working_dir'], 'failed.pkl')
    #pickle.dump(failed, open(fail, 'wb'))

    task_list = [tasks.glacier_masks, tasks.compute_centerlines]
    for task in task_list:
        execute_entity_task(task, gdirs)
    graphics.plot_centerlines(gdirs[0])
    #lt.show()

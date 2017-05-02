from oggm import workflow, cfg, graphics, tasks
import salem
import geopandas as gpd
import pickle
import os
import matplotlib.pyplot as plt
from oggm.workflow import execute_entity_task


if __name__ == '__main__':

    RUN_DIVIDES = True
    CREATE_SHAPEFILE_ALL = False
    PLOT = False
    cfg.initialize()
    # check the paths
    base_dir = '/home/juliaeis/Dokumente/OGGM/work_dir'
    rgi_file = os.path.join('/home/juliaeis/Dokumente/rgi50/05_rgi50_GreenlandPeriphery', '05_rgi50_GreenlandPeriphery.shp')
    cfg.PATHS['topo_dir'] = '/home/juliaeis/Dokumente/OGGM/input_data/topo'
    cfg.PATHS['working_dir'] = base_dir
    #
    cfg.PARAMS['divides_gdf'] = gpd.GeoDataFrame()
    cfg.PARAMS['use_multiprocessing'] = False
    # set dem to 40 meters
    cfg.PARAMS['grid_dx_method'] = 'fixed'
    cfg.PARAMS['fixed_dx'] = 40

    cfg.PARAMS['border'] = 10

    rgidf = salem.read_shapefile(rgi_file, cached=True)
    indices = [(i in ['RGI50-05.10315']) for i in rgidf.RGIId]

    gdirs_orig = workflow.init_glacier_regions(rgidf[indices], reset=True)

    cfg.PATHS['working_dir'] = os.path.join(base_dir, 'divides')
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
                os.system(python+' ' + script + ' ' + input_shp + ' ' + input_dem )#+ ' > /dev/null')
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
        all_shp = os.path.join(base_dir, 'divides', 'CentralEurope_divides.shp')
        all_divides.to_file(all_shp)
        print(div)

    if PLOT:
        task_list = [
            tasks.glacier_masks]
        for task in task_list:
            pass
            #execute_entity_task(task, gdirs_orig)
            execute_entity_task(task, gdirs)
        graphics.plot_domain(gdirs[0])
        plt.show()
        '''
        for i, gdir_orig in enumerate(gdirs_orig):
            fig = plt.figure(figsize=(20, 10))
            ax0 = fig.add_subplot(1, 2, 2)
            ax1 = fig.add_subplot(1, 2, 1)
            graphics.plot_domain(gdir_orig, ax=ax1)
            graphics.plot_domain(gdir, ax=ax1)
            #plt.savefig(os.path.join(base_dir, 'plots',
                                     #str(gdir_orig.rgi_id) + '.png'))
            plt.show()
        '''

    fail = os.path.join(cfg.PATHS['working_dir'], 'failed.pkl')
    pickle.dump(failed, open(fail, 'wb'))

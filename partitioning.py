'''
Created on 31.10.2016

@author: juliaeis
'''

import os
import geopandas as gpd
import numpy as np
from salem.utils import get_demo_file
import oggm
from oggm import tasks
import oggm.cfg as cfg
import matplotlib.pyplot as plt

import math
import fiona
import shutil
from shapely.geometry import mapping, shape
import rasterio
from rasterio.tools.mask import mask
from pygeoprocessing import routing

from skimage import img_as_float
from skimage.feature import peak_local_max
from shapely.geometry import Point,Polygon,MultiPolygon

def buffer(input_shp,width):
    output_shp = os.path.dirname(input_shp) + '/buffer_' + str(width) + '.shp'
    with fiona.open(input_shp, "r") as input:
        with fiona.open(output_shp, "w", "ESRI Shapefile", input.schema, crs = input.crs) as output:
            for f in input:
                output.write({'properties': f['properties'],'geometry': mapping(shape(f['geometry']).buffer(width))})
    return output_shp  

def clip(input_dem,shp,out_name):
        
    output_dem=os.path.dirname(input_dem)+'/'+out_name+'.tif'
    with fiona.open(shp, "r") as shapefile:
        geoms = [feature['geometry'] for feature in shapefile]
    with rasterio.open(input_dem) as src:
        out_image, out_transform = mask(src, geoms,nodata=np.nan, crop=False)
        out_meta = src.meta.copy()
    
    out_meta.update({"driver": "GTiff",
                     "height": out_image.shape[1],
                     "width": out_image.shape[2],
                     "nodata": np.nan,
                     "transform": out_transform})
    with rasterio.open(output_dem, "w", **out_meta) as dest:
        dest.write(out_image)
    return output_dem

def compactness(polygon):
    coord=np.array(polygon.boundary.coords)
    #calculate max distance(perimeter)
    max_dist=Point(coord[np.argmin(coord[:,1])]).distance(Point(coord[np.argmax(coord[:,1])]))
    x_dist=Point(coord[np.argmin(coord[:,0])]).distance(Point(coord[np.argmax(coord[:,0])]))
    if x_dist>max_dist:
        max_dist=x_dist
    if max_dist*math.pi/polygon.boundary.length > 0.5:
        return True
    else:
        return False

def difference(shp_1,shp_2):
    
    output_shp=os.path.dirname(input_shp)+'/gutter.shp'
    with fiona.open(shp_1,"r") as shapefile1:
        print shapefile1.crs
        with fiona.open(shp_2,"r") as shapefile2:
            geom1=next(shapefile1)
            geom2=next(shapefile2)
            with fiona.open(output_shp, "w", "ESRI Shapefile", shapefile1.schema, crs = shapefile1.crs) as output:
                output.write({'properties': geom1['properties'],'geometry':mapping(shape(geom1['geometry']).difference(shape(geom2['geometry'])))})
    return output_shp

def flowacc(input_dem):
        
    new_flow_direction_map_uri =os.path.dirname(input_dem)+'/flow_dir.tif'
    new_flow_map_accumulation_uri = os.path.dirname(input_dem)+'/flow_accumulation.tif'   
    #calculate flow direction
    routing.flow_direction_d_inf(input_dem, new_flow_direction_map_uri)
    #calculate flow_accumulation
    routing.flow_accumulation(new_flow_direction_map_uri,input_dem, new_flow_map_accumulation_uri)
    clip(new_flow_map_accumulation_uri, os.path.dirname(input_dem)+'/gutter.shp', 'flow_gutter')
    return os.path.dirname(input_dem)+'/flow_gutter.tif'

def flowsheds(input_dem):

    #open gutter with flow accumulation
    with rasterio.open(input_dem) as src:
        transform=src.transform
        band=np.array(src.read(1))
    im=img_as_float(band)
    nan=np.where(np.isnan(im))
    #set nan to zero
    im[nan]=0
    #calculate maxima     
    coordinates = peak_local_max(im, min_distance=4)
    #transform maxima to (flowaccumulation,coordinates)
    new_coord=[]
    dtype=[('flowaccumulation',float),('coordinates',np.float64, (2,))]
    for coord in coordinates:
        new_coord.append((im[coord[0]][coord[1]],(transform[0]+(coord[1]+1)*transform[1]-transform[1]/2,transform[3]+coord[0]*transform[-1]-transform[1]/2)))
    new_coord=np.array(new_coord,dtype=dtype)
    #sort array  by flowaccumulation
    new_coord=np.sort(new_coord, order='flowaccumulation')
    #reverse array
    new_coord=new_coord[::-1]
    with fiona.open(os.path.dirname(input_dem)+'/outlines.shp', "r") as outlines:
        out=outlines.next()['geometry']
        crs= outlines.crs
        global schema
        schema=outlines.schema
    with fiona.open(os.path.dirname(input_dem)+'/all_pour_point.shp', "w", "ESRI Shapefile", {'geometry': 'Point', 'properties': {'flow_acc':'float','id':'int','p_glac':'int'}}, crs ) as all_pourP:

        with fiona.open(os.path.dirname(input_dem)+ '/P_glac.shp', "w", "ESRI Shapefile",{'geometry': 'MultiPolygon', 'properties': {'flow_acc': 'float','id':'int'}}, crs) as p_glac:
    
            #for each pour point: create shapefile and run delinate watershed
            with fiona.open(os.path.dirname(input_dem)+'/all_watershed.shp', "w", "ESRI Shapefile", {'geometry': 'MultiPolygon', 'properties': {'flow_acc':'float','id':'int'}}, crs) as all_watershed:
                i = 0
                m = len(new_coord)
                while len(new_coord) is not 0:
                    #create directory
                    dir=os.path.dirname(input_dem)+'/'+str(i)
                    if not os.path.isdir(dir):
                        os.makedirs(dir)

                    coord=new_coord[0]
                    #remove first element
                    new_coord=new_coord[1:]
                    #create radius around PPs and clip with outlines
                    area=({'properties': {'flow_acc': coord['flowaccumulation'],'id':i},'geometry': mapping(shape(out).intersection(shape(Point(coord['coordinates']).buffer(p_glac_radius(14.3,0.5,coord['flowaccumulation'])))))})
                    #if result is Polygon, add it to shapefile
                    if area['geometry']['type'] is 'Polygon':
                        p_glac.write(area)
                        all_pourP.write({'properties': {'flow_acc': coord['flowaccumulation'], 'id': i,'p_glac': len(area['geometry']['coordinates'])},'geometry': {'type': 'Point', 'coordinates': coord['coordinates']}})
                    #if result is MultiPolygon, add only the polygon whose perimeter is closest to PP
                    elif area['geometry']['type'] is 'MultiPolygon':
                        all_pourP.write({'properties': {'flow_acc': coord['flowaccumulation'], 'id': i,'p_glac': len(area['geometry']['coordinates'])},'geometry': {'type': 'Point', 'coordinates': coord['coordinates']}})
                        min_dist=[]
                        for j in shape(area['geometry']):
                            min_dist.append(shape(j).distance(Point(coord['coordinates'])))
                        area['geometry']=mapping(shape(area['geometry'])[np.argmin(min_dist)])
                        p_glac.write(area)
                    #add only PP to all_pourPoints.shp
                    else:
                        all_pourP.write({'properties': {'flow_acc': coord['flowaccumulation'], 'id': i,'p_glac':0},'geometry': {'type': 'Point', 'coordinates': coord['coordinates']}})

                    # write shapefile with ONE pour_point for watershed (transform unicode to ascii, otherwise segmentation fault)
                    with fiona.open(dir+'/pour_point.shp', "w", "ESRI Shapefile", {'geometry': 'Point', 'properties': {'flow_acc':'float'}}, {k.encode('ascii'): v for k, v in crs.items()}) as output:
                        output.write({'properties': {'flow_acc':coord['flowaccumulation']},'geometry': {'type':'Point','coordinates':coord['coordinates']}})

                    #calculate watershed for pour point
                    routing.delineate_watershed(os.path.dirname(input_dem)+'/gutter.tif',dir+'/pour_point.shp',0,100, dir+'/watershed_out.shp', dir+'/snapped_outlet_points_uri.shp', dir+'/stream_out_uri.tif')

                    #add watershed polygon to watershed_all.shp
                    with fiona.open(dir+'/watershed_out.shp', "r", "ESRI Shapefile") as watershed:
                        w=watershed.next()
                    #cut watershed wit outlines
                    w['geometry']=mapping(shape(out).intersection(shape(w['geometry'])))
                    w['properties']['id']=i

                    if w['geometry']['type'] is 'Polygon':
                        all_watershed.write(w)
                    if w['geometry']['type'] is 'MultiPolygon':
                        #find polygon with minimal distance to pour point
                        dist=[]
                        for k in shape(w['geometry']):
                            dist.append(shape(k).distance(Point(coord['coordinates'])))
                        #add each polygon to all_watershed.shp
                        n=0
                        for l in shape(w['geometry']):
                            w['geometry'] =mapping(shape(l))
                            #polygon nearest to PP get current id
                            if n == np.argmin(dist):
                                w['properties']['id'] = i
                            # all other poylgons get new id
                            else:
                                m=m+1
                                w['properties']['id']=m
                            all_watershed.write(w)
                            n=n+1

                    shutil.rmtree(dir)

                    i=i+1
    return [os.path.dirname(input_dem)+'/all_pour_point.shp',os.path.dirname(input_dem)+ '/P_glac.shp',os.path.dirname(input_dem)+'/all_watershed.shp']

def gutter(masked_dem, outline_shp, buffer2_shp, depth):
    buffer3_shp = buffer(outline_shp, pixelsize)
    gutter_shp = difference(buffer2_shp, buffer3_shp)
    gutter_dem = clip(masked_dem, gutter_shp, 'gutter')

    with rasterio.open(masked_dem) as src1:
        mask_band = np.array(src1.read(1))
        with rasterio.open(gutter_dem) as src:
            mask_band = np.float32(mask_band - depth * (~np.isnan(np.array(src.read(1)))))
            with rasterio.open(gutter_dem, "w", **src.meta.copy()) as dest:
                dest.write_band(1, mask_band)
    return gutter_dem

def merge_flowsheds(P_glac_dir,watershed_dir):

    pp_merged={}
    all_poly_glac={}

    #determinde overlaps from P_glac with watershed
    with fiona.open(watershed_dir,"r") as watersheds:
        global crs
        crs=watersheds.crs
        silver_poly_check={}
        watershed_out=Polygon()
        for shed in watersheds:
            watershed_out=watershed_out.union(shape(shed['geometry']))
            shed_status=False
            with fiona.open(P_glac_dir, "r") as P_glac:
                for P in P_glac:
                    if shape(P['geometry']).intersects(shape(shed['geometry'])):
                        shed_status=True
                        if 'PP_'+str(P['properties']['id']) in pp_merged:
                            pp_merged['PP_'+str(P['properties']['id'])]=pp_merged['PP_'+str(P['properties']['id'])].union({shed['properties']['id']})
                            all_poly_glac['PP_'+str(P['properties']['id'])] = all_poly_glac['PP_'+str(P['properties']['id'])].union(shape(shed['geometry']))
                        else:
                            pp_merged.update({'PP_'+str(P['properties']['id']):{shed['properties']['id']}})
                            all_poly_glac.update({'PP_'+str(P['properties']['id']):shape(shed['geometry'])})
            #if shed don't overlay with any P_glac
            if shed_status is False :
                silver_poly_check.update({shed['properties']['id']:shape(shed['geometry'])})


    #merge overlaps together
    glacier_n=0
    glacier_id={}
    glacier_poly={}
    for PP in pp_merged:
        pp_status=False
        for glac in glacier_id:
            if len(pp_merged[PP].intersection(glacier_id[glac])) is not 0:
                glacier_id[glac]=pp_merged[PP].union(glacier_id[glac])
                glacier_poly[glac] = all_poly_glac[PP].union(shape(glacier_poly[glac]))
                pp_status=True
        if not pp_status and len(pp_merged[PP])>1:
            glacier_id.update({'glacier'+str(glacier_n):pp_merged[PP]})
            glacier_poly.update({'glacier' + str(glacier_n): all_poly_glac[PP]})
            glacier_n=glacier_n+1
        if not pp_status and len(pp_merged[PP])==1:
            silver_poly_check.update({pp_merged[PP].pop():all_poly_glac[PP]})

    #check for sliver_polygons
    for polygon_id,polygon in silver_poly_check.iteritems():
        if polygon.area < 100000 or (polygon.area < 200000 and compactness(polygon)):
            glacier_poly=merge_silver_poly(glacier_poly,polygon)

        else:
            glacier_id.update({'glacier'+str(glacier_n):{polygon_id}})
            glacier_poly.update({'glacier' + str(glacier_n): polygon})
            glacier_n=glacier_n+1
    #add regions, where no watersheds exists to glaciers   --> these are watersheds from pour points inside the glacier region
    #TODO: --> fill pits at DEM should avoid this, but function in pygeoprocessing is not working yet
    total_glacier=MultiPolygon()
    for glacier in glacier_poly:
        total_glacier=shape(total_glacier).union(shape(glacier_poly[glacier]))
    with fiona.open(os.path.dirname(watershed_dir) + '/outlines.shp', 'r') as outline:
        with fiona.open(os.path.dirname(P_glac_dir) + '/sliver.shp', "w", "ESRI Shapefile",{'geometry': 'Polygon', 'properties': {}}, crs) as sli:
            for polygon in shape(outline.next()['geometry']).difference(total_glacier).buffer(-0.1):
                glacier_poly=merge_silver_poly(glacier_poly,polygon.buffer(0.2))
                sli.write({'properties': {},'geometry': mapping(polygon)})
    with fiona.open(os.path.dirname(P_glac_dir) + '/glaciers.shp', "w", "ESRI Shapefile",{'geometry': 'Polygon', 'properties': {'name':'str'}}, crs) as gla:
        for id,polygon in glacier_poly.iteritems():
            gla.write({'properties': {'name':id}, 'geometry': mapping(polygon)})

    from itertools import combinations
    glacier_keys = glacier_poly.keys()
    inter={(pair[0],pair[1]):glacier_poly[pair[0]].intersection(glacier_poly[pair[1]]) for pair in combinations(glacier_poly.keys(), 2)}
    #print inter.keys()
    #for key in inter.keys():
    while len(inter.keys()) is not 0:
        key=inter.keys()[0]
        if inter[key].type in ['Polygon','MultiPolygon','GeometryCollection']:
            if inter[key].type in ['GeometryCollection']:
                poly = MultiPolygon()
                for polygon in inter[key]:
                    if polygon.type in ['Polygon', 'Mulltipolygon']:
                        poly = poly.union(polygon)
                inter[key]=poly
            if inter[key].area / shape(glacier_poly[key[0]]).area > 0.5 or inter[key].area / shape(glacier_poly[key[1]]).area > 0.5:
                #union of both glaciers
                glacier_poly[key[0]]=shape(glacier_poly[key[0]]).union(glacier_poly[key[1]])
                # delete 2nd glacier
                for tupel in inter.keys():
                    if tupel is not key:
                        if key[1] is tupel[0]:
                            #inter.update({(key[0],tupel[1]):inter[tupel]})
                            inter.update({(key[0], tupel[1]):glacier_poly[key[0]].intersection(glacier_poly[tupel[1]])})
                            del inter[tupel]
                        elif key[1] is tupel[1]:
                            #inter.update({(tupel[0],key[0]):inter[tupel]})
                            inter.update({(tupel[0], key[0]): glacier_poly[tupel[0]].intersection(glacier_poly[key[0]])})
                            del inter[tupel]
                del glacier_poly[key[1]]
            elif shape(glacier_poly[key[0]]).area > shape(glacier_poly[key[1]]).area:
                if (shape(glacier_poly[key[1]]).difference(inter[key]).buffer(-0.1)).buffer(0.1).type is 'Polygon':
                    #glacier_poly[key[1]]=(shape(glacier_poly[key[1]]).difference(inter[key]).buffer(-0.1)).buffer(0.1)
                    glacier_poly[key[1]] = shape(glacier_poly[key[1]]).difference(inter[key])
                else:
                    #glacier_poly[key[0]] = (shape(glacier_poly[key[0]]).difference(inter[key]).buffer(-0.1)).buffer(0.1)
                    glacier_poly[key[0]] = shape(glacier_poly[key[0]]).difference(inter[key])
            else:
                if (shape(glacier_poly[key[0]]).difference(inter[key]).buffer(-0.1)).buffer(0.1).type is 'Polygon':
                    #glacier_poly[key[0]] = (shape(glacier_poly[key[0]]).difference(inter[key]).buffer(-0.1)).buffer(0.1)
                    glacier_poly[key[0]] = shape(glacier_poly[key[0]]).difference(inter[key])
                else:
                    #glacier_poly[key[1]] = (shape(glacier_poly[key[1]]).difference(inter[key]).buffer(-0.1)).buffer(0.1)
                    glacier_poly[key[1]] = shape(glacier_poly[key[1]]).difference(inter[key])
        del inter[key]
    #check if final_glaciers are not sliver polygon:
    keys=glacier_poly.keys()
    for glac_id in keys:
        glac=glacier_poly[glac_id]
        if glac.area < 100000 or (glac.area < 200000 and compactness(glac)):
            del glacier_poly[glac_id]
            glacier_poly=merge_silver_poly(glacier_poly,glac)

    with fiona.open(os.path.dirname(P_glac_dir) + '/outlines.shp', 'r') as outline:
        properties=outline.next()['properties']
        i=1
        for pol in glacier_poly:
            if not os.path.isdir(os.path.dirname(P_glac_dir)+'/divide_'+str(i).zfill(2)):
                os.mkdir(os.path.dirname(P_glac_dir)+'/divide_'+str(i).zfill(2))
            with fiona.open(os.path.dirname(P_glac_dir)+'/divide_'+str(i).zfill(2)+'/outlines.shp',"w", "ESRI Shapefile",outline.schema, crs) as gla:
                #for pol in glacier_poly
                properties['AREA']=glacier_poly[pol].area/1000000
                gla.write({'properties': properties,'geometry': mapping(glacier_poly[pol])})
            i=i+1
        print 'glaciers:',i-1

def merge_silver_poly(glacier_poly,polygon):
    max_boundary = 0
    max_boundary_id = -1
    for i, glac in glacier_poly.iteritems():
        if polygon.boundary.intersection(glac).length > max_boundary:
            max_boundary_id = i
            max_boundary = polygon.boundary.intersection(glac).length
    if not max_boundary_id == -1:
        glacier_poly[max_boundary_id] = glacier_poly[max_boundary_id].union(shape(polygon))
    return glacier_poly

def p_glac_radius(a, b, F):
    if a * (F ** b) < 3500:
        return a * (F ** b)
    else:
        return 3500

def preprocessing(input_dem,input_shp):
    #buffer1
    buffer1_shp=buffer(input_shp,pixelsize*4)
    #buffer2
    buffer2_shp=buffer(input_shp,pixelsize*2)
    #clip dem along buffer1
    masked_dem=clip(input_dem,buffer1_shp,'masked')
    #create gutter
    gutter_dem=gutter(masked_dem,input_shp,buffer2_shp,100)
    return gutter_dem

if __name__ == '__main__':
    
    cfg.initialize()
    base_dir = os.path.join(os.path.expanduser('/home/juliaeis/Dokumente/OGGM/work_dir'), 'GlacierDir_Example')
    #entity = gpd.GeoDataFrame.from_file(get_demo_file('Hintereisferner.shp')).iloc[0]
    #gdir = oggm.GlacierDirectory(entity, base_dir=base_dir)
    gdir=oggm.GlacierDirectory('RGI40-11.00746',base_dir=base_dir)
    entity = gpd.GeoDataFrame.from_file(
        '/home/juliaeis/Dokumente/OGGM/work_dir/GlacierDir_Example/RGI40-11.00687/outlines.shp').iloc[0]
    print gdir.dir
    #check if required files exists
    if gdir.has_file('outlines',div_id=0) and gdir.has_file('dem', div_id=0):
        pass
    else:
        tasks.define_glacier_region(gdir,entity=entity)
    #tasks.glacier_masks(gdir)
    ###################preprocessing########################
    input_shp =gdir.get_filepath('outlines',div_id=0)
    input_dem=gdir.get_filepath('dem',div_id=0)

    #get pixel size
    with rasterio.open(input_dem) as dem:
        global pixelsize
        pixelsize=int(dem.transform[1])
    gutter_dem=preprocessing(input_dem, input_shp)

    ####flow accumulation
    flow_gutter=flowacc(gutter_dem)

    #flowshed calculation
    [all_PP,P_glac,watersheds]=flowsheds(flow_gutter)

    merge_flowsheds(P_glac,watersheds)
    #delete files which are not needed anymore
    for file in os.listdir(os.path.dirname(input_shp)):
        for word in ['buffer','flow','gutter','masked']:
            if file.startswith(word):
                os.remove(os.path.dirname(input_shp)+'/'+file)

    #test if it works
    '''
    from oggm import graphics

    tasks.glacier_masks(gdir)
    tasks.compute_centerlines(gdir)
    tasks.compute_downstream_lines(gdir)
    tasks.catchment_area(gdir)
    tasks.initialize_flowlines(gdir)
    tasks.catchment_width_geom(gdir)

    graphics.plot_centerlines(gdir)
    plt.show()
    '''

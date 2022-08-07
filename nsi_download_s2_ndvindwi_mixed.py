import os
from tempfile import TemporaryFile
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
from rasterio.enums import Resampling
import numpy as np
import rasterio as rio
import rioxarray
import xarray as xr
import rasterio
def readLastRowNDWI(NDWIpath):
    NDWIpath = NDWIpath
    NDWIfile = "NDWIList.txt"

    try:
        with open(f"{NDWIpath}/{NDWIfile}", "r") as file:
            last_line = file.readlines()[-1]
            if len(last_line)!= 0:
                data = last_line.split(" ")
                path =data[-1].strip("\n")
                print(path)
                tile = path.split("/")[2]
                date = path.split("/")[3]
                print(tile, date)
                return tile, date
            else:
                print("There is nothing in the NDWIfile.txt")


    except Exception as e:
        print("Exception error during read NDWIlist file")
def updateListNDWI(ti):
    NDWIpath = "/home/airflow/NDWIList"
    bucket = "nsi-satellite-prototype"
    bucket_folder = "sprectral_index"
    idxc = "NDWI"
    command = f"aws s3 ls s3://{bucket}/{bucket_folder}/{idxc}/ --recursive > {NDWIpath}/NDWIList.txt"


    try:
        os.system(command)
        print(command)
        tile, date = readLastRowNDWI(NDWIpath)
        ti.xcom_push(key='tile_date', value=[tile,date])

    except Exception as e:
        print("Exception error while processing update list of NDWI from S3")
def createListDownload(tile, date):
    print("Create List Download ", tile, date)
    outputFolder = "/home/airflow/raw"

    if not os.path.exists(outputFolder+"/"+tile):
        os.mkdir(outputFolder+"/"+tile)
    b2 =  f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B02*.jp2' {outputFolder}/{tile}/{date}/{date}_B02.jp2"
    b3 =  f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B03*.jp2' {outputFolder}/{tile}/{date}/{date}_B03.jp2"
    b4 =  f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B04*.jp2' {outputFolder}/{tile}/{date}/{date}_B04.jp2"
    b8 =  f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B08*.jp2' {outputFolder}/{tile}/{date}/{date}_B08.jp2"
    tci = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*TCI*.jp2' {outputFolder}/{tile}/{date}/{date}_tci.jp2"
    scl = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R20m/*SCL*.jp2' {outputFolder}/{tile}/{date}/{date}_scl.jp2"
    meta = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/MTD_MSIL2A.xml' {outputFolder}/{tile}/{date}/{date}_meta.xml"

    listdownload_command = [b2, b3, b4, b8, tci, scl, meta]
    return listdownload_command

# def downloadS2(ti):
#     tile, backdate = ti.xcom_pull(key="tile_date", task_ids=["updateListNDWI_process"])[0]
#     today = str(datetime.today().date()).replace("-","")
#     list_date = pd.date_range(start=backdate, end=today)
#     list_date = sorted([str(i.date()).replace("-","") for i in list_date])
#     list_date.pop(0)
#     print(list_date)
#     print(f"start : {backdate} and enddate : {today}")
#     for date_i in list_date[1:]:
#         listDownload_command = createListDownload(tile, date_i)
#         for i in listDownload_command:
#             print("Download Command ", i)
#             os.system(i)
#     ti.xcom_push(key='tile_date', value=[tile,list_date])
def calculateNDWI(B8:str, B3:str):
     try:
        np.seterr(invalid='ignore')
        B8_src = rio.open(B8)
        B3_src = rio.open(B3)
        B8_arr = B8_src.read(1)
        B3_arr = B3_src.read(1)

        ndwi = (B3_arr.astype(np.float32)-B8_arr.astype(np.float32))/(B8_arr+B3_arr)
        ndwi_src = B3_src.profile
        print(B3, B8)
        print(ndwi_src)
        ndwi_src.update(count=1,compress='lzw', driver="GTiff",dtype=rio.float32)

        B8_src.close()
        B3_src.close()

        return ndwi_src, ndwi

     except:
        print("Read Image Error B8 :", B8, datetime.today())
def processndwi(ti):
    tile, backdate = ti.xcom_pull(key="tile_date", task_ids=["downloadS2_process"])[0]
    fullpath = "home/airflow"
    print(backdate)
    raw_folder = "raw"
    ndwi_folder = "ndwi"
    available_date = []
    if len(backdate)!=0:
        for i in backdate:
            #read from raw folder
            print(i)
            if os.path.exists(f"{fullpath}/{raw_folder}/{tile}/{i}"):
                available_date.append(i) #append date
                print(f"path exist: {fullpath}/{raw_folder}/{tile}/{i}")

                b8 = f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_B08.jp2"
                b3 = f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_B03.jp2"
                ndwi_src, ndwi = calculateNDWI(b8, b3)
                ndwi = ndwi.astype(np.float16)

                if not os.path.exists(f"{fullpath}/{ndwi_folder}"):
                    os.mkdir(f"{fullpath}/{ndwi_folder}")

                if not os.path.exists(f"{fullpath}/{ndwi_folder}/{tile}"):
                    os.mkdir(f"{fullpath}/{ndwi_folder}/{tile}")


                print("writing ndwi filename: ", f"{fullpath}/{ndwi_folder}/{tile}/{i}_NDWI.tif")

                with rio.open(f"{fullpath}/{ndwi_folder}/{tile}/{i}_NDWI.tif","w",**ndwi_src) as dst:
                    dst.write(ndwi.astype(rio.float32),1)
                    print("Wrote ndwi successfully")

        ti.xcom_push(key='available_date', value=[tile,available_date])

def processCloudremoved(ti):
    tile, available_date = ti.xcom_pull(key="available_date",task_ids=["writeNDWI_process"])[0]
    fullpath = "home/airflow"
    ndwi_folder = "ndwi"
    raw_folder = "raw"
    ndwiRC_folder = "ndwiRC"

    NDWIrc_list = []
    if len(available_date)!=0:
        for date in available_date:
            print("available date",date)
            scl_band = f"{fullpath}/{raw_folder}/{tile}/{date}/{date}_scl.jp2"
            ndwi_band = f"{fullpath}/{ndwi_folder}/{tile}/{date}_NDWI.tif"
            ndwirc_band = f"{fullpath}/{ndwiRC_folder}/{tile}/{date}_NDWI.tif"

            if not os.path.exists(f"{fullpath}/{ndwiRC_folder}"):
                os.mkdir(f"{fullpath}/{ndwiRC_folder}")
            if not os.path.exists(f"{fullpath}/{ndwiRC_folder}/{tile}"):
                os.mkdir(f"{fullpath}/{ndwiRC_folder}/{tile}")

            band_img, meta = removeCloud(scl_band, ndwi_band)
            with  rio.open(ndwirc_band,"w",**meta) as dst:
                dst.write(band_img.astype(rio.float32))
                print("NDWIrc was written successfully")
            NDWIrc_list.append(ndwirc_band)

        ti.xcom_push(key='NDWIrc_list', value=[tile,NDWIrc_list])
def cld_free_rgb(ti):
    tile, backdate = ti.xcom_pull(key="tile_date", task_ids=["downloadS2_process"])[0]
    fullpath = "home/airflow"
    print(backdate)
    raw_folder = "raw"
    rgb_folder = "rgbRC"
    available_date = []
    #backdate=['2','3','20220802']
    if len(backdate)!=0:
        for i in backdate:
            print(i)
            try:
                print('l')
                print('finddatetci')
                xds = rioxarray.open_rasterio(f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_scl.jp2")
                new_width = xds.rio.width * 2
                new_height = xds.rio.height * 2
                xds_upsampled = xds.rio.reproject(
                    xds.rio.crs,
                    shape=(new_height, new_width),
                    resampling=Resampling.bilinear,)
                scl_upsample=xds_upsampled

#                img_raw=rio.open(f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_tci.jp2")
#                img1=img_raw.read(1)
                img1=rioxarray.open_rasterio(f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_tci.jp2")[0]
                img2=rioxarray.open_rasterio(f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_tci.jp2")[1]
                img3=rioxarray.open_rasterio(f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_tci.jp2")[2]
                print('passtci')
                imgscl=scl_upsample[0]
                #print(imgscl)
            #except:
                #print('d')
                img1=img1.where((imgscl>=3) & (imgscl<=7),np.nan)
                #img1=img1.where((scl>3) & (scl<=7),np.nan)
                img2=img2.where((imgscl>=3) & (imgscl<=7),np.nan)
                #img2=img2.where((scl>3) & (scl<=7),np.nan)
                img3=img3.where((imgscl>=3) & (imgscl<=7),np.nan)
                #img3=img3.where((scl>3) & (scl<=7),np.nan)
                print(img1)
            #except:
                #print('d')
                img1.rio.to_raster('/home/airflow/rgbtemp/img1_tci.tif')
                img2.rio.to_raster('/home/airflow/rgbtemp/img2_tci.tif')
                img3.rio.to_raster('/home/airflow/rgbtemp/img3_tci.tif')
                bands=['/home/airflow/rgbtemp/img3_tci.tif','/home/airflow/rgbtemp/img2_tci.tif','/home/airflow/rgbtemp/img1_tci.tif']
            #except:
                #print('l')
                with rasterio.open(bands[0]) as src:
                    meta=src.meta
                meta.update(count = 3,compress='lzw',dtype=rasterio.uint8,nodata=0,nodatavals=(0,0,0))
                output_band = f"{fullpath}/{rgb_folder}/{tile}/{i}_tci.tif"
                with rasterio.open(output_band,'w',**meta) as dst:
                    for id, layer in enumerate(bands,start=1):
                        with rasterio.open(layer) as src1:
                            norm=src1.read(1).astype(np.uint8)
                            #norm=(src1.read(1) * (255 / np.max(src1.read(1)))).astype(np.uint16)
                            dst.write_band(id,norm)
            except:
                print('nodate')

def uploadS3_ndwi(ti):
    tile, available_date = ti.xcom_pull(key="available_date",task_ids=["writeNDWI_process"])[0]
    fullpath = "home/airflow"
    ndwiRC_folder = "ndwiRC"
    ndwiCL_folder='ndwiCL'
    if len(available_date)!=0:
        try:
            for date in available_date:
                bucket = f"s3://nsi-satellite-prototype/sprectral_index/NDWI/{tile}/{date}/{date}_NDWI.tif"
                source = f"{fullpath}/{ndwiRC_folder}/{tile}/{date}_NDWI.tif"
                os.system(f"aws s3 cp {source} {bucket}")
                print("uploaded to S3: ", bucket)
                bucket = f"s3://nsi-satellite-prototype/sprectral_index/NDWICL/{tile}/{date}/{date}_ndwi_colormap.tif"
                source = f"{fullpath}/{ndwiCL_folder}/{tile}/{date}_ndwi_colormap.tif"
                os.system(f"aws s3 cp {source} {bucket}")
                print("uploaded to S3: ", bucket)

        except Exception as e:
            print("Error happened during upload to S3")

def uploadS3_tci(ti):
    tile, available_date = ti.xcom_pull(key="available_date",task_ids=["writeNDWI_process"])[0]
    fullpath = "home/airflow"
    rgbRC_folder = "rgbRC"
    #available_date=['s','ew','20220802']
    if len(available_date)!=0:
        try:
            for date in available_date:
                print(date)
                bucket = f"s3://nsi-satellite-prototype/sprectral_index/TCI/{tile}/{date}/{date}_tci.tif"
                source = f"{fullpath}/{rgbRC_folder}/{tile}/{date}_tci.tif"
                os.system(f"aws s3 cp {source} {bucket}")
                print("uploaded to S3: ", bucket)
        except Exception as e:
            print("Error happened during upload to S3")
def cmap_indicator_ndwi(ti):
    tile, available_date = ti.xcom_pull(key="available_date",task_ids=["writeNDWI_process"])[0]
    print(available_date)
    if len(available_date)!=0:
        print('pass')
        #try:
        for date in available_date:
            ndwiRC_folder='ndwiRC'
            fullpath = "home/airflow"
            idx_dst=f"{fullpath}/{ndwiRC_folder}/{tile}/{date}_NDWI.tif"
            idx_col='/home/airflow/colormap/ndwi/color_ndwi.txt'
            fullpath='/home/airflow/colormap'
            clmapfolder='ndwi'
            dst=f"{fullpath}/{clmapfolder}/{tile}/{date}_ndwi_colormap.tif"
                #try: 
            os.system("/bin/gdaldem color-relief -nearest_color_entry -alpha {} {} {}".format(idx_dst,idx_col,dst))
            print('generate cmap')
def uploadS3_ndwi_colormap(ti):
    tile, available_date = ti.xcom_pull(key="available_date",task_ids=["writeNDWI_process"])[0]
    fullpath = "home/airflow/colormap"
    rgbRC_folder = "ndwi"
    if len(available_date)!=0:
        try:
            for date in available_date:
                print(date)
                bucket = f"s3://nsi-satellite-prototype/sprectral_index/NDWICL/{tile}/{date}/{date}_ndwi_colormap.tif"
                source = f"{fullpath}/{rgbRC_folder}/{tile}/{date}_ndwi_colormap.tif"
                os.system(f"aws s3 cp {source} {bucket}")
                print("uploaded to S3: ", bucket)
        except Exception as e:
            print("Error happened during upload to S3")


def cmap_indicator_ndvi(ti):
    tile, available_date = ti.xcom_pull(key="available_date",task_ids=["writeNDWI_process"])[0]
    print(available_date)
    if len(available_date)!=0:
        print('pass')
        #try:
        for date in available_date:
            ndwiRC_folder='ndviRC'
            fullpath = "home/airflow"
            idx_dst=f"{fullpath}/{ndwiRC_folder}/{tile}/{date}_NDVI.tif"
            idx_col='/home/airflow/colormap/ndwi/color_ndvi.txt'
            fullpath='/home/airflow/colormap'
            clmapfolder='ndvi'
            dst=f"{fullpath}/{clmapfolder}/{tile}/{date}_ndvi_colormap.tif"
                #try: 
            os.system("/bin/gdaldem color-relief -nearest_color_entry -alpha {} {} {}".format(idx_dst,idx_col,dst))
            print('generate cmap')
def uploadS3_ndvi_colormap(ti):
    tile, available_date = ti.xcom_pull(key="available_date",task_ids=["writeNDWI_process"])[0]
    fullpath = "home/airflow/colormap"
    rgbRC_folder = "ndvi"
    if len(available_date)!=0:
        try:
            for date in available_date:
                print(date)
                bucket = f"s3://nsi-satellite-prototype/sprectral_index/NDVICL/{tile}/{date}/{date}_ndvi_colormap.tif"
                source = f"{fullpath}/{rgbRC_folder}/{tile}/{date}_ndvi_colormap.tif"
                os.system(f"aws s3 cp {source} {bucket}")
                print("uploaded to S3: ", bucket)
        except Exception as e:
            print("Error happened during upload to S3")

def readLastRowNDVI(NDVIpath):
    NDVIpath = NDVIpath
    NDVIfile = "NDVIList.txt"

    try:
        with open(f"{NDVIpath}/{NDVIfile}", "r") as file:
            last_line = file.readlines()[-1]
            if len(last_line)!= 0:
                data = last_line.split(" ")
                path =data[-1].strip("\n")
                print(path)
                tile = path.split("/")[2]
                date = path.split("/")[3]
                print(tile, date)
                return tile, date
            else:
                print("There is nothing in the NDVIfile.txt")
    except Exception as e:
        print("Exception error during read NDVIlist file")


def updateListNDVI(ti):
    NDVIpath = "/home/airflow/NDVIList"
    bucket = "nsi-satellite-prototype"
    bucket_folder = "sprectral_index/NDVI"
    idxc = "NDVI"
    command = f"aws s3 ls s3://{bucket}/{bucket_folder}/{idxc}/ --recursive > {NDVIpath}/NDVIList.txt"


    try:
        os.system(command)
        print(command)
        tile, date = readLastRowNDVI(NDVIpath)
    except Exception as e:
        print("Exception error while processing update list of NDVI from S3")


# def createListDownload(tile, date):
#     print("Create List Download ", tile, date)
#     outputFolder = "/home/airflow/raw"

#     if not os.path.exists(outputFolder+"/"+tile):
#         os.mkdir(outputFolder+"/"+tile)

#     b2 =  f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B02*.jp2' {outputFolder}/{tile}/{date}/{date}_B02.jp2"
#     b3 =  f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B03*.jp2' {outputFolder}/{tile}/{date}/{date}_B03.jp2"
#     b4 =  f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B04*.jp2' {outputFolder}/{tile}/{date}/{date}_B04.jp2"
#     b8 =  f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B08*.jp2' {outputFolder}/{tile}/{date}/{date}_B08.jp2"
#     tci = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*TCI*.jp2' {outputFolder}/{tile}/{date}/{date}_tci.jp2"
#     scl = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R20m/*SCL*.jp2' {outputFolder}/{tile}/{date}/{date}_scl.jp2"
#     meta = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/MTD_MSIL2A.xml' {outputFolder}/{tile}/{date}/{date}_meta.xml"

#     listdownload_command = [b2, b3, b4, b8, tci, scl, meta]
#     return listdownload_command

def downloadS2(ti):

    tile, backdate = ti.xcom_pull(key="tile_date", task_ids=["updateListNDVI_process"])[0]
    today = str(datetime.today().date()).replace("-","")
    list_date = pd.date_range(start=backdate, end=today)
    list_date = sorted([str(i.date()).replace("-","") for i in list_date])
    list_date.pop(0)
    print(list_date)
    print(f"start : {backdate} and enddate : {today}")
    for date_i in list_date[1:]:
        listDownload_command = createListDownload(tile, date_i)
        for i in listDownload_command:
            print("Download Command ", i)
            os.system(i)
    ti.xcom_push(key='tile_date', value=[tile,list_date])
def calculateNDVI(B8:str, B4:str):
     try:
        np.seterr(invalid='ignore')
        B8_src = rio.open(B8)
        B4_src = rio.open(B4)

        B8_arr = B8_src.read(1)
        B4_arr = B4_src.read(1)

        ndvi = (B8_arr.astype(np.float32)-B4_arr.astype(np.float32))/(B8_arr+B4_arr)
        ndvi_src = B4_src.profile
        print(B4, B8)
        print(ndvi_src)
        ndvi_src.update(count=1,compress='lzw', driver="GTiff",dtype=rio.float32)

        B8_src.close()
        B4_src.close()
     except:
        print("Read Image Error B8 :", B8, datetime.today())
        print("Read Image Error B4 :", B4, datetime.today())
def processNDVI(ti):
    tile, backdate = ti.xcom_pull(key="tile_date", task_ids=["downloadS2_process"])[0]
    fullpath = "home/airflow"
    print(backdate)
    raw_folder = "raw"
    ndvi_folder = "ndvi"
    available_date = []
    if len(backdate)!=0:
        for i in backdate:
            #read from raw folder
            print(i)
            if os.path.exists(f"{fullpath}/{raw_folder}/{tile}/{i}"):
                available_date.append(i) #append date
                print(f"path exist: {fullpath}/{raw_folder}/{tile}/{i}")

                b8 = f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_B08.jp2"
                b4 = f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_B04.jp2"

                ndvi_src, ndvi = calculateNDVI(b8, b4)
                ndvi = ndvi.astype(np.float16)

                if not os.path.exists(f"{fullpath}/{ndvi_folder}"):
                    os.mkdir(f"{fullpath}/{ndvi_folder}")

                if not os.path.exists(f"{fullpath}/{ndvi_folder}/{tile}"):
                    os.mkdir(f"{fullpath}/{ndvi_folder}/{tile}")


                print("writing NDVI filename: ", f"{fullpath}/{ndvi_folder}/{tile}/{i}_NDVI.tif")

                with rio.open(f"{fullpath}/{ndvi_folder}/{tile}/{i}_NDVI.tif","w",**ndvi_src) as dst:
                    dst.write(ndvi.astype(rio.float32),1)
                    print("Wrote NDVI successfully")

        ti.xcom_push(key='available_date', value=[tile,available_date])

def removeCloud(scl, band):

    try:
        scl_src = rio.open(scl)
        band_src = rio.open(band)
        #scl_img = scl_src.read(1) #keep scl only 4,5 6
        band_img = band_src.read()
        band_img = band_img.astype(np.float16)
        meta = band_src.profile
        #upsamling SCL
        data = scl_src.read(out_shape=(scl_src.count,
                                        int(scl_src.height*2), int(scl_src.width*2)),
                                        resampling=Resampling.nearest)
        #transform = scl_src.transform*scl_src.transform.scale((scl_src.width/data.shape[-1]), (scl_src.height/data.shape[-2]))
        band_img[(data<4)|(data>=7)] = np.nan
        band_img = band_img.astype(np.float16)
        meta.update(dtype=rio.float32, nodata=np.nan,compress='lzw')
        clremoved_meta = meta.copy()
        print("Cloud was removed")
        scl_src.close()
        band_src.close()

        return band_img, clremoved_meta

    except Exception as e:
        print("error happened in removeCloud process")
def processCloudremovedndvi(ti):
    tile, available_date = ti.xcom_pull(key="available_date",task_ids=["writeNDVI_process"])[0]
    fullpath = "home/airflow"
    ndvi_folder = "ndvi"
    raw_folder = "raw"
    ndviRC_folder = "ndviRC"

    NDVIrc_list = []
    if len(available_date)!=0:
        for date in available_date:
            print("available date",date)
            scl_band = f"{fullpath}/{raw_folder}/{tile}/{date}/{date}_scl.jp2"
            ndvi_band = f"{fullpath}/{ndvi_folder}/{tile}/{date}_NDVI.tif"
            ndvirc_band = f"{fullpath}/{ndviRC_folder}/{tile}/{date}_NDVI.tif"

            if not os.path.exists(f"{fullpath}/{ndviRC_folder}"):
                os.mkdir(f"{fullpath}/{ndviRC_folder}")
            if not os.path.exists(f"{fullpath}/{ndviRC_folder}/{tile}"):
                os.mkdir(f"{fullpath}/{ndviRC_folder}/{tile}")

            band_img, meta = removeCloud(scl_band, ndvi_band)
            with  rio.open(ndvirc_band,"w",**meta) as dst:
                dst.write(band_img.astype(rio.float32))
                print("NDVIrc was written successfully")
            NDVIrc_list.append(ndvirc_band)

        ti.xcom_push(key='NDVIrc_list', value=[tile,NDVIrc_list])

def uploadS3ndvi(ti):
    tile, available_date = ti.xcom_pull(key="available_date",task_ids=["writeNDVI_process"])[0]
    fullpath = "home/airflow"
    ndviRC_folder = "ndviRC"
    if len(available_date)!=0:
        try:
            for date in available_date:
                bucket = f"s3://nsi-satellite-prototype/sprectral_index/NDVI/{tile}/{date}/{date}_NDVI.tif"
                source = f"{fullpath}/{ndviRC_folder}/{tile}/{date}_NDVI.tif"
                os.system(f"aws s3 cp {source} {bucket}")
                print("uploaded to S3: ", bucket)
        except Exception as e:
            print("Error happened during upload to S3")
def remove_all_idx():
    ndwi_dir='/home/airflow/ndwi/54SVE/*'
    ndvi_dir='/home/airflow/ndvi/54SVE/*'
    raw_dir='/home/airflow/raw/54SVE/*'
    ndwi_rc='/home/airflow/ndwiRC/54SVE/*'
    ndvi_rc='/home/airflow/ndviRC/54SVE/*'
    tci_rc='/home/airflow/rgbRC/54SVE/*'
    tci_temp='/home/airflow/rgbtemp/*'
    cmap_ndwi='/home/airflow/colormap/ndwi/54SVE/*'
    cmap_ndvi='/home/airflow/colormap/ndvi/54SVE/*'
    list_rm=[ndwi_dir,raw_dir,ndwi_rc,tci_rc,tci_temp,cmap_ndwi,cmap_ndvi,ndvi_dir,ndvi_rc]
    for i in list_rm:
        os.system("rm -rf"+' '+i)
with DAG("NSI_preprocess_ndvindwi_mixed", start_date=datetime(2022,7,11),schedule_interval="@daily", catchup=False) as dag:
    updateNDVI = PythonOperator(task_id="updateListNDVI_process", python_callable=updateListNDVI)
    updateNDWI = PythonOperator(task_id="updateListNDWI_process", python_callable=updateListNDWI)
    download_s2 = PythonOperator(task_id="downloadS2_process",python_callable=downloadS2)
    writeNDVIFile = PythonOperator(task_id="writeNDVI_process", python_callable=processNDVI)
    writeNDVIRCFile = PythonOperator(task_id="writeNDVIRC_process", python_callable=processCloudremovedndvi)
    uploadtoS3ndvi = PythonOperator(task_id="uploadS3_processndvi", python_callable=uploadS3ndvi)
    writeNDWIFile = PythonOperator(task_id="writeNDWI_process", python_callable=processndwi)
    writeNDWIRCFile = PythonOperator(task_id="writeNDWIRC_process", python_callable=processCloudremoved)
    uploadtoS3_ndwi = PythonOperator(task_id="uploadS3_ndwi_process", python_callable=uploadS3_ndwi)
    rgb_RC=PythonOperator(task_id="tci_process", python_callable=cld_free_rgb)
    uploadtoS3_tci = PythonOperator(task_id="uploadS3_tci_process", python_callable=uploadS3_tci)
    writendwiCL=PythonOperator(task_id="ndwicmaps", python_callable=cmap_indicator_ndwi)
    uploadtoS3_ndwi_colormap = PythonOperator(task_id="uploadS3_ndwicolormap_process", python_callable=uploadS3_ndwi_colormap)
    writendviCL=PythonOperator(task_id="ndvicmaps", python_callable=cmap_indicator_ndvi)
    uploadtoS3_ndvi_colormap = PythonOperator(task_id="uploadS3_ndvicolormap_process", python_callable=uploadS3_ndvi_colormap)
    rmidx=PythonOperator(task_id="remove", python_callable=remove_all_idx)
updateNDVI >> download_s2 >> writeNDVIFile >>writeNDVIRCFile >> uploadtoS3ndvi >> writeNDWIFile >> writeNDWIRCFile >> uploadtoS3_ndwi >> rgb_RC >> uploadtoS3_tci >> writendwiCL >> uploadtoS3_ndwi_colormap >> writendviCL >> uploadtoS3_ndvi_colormap>> rmidx
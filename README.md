Feature Layer Updating Tool for Rivers (FLUTR)

Summary:
'create_feature_layer.py' gets geo data from a website via bulk downloads or an api. It loads the data into a 'pandas dataframe' and processes it as required, including the determination of CaBA catchments for each feature using a spatial join. It is then converted to GeoJSON and written to file in the working directory. Finally, it is published as a hosted feature layer and corresponding feature layer view in ArcGIS Online. The program is object oriented in design. This reduces repeated code and makes it amenable to the addition of functionality, e.g. adding a new web-based data source.

'overwrite_feature_layer.py' differs from 'create_feature_layer.py' in that it uses the geo data to update an existing hosted feature layer rather than publishing a new one. N.B. the corresponding feature layer view will reflect the new data set without modification of the symbology.

NB All CC-BY-NC licenced records have been removed as these records cannot be used for commercial purposes without prior agreement of the data provider. To include these in future, comment out relevant four lines of code in construct_df() in nbn_occurrences.py

Dependencies:
 - Windows operating system
 - ArcGIS Online login
 - ArcGIS Pro installation
 - 'osgb' library - do a 'pip install osgb' from command line once within conda environment (see below)
 - GeoPandas - see installation instructions below
 - Unzipped folder in working directory named CaBA_Partnership_boundaries containing CaBA_Partnership_boundaries.shp shapefile


Instructions:
 - There are two scripts:
	- create_feature_layer.py
	- overwrite_feature_layer.py
 - Run scripts from within a clone of the ArcGIS Pro Python3 conda environment:
    - Create a new folder which will be used as a working directory
    - Go to Start Menu and type and open Python Command Prompt
	- *Enter 'conda create --clone arcgispro-py3 --name arcgispro-py3_clone' This can take some time to process
	- Then enter 'proswap arcgispro-py3_clone'
	- *Enter 'conda install geopandas -y' NB an older version is installed - this means that some geopandas functions do not work eg geopandas.GeoDataFrame.sjoin() This step can also take some time
        - *If above fails, try installing geopandas as follows:
		- 'conda config --add channels conda-forge'
		- 'conda config --add channels defaults'
		- 'conda install -c conda-forge geopandas' If this doesn't work, you need to find a way of installing geopandas. This is a known conflict and bug in ArcGIS Pro 08/11/2022
	- *'pip install osgb'	
	- Then type 'cd' followed by the path to your working directory
	- Save all the python scripts from Github into this directory 
	- Export and then download a copy of the CaBA Catchment Partnerships layer as a shapefile. Unzip and save in the working directory in a sub folder named CaBA_Partnership_boundaries. Ensure all items in this sub folder are renamed to 'CaBA_Partnership_boundaries', but retaining the original file extension
	- Enter arguments as required on command line - see examples below. To overwrite a layer, the item ID of the feature layer needs to be entered, NOT the item ID of the view layer.
	- If multifactor authorisation is enabled in AGOL, this will need to be temporarily disabled to allow the tool to log in to AGOL

- *Means only required to be done once. Not required on subsequent running of tool.
	
python create_feature_service.py EA_survey_sites biosys RTMerlin.March password
python create_feature_service.py EA_survey_sites fish RTMerlin.March password
python create_feature_service.py EA_water_qual_archives sampling_points RTMerlin.March password
python create_feature_service.py NBNatlas_occurrences SignalCrayfish RTMerlin.March password
python create_feature_service.py NBNatlas_occurrences AmericanMink RTMerlin.March password
python create_feature_service.py NBNatlas_occurrences GoldenEagle RTMerlin.March password

python overwrite_feature_service.py EA_survey_sites_biosys 22b6fd8a360b417a825a2e95b6b65c8f RTMerlin.March password
python overwrite_feature_service.py EA_survey_sites_fish edcb5824c39b4f808a1e93ee660d0766 RTMerlin.March password
python overwrite_feature_service.py EA_water_qual_archives_sampling_points 37d45299fa1647d1b0881c0785bcb8a6 RTMerlin.March password
python overwrite_feature_service.py NBNatlas_occurrences_SignalCrayfish 50fb9a8c19a3404cbf042b82d424e300 RTMerlin.March password
python overwrite_feature_service.py NBNatlas_occurrences_AmericanMink 5adc3b4082e742a6acfe2f1a8e0bf644 RTMerlin.March password
python overwrite_feature_service.py NBNatlas_occurrences_GoldenEagle b1cbbc659e824855aece445e67471c40 RTMerlin.March password


Supporting documents:
 - This one!
 - Diagrams showing function call order and hierarchy, and program filing structure.
 
 
NBN Atlas Occurrences Notes:
The output feature layer comprises of a polygon layer and a corresponding point layer. 

The polygons are square regions indicating the area in which the species occurrence record is located. The polygons are inferred from the NBNatlas 'Coordinate uncertainty in meters' and the NBNatlas 'Latitude (WGS84)' and 'Longitude (WGS84)' data supplied via the NBNatlas web service API. As a result, the polygons do not strictly agree with the supplied NBNatlas 'Grid reference' due to (1) rounding error, and (2) occasional discrepencies in the NBNatlas data between 'Latitude (WGS84)' and 'Longitude (WGS84)', and 'Grid reference'.

A different approach here, in which the polygons were calculated directly from the the supplied NBNatlas 'Grid reference', would be an improvement. This however would present a further programming challenge. In particular, some of the supplied NBNatlas 'Grid reference' values are unusual e.g. NTSE, TL97G or SO5291644612.

The points indicate the supplied NBNatlas 'Latitude (WGS84)' and 'Longitude (WGS84)' data for each species occurrence record. The purpose of the points is to indicate the centre of the corresponding polygon. This is important because many of the polygons only become visible on the map when sufficiently 'zoomed-in'. The points are also accompanied by the NBNatlas 'Coordinate uncertainty in meters' for symbology purposes.

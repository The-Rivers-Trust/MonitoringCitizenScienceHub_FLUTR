# Standard library imports (https://docs.python.org/3/py-modindex.html)
import json
import math
import re

# Related third party imports
import geopandas
import osgb # need to do a 'pip3 install osgb' from command line
import pandas
import requests
from shapely.wkt import loads
from shapely import wkt

# Local application imports
from geo_data import GeoData



''' 
A derived class that inherits from the base class GeoData
NB __init__ is not defined so the base class __init__ is inherited and 
used to construct a class object
'''

class NBNatlasOccurrences(GeoData):
   
    # Define class attributes:
    url_records = 'https://records-ws.nbnatlas.org/occurrences/search?q='
    occurrences_list_of_dicts = []

                
    def get_data(self): # Using NBN Atlas API
    
        # Determine number of observations
        NBNatlasOccurrences.url_records = \
            NBNatlasOccurrences.url_records + '"' + self.dataobject + '"' \
                + '&fq=occurrence_status:present'
                  
        print(f'\nGetting occurrence count of {self.dataobject} ' \
              f'from NBN Atlas API:\n{NBNatlasOccurrences.url_records}\n')
        occurrences_response = requests.get(f'{NBNatlasOccurrences.url_records}')

        occurrences_response_dict = json.loads(occurrences_response.text) # JSON object

        num_observations = occurrences_response_dict['totalRecords']
        print(f'\n{num_observations} occurrences of {self.dataobject} found.')

        if (num_observations == 0):
            raise SystemExit()

        # Get data, num_returned_max at a time
        num_returned_max = 10000
        page_size = num_returned_max
        start_index = 0
        total_num_downloaded = 0
        
        while total_num_downloaded < num_observations:
        
            url = \
                NBNatlasOccurrences.url_records \
                    + '&pageSize=' \
                    + str(num_returned_max) \
                    + '&startIndex=' \
                    + str(start_index)

            print(f'\nGetting occurrence data of {self.dataobject} ' \
                  f'from NBN Atlas API:\n{url}\n')
            occurrences_response = requests.get(f'{url}')
            
            # Join any downloaded occurrences to NBNatlasOccurrences.occurrences_list_of_dicts
            NBNatlasOccurrences.occurrences_list_of_dicts = \
                NBNatlasOccurrences.occurrences_list_of_dicts \
                    + json.loads(occurrences_response.text)['occurrences'] 
                                      
            num_downloaded = len(json.loads(occurrences_response.text)['occurrences']) 
            total_num_downloaded += num_downloaded
            print(f'Number of occurrences downloaded: {num_downloaded}')
            print(f'Total number of occurrences downloaded: {total_num_downloaded}')
                       
            start_index += num_returned_max
                    

    def process_data(self):
        # Construct dataframe and write result to base class attribute 'dataframe'
        self.dataframe = \
            self.construct_df(NBNatlasOccurrences.occurrences_list_of_dicts)
         
        # Calculate square occurrence regions to display on map
        self.dataframe = \
            self.calculate_osgb_polygon(self.dataframe)

        # Determine CaBA catchments 
        self.determine_catchment()            
#        raise SystemExit()
              
         # Append 'placeholder'
        self.append_placeholder()
       
       
    @staticmethod # Since does not access or write to any class attributes  
    def construct_df(dicts): 
#        # Flatten data
#        dataframe = pandas.json_normalize(dict, record_path=['occurrences'])
        dataframe = pandas.DataFrame(dicts)
                 
        #  Drop fields, reorder, re'type', reindex and rename
        columns_required = ['dataProviderName',
                            'decimalLatitude',
                            'decimalLongitude',
                            'coordinateUncertaintyInMeters',
                            'gridReference',
                            'identificationVerificationStatus',
                            'license',
                            'locationId',
                            'scientificName',
                            'vernacularName',
#                            'month',
                            'year']
        dataframe = dataframe[columns_required]
        dataframe = dataframe.sort_values(['locationId'], ascending=[True], ignore_index=True)
#        dataframe = dataframe.sort_values(['coordinateUncertaintyInMeters'], ascending=[False], ignore_index=True)
        renaming = {'decimalLatitude': 'lat', 
                    'decimalLongitude': 'long', 
                    'coordinateUncertaintyInMeters': 'coordUnc', 
                    'gridReference': 'gridRef', 
                    'identificationVerificationStatus': 'idVerificationStatus'}
        dataframe = dataframe.rename(columns=renaming, errors='raise')
        
        num_rows = len(dataframe)
        
        # Drop any rows that lack lat/long data
        dataframe = dataframe.dropna(subset=['lat', 'long'], axis=0)
        num_ungeotagged_rows = num_rows - len(dataframe)
        print(f'\n{num_ungeotagged_rows} occurrence(s) removed without lat and long data.')
        dataframe = dataframe.reset_index(drop=True)        
        num_rows = len(dataframe)

        # Drop any rows that contain the license type CC-BY-NC (Change requested by Catherine after dicussion with NBN Atlas)
        dataframe = dataframe[dataframe["license"].str.fullmatch("CC-BY-NC")==False]
        num_commercial_license_rows = num_rows - len(dataframe)
        print(f'{num_commercial_license_rows} occurrence(s) removed containing CC-BY-NC license.')
        dataframe = dataframe.reset_index(drop=True)        
        num_rows = len(dataframe)
        
        # Drop any rows with coordinateUncertaintyInMeters unspecified 
        dataframe = dataframe.dropna(subset=["coordUnc"])
        num_coordUnc_unspecified_rows = num_rows - len(dataframe)
        print(f'{num_coordUnc_unspecified_rows} occurrence(s) removed containing unspecified coordinateUncertaintyInMeters.\n')
        dataframe = dataframe.reset_index(drop=True)
        num_rows = len(dataframe)
        
        # Replace any NaN values with ""
        dataframe = dataframe.fillna("") 
        
        '''Because some dates are in years and months, and some are in years
        only, we decided only publish year data. AGOL doesn't recognise YYYY
        as a date format so analysis on a feature layer with a mixture of 
        YYYY and YYYY-MM is not straightforward.'''
        # Re'type' 'year'
        dataframe['year'] = dataframe['year'].astype(str) 
        # NB 'year' in form 2005.0 so remove '.0'         
        for row in dataframe.itertuples():
            dataframe.at[row.Index, 'year'] = \
                dataframe.at[row.Index, 'year'].rsplit('.', 1)[0]
        
        columns_required = ['dataProviderName',
                            'lat',
                            'long',
                            'coordUnc',
                            'gridRef',                    
                            'idVerificationStatus',
                            'license',
                            'locationId',
                            'scientificName',
                            'vernacularName',
                            'year']                            
        dataframe = dataframe[columns_required]
        
        return dataframe
        
    
    @staticmethod # Since does not access or write to any class attributes  
    def calculate_osgb_polygon(dataframe):
        ''' Create an osgb_polygon column then, using NBNatlas inferred lat and long 
        (in WGS84) of centre of square geographic region, and coordUnc(ertaintyInMetres), 
        infer coords of region in eastings and northings then convert to WGS84. 
        Write list of 5 coords to osgb_polygon - 1st and 5th equal.'''
        
        print(f'\nCalculating occurrence polygons...\n')
        dataframe['osgb_polygon'] = ''
        dataframe['inferred_grid_size'] = ''
        
        for row in dataframe.itertuples():

            half_diagonal = dataframe.at[row.Index, 'coordUnc']
            half_square_side = 0
            if half_diagonal == 0.7:
                half_square_side = 0.5
            elif half_diagonal == 7.1:
                half_square_side = 5
            elif half_diagonal == 70.7:
                half_square_side = 50
            elif half_diagonal == 707.1:
                half_square_side = 500
            elif half_diagonal == 1414.2:
                half_square_side = 1000
            elif half_diagonal == 7071.1:
                half_square_side = 5000
#            # Catch erroneous coordUncs which are given as square side lengths
#            elif half_diagonal % 10 == 0:
#                half_square_side = half_diagonal / 2
            elif half_diagonal == "":
                print(f'No coordinateUncertaintyInMeters provided for occurrence: ' \
                      f'{dataframe.iloc[[row.Index]]}]')
                continue
            else:        
                '''print(f"Unusual square size found with half diagonal {half_diagonal} " \
                      f"metres, grid reference {dataframe.at[row.Index, 'gridRef']}")'''
                # Pythagoras' Theorem to determine shorter side length
                half_square_side = math.sqrt((half_diagonal*half_diagonal)/2)
                    
            (centre_lat, centre_long) = \
                (dataframe.at[row.Index, 'lat'], dataframe.at[row.Index, 'long'])    
            (centre_easting, centre_northing) = osgb.ll_to_grid(centre_lat, centre_long)

            (sw_easting, sw_northing) = \
                (centre_easting - half_square_side, centre_northing - half_square_side)
            (nw_easting, nw_northing) = \
                (centre_easting - half_square_side, centre_northing + half_square_side)
            (ne_easting, ne_northing) = \
                (centre_easting + half_square_side, centre_northing + half_square_side)
            (se_easting, se_northing) = \
                (centre_easting + half_square_side, centre_northing - half_square_side)

            (sw_lat, sw_long) = osgb.grid_to_ll(sw_easting, sw_northing)
            (nw_lat, nw_long) = osgb.grid_to_ll(nw_easting, nw_northing)
            (ne_lat, ne_long) = osgb.grid_to_ll(ne_easting, ne_northing)
            (se_lat, se_long) = osgb.grid_to_ll(se_easting, se_northing)

            # Write as (long, lat) ready for GeoJSON
            sw_coord = [round(sw_long, 6), round(sw_lat, 6)] 
            nw_coord = [round(nw_long, 6), round(nw_lat, 6)]
            ne_coord = [round(ne_long, 6), round(ne_lat, 6)]
            se_coord = [round(se_long, 6), round(se_lat, 6)]

            dataframe.at[row.Index, 'osgb_polygon'] = \
                [sw_coord, nw_coord, ne_coord, se_coord, sw_coord]
            dataframe.at[row.Index, 'inferred_grid_size'] = \
                2 * half_square_side 
            
            square_side = 2 * half_square_side

            # Round to nearest integer                        
#            half_square_side = int(round(half_square_side, 1))            
            def round_half_up(n, decimals=0):
                multiplier = 10 ** decimals
                return math.floor(n*multiplier + 0.5) / multiplier
                                      
            square_side = int(round_half_up(square_side, 0)) 
                     
#            print(f"{dataframe.at[row.Index, 'coordUnc']}, {square_side}, {dataframe.at[row.Index, 'gridRef']}")
        
#        raise SystemExit()    
        return dataframe


    def determine_catchment(self):

        print(f'\nDetermining catchments using sjoin()...\n')

        # Set filepath - this could be a class variable or it could be downloaded
        caba_data_shapefile = '\\CaBA_Partnership_boundaries\\CaBA_Partnership_boundaries.shp'
        fp = self.user.working_dir + caba_data_shapefile

        # Construct a geodataframe from catchment data: read file using geopandas.read_file()
        gdf_caba = geopandas.read_file(fp)

        # Convert from coord reference system EPSG:27700 (OSGB) to ESPG:4326 (WGS84)
        gdf_caba = gdf_caba.to_crs(epsg=4326)

        # Round lat-long to 6 d.p. NB taken from somewhere online!
        simpledec = re.compile(r"\d*\.\d+")
        def mround(match):
            return "{:.6f}".format(float(match.group()))
        gdf_caba.geometry = gdf_caba.geometry.apply(lambda x: loads(re.sub(simpledec, mround, x.wkt)))

        # Construct a geodataframe from self.dataframe. NB EPSG:4326 is WGS84:
        
        # Convert the osgb polygons into WKT (well-known text representation of geometry) format
        wkt_df = self.dataframe 

        # A new column to store the converted osgb polygons:
        wkt_df['wkt_geometry'] = wkt_df['osgb_polygon']

        for row in wkt_df.itertuples():
            # Convert to str
            wkt_df.at[row.Index, 'wkt_geometry'] = \
                str(wkt_df.at[row.Index, 'wkt_geometry'])
                
            # Replace digit followed by a ',' with same digit followed by ''
            wkt_df.at[row.Index, 'wkt_geometry'] = \
                (re.sub(r'(\d),', r'\1', wkt_df.at[row.Index, 'wkt_geometry']))
                
            # Replace '[' with ''
            wkt_df.at[row.Index, 'wkt_geometry'] = \
                wkt_df.at[row.Index, 'wkt_geometry'].replace('[', '')

            # Replace ']' with ''
            wkt_df.at[row.Index, 'wkt_geometry'] = \
                wkt_df.at[row.Index, 'wkt_geometry'].replace(']', '')

            # Insert 'POLYGON '
            wkt_df.at[row.Index, 'wkt_geometry'] = \
                'POLYGON ((' + wkt_df.at[row.Index, 'wkt_geometry'] + '))'
        
        # Parse the WKT format
        wkt_df['wkt_geometry'] = geopandas.GeoSeries.from_wkt(wkt_df['wkt_geometry'])     
        
        # Construct geodataframe setting the geometry to converted osbg column data
        gdf_data_obj = geopandas.GeoDataFrame(
            wkt_df, 
            geometry=wkt_df['wkt_geometry'],
            crs='EPSG:4326'
            )       
        
        ''' Using top-level function geopandas.sjoin() rather than 
             geopandas.GeoDataFrame.sjoin(), since the latter is not defined in the 
             version of geopandas installed within cloned ArcGIS Pro (v2.9) conda
             environment.
            In a LEFT OUTER JOIN (how='left'), we keep all rows from the left and 
             duplicate them if necessary to represent multiple hits between the two 
             dataframes.'''
        gdf_new = geopandas.sjoin(gdf_data_obj, gdf_caba, how='left')
#        print(gdf_new.columns.tolist())

        # Delete unwanted columns
        to_drop = [
            'wkt_geometry',
            'geometry',
            'index_right',
            'CaBA_Websi',
            'EA_NE_Area',
            'EA_WM_Area',
            'X',
            'Y',
            'Shape__Are',
            'Shape__Len',
            #'Shape__A_1', These fields don't appear in newer version of CaBa partnership boundaries shapefile.
            #'Shape__L_1',
            'CaBA_Partn',
            'WFD_RBD'
            ]
        gdf_new = gdf_new.drop(columns=to_drop)
                
        ''' Check number of rows consistent then write results to 
             self.dataframe '''
        if (len(self.dataframe.index) == len(gdf_new.index)):
            self.dataframe = gdf_new
        elif (len(self.dataframe.index) < len(gdf_new.index)):
            self.dataframe = gdf_new
            self.dataframe = self.dataframe.reset_index(drop=True)
            print ("'geopandas.sjoin()' increased number of rows in dataframe. " \
                   "Some occurrence regions must lie in multiple catchments.\n")
        else:
            raise SystemExit("'geopandas.sjoin()' decreased number of rows in dataframe.")
            
#        print(self.dataframe.columns.tolist())
#        raise SystemExit()
        
        # Write empty strings to any Null values in new CaBA columns
        self.dataframe = self.dataframe.fillna("")
        
        # Convert 'CaBA_ID' values to type str 
        self.dataframe['CaBA_ID'] = self.dataframe['CaBA_ID'].astype(str) 
        # NB 'CaBA_ID' in form 78.0 so remove '.0'            
        for row in self.dataframe.itertuples():
            self.dataframe.at[row.Index, 'CaBA_ID'] = \
                self.dataframe.at[row.Index, 'CaBA_ID'].rsplit('.', 1)[0]   
      

    def append_placeholder(self):
        ''' Append a 'placeholder' row on to the top of the <species> dataframe to ensure 
             AGOL infers correct field type for 'year' (we want string). This will be 
             deleted automatically once the feature layer has been created in ArcGIS Online. '''                
        placeholder = {
            'dataProviderName': 'X',
            'lat': 0.0,
            'long': 0.0,
            'coordUnc': 0.0,
            'gridRef': 'X',
            'inferred_grid_size': 0,
            'idVerificationStatus': 'X',
            'license': 'X',
            'locationId': 'PLACEHOLDER',
            'scientificName': 'X',
            'vernacularName': 'X',
            'year': '',
            'osgb_polygon': [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]],
            'CaBA_ID': '0',
            'CaBA_Catch': ''
            }
            
        temp_data = []
        temp_data.insert(0, placeholder)
        self.dataframe = \
            pandas.concat([pandas.DataFrame(temp_data), self.dataframe], ignore_index=True)
        
        print("\nAppended 'placeholder' row.\n")
        self.placeholder = True
        self.placeholder_fieldname = 'locationId'

     
    def dataframe_to_geojson(self):
        # Convert these columns into geojson
        poly_properties = ['dataProviderName', 
                           'idVerificationStatus', 
                           'gridRef', 
                           'inferred_grid_size',
                           'license',
                           'locationId', 
                           'scientificName', 
                           'vernacularName', 
                           'year',
                           'CaBA_ID',
                           'CaBA_Catch']
        
        # We only need the point and its coordUnc for symbology purposes
        point_properties = ['coordUnc',
                            'locationId'] 
               
        # NB This code was based on 
        #  https://notebook.community/gnestor/jupyter-renderers/notebooks/nteract/pandas-to-geojson
        
        # Create a new python dict to contain our geojson data, using geojson format
        geojson = {'type':'FeatureCollection', 'features':[]}

        # Loop through each row in the dataframe and convert each row to geojson format
        for _, row in self.dataframe.iterrows():
            # Create a feature template to fill in
            poly_feature = {'type':'Feature',
                       'properties':{},
                       'geometry':{'type':'Polygon',
                                   'coordinates':[]}}
            point_feature = {'type':'Feature',
                       'properties':{},
                       'geometry':{'type':'Point',
                                   'coordinates':[]}}

            # Fill in the coordinates (geometry)
            poly_feature['geometry']['coordinates'] = [row['osgb_polygon']]
            point_feature['geometry']['coordinates'] = [row['long'],row['lat']]

            # For each column, get the value and add it as a new feature property
            for prop in poly_properties:
                poly_feature['properties'][prop] = row[prop]
            for prop in point_properties:
                point_feature['properties'][prop] = row[prop]
            
            # Add this feature (aka, converted dataframe row) to the list of features inside our dict
            geojson['features'].append(poly_feature)
            geojson['features'].append(point_feature)
        
        return geojson




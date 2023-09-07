# Standard library imports (https://docs.python.org/3/py-modindex.html)
import io
import json
import os
import re
import zipfile

# Related third party imports
from arcgis.features import FeatureLayerCollection
import geopandas
import pandas
import requests
from shapely.wkt import loads

# Local application imports



class GeoData: # Base class
          
    def __init__(self, source, dataobject, user):
        self.source = source
        self.dataobject = dataobject
        self.name = '_'.join([source, dataobject])
        self.user = user        
        self.dataframe = pandas.DataFrame() # Set in process_data()
        self.placeholder = False # Set to True if placeholder appended
        self.placeholder_fieldname = '' # Set in append_placeholder()
        self.geojson_filename = self.name + '.geojson'
        self.geojson_file = os.path.join(user.working_dir, self.geojson_filename)
        self.agol_data_item = '' # Set in add_data_item()
        self.agol_f_layer = ''
        self.agol_f_layer_id = ''
        self.agol_f_layer_view = ''
        self.agol_item_to_overwrite = ''

        '''         
        self.df_timestamp = 
        '''
     

    def check_item_already_exists(self):
        # Check existence of AGOL item with id <self.agol_f_layer_id>:       
        if self.agol_f_layer_id != '':
            # CASE: method called from overwrite_feature_layer.py 
            
            # Locate item to overwrite
            self.agol_item_to_overwrite = self.user.gis.content.get(self.agol_f_layer_id)  

            if self.agol_item_to_overwrite == None or self.agol_item_to_overwrite == '':
                return False
            
            print(f'Found ArcGIS Online item to be overwritten: ' \
                  f'{self.agol_item_to_overwrite}\n')
            
            # Check its name agrees with self.name
            if self.agol_item_to_overwrite.title != self.name:
                print(f"ArcGIS Online item name '{self.agol_item_to_overwrite.title}' " \
                      f"does not match user input name '{self.name}'.\n")
                raise SystemExit()
                
            return True
        
        else: # CASE: method called from create_feature_layer.py
            # Check existence of AGOL items with name <self.name>:       
            # Replace underscores with spaces to improve precision of search
            name = self.name.replace('_', ' ') 
            name = '"' + name + '"'
            items = self.user.gis.content.search(query='title:' + name)
            
            if items != []:
                print(f"ArcGIS Online item(s) of name '{self.name}' found:\
                      \n{items}\n")
                return True 
            
            return False
        
        
    def download_and_extract(self, url, file): 
        # Similar to urllib.request.urlretrieve
        
        foldername = os.path.splitext(file)[0] # Remove file extension
        to_location = self.user.working_dir + '\\' + foldername
         
        zip_file_request = requests.get(url + file)
        if zip_file_request.status_code != 200:
            print(f'\nCould not download file {file} from:\n{url}')
            raise SystemExit()
        else:
            print(f'\nDownloaded file {file} from:\n{url}')

        z = zipfile.ZipFile(io.BytesIO(zip_file_request.content))
        z.extractall(to_location)
        print(f'\nExtracted {file} to:\n{to_location}\\')
        
        datafiles_path = self.user.working_dir + '\\' + foldername + '\\'

        return datafiles_path


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

        # Construct a geodataframe from self.dataframe. NB EPSG:4326 is WGS84
        gdf_data_obj = geopandas.GeoDataFrame(
            self.dataframe, 
            geometry=geopandas.points_from_xy(self.dataframe.long, self.dataframe.lat, crs='EPSG:4326')
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
        else:
            raise SystemExit("'geopandas.sjoin()' changed number of rows in dataframe.")
        
#        print(self.dataframe.columns.tolist())
#        raise SystemExit()

        # Write empty strings to any Null values in new CaBA columns
        self.dataframe = self.dataframe.fillna("")
        
        # Convert 'CaBA_ID' values to type str 
        self.dataframe['CaBA_ID'] = self.dataframe['CaBA_ID'].astype(str) 
        # NB 'CaBA_ID' in form 78.0 so remove '.0'         
        #for row in self.dataframe.itertuples():
        #    self.dataframe.at[row.Index, 'CaBA_ID'] = \
        #        self.dataframe.at[row.Index, 'CaBA_ID'].rsplit('.', 1)[0]   

        return
      

    def df_print(self):
    
        self.dataframe.info(verbose=True)
        print(f'\n{self.dataframe}')
    
        num_features = len(self.dataframe.index)
        print(f'\n{num_features} features found.')
        
        
    def create_geojson_file(self):
        # Convert dataframe into geojson
        
        #print(f'{self.dataframe.iloc[0]}')
        #print(f'{self.dataframe.iloc[1]}')
        
        geojson = self.dataframe_to_geojson()

        self.df_print()
        #raise SystemExit()


        with open(self.geojson_filename, 'w') as output_file:
#            json_text = json.dumps(geojson, sort_keys=True, indent=1)
            json_text = json.dumps(geojson)
            output_file.write(json_text)
        print(f'\nWrote data to {self.geojson_filename} ready for import to ArcGIS.\n')


    def dataframe_to_geojson(self, properties): 
        # 'properties' provided by method of same name in child class
    
        # NB This code was based on
        #  https://notebook.community/gnestor/jupyter-renderers/notebooks/nteract/pandas-to-geojson

        # Create a new python dict to contain our geojson data, using geojson format
        geojson = {'type':'FeatureCollection', 'features':[]}

        # Loop through each row in the dataframe and convert each row to geojson format
        for _, row in self.dataframe.iterrows():
            # Create a feature template to fill in
            feature = {'type':'Feature',
                       'properties':{},
                       'geometry':{'type':'Point',
                                   'coordinates':[]}}

            # Fill in the coordinates
            feature['geometry']['coordinates'] = [row['long'],row['lat']]

            # For each column, get the value and add it as a new feature property
            for prop in properties:
                feature['properties'][prop] = row[prop]
            
            # Add this feature (aka, converted dataframe row) to the list of features inside our dict
            geojson['features'].append(feature)
        
        return geojson        
    
    
    def add_data_item(self):
        print(f'Adding data item {self.name} to ArcGIS Online from:\n{self.geojson_file}...')

        # Add the geojson file as an item to ArcGIS Online. 
        online_layer_properties = {'title': self.name, 
                                   'type': 'GeoJson'}

        ''' What if a feature layer has already been created from a GeoJson file named 
              {self.geojson_filename}? Could this ever happen? '''
        self.agol_data_item = \
            self.user.gis.content.add(item_properties=online_layer_properties,
                                      data=self.geojson_file)
        
        print(self.agol_data_item)
                                      
        if self.agol_data_item == None:
            print('Failed to create AGOL data item.\n')
            raise SystemExit()


    def publish_f_layer(self):
        ''' Create an ArcGIS Online feature layer.
             Crashes here if format of GeoJSON is illegal '''
        self.agol_f_layer = self.agol_data_item.publish() 
        
        self.agol_f_layer_id = self.agol_f_layer.id
        print(f'\nPublished feature layer {self.name} using data from:\n{self.geojson_file}...')
        print(self.agol_f_layer)
        
        if self.placeholder == True: # If placeholder appended then delete it
            self.delete_placeholder()


    def delete_placeholder(self):    
        ''' Delete the placeholder feature which was added to force ArcGIS Online 
        to infer field types correctly.'''        
            
        success = self.delete_features(self.placeholder_fieldname, 'PLACEHOLDER')
        
        if success:
            print(f"\nDeleted placeholder feature(s).")
        else:
            print(f"\nFailed to delete placeholder feature(s).")
            
    
    def delete_features(self, attribute, value): 
        # See https://developers.arcgis.com/python/guide/editing-features/
        
        # Query the features using e.g. where="bio_water_body = 'PLACEHOLDER'" 
        agol_item = self.user.gis.content.get(self.agol_f_layer_id)
        layers = agol_item.layers
                
        features_found = False
        layer_counter = 0
        while layer_counter < len(layers): # e.g. for NBNatlas_occurrences there are 
                                           #  two layers - a polygon layer and a point
                                           #  layer
                                                
            layer = layers[layer_counter]
            query_str = ''.join([attribute,'=', "'", value, "'"])
            fset = layer.query(where=query_str) # Returns max 1000            
            features = fset.features
            
#            print(features)
            if features != []:
                features_found = True
                feature_to_del = \
                    [f for f in features if f.attributes[attribute] == value][0]
            
                # Find object id for feature_to_del
                obj_id = feature_to_del.get_value('ObjectId')
                print(f"\nDeleting feature with name '{value}'...")
            
                # Pass the object id as a string to the delete parameter
                layer.edit_features(deletes=str(obj_id))
            
            layer_counter += 1

        return features_found
        
        
    def publish_f_layer_view(self):

        # Create a feature layer view
        flc = FeatureLayerCollection.fromitem(self.agol_f_layer)
        self.agol_f_layer_view = flc.manager.create_view(name=self.name + '_view')
        print(f'\nCreated feature layer view {self.name}_view for sharing and editing.')
        print(self.agol_f_layer_view)

        
    def overwrite_f_layer(self):        
        feature_service_url = self.agol_item_to_overwrite.url
        online_layer_name = self.agol_item_to_overwrite.title

        # Instantiate a FeatureLayerCollection from the url
        flc = FeatureLayerCollection(feature_service_url, self.user.gis)
        
        # Call the overwrite() method which can be accessed using the manager property
        print(f'Overwriting feature layer {online_layer_name} using data from:\n' \
              f'{self.geojson_file}...\n')
        success = flc.manager.overwrite(self.geojson_file)
        print(success)
        
        if 'error' in success:       
            print('Overwrite failed. Data item used to recreate feature layer missing.\n')
            raise SystemExit()
        
        if self.placeholder == True: # If placeholder appended then delete it
            self.delete_placeholder() 
        
    
            




        
     
     

        

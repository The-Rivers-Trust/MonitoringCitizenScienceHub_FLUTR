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

class EAHydrologyFlow(GeoData):
   
    # Define class attributes:
    url_records =  'https://environment.data.gov.uk/hydrology/id/stations'
    occurrences_list_of_dicts = []
    '''
    Get 1st 10000 sampling points
    https://environment.data.gov.uk/hydrology/id/sampling-point?_limit=10000
    Get 10000 sampling points starting at 70000th item
    https://environment.data.gov.uk/hydrology/id/sampling-point?_limit=10000&_offset=70000
    '''
                
    def get_data(self): # Using Water Quality Archive API  
    
        '''
        TRIED GETTING DATA IN A 'ONER', AND '10000 AT A TIME'. BOTH RETURN 61228 SAMPLING POINTS DESPITE 
        DISCREPENCY BETWEEN NUMBER OF ITEMS REQUESTED AND NUMBER RETURNED IN '10000 AT A TIME' METHOD.
        16/06/2022
        '''
        # Get data, in one go (assuming not more than 100000 items)
        # Use dissolved-oxygen as proxy for getting water quality sampling points
        url = EAHydrologyFlow.url_records + '?observedProperty=waterFlow&_limit=100000' # Get 100000 sampling points
        print(f'\nGetting {self.dataobject} data ' \
              f'from EA Hydrology API:\n{url}\n')
        occurrences_response = requests.get(f'{url}')

        num_observations = len(json.loads(occurrences_response.text)['items'])

        EAHydrologyFlow.occurrences_list_of_dicts = \
            json.loads(occurrences_response.text)['items'] 
 
        num_downloaded = len(json.loads(occurrences_response.text)['items']) 
        total_num_downloaded = num_downloaded
        print(f'Total number of items downloaded: {total_num_downloaded}')

        '''
        # Get data, num_returned_max at a time
        num_returned_max = 10000
        page_size = num_returned_max
        start_index = 0
        total_num_downloaded = 0
        done = False
        
        while not done:
        
            url = \
                EAWaterQualArchives.url_records \
                    + '?_limit=' \
                    + str(num_returned_max) \
                    + '&_offset=' \
                    + str(start_index)

            print(f'\nGetting {self.dataobject} data ' \
                  f'from EA Water Quality Archive API:\n{url}\n')
            occurrences_response = requests.get(f'{url}')
            
            #print(f"json.loads(occurrences_response.text)['items']: {json.loads(occurrences_response.text)['items']}")
            
            # Join any downloaded occurrences to EAWaterQualArchives.occurrences_list_of_dicts
            EAWaterQualArchives.occurrences_list_of_dicts = \
                EAWaterQualArchives.occurrences_list_of_dicts \
                    + json.loads(occurrences_response.text)['items'] 
                                      
            num_downloaded = len(json.loads(occurrences_response.text)['items']) 
            total_num_downloaded += num_downloaded
            print(f'Number of items downloaded: {num_downloaded}')
            print(f'Total number of items downloaded: {total_num_downloaded}')
                       
            start_index += num_returned_max
            if num_downloaded == 0:
                done = True
        '''
        
        if (total_num_downloaded == 0):
            print(f'No data found.')        
            raise SystemExit()

        return            


    def process_data(self):
        # Construct dataframe and write result to base class attribute 'dataframe'
        self.dataframe = \
            self.construct_df(EAHydrologyFlow.occurrences_list_of_dicts)
         
        # Calculate square occurrence regions to display on map
        #self.dataframe = \
        #    self.calculate_osgb_polygon(self.dataframe)

        # Determine CaBA catchments 
        self.determine_catchment()            
#        raise SystemExit()
              
        # Append 'placeholder' NOT NEEDED SINCE AGOL INFERS TYPES CORRECTLY
        #self.append_placeholder()
       
       
    @staticmethod # Since does not access or write to any class attributes  
    def construct_df(dicts): 
#        # Flatten data
#        dataframe = pandas.json_normalize(dict, record_path=['occurrences'])
        dataframe = pandas.DataFrame(dicts)
        
        #print(dataframe.columns.tolist())
        #raise SystemExit()
                 
        #  Drop fields, reorder, re'type', reindex and rename
        columns_required = ['@id',
                            'label',
                            'lat',
                            'long',
                            'notation',
                            'riverName',
                            'dateOpened',
                            'dateClosed',
                            'observedProperty',
                            'status']
        dataframe = dataframe[columns_required]
        """ dataframe = dataframe.sort_values(['label'], ascending=[True], ignore_index=True) """
        
        
        # Rename @id to URI_view
        renaming = {'@id': 'URI_view'}        
        dataframe = dataframe.rename(columns=renaming, errors='raise')
     
         
        # Convert samplingPointStatus values into samplingPointStatus.label values
        for row in dataframe.itertuples():
            # NB values in 'samplingPointStatus' column are of type dict
            dataframe.at[row.Index, 'status'] = dataframe.at[row.Index, 'status']['label'] 
        
        for row in dataframe.itertuples():
            observedProperties = []
            for dict in dataframe.at[row.Index, 'observedProperty']:
                value = list(dict.values())[0]
                value = value.replace("http://environment.data.gov.uk/reference/def/op/", '')
                observedProperties.append(value)
                observedProperties_str = ', '.join([str(elem) for elem in observedProperties])
            dataframe.at[row.Index, 'observedProperty'] = observedProperties_str
        
        # Drop any rows that lack lat/long data
        dataframe = dataframe.dropna(subset=['lat', 'long'], axis=0)
        num_geotagged_rows = format(len(dataframe))
        print(f'\nWe have {num_geotagged_rows} geotagged rows.\n')
        dataframe = dataframe.reset_index(drop=True)

        # Replace any NaN values with ''
        dataframe = dataframe.fillna('')   
        
        '''
        Because some dates are in years and months, and some are in years
        only, we decided only publish year data. AGOL doesn't recognise YYYY
        as a date format so analysis on a feature layer with a mixture of 
        YYYY and YYYY-MM is not straightforward.
        # Re'type' 'year'
        dataframe['year'] = dataframe['year'].astype(str) 
        # NB 'year' in form 2005.0 so remove '.0'         
        for row in dataframe.itertuples():
            dataframe.at[row.Index, 'year'] = \
                dataframe.at[row.Index, 'year'].rsplit('.', 1)[0]
        '''
        '''
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
        '''
        
        return dataframe
        
    
      
    
    def append_placeholder(self):
        ''' Append a 'placeholder' row on to the top of the dataframe to ensure 
             AGOL infers correct field type for 'year' (we want string). This will be 
             deleted automatically once the feature layer has been created in ArcGIS Online. '''                
        '''columns_required = ['URI_view',
                            'area',
                            'comment',
                            'label',
                            'lat',
                            'long',
                            'notation',
                            'samplingPointStatus',
                            'samplingPointType',
                            'subArea']
        placeholder = {
            '@id': 'X',
            'area': '',
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
        '''
        return

     
    def dataframe_to_geojson(self):
        # Declare properties then call parent method 
        
        # Convert these columns into geojson
        properties = ['URI_view',
                      'label',
                      'notation',
                      'riverName',
                      'dateOpened',
                      'dateClosed',
                      'observedProperty',
                      'status',
                      'CaBA_ID',
                      'CaBA_Catch']
    
        geojson = super().dataframe_to_geojson(properties)
        
        return geojson




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

class EAWaterQualArchives(GeoData):
   
    # Define class attributes:
    url_records =  'https://environment.data.gov.uk/water-quality/id/sampling-point'
    occurrences_list_of_dicts = []
    '''
    Get 1st 10000 sampling points
    https://environment.data.gov.uk/water-quality/id/sampling-point?_limit=10000
    Get 10000 sampling points starting at 70000th item
    https://environment.data.gov.uk/water-quality/id/sampling-point?_limit=10000&_offset=70000
    '''
                
    def get_data(self): # Using Water Quality Archive API  
    
        '''
        TRIED GETTING DATA IN A 'ONER', AND '10000 AT A TIME'. BOTH RETURN 61228 SAMPLING POINTS DESPITE 
        DISCREPENCY BETWEEN NUMBER OF ITEMS REQUESTED AND NUMBER RETURNED IN '10000 AT A TIME' METHOD.
        16/06/2022
        '''
        # Get data, in one go (assuming not more than 100000 items)
        url = EAWaterQualArchives.url_records + '?_limit=100000' # Get 100000 sampling points
        print(f'\nGetting {self.dataobject} data ' \
              f'from EA Water Quality Archive API:\n{url}\n')
        occurrences_response = requests.get(f'{url}')

        num_observations = len(json.loads(occurrences_response.text)['items'])

        EAWaterQualArchives.occurrences_list_of_dicts = \
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
            self.construct_df(EAWaterQualArchives.occurrences_list_of_dicts)
         
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
                            'area',
                            'comment',
                            'label',
                            'lat',
                            'long',
                            'notation',
                            'samplingPointStatus',
                            'samplingPointType',
                            'subArea']
        dataframe = dataframe[columns_required]
        dataframe = dataframe.sort_values(['label'], ascending=[True], ignore_index=True)
        
        
        # Convert link values into URLs displaying sampling points - replace /id/ with /view/ 
        dataframe['@id'] = dataframe['@id'].str.replace('/id/', '/view/')
        # Rename @id to URI_view
        renaming = {'@id': 'URI_view'}        
        dataframe = dataframe.rename(columns=renaming, errors='raise')

        #print(f"type(dataframe['area']): {type(dataframe['area'])}")
        #print(f"dataframe['area'].iloc[0]: {dataframe['area'].iloc[0]}")
        #print(f"type(dataframe['area'].iloc[0]): {type(dataframe['area'].iloc[0])}")
        #print(f"type(dataframe['URI_view'].iloc[0]): {type(dataframe['URI_view'].iloc[0])}")
            
        # Convert area values into area.label values
        for row in dataframe.itertuples():
            # NB values in 'area' column are of type dict
            dataframe.at[row.Index, 'area'] = dataframe.at[row.Index, 'area']['label']
        # Rename area to area_label
        renaming = {'area': 'area_label'}        
        dataframe = dataframe.rename(columns=renaming, errors='raise')        
         
        # Convert samplingPointStatus values into samplingPointStatus.label values
        for row in dataframe.itertuples():
            # NB values in 'samplingPointStatus' column are of type dict
            dataframe.at[row.Index, 'samplingPointStatus'] = dataframe.at[row.Index, 'samplingPointStatus']['label']        
        # Rename samplingPointStatus to samplingPointStatus_label
        renaming = {'samplingPointStatus': 'samplingPointStatus_label'}        
        dataframe = dataframe.rename(columns=renaming, errors='raise')  

        # Convert samplingPointType values into samplingPointType.label and 
        #  samplingPointType.group values
        # Create new columns for samplingPointType.label and samplingPointType.group 
        dataframe["samplingPointType_label"] = ""
        dataframe["samplingPointType_group"] = ""
        for row in dataframe.itertuples():
            # NB values in 'samplingPointType' column are of type dict
            dataframe.at[row.Index, 'samplingPointType_label'] = dataframe.at[row.Index, 'samplingPointType']['label']        
            dataframe.at[row.Index, 'samplingPointType_group'] = dataframe.at[row.Index, 'samplingPointType']['group']        
        #    print(f"samplingPointType_group value: {dataframe.at[row.Index, 'samplingPointType']['group']}")
        #    raise SystemExit()
        
        # Convert samplingPointType_group link values into one of 17 letter codes - see 
        #  https://environment.data.gov.uk/water-quality/def/sampling-point-type-groups.html?_sort=label
        dataframe['samplingPointType_group'] = dataframe['samplingPointType_group'].str.replace('http://environment.data.gov.uk/water-quality/def/sampling-point-type-groups/', '')
        
        # Convert samplingPointType_group letter codes into group labels
        for row in dataframe.itertuples():
            to_match = dataframe.at[row.Index, 'samplingPointType_group']
            if to_match == 'A':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'AGRICULTURE'
            elif to_match == 'Z':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'EXEMPTION'
            elif to_match == 'F':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'FRESHWATER'
            elif to_match == 'B':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'GROUNDWATER'
            elif to_match == 'M':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'MINEWATER'
            elif to_match == 'D':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'MISCELLANEOUS DISCHARGES'
            elif to_match == 'E':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'MISCELLANEOUS ENVIRONMENT'
            elif to_match == 'P':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'POLLUTION/INVESTIGATION POINTS'
            elif to_match == 'N':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'RAINWATER'
            elif to_match == 'C':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'SALINE WATER'
            elif to_match == 'R':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'SEWAGE'
            elif to_match == 'Y':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'SEWAGE & TRADE COMBINED'
            elif to_match == 'U':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'SEWAGE DISCHARGES - NOT WATER COMPANY'
            elif to_match == 'S':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'SEWAGE DISCHARGES - WATER COMPANY'
            elif to_match == 'V':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'SEWERAGE SYSTEM DISCHARGE'
            elif to_match == 'T':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'TRADE DISCHARGES'
            elif to_match == 'W':
                dataframe.at[row.Index, 'samplingPointType_group'] = 'WASTE SITE'
            else:
                print(f'Sampling point type group code unrecognised: {to_match}')

        # Convert subArea values into subArea.label values
        for row in dataframe.itertuples():
            # NB values in 'subArea' column are of type dict
            dataframe.at[row.Index, 'subArea'] = dataframe.at[row.Index, 'subArea']['label']        
        # Rename subArea to subArea_label
        renaming = {'subArea': 'subArea_label'}        
        dataframe = dataframe.rename(columns=renaming, errors='raise')  

        
        #dataframe['area'] = dataframe['area'].str.extract(r'\'label\':(.+?)\}', expand=False)
        #result = dataframe['area'].str.extract(r'(.*)', expand=True)

        #print(f'dataframe: {dataframe}')
        #print(f'result: {result}')
        #dataframe['area'] = dataframe['area'].str.extract('\'label\':(.+?)\}', expand=False).str.strip()
        
        #print(f"dataframe['area'].iloc[0]: {dataframe['area'].iloc[0]}")
        #raise SystemExit()
        
        
       
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
                      'area_label',
                      'comment',
                      'label',
                      'notation',
                      'samplingPointStatus_label',
                      'samplingPointType_label',
                      'samplingPointType_group',
                      'subArea_label',
                      'CaBA_ID',
                      'CaBA_Catch']
    
        geojson = super().dataframe_to_geojson(properties)
        
        return geojson




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

class EAWaterQualSampleArchives2(GeoData):
   
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
        url = EAWaterQualSampleArchives2.url_records + '?_limit=100000' # Get 100000 sampling points
        print(f'\nGetting {self.dataobject} data ' \
              f'from EA Water Quality Archive API:\n{url}\n')
        occurrences_response = requests.get(f'{url}')

        num_observations = len(json.loads(occurrences_response.text)['items'])

        EAWaterQualSampleArchives2.occurrences_list_of_dicts = \
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
                EAWaterQualSampleArchives2.url_records \
                    + '?_limit=' \
                    + str(num_returned_max) \
                    + '&_offset=' \
                    + str(start_index)

            print(f'\nGetting {self.dataobject} data ' \
                  f'from EA Water Quality Archive API:\n{url}\n')
            occurrences_response = requests.get(f'{url}')
            
            #print(f"json.loads(occurrences_response.text)['items']: {json.loads(occurrences_response.text)['items']}")
            
            # Join any downloaded occurrences to EAWaterQualSampleArchives2.occurrences_list_of_dicts
            EAWaterQualSampleArchives2.occurrences_list_of_dicts = \
                EAWaterQualSampleArchives2.occurrences_list_of_dicts \
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
            self.construct_df(EAWaterQualSampleArchives2.occurrences_list_of_dicts)
         
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
        dataframe_site = pandas.DataFrame(dicts)
        
        #print(dataframe.columns.tolist())
        #raise SystemExit()
                 
        #  Drop fields, reorder, re'type', reindex and rename
        columns_required = ['notation',
                            'samplingPointStatus',
                            'samplingPointType',
                            'lat',
                            'long']
        dataframe_site = dataframe_site[columns_required]
        dataframe_site = dataframe_site.sort_values(['notation'], ascending=[True], ignore_index=True)
       
        # retrieve sample status
        for row in dataframe_site.itertuples() :
                dataframe_site.at[row.Index, 'samplingPointStatus'] = dataframe_site.at[row.Index, 'samplingPointStatus']['label']

        # Drop any rows that lack notation
        dataframe_site = dataframe_site.dropna(subset=['notation'], axis=0)
        dataframe_site = dataframe_site.reset_index(drop=True)
        # Drop any rows that lack lat/long data
        dataframe = dataframe_site.dropna(subset=['lat', 'long'], axis=0)
        num_geotagged_rows = format(len(dataframe_site))
        print(f'\nWe have {num_geotagged_rows} geotagged sites.\n')
        dataframe_site = dataframe_site.reset_index(drop=True)

        # Convert samplingPointType values into samplingPointType.label and 
        #  samplingPointType.group values
        # Create new columns for samplingPointType.label and samplingPointType.group 
        dataframe_site["samplingPointType_label"] = ""
        dataframe_site["samplingPointType_group"] = ""
        for row in dataframe_site.itertuples():
            # NB values in 'samplingPointType' column are of type dict
            dataframe_site.at[row.Index, 'samplingPointType_label'] = dataframe_site.at[row.Index, 'samplingPointType']['label']        
            dataframe_site.at[row.Index, 'samplingPointType_group'] = dataframe_site.at[row.Index, 'samplingPointType']['group']        
        #    print(f"samplingPointType_group value: {dataframe_site.at[row.Index, 'samplingPointType']['group']}")
        #    raise SystemExit()
        
        # Convert samplingPointType_group link values into one of 17 letter codes - see 
        #  https://environment.data.gov.uk/water-quality/def/sampling-point-type-groups.html?_sort=label
        dataframe_site['samplingPointType_group'] = dataframe_site['samplingPointType_group'].str.replace('http://environment.data.gov.uk/water-quality/def/sampling-point-type-groups/', '')
        
        # Convert samplingPointType_group letter codes into group labels
        for row in dataframe_site.itertuples():
            to_match = dataframe_site.at[row.Index, 'samplingPointType_group']
            if to_match == 'A':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'AGRICULTURE'
            elif to_match == 'Z':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'EXEMPTION'
            elif to_match == 'F':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'FRESHWATER'
            elif to_match == 'B':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'GROUNDWATER'
            elif to_match == 'M':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'MINEWATER'
            elif to_match == 'D':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'MISCELLANEOUS DISCHARGES'
            elif to_match == 'E':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'MISCELLANEOUS ENVIRONMENT'
            elif to_match == 'P':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'POLLUTION/INVESTIGATION POINTS'
            elif to_match == 'N':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'RAINWATER'
            elif to_match == 'C':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'SALINE WATER'
            elif to_match == 'R':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'SEWAGE'
            elif to_match == 'Y':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'SEWAGE & TRADE COMBINED'
            elif to_match == 'U':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'SEWAGE DISCHARGES - NOT WATER COMPANY'
            elif to_match == 'S':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'SEWAGE DISCHARGES - WATER COMPANY'
            elif to_match == 'V':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'SEWERAGE SYSTEM DISCHARGE'
            elif to_match == 'T':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'TRADE DISCHARGES'
            elif to_match == 'W':
                dataframe_site.at[row.Index, 'samplingPointType_group'] = 'WASTE SITE'
            else:
                print(f'Sampling point type group code unrecognised: {to_match}')

        # define base url for getting list of samples
        base_url_samples = 'https://environment.data.gov.uk/water-quality/data/sample.json?'

        total_samples = 0

        # create empty dataframe
        dataframe = pandas.DataFrame()

        # get site data as dictionary
        notation_list = [x for x in dataframe_site['notation']]
        type_list = [x for x in dataframe_site['samplingPointType_group']]
        status_list = [x for x in dataframe_site['samplingPointStatus']]
        lat_list = [x for x in dataframe_site['lat']]
        long_list = [x for x in dataframe_site['long']]
        site_list_dict = []
        for index in range(len(notation_list)) :
            site_dict = {'notation': notation_list[index], 'lat': lat_list[index], 'long': long_list[index]}
            site_list_dict.append(site_dict)

        # loop through every site to get all samples from each site
        # for i in range(len(site_list_dict)) :
        for i in range(50) :
            url_samples = base_url_samples + "samplingPoint=" + site_list_dict[i]['notation'] + "&_limit=10000"
            samples_response = requests.get(f'{url_samples}')

            EAWaterQualSampleArchives2.samples_list_of_dicts = \
                json.loads(samples_response.text)['items']

            dataframe_sample = pandas.DataFrame(EAWaterQualSampleArchives2.samples_list_of_dicts)
            
            columns_required = ['purpose',
                                'sampleDateTime']
            dataframe_sample = dataframe_sample[columns_required]

            # edit purpose
            for row in dataframe_sample.itertuples() :
                dataframe_sample.at[row.Index, 'purpose'] = dataframe_sample.at[row.Index, 'purpose']['label']

            # number of samples at the site
            num_samples = len(dataframe_sample)
            total_samples += num_samples

            # create column for year
            dataframe_sample['year'] = [y[:4] for y in dataframe_sample['sampleDateTime']]
            sample_year_list = dataframe_sample['year'].to_list()

            # compress dataframe so each row is a year of samples at one site
            annual_list = [str(y) for y in range(2000,2024)]
            annual_dict = {}
            for y in annual_list:
                if y in sample_year_list:
                    annual_dict[y] = sample_year_list.count(str(y))
                else :
                    annual_dict[y] = 0

            # create link to site samples
            site_url = "http://environment.data.gov.uk/water-quality/view/sampling-point/" + notation_list[i] + ".html"

            # create row for site metadata
            for year in range(len(list(annual_dict.keys()))):

                new_row = {}
                new_row['notation'] = notation_list[i]
                new_row['site_url'] = site_url
                new_row['sample_type'] = type_list[i]
                new_row['status'] = status_list[i]
                new_row['lat'] = lat_list[i]
                new_row['long'] = long_list[i]
                new_row['purpose'] = dataframe_sample['purpose'][0]
                new_row['year'] = list(annual_dict.keys())[year]
                new_row['annual_sample_count'] = list(annual_dict.values())[year]
                new_row_series = pandas.Series(new_row)
                # append row to dataframe
                dataframe = pandas.concat([dataframe, new_row_series.to_frame().T], ignore_index=True)

        dataframe = dataframe.fillna('')
        total_num_sites = format(len(dataframe))
        print(f'\nWe have {total_num_sites} sites, totalling {total_samples}.\n')

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
        properties = ['notation',
                      'site_url',
                      'sample_type',
                      'status',
                      'purpose',
                      'year',
                      'annual_sample_count',
                      'CaBA_ID',
                      'CaBA_Catch']
    
        geojson = super().dataframe_to_geojson(properties)
        
        return geojson




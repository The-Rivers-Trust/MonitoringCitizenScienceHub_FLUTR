# Standard library imports (https://docs.python.org/3/py-modindex.html)


# Related third party imports
import geopandas
import pandas


# Local application imports
from ea_ecology_and_fish_data import EAEcologyAndFishData



'''
A derived class that inherits from the base class GeoData and parent class EAEcologyAndFishData
NB __init__ is not defined so the base class __init__ is inherited and 
used to construct a class object
'''

class EASurveySitesFishHistory2(EAEcologyAndFishData):

    # Define class attributes:
    fish_zipfile = 'FW_Fish_Counts.zip' 
    fish_filename = 'FW_Fish_Counts.csv'
    fish_datafile = ''
    
  
    def get_data(self):
    
        ''' Download and extract EA Freshwater fish counts (NFPD) data 
                - see superclass for url '''
        EASurveySitesFishHistory2.fish_datafile = \
            super().download_and_extract(EASurveySitesFishHistory2.fish_zipfile) \
                + EASurveySitesFishHistory2.fish_filename
       
       
    def process_data(self):
        
        # Construct dataframe and write result to base class attribute 'dataframe'
        self.dataframe = self.construct_df(EASurveySitesFishHistory2.fish_datafile)
                 
        # Convert eastings and northings into lats and longs
        super().easting_northing_to_wgs84()
        
        # Determine CaBA catchments 
        super().determine_catchment()            
        #raise SystemExit()
       
        # Append 'placeholder'
        self.append_placeholder()


    @staticmethod # Since does not access or write to any class attributes      
    def construct_df(file): # Read csv file to dataframe as required
        print(f'\nReading fish sites from bulk download file:\n{file}\n')

        columns_required = ['SITE_ID', 
                            'SITE_NAME', 
                            'EVENT_DATE', 
                            'SURVEY_RANKED_EASTING', 
                            'SURVEY_RANKED_NORTHING']
        dataframe = pandas.read_csv(file, 
                                    usecols=columns_required, 
                                    dtype=str, 
                                    parse_dates=['EVENT_DATE'])
                                    
        # Sort on site_id and last_survey then keep only the first row for each site_id
        dataframe = dataframe.sort_values(['SITE_ID', 'EVENT_DATE'], 
                                          ascending=[True, False])
        # Add 'year' column
        dataframe['year'] = ''
        dataframe['year'] = dataframe['EVENT_DATE'].dt.year
        dataframe['year'] = dataframe['year'].astype(str)
                                  
        # Remove timestamps                                  
        dataframe['EVENT_DATE'] = pandas.to_datetime(dataframe['EVENT_DATE']).dt.date
        
        # Convert dates back to type string
        dataframe['EVENT_DATE'] = dataframe['EVENT_DATE'].astype(str)        
        
        # Loop through all sites to get first and last sample for each site. Add to columns, remove all other rows
        # start by creating new columns
        dataframe['recent_sample'] = "x"
        dataframe['num_samples'] = "x"
        # create a list of unique site ids
        id_list = dataframe['SITE_ID'].to_list()
        id_set = [*set(id_list)]
        # now loop through a list of unique site ids

        for site in id_set:
            site_df = dataframe[dataframe['SITE_ID'] == site]
            dataframe.loc[dataframe['SITE_ID'] == site, 'first_fish_survey'] = site_df['EVENT_DATE'].iloc[-1]
            dataframe.loc[dataframe['SITE_ID'] == site, 'num_samples'] = id_list.count(site)
        
        # Drop any rows with duplicated site_id  
        dataframe = dataframe.drop_duplicates('SITE_ID', keep='first', ignore_index=True)

        # Rename and reorder columns
        renaming = {'SITE_ID': 'fish_site_id', 
                    'SITE_NAME': 'fish_site_name', 
                    'EVENT_DATE': 'last_fish_survey', 
                    'SURVEY_RANKED_EASTING': 'easting', 
                    'SURVEY_RANKED_NORTHING': 'northing'}
        dataframe = dataframe.rename(columns=renaming, errors='raise')
        reordering = ['fish_site_id', 
                      'fish_site_name', 
                      'easting', 
                      'northing', 
                      'num_samples',
                      'first_fish_survey',
                      'last_fish_survey']
        dataframe = dataframe.reindex(columns=reordering)
        
        # Drop any rows that lack easting/northing data
        dataframe = dataframe.dropna(subset=['easting', 'northing'], axis=0)
        num_geotagged_rows = format(len(dataframe))
        print(f'We have {num_geotagged_rows} geotagged rows.\n')
        dataframe = dataframe.reset_index(drop=True)
        
        return dataframe
        
        
    def append_placeholder(self):
        ''' Append a 'placeholder' row on to the top of the fish dataframe to ensure 
             AGOL infers correct field type for 'year' (we want string). This will be 
             deleted automatically once the feature layer has been created in ArcGIS Online. '''                
        placeholder = {
            'fish_site_id': 'id',
            'fish_site_name': 'PLACEHOLDER',
            'lat': 0.0,
            'long': 0.0,
            'num_samples': 0,
            'first_fish_survey': '2000-01-01',
            'last_fish_survey': '2000-01-01',
#            'CaBA_Partn': '',
            'CaBA_ID': 'number',
            'CaBA_Catch': ''
#            'WFD_RBD': ''
            }
        temp_data = []
        temp_data.insert(0, placeholder)
        self.dataframe = \
            pandas.concat([pandas.DataFrame(temp_data), self.dataframe], ignore_index=True)
        
        print("\nAppended 'placeholder' row.\n")
        self.placeholder = True
        self.placeholder_fieldname = 'fish_site_name'
  
  
    def dataframe_to_geojson(self): 
        # Declare properties then call parent method 
        
        # Convert these columns into geojson
        properties = ['fish_site_id', 
                      'fish_site_name', 
                      'num_samples',
                      'first_fish_survey',
                      'last_fish_survey',
#                      'CaBA_Partn',
                      'CaBA_ID',
                      'CaBA_Catch'
#                      'WFD_RBD'
                      ]


        geojson = super().dataframe_to_geojson(properties)

        return geojson
    
        

        

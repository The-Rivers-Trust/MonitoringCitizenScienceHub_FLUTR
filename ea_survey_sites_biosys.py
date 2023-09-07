# Standard library imports (https://docs.python.org/3/py-modindex.html)


# Related third party imports
import pandas


# Local application imports
from ea_ecology_and_fish_data import EAEcologyAndFishData

'''
A derived class that inherits from the base class GeoData and parent class EAEcologyAndFishData
NB __init__ is not defined so the base class __init__ is inherited and 
used to construct a class object
'''

class EASurveySitesBiosys(EAEcologyAndFishData):

    # Define class attributes:
    inv_zipfile = 'INV_OPEN_DATA.zip'
    macp_zipfile = 'MACP_OPEN_DATA.zip'
    diat_zipfile = 'DIAT_OPEN_DATA.zip'
    inv_filename = 'INV_OPEN_DATA_SITE.csv'
    macp_filename = 'MACP_OPEN_DATA_SITE.csv'
    diat_filename = 'DIAT_OPEN_DATA_SITE.csv'
    inv_datafile = ''
    macp_datafile = ''
    diat_datafile = ''
   
   
    def get_data(self):
     
        ''' Download and extract EA Freshwater river macroinvertevrate surveys (Biosys) data 
                - see superclass for url '''        
        EASurveySitesBiosys.inv_datafile = \
            super().download_and_extract(EASurveySitesBiosys.inv_zipfile) \
                + EASurveySitesBiosys.inv_filename

        # Download and extract EA Freshwater river macrophyte surveys (Biosys) data 
        EASurveySitesBiosys.macp_datafile = \
            super().download_and_extract(EASurveySitesBiosys.macp_zipfile) \
                + EASurveySitesBiosys.macp_filename

        # Download and extract EA Freshwater river diatom surveys (Biosys) data 
        EASurveySitesBiosys.diat_datafile = \
            super().download_and_extract(EASurveySitesBiosys.diat_zipfile) \
                + EASurveySitesBiosys.diat_filename

              
    def process_data(self):
               
        # Read site data from csv files to dataframe
        inv_dataframe = \
            self.construct_df(EASurveySitesBiosys.inv_datafile, 'inv')
        macp_dataframe = \
            self.construct_df(EASurveySitesBiosys.macp_datafile, 'macp')
        diat_dataframe = \
            self.construct_df(EASurveySitesBiosys.diat_datafile, 'diat')
    
        ''' Join three biosys dataframes into one and 
             write result to base class instance attribute 'dataframe' '''
        self.dataframe = \
            self.join_dataframes(inv_dataframe, macp_dataframe, diat_dataframe)
      
        # Convert eastings and northings into lats and longs
        super().easting_northing_to_wgs84()
               
        # Determine CaBA catchments 
        super().determine_catchment()            

        # Append 'placeholder'
        self.append_placeholder()

        
    @staticmethod # Since does not access or write to any class attributes      
    def construct_df(file, type): # Read csv file to dataframe as required
        print(f"\nReading '{type}' sites from bulk download file:\n{file}\n")
        
        columns_required = ['WATER_BODY', 
                            'SITE_ID', 
                            'FULL_EASTING', 
                            'FULL_NORTHING', 
                            'MAX_SAMPLE_DATE']
        dataframe = pandas.read_csv(file, 
                                    usecols=columns_required, 
                                    dtype=str)
                                    
        # Sort on site_id 
        dataframe = dataframe.sort_values(['SITE_ID'], 
                                          ascending=[True], 
                                          ignore_index=True)
        
        # Remove timestamps      
        dataframe['MAX_SAMPLE_DATE'] = \
            pandas.to_datetime(dataframe['MAX_SAMPLE_DATE']).dt.date 

        # Rename and reorder columns
        renaming = {'WATER_BODY': 'bio_water_body', 
                    'SITE_ID': 'bio_site_id', 
                    'MAX_SAMPLE_DATE': 'last_bio_survey', 
                    'FULL_EASTING': 'easting', 
                    'FULL_NORTHING': 'northing'}
        dataframe = dataframe.rename(columns=renaming, errors='raise')
        reordering = ['bio_site_id', 
                      'bio_water_body', 
                      'easting', 
                      'northing', 
                      'last_bio_survey']
        dataframe = dataframe.reindex(columns=reordering)
        
        # Drop any rows that lack easting/northing data
        dataframe = dataframe.dropna(subset=['easting', 'northing'], axis=0)
        num_geotagged_rows = format(len(dataframe))
        print(f'We have {num_geotagged_rows} geotagged rows.\n')
        dataframe = dataframe.reset_index(drop=True)

        # Add three new columns to contain last_<inv, macp or diat>_survey
        dataframe['last_inv_survey'] = ''
        dataframe['last_macp_survey'] = ''
        dataframe['last_diat_survey'] = ''
        reordering = ['bio_site_id', 
                      'bio_water_body', 
                      'easting', 
                      'northing', 
                      'last_inv_survey', 
                      'last_macp_survey', 
                      'last_diat_survey', 
                      'last_bio_survey']

        # Copy last_bio_survey date into relevant column
        fill_column = 'last_' + type + '_survey'
        dataframe[fill_column] = dataframe['last_bio_survey']

        return dataframe
     
     
    @staticmethod # Since does not access or write to any class attributes         
    def join_dataframes(inv_df, macp_df, diat_df):
        print(f'\nJoining biosys databases...\n')
        dataframe = pandas.concat([inv_df, macp_df, diat_df])
                                        
        # Sort on bio_site_id and last_bio_survey
        dataframe = dataframe.sort_values(['bio_site_id', 'last_bio_survey'], 
                                            ascending=[True, False], 
                                            ignore_index=True)
        
        # Convert dates back to type string
        dataframe['last_bio_survey'] = dataframe['last_bio_survey'].astype(str)
        dataframe['last_inv_survey'] = dataframe['last_inv_survey'].astype(str)
        dataframe['last_macp_survey'] = dataframe['last_macp_survey'].astype(str)
        dataframe['last_diat_survey'] = dataframe['last_diat_survey'].astype(str) 
              
        # Collapse dataframe      
        ''' For each set of rows with the same bio_site_id, collapse rows, 
             preserving most recent last_bio_survey date information
             NB This for loop is slow - could be much more efficient '''                    
        prev_site_id = ''
            
        for row in dataframe.itertuples(): 
    
            site_id = dataframe.at[row.Index, 'bio_site_id']

            if site_id == prev_site_id: # Same site_id
                # Overwrite current site_id's last_bio_survey date with previous one 
                dataframe.at[row.Index, 'last_bio_survey'] = \
                    dataframe.at[row.Index-1, 'last_bio_survey'] 
                # Similarly, overwrite any empty dates with previous one
                if dataframe.at[row.Index, 'last_inv_survey'] == '':
                    dataframe.at[row.Index, 'last_inv_survey'] = \
                        dataframe.at[row.Index-1, 'last_inv_survey']
                if dataframe.at[row.Index, 'last_macp_survey'] == '':
                    dataframe.at[row.Index, 'last_macp_survey'] = \
                        dataframe.at[row.Index-1, 'last_macp_survey']
                if dataframe.at[row.Index, 'last_diat_survey'] == '':
                    dataframe.at[row.Index, 'last_diat_survey'] = \
                        dataframe.at[row.Index-1, 'last_diat_survey']
                
                # Drop prev row now its inv, macp or diat date has been stored in current row
                dataframe = dataframe.drop(row.Index-1) 
                
            prev_site_id = site_id

        dataframe = dataframe.reset_index(drop=True)
        
        return dataframe
        

    def append_placeholder(self):
        ''' Append a 'placeholder' row on to the top of the biosys dataframe to ensure 
             AGOL infers correct field types. This will be deleted automatically 
             once the feature layer has been created in ArcGIS Online. '''                
        placeholder = {
            'bio_site_id': '0',
            'bio_water_body': 'PLACEHOLDER',
            'lat': 0.0,
            'long': 0.0,
            'last_bio_survey': '2000-01-01',
            'last_diat_survey': '2000-01-01',
            'last_inv_survey': '2000-01-01',
            'last_macp_survey': '2000-01-01',
#            'CaBA_Partn': '',
            'CaBA_ID': '0',
            'CaBA_Catch': ''
#            'WFD_RBD': ''
        }
        temp_data = []
        temp_data.insert(0, placeholder)
        self.dataframe = \
            pandas.concat([pandas.DataFrame(temp_data), self.dataframe], ignore_index=True)
        
        print("\nAppended 'placeholder' row.\n")
        self.placeholder = True
        self.placeholder_fieldname = 'bio_water_body'


    def dataframe_to_geojson(self): 
        # Declare properties then call parent method 
        
        # Convert these columns into geojson
        properties = ['bio_site_id', 
                      'bio_water_body', 
                      'last_bio_survey', 
                      'last_inv_survey', 
                      'last_macp_survey', 
                      'last_diat_survey',
#                      'CaBA_Partn',
                      'CaBA_ID',
                      'CaBA_Catch'
#                      'WFD_RBD'
        ]

        geojson = super().dataframe_to_geojson(properties)

        return geojson
                                                  
    


   

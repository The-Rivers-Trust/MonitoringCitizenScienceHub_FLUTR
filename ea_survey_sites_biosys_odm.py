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

class EASurveySitesBiosysODM(EAEcologyAndFishData):

    # Define class attributes:
    inv_zipfile = 'INV_OPEN_DATA.zip'
    macp_zipfile = 'MACP_OPEN_DATA.zip'
    diat_zipfile = 'DIAT_OPEN_DATA.zip'
    inv_filename = 'INV_OPEN_DATA_METRICS.csv'
    macp_filename = 'MACP_OPEN_DATA_METRICS.csv'
    diat_filename = 'DIAT_OPEN_DATA_METRICS.csv'
    inv_datafile = ''
    macp_datafile = ''
    diat_datafile = ''
   
   
    def get_data(self):
     
        ''' Download and extract EA Freshwater river macroinvertevrate surveys (Biosys) data 
                - see superclass for url '''        
        EASurveySitesBiosysODM.inv_datafile = \
            super().download_and_extract(EASurveySitesBiosysODM.inv_zipfile) \
                + EASurveySitesBiosysODM.inv_filename

        # Download and extract EA Freshwater river macrophyte surveys (Biosys) data 
        EASurveySitesBiosysODM.macp_datafile = \
            super().download_and_extract(EASurveySitesBiosysODM.macp_zipfile) \
                + EASurveySitesBiosysODM.macp_filename

        # Download and extract EA Freshwater river diatom surveys (Biosys) data 
        EASurveySitesBiosysODM.diat_datafile = \
            super().download_and_extract(EASurveySitesBiosysODM.diat_zipfile) \
                + EASurveySitesBiosysODM.diat_filename

              
    def process_data(self):
               
        # Read site data from csv files to dataframe
        inv_dataframe = \
            self.construct_df(EASurveySitesBiosysODM.inv_datafile, 'inv')
        macp_dataframe = \
            self.construct_df(EASurveySitesBiosysODM.macp_datafile, 'macp')
        diat_dataframe = \
            self.construct_df(EASurveySitesBiosysODM.diat_datafile, 'diat')
    
        ''' Join three biosys dataframes into one and 
             write result to base class instance attribute 'dataframe' '''
        self.dataframe = \
            self.join_dataframes(inv_dataframe, macp_dataframe, diat_dataframe)
      
        """ # Convert eastings and northings into lats and longs
        super().easting_northing_to_wgs84()
               
        # Determine CaBA catchments 
        super().determine_catchment()          """   

        # Append 'placeholder'
        self.append_placeholder()

        
    @staticmethod # Since does not access or write to any class attributes      
    def construct_df(file, type): # Read csv file to dataframe as required
        print(f"\nReading '{type}' sites from bulk download file:\n{file}\n")
        
        columns_required = ['SITE_ID', 
                            'SAMPLE_ID',
                            'SAMPLE_DATE',
                            'SAMPLE_TYPE',
                            'SAMPLE_TYPE_DESCRIPTION',
                            'SAMPLE_METHOD',
                            'SAMPLE_METHOD_DESCRIPTION',
                            'SAMPLE_REASON',
                            'ANALYSIS_ID',
                            'DATE_OF_ANALYSIS',
                            'ANALYSIS_TYPE',
                            'ANALYSIS_TYPE_DESCRIPTION',
                            'ANALYSIS_METHOD',
                            'ANALYSIS_METHOD_DESCRIPTION',
                            ]
        dataframe = pandas.read_csv(file, 
                                    usecols=columns_required, 
                                    dtype=str)
                                    
        # Sort on site_id 
        dataframe = dataframe.sort_values(['SITE_ID'], 
                                          ascending=[True], 
                                          ignore_index=True)
        
        """ # Create new column for verbatim date
        dataframe['SAMPLE_DATE_VERBATIM'] = \
            dataframe['SAMPLE_DATE'] """
        
        # Convert to datetime      
        dataframe['SAMPLE_DATE'] = \
            pandas.to_datetime(dataframe['SAMPLE_DATE']).dt.date 
        
        # Rename columns
        renaming = {'SAMPLE_DATE': 'LAST_BIO_SURVEY'}
        dataframe = dataframe.rename(columns=renaming, errors='raise')

        """ # Reorder columns
        dataframe = dataframe.rename(columns=renaming, errors='raise')
        reordering = ['bio_site_id', 
                      'bio_water_body', 
                      'easting', 
                      'northing', 
                      'last_bio_survey']
        dataframe = dataframe.reindex(columns=reordering) """
        
        # Drop any rows that lack easting/northing data
        """ dataframe = dataframe.dropna(subset=['easting', 'northing'], axis=0) """
        num_geotagged_rows = format(len(dataframe))
        print(f'We have {num_geotagged_rows} geotagged rows.\n')
        dataframe = dataframe.reset_index(drop=True)

        # Add three new columns to contain last_<inv, macp or diat>_survey
        dataframe['LAST_INV_SURVEY'] = ''
        dataframe['LAST_MACP_SURVEY'] = ''
        dataframe['LAST_DIAT_SURVEY'] = ''
        reordering = ['LAST_INV_SURVEY', 
                      'LAST_MACP_SURVEY', 
                      'LAST_DIAT_SURVEY', 
                      'LAST_BIO_SURVEY']
        dataframe = dataframe.reindex(columns=reordering)

        # Copy last_bio_survey date into matching column
        fill_column = 'LAST_' + type.capitalize() + '_SURVEY'
        dataframe[fill_column] = dataframe['LAST_BIO_SURVEY']

        return dataframe
     
     
    @staticmethod # Since does not access or write to any class attributes         
    def join_dataframes(inv_df, macp_df, diat_df):
        print(f'\nJoining biosys databases...\n')
        dataframe = pandas.concat([inv_df, macp_df, diat_df])
                                        
        """ # Sort on bio_site_id and last_bio_survey
        dataframe = dataframe.sort_values(['SITE_ID', 'LAST_BIO_SURVEY'], 
                                            ascending=[True, False], 
                                            ignore_index=True)
        
        # Convert dates back to type string
        dataframe['LAST_BIO_SURVEY'] = dataframe['LAST_BIO_SURVEY'].astype(str)
        dataframe['LAST_INV_SURVEY'] = dataframe['LAST_INV_SURVEY'].astype(str)
        dataframe['LAST_MACP_SURVEY'] = dataframe['LAST_MACP_SURVEY'].astype(str)
        dataframe['LAST_DIAT_SURVEY'] = dataframe['LAST_DIAT_SURVEY'].astype(str) 
              
        # Collapse dataframe      
        ''' For each set of rows with the same bio_site_id, collapse rows, 
             preserving most recent last_bio_survey date information
             NB This for loop is slow - could be much more efficient '''                    
        prev_site_id = ''
            
        for row in dataframe.itertuples(): 
    
            site_id = dataframe.at[row.Index, 'bio_site_id']

            if site_id == prev_site_id: # Same site_id
                # Overwrite current site_id's last_bio_survey date with previous one 
                dataframe.at[row.Index, 'LAST_BIO_SURVEY'] = \
                    dataframe.at[row.Index-1, 'LAST_BIO_SURVEY'] 
                # Similarly, overwrite any empty dates with previous one
                if dataframe.at[row.Index, 'LAST_INV_SURVEY'] == '':
                    dataframe.at[row.Index, 'LAST_INV_SURVEY'] = \
                        dataframe.at[row.Index-1, 'LAST_INV_SURVEY']
                if dataframe.at[row.Index, 'LAST_MACP_SURVEY'] == '':
                    dataframe.at[row.Index, 'LAST_MACP_SURVEY'] = \
                        dataframe.at[row.Index-1, 'LAST_MACP_SURVEY']
                if dataframe.at[row.Index, 'LAST_DIAT_SURVEY'] == '':
                    dataframe.at[row.Index, 'LAST_DIAT_SURVEY'] = \
                        dataframe.at[row.Index-1, 'LAST_DIAT_SURVEY']
                
                # Drop prev row now its inv, macp or diat date has been stored in current row
                dataframe = dataframe.drop(row.Index-1) 
                
            prev_site_id = site_id """
        
        dataframe = dataframe.drop(columns=['LAST_BIO_SURVEY'])

        dataframe = dataframe.reset_index(drop=True)
        
        return dataframe
        

    def append_placeholder(self):
        ''' Append a 'placeholder' row on to the top of the biosys dataframe to ensure 
             AGOL infers correct field types. This will be deleted automatically 
             once the feature layer has been created in ArcGIS Online. '''                
        placeholder = {
            'SITE_ID': 'xxxx', 
            'SAMPLE_ID': 'xxxx',
            'SAMPLE_DATE': 'xxxx',
            'SAMPLE_TYPE': 'xxxx',
            'SAMPLE_TYPE_DESCRIPTION': 'xxxx',
            'SAMPLE_METHOD': 'xxxx',
            'SAMPLE_METHOD_DESCRIPTION': 'xxxx',
            'SAMPLE_REASON': 'xxxx',
            'ANALYSIS_ID': 'xxxx',
            'DATE_OF_ANALYSIS': 'xxxx',
            'ANALYSIS_TYPE': 'xxxx',
            'ANALYSIS_TYPE_DESCRIPTION': 'xxxx',
            'ANALYSIS_METHOD': 'xxxx',
            'ANALYSIS_METHOD_DESCRIPTION': 'xxxx',
            'LAST_BIO_SURVEY': '2000-01-01',
            'LAST_DIAT_SURVEY': '2000-01-01',
            'LAST_INV_SURVEY': '2000-01-01',
            'LAST_MACP_SURVEY': '2000-01-01',
#            'CaBA_Partn': '',
            'CaBA_ID': '0',
            'CaBA_Catch': 'xxxx'
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
        properties = ['SITE_ID', 
            'SAMPLE_ID',
            'SAMPLE_DATE',
            'SAMPLE_TYPE',
            'SAMPLE_TYPE_DESCRIPTION',
            'SAMPLE_METHOD',
            'SAMPLE_METHOD_DESCRIPTION',
            'SAMPLE_REASON',
            'ANALYSIS_ID',
            'DATE_OF_ANALYSIS',
            'ANALYSIS_TYPE',
            'ANALYSIS_TYPE_DESCRIPTION',
            'ANALYSIS_METHOD',
            'ANALYSIS_METHOD_DESCRIPTION',
            'LAST_BIO_SURVEY',
            'LAST_DIAT_SURVEY',
            'LAST_INV_SURVEY',
            'LAST_MACP_SURVEY',
#            'CaBA_Partn': '',
            'CaBA_ID',
            'CaBA_Catch'
        ]

        geojson = super().dataframe_to_geojson(properties)

        return geojson
                                                  
    


   

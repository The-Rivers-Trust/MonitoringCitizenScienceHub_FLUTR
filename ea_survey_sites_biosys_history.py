# Standard library imports (https://docs.python.org/3/py-modindex.html)


# Related third party imports
import pandas
import datetime

# Local application imports
from ea_ecology_and_fish_data import EAEcologyAndFishData

'''
A derived class that inherits from the base class GeoData and parent class EAEcologyAndFishData
NB __init__ is not defined so the base class __init__ is inherited and 
used to construct a class object
'''

class EASurveySitesSampleBiosys(EAEcologyAndFishData):

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
        EASurveySitesSampleBiosys.inv_datafile = \
            super().download_and_extract(EASurveySitesSampleBiosys.inv_zipfile) \
                + EASurveySitesSampleBiosys.inv_filename

        # Download and extract EA Freshwater river macrophyte surveys (Biosys) data 
        EASurveySitesSampleBiosys.macp_datafile = \
            super().download_and_extract(EASurveySitesSampleBiosys.macp_zipfile) \
                + EASurveySitesSampleBiosys.macp_filename

        # Download and extract EA Freshwater river diatom surveys (Biosys) data 
        EASurveySitesSampleBiosys.diat_datafile = \
            super().download_and_extract(EASurveySitesSampleBiosys.diat_zipfile) \
                + EASurveySitesSampleBiosys.diat_filename

              
    def process_data(self):
               
        # Read site data from csv files to dataframe
        inv_dataframe = \
            self.construct_df(EASurveySitesSampleBiosys.inv_datafile, 'inv')
        macp_dataframe = \
            self.construct_df(EASurveySitesSampleBiosys.macp_datafile, 'macp')
        diat_dataframe = \
            self.construct_df(EASurveySitesSampleBiosys.diat_datafile, 'diat')
    
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
                            'COUNT_OF_SAMPLES',
                            'MIN_SAMPLE_DATE', 
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
        dataframe['MIN_SAMPLE_DATE'] = \
            pandas.to_datetime(dataframe['MAX_SAMPLE_DATE']).dt.date 

        # Rename and reorder columns
        renaming = {'WATER_BODY': 'bio_water_body', 
                    'SITE_ID': 'bio_site_id', 
                    'COUNT_OF_SAMPLES': 'total_bio_survey_count',
                    'MIN_SAMPLE_DATE': 'first_bio_survey',
                    'MAX_SAMPLE_DATE': 'last_bio_survey', 
                    'FULL_EASTING': 'easting', 
                    'FULL_NORTHING': 'northing'}
        dataframe = dataframe.rename(columns=renaming, errors='raise')
        reordering = ['bio_site_id', 
                      'bio_water_body', 
                      'easting', 
                      'northing', 
                      'total_bio_survey_count',
                      'first_bio_survey',
                      'last_bio_survey']
        dataframe = dataframe.reindex(columns=reordering)
        
        # Drop any rows that lack easting/northing data
        dataframe = dataframe.dropna(subset=['easting', 'northing'], axis=0)
        num_geotagged_rows = format(len(dataframe))
        print(f'We have {num_geotagged_rows} geotagged rows.\n')
        dataframe = dataframe.reset_index(drop=True)

        # Add six new columns to contain first and last_<inv, macp or diat>_survey
        dataframe['first_inv_survey'] = ''
        dataframe['first_macp_survey'] = ''
        dataframe['first_diat_survey'] = ''
        dataframe['last_inv_survey'] = ''
        dataframe['last_macp_survey'] = ''
        dataframe['last_diat_survey'] = ''
        dataframe['total_inv_survey_count'] = 0
        dataframe['total_macp_survey_count'] = 0
        dataframe['total_diat_survey_count'] = 0
        reordering = ['bio_site_id', 
                      'bio_water_body', 
                      'easting', 
                      'northing', 
                      'first_inv_survey',
                      'last_inv_survey', 
                      'total_inv_survey_count',
                      'first_macp_survey', 
                      'last_macp_survey', 
                      'total_macp_survey_count',
                      'first_diat_survey', 
                      'last_diat_survey', 
                      'total_diat_survey_count',
                      'first_bio_survey',
                      'last_bio_survey',
                      'total_bio_survey_count']

        # Copy last_bio_survey date into relevant column
        fill_column1 = 'last_' + type + '_survey'
        dataframe[fill_column1] = dataframe['last_bio_survey']
        fill_column2 = 'first_' + type + '_survey'
        dataframe[fill_column2] = dataframe['first_bio_survey']
        fill_column3 = 'total_' + type + '_survey_count'
        dataframe[fill_column3] = [int(i) for i in dataframe['total_bio_survey_count'].astype(str)]
        values = {fill_column3: 0}
        dataframe.fillna(value=values)

        return dataframe
     
     
    @staticmethod # Since does not access or write to any class attributes         
    def join_dataframes(inv_df, macp_df, diat_df):
        print(f'\nJoining biosys databases...\n')
        dataframe_full = pandas.concat([inv_df, macp_df, diat_df])
        
        columns_first = ['bio_site_id', 'first_inv_survey', 'first_macp_survey', 'first_diat_survey', 'first_bio_survey']
        columns_last = ['bio_site_id', 'last_inv_survey', 'last_macp_survey', 'last_diat_survey', 'last_bio_survey']
        columns_count = ['bio_site_id', 'total_inv_survey_count', 'total_macp_survey_count', 'total_diat_survey_count', 'total_bio_survey_count']
        columns_metadata = ['bio_site_id', 'bio_water_body', 'easting', 'northing']

        dataframe_first = dataframe_full[columns_first]
        dataframe_last = dataframe_full[columns_last]
        dataframe_count = dataframe_full[columns_count]
        dataframe_metadata = dataframe_full[columns_metadata]

        # Sort on bio_site_id and last_bio_survey
        dataframe_last = dataframe_last.sort_values(['bio_site_id', 'last_bio_survey'], 
                                            ascending=[True, False], 
                                            ignore_index=True)
        
        # Convert dates back to type string
        dataframe_last['last_bio_survey'] = dataframe_last['last_bio_survey'].astype(str)
        dataframe_last['last_inv_survey'] = dataframe_last['last_inv_survey'].astype(str)
        dataframe_last['last_macp_survey'] = dataframe_last['last_macp_survey'].astype(str)
        dataframe_last['last_diat_survey'] = dataframe_last['last_diat_survey'].astype(str)
              
        # Collapse dataframe      
        ''' For each set of rows with the same bio_site_id, collapse rows, 
             preserving most recent last_bio_survey date information
             NB This for loop is slow - could be much more efficient '''                    
        prev_site_id = ''
            
        for row in dataframe_last.itertuples(): 
    
            site_id = dataframe_last.at[row.Index, 'bio_site_id']

            if site_id == prev_site_id: # Same site_id
                # Overwrite current site_id's last_bio_survey date with previous one 
                dataframe_last.at[row.Index, 'last_bio_survey'] = \
                    dataframe_last.at[row.Index-1, 'last_bio_survey'] 
                # Similarly, overwrite any empty dates with previous one
                if dataframe_last.at[row.Index, 'last_inv_survey'] == '':
                    dataframe_last.at[row.Index, 'last_inv_survey'] = \
                        dataframe_last.at[row.Index-1, 'last_inv_survey']
                if dataframe_last.at[row.Index, 'last_macp_survey'] == '':
                    dataframe_last.at[row.Index, 'last_macp_survey'] = \
                        dataframe_last.at[row.Index-1, 'last_macp_survey']
                if dataframe_last.at[row.Index, 'last_diat_survey'] == '':
                    dataframe_last.at[row.Index, 'last_diat_survey'] = \
                        dataframe_last.at[row.Index-1, 'last_diat_survey']
                
                # Drop prev row now its inv, macp or diat date has been stored in current row
                dataframe_last = dataframe_last.drop(row.Index-1) 

            prev_site_id = site_id

        dataframe_last = dataframe_last.reset_index(drop=True)

        print("Created last survey dataframe")

        # Sort on bio_site_id and first_bio_survey
        dataframe_first = dataframe_first.sort_values(['bio_site_id', 'first_bio_survey'], 
                                            ascending=[True, True], 
                                            ignore_index=True)
        
        # Convert dates back to type string
        dataframe_first['first_bio_survey'] = dataframe_first['first_bio_survey'].astype(str)
        dataframe_first['first_inv_survey'] = dataframe_first['first_inv_survey'].astype(str)
        dataframe_first['first_macp_survey'] = dataframe_first['first_macp_survey'].astype(str)
        dataframe_first['first_diat_survey'] = dataframe_first['first_diat_survey'].astype(str) 
        # dataframe_first['total_bio_survey_count'] = [int(i) for i in dataframe_first['total_bio_survey_count'].astype(str)]
              
        # Collapse dataframe      
        ''' For each set of rows with the same bio_site_id, collapse rows, 
             preserving most recent first_bio_survey date information
             NB This for loop is slow - could be much more efficient '''                    
        prev_site_id = ''
            
        for row in dataframe_first.itertuples(): 
    
            site_id = dataframe_first.at[row.Index, 'bio_site_id']

            if site_id == prev_site_id: # Same site_id
                # Overwrite current site_id's first_bio_survey date with previous one 
                dataframe_first.at[row.Index, 'first_bio_survey'] = \
                    dataframe_first.at[row.Index-1, 'first_bio_survey'] 
                # Similarly, overwrite any empty dates with previous one
                if dataframe_first.at[row.Index, 'first_inv_survey'] == '':
                    dataframe_first.at[row.Index, 'first_inv_survey'] = \
                        dataframe_first.at[row.Index-1, 'first_inv_survey']
                if dataframe_first.at[row.Index, 'first_macp_survey'] == '':
                    dataframe_first.at[row.Index, 'first_macp_survey'] = \
                        dataframe_first.at[row.Index-1, 'first_macp_survey']
                if dataframe_first.at[row.Index, 'first_diat_survey'] == '':
                    dataframe_first.at[row.Index, 'first_diat_survey'] = \
                        dataframe_first.at[row.Index-1, 'first_diat_survey']
                
                # Drop prev row now its inv, macp or diat date has been stored in current row
                dataframe_first = dataframe_first.drop(row.Index-1) 
                
            prev_site_id = site_id
        
        dataframe_first = dataframe_first.reset_index(drop=True)

        print("Created first survey dataframe")

        # Sort on bio_site_id and first_bio_survey
        dataframe_count = dataframe_count.sort_values(['bio_site_id'], 
                                            ascending=True, 
                                            ignore_index=True)
        
        # Ensure counts are set to integers
        dataframe_count['total_inv_survey_count'] = dataframe_count['total_inv_survey_count'].astype(int)
        dataframe_count['total_macp_survey_count'] = dataframe_count['total_macp_survey_count'].astype(int)
        dataframe_count['total_diat_survey_count'] = dataframe_count['total_diat_survey_count'].astype(int)
        dataframe_count['total_bio_survey_count'] = dataframe_count['total_bio_survey_count'].astype(int)
        
        # Collapse dataframe      
        ''' For each set of rows with the same bio_site_id, collapse rows, 
             preserving most recent last_bio_survey date information
             NB This for loop is slow - could be much more efficient '''                    
        prev_site_id = ''
            
        for row in dataframe_count.itertuples(): 
    
            site_id = dataframe_count.at[row.Index, 'bio_site_id']

            if site_id == prev_site_id: # Same site_id
                # Sum the survey counts
                """ dataframe_count.at[row.Index, 'total_bio_survey_count'] = \
                    dataframe_count.at[row.Index, 'total_bio_survey_count'] + dataframe_count.at[row.Index-1, 'total_bio_survey_count']  """ 
                # Overwrite current site_id's count_bio_survey date with previous one 
                # Similarly, overwrite any empty dates with previous one
                if dataframe_count.at[row.Index, 'total_inv_survey_count'] == 0:
                    dataframe_count.at[row.Index, 'total_inv_survey_count'] = \
                        dataframe_count.at[row.Index-1, 'total_inv_survey_count']
                if dataframe_count.at[row.Index, 'total_macp_survey_count'] == 0:
                    dataframe_count.at[row.Index, 'total_macp_survey_count'] = \
                        dataframe_count.at[row.Index-1, 'total_macp_survey_count']
                if dataframe_count.at[row.Index, 'total_diat_survey_count'] == 0:
                    dataframe_count.at[row.Index, 'total_diat_survey_count'] = \
                        dataframe_count.at[row.Index-1, 'total_diat_survey_count']

                # Drop prev row now its inv, macp or diat date has been stored in current row
                dataframe_count = dataframe_count.drop(row.Index-1) 
                
            prev_site_id = site_id
        
        dataframe_count = dataframe_count.reset_index(drop=True)

        dataframe_count['total_bio_survey_count'] = dataframe_count['total_inv_survey_count'] + dataframe_count['total_macp_survey_count'] + dataframe_count['total_diat_survey_count']
        
        print("Created survey count dataframe")

        # Sort on bio_site_id and first_bio_survey
        dataframe_metadata = dataframe_metadata.sort_values(['bio_site_id'], 
                                            ignore_index=True)
        
        # Collapse dataframe      
        ''' For each set of rows with the same bio_site_id, collapse rows, 
             preserving most recent last_bio_survey date information
             NB This for loop is slow - could be much more efficient '''                    
        prev_site_id = ''
            
        for row in dataframe_metadata.itertuples(): 
    
            site_id = dataframe_metadata.at[row.Index, 'bio_site_id']

            if site_id == prev_site_id: # Same site_id
                
                dataframe_metadata = dataframe_metadata.drop(row.Index-1) 
                
            prev_site_id = site_id
        
        dataframe_metadata = dataframe_metadata.reset_index(drop=True)     

        print("Created survey metadata dataframe")   

        dataframe = pandas.merge(dataframe_metadata, dataframe_first, on = 'bio_site_id', how= 'outer')
        dataframe = pandas.merge(dataframe, dataframe_last, on = 'bio_site_id', how= 'outer')
        dataframe = pandas.merge(dataframe, dataframe_count, on = 'bio_site_id', how= 'outer')

        reordering = ['bio_site_id', 
                      'bio_water_body', 
                      'easting', 
                      'northing', 
                      'first_bio_survey',
                      'last_bio_survey',
                      'total_bio_survey_count',
                      'first_inv_survey',
                      'last_inv_survey', 
                      'total_inv_survey_count',
                      'first_macp_survey', 
                      'last_macp_survey', 
                      'total_macp_survey_count',
                      'first_diat_survey', 
                      'last_diat_survey', 
                      'total_diat_survey_count']
        
        dataframe = dataframe[reordering]

        """ dataframe_dates = ['first_inv_survey', 'first_macp_survey', 'first_diat_survey', 'first_bio_survey', 'last_inv_survey', 'last_macp_survey', 'last_diat_survey', 'last_bio_survey']
        for column in dataframe_dates :
            dataframe[column] = pandas.to_datetime(dataframe[column]) """
        
        print("Merged each dataframe by site id to create dataframe")

        return dataframe
        

    def append_placeholder(self):
        ''' Append a 'placeholder' row on to the top of the biosys dataframe to ensure 
             AGOL infers correct field types. This will be deleted automatically 
             once the feature layer has been created in ArcGIS Online. '''                
        placeholder = {
            'bio_site_id': 'number',
            'bio_water_body': 'PLACEHOLDER',
            'lat': 0.0,
            'long': 0.0,
            'first_bio_survey': '2000/12/24',
            'first_diat_survey': '2000/12/24',
            'first_inv_survey': '2000/12/24',
            'first_macp_survey': '2000/12/24', 
            'last_bio_survey': '2000/12/24',
            'last_diat_survey': '2000/12/24',
            'last_inv_survey': '2000/12/24',
            'last_macp_survey': '2000/12/24', 
            'total_bio_survey_count': 0,
            'total_diat_survey_count' : 0,
            'total_inv_survey_count': 0,
            'total_macp_survey_count': 0,
#            'CaBA_Partn': '',
            'CaBA_ID': 'id',
            'CaBA_Catch': 'catchment'
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
                      'first_inv_survey',
                      'last_inv_survey', 
                      'total_inv_survey_count',
                      'first_macp_survey', 
                      'last_macp_survey', 
                      'total_macp_survey_count',
                      'first_diat_survey', 
                      'last_diat_survey', 
                      'total_diat_survey_count',
                      'first_bio_survey',
                      'last_bio_survey',
                      'total_bio_survey_count',
#                      'CaBA_Partn',
                      'CaBA_ID',
                      'CaBA_Catch'
#                      'WFD_RBD'
        ]

        geojson = super().dataframe_to_geojson(properties)

        return geojson
                                                  
    


   

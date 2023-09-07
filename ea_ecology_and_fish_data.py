from geo_data import GeoData
import osgb # need to do a 'pip3 install osgb' from command line
from arcgis.features import FeatureLayerCollection


''' 
A derived class that inherits from the base class GeoData
NB __init__ is not defined so the base class __init__ is inherited and 
used to construct a class object
'''


class EAEcologyAndFishData(GeoData):
    
    # Define a class attribute:
    fish_and_bio_url = 'https://environment.data.gov.uk/ecology/explorer/downloads/'
    

    def download_and_extract(self, file):     
        
        # Download and extract data from Ecology and Fish Bulk Downloads webpage
        datafile = super().download_and_extract(EAEcologyAndFishData.fish_and_bio_url, file)
        
        return datafile
        

    def easting_northing_to_wgs84(self):
        print('\nConverting easting-northing to lat-long...\n')
    
        # Convert easting-northing to floats
        self.dataframe['easting'] = self.dataframe['easting'].astype(float)
        self.dataframe['northing'] = self.dataframe['northing'].astype(float)

        # Convert eastings and northings to lat and long 
        for row in self.dataframe.itertuples():
            easting = self.dataframe.at[row.Index, 'easting']
            northing = self.dataframe.at[row.Index, 'northing']
            (lat, long) = osgb.grid_to_ll(easting, northing)
            (lat, long) = (round(lat, 6), round(long, 6))
            self.dataframe.at[row.Index, 'easting'] = lat
            self.dataframe.at[row.Index, 'northing'] = long

        renaming = {'easting': 'lat', 'northing': 'long'}
        self.dataframe = self.dataframe.rename(columns=renaming, errors='raise')


    ''' MOVED TO GEO_DATA.PY - NEED TO TEST STILL WORKS AFTER CHANGE
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
    '''
        
        





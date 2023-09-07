# Standard library imports (https://docs.python.org/3/py-modindex.html)
import os


# Related third party imports
from arcgis.gis import GIS


# Local application imports


class AGOLUser:

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.working_dir = os.getcwd()
        self.gis = '' 
        
        # Log into ArcGIS Online account
        print('\nLogging in to ArcGIS Online...')
        self.gis = GIS("https://www.arcgis.com", username, password)     
        print('Logged in as ' + str(self.gis.properties.user.username) + '\n')
                

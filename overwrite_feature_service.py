# Standard library imports (https://docs.python.org/3/py-modindex.html)
import dataclasses
import sys
from typing import List
import time

# Related third party imports

# Local application imports
from agol_user import AGOLUser
from geo_data import GeoData
from ea_ecology_and_fish_data import EAEcologyAndFishData
from ea_survey_sites_biosys import EASurveySitesBiosys
from ea_survey_sites_biosys_history import EASurveySitesSampleBiosys
from ea_survey_sites_fish import EASurveySitesFish
from ea_survey_sites_fish_sample_history import EASurveySitesFishHistory
from ea_survey_sites_fish_sample_history_2 import EASurveySitesFishHistory2
from ea_water_qual_archives import EAWaterQualArchives
from ea_water_qual_archives_sample_history import EAWaterQualSampleArchives
from ea_water_qual_archives_sample_history_2 import EAWaterQualSampleArchives2
from ea_water_qual_archives_sample_history_3 import EAWaterQualSampleArchives3
from nbnatlas_occurrences import NBNatlasOccurrences


USAGE = f'Usage: python {sys.argv[0]} <AGOL itemname> <AGOL f layerid> <username> <password>'
valid_datasource = ["EA_survey_sites", "EA_water_qual_archives", "NBNatlas_occurrences"]
valid_EA_survey_sites_dataobject = ["biosys", "biosys_history", "fish", "fish_history", "fish_history2"]
valid_EA_water_qual_archives_dataobject = ["sampling_points", "sampling_history", "sampling_history_2", "sampling_history_3"]



@dataclasses.dataclass # See e.g. at https://realpython.com/python-kwargs-and-args/
class Arguments:
    itemname: str
    itemid: str
    username: str
    password: str
     

def validate(args: List[str]):
    if len(args) != 4:
        print('Incorrect number of arguments.')
        raise SystemExit(USAGE)
    arguments = Arguments(*args)
     
    # Infer dataobject and datasource from itemname and assign to arguments
    if 'EA_water_qual_archives' in arguments.itemname:
        arguments.dataobject = arguments.itemname.replace('EA_water_qual_archives_', '')
        arguments.datasource = 'EA_water_qual_archives'
    elif 'EA_survey_sites' in arguments.itemname:
        arguments.dataobject = arguments.itemname.replace('EA_survey_sites_', '')
        arguments.datasource = 'EA_survey_sites'
    else:
        arguments.dataobject = arguments.itemname[arguments.itemname.rindex('_')+1:]
        arguments.datasource = arguments.itemname.replace('_'+arguments.dataobject, '')
    
    print(f'\nItem name: {arguments.itemname}\nItem id: {arguments.itemid}') 
    print(f'\nData source: {arguments.datasource}\nData object: {arguments.dataobject}') 
    
    if arguments.datasource not in valid_datasource:
        print(f'Invalid data source. Must be one of:\n{valid_datasource}.')
        raise SystemExit(USAGE)
        
    if (arguments.datasource == 'EA_survey_sites' 
        and arguments.dataobject not in valid_EA_survey_sites_dataobject):
        print(f'Invalid dataobject. Must be one of:\n{valid_EA_survey_sites_dataobject}.')
        raise SystemExit(USAGE)   
        
    if (arguments.datasource == 'EA_water_qual_archives' 
        and arguments.dataobject not in valid_EA_water_qual_archives_dataobject):
        print(f'Invalid dataobject. Must be one of:\n{valid_EA_water_qual_archives_dataobject}.')
        raise SystemExit(USAGE)    
        
    return arguments
       

def overwrite_f_service(args):

    # Create AGOLUser object
    user_obj = AGOLUser(args.username, args.password)
    
    # Create GeoData object
    if args.datasource == "EA_survey_sites" and args.dataobject == "fish":
        geodata_obj = EASurveySitesFish(args.datasource, args.dataobject, user_obj)
    elif args.datasource == "EA_survey_sites" and args.dataobject == "fish_history":
        geodata_obj = EASurveySitesFishHistory(
            args.datasource, args.dataobject, user_obj
        )
    elif args.datasource == "EA_survey_sites" and args.dataobject == "fish_history2":
        geodata_obj = EASurveySitesFishHistory2(
            args.datasource, args.dataobject, user_obj
        )
    elif args.datasource == "EA_survey_sites" and args.dataobject == "biosys":
        geodata_obj = EASurveySitesBiosys(args.datasource, args.dataobject, user_obj)
    elif args.datasource == "EA_survey_sites" and args.dataobject == "biosys_history":
        geodata_obj = EASurveySitesSampleBiosys(args.datasource, args.dataobject, user_obj)
    elif (
        args.datasource == "EA_water_qual_archives"
        and args.dataobject == "sampling_points"
    ):
        geodata_obj = EAWaterQualArchives(args.datasource, args.dataobject, user_obj)
    elif (
        args.datasource == "EA_water_qual_archives"
        and args.dataobject == "sampling_history"
    ):
        geodata_obj = EAWaterQualSampleArchives(args.datasource, args.dataobject, user_obj)
    elif (
        args.datasource == "EA_water_qual_archives"
        and args.dataobject == "sampling_history_2"
    ):
        geodata_obj = EAWaterQualSampleArchives2(args.datasource, args.dataobject, user_obj)
    elif (
        args.datasource == "EA_water_qual_archives"
        and args.dataobject == "sampling_history_3"
    ):
        geodata_obj = EAWaterQualSampleArchives3(args.datasource, args.dataobject, user_obj)
    elif args.datasource == "NBNatlas_occurrences":
        geodata_obj = NBNatlasOccurrences(args.datasource, args.dataobject, user_obj)
    else:
        print("Unable to create GeoData object\n")
        raise SystemExit()    
           
    print(geodata_obj)
    print(f'Object name: {geodata_obj.name}')
    
    # Check feature service items of given name and id already exists
    geodata_obj.agol_f_layer_id = args.itemid
    
    if geodata_obj.check_item_already_exists() == False:
        print(f"\nCould not find ArcGIS Online item with " \
              f"id '{geodata_obj.agol_f_layer_id}'.")
        raise SystemExit()      
   
#    raise SystemExit()
    geodata_obj.get_data()  
    geodata_obj.process_data()
    geodata_obj.create_geojson_file()
    #raise SystemExit()
    geodata_obj.overwrite_f_layer()

    print("\nDone!\n")
    

def main() -> None:
    args = sys.argv[1:]
    if not args:
        raise SystemExit(USAGE)

    # Validate the arguments passed at the command line
    validated_args = validate(args)
        
    # Create a GeoData object and associated feature service
    overwrite_f_service(validated_args)


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    runtime = round((end - start) / 60, 2)
    print(f"Function took {runtime} minutes to run.")
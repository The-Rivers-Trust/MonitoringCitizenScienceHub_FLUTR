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
from ea_survey_sites_biosys_odm import EASurveySitesBiosysODM
from ea_survey_sites_fish import EASurveySitesFish
from ea_survey_sites_fish_sample_history import EASurveySitesFishHistory
from ea_survey_sites_fish_sample_history_2 import EASurveySitesFishHistory2
from ea_water_qual_archives import EAWaterQualArchives
from ea_water_qual_archives_sample_history import EAWaterQualSampleArchives
from ea_water_qual_archives_sample_history_2 import EAWaterQualSampleArchives2
from ea_water_qual_archives_sample_history_3 import EAWaterQualSampleArchives3
from ea_water_qual_archives_sample_history_1_yorkshire import EAWaterQualSampleArchives1Yorkshire
from ea_water_qual_archives_sample_history_2_yorkshire import EAWaterQualSampleArchives2Yorkshire
from ea_hydrology_water_qual import EAHydrologyWQ
from ea_hydrology_flow import EAHydrologyFlow
from nbnatlas_occurrences import NBNatlasOccurrences


USAGE = f"Usage: python {sys.argv[0]} <datasource> <dataobject> <username> <password>"
valid_datasource = ["EA_survey_sites", "EA_water_qual_archives", "EA_hydrology", "NBNatlas_occurrences"]
valid_EA_survey_sites_dataobject = ["biosys", "biosys_history", "biosys_odm", "fish", "fish_history", "fish_history2"]
valid_EA_water_qual_archives_dataobject = ["sampling_points", "sampling_history", "sampling_history_2", "sampling_history_3", "sampling_history_1_yorkshire", "sampling_history_2_yorkshire"]
valid_EA_hydrology_dataobject = ["stations", "water_qual", "flow"]

@dataclasses.dataclass  # See e.g. at https://realpython.com/python-kwargs-and-args/
class Arguments:
    datasource: str
    dataobject: str
    username: str
    password: str


def validate(args: List[str]):
    if len(args) != 4:
        print("Incorrect number of arguments.")
        raise SystemExit(USAGE)
    arguments = Arguments(*args)
    print(f"\nData source: {arguments.datasource}\nData object: {arguments.dataobject}")

    if arguments.datasource not in valid_datasource:
        print(f"Invalid data source. Must be one of:\n{valid_datasource}.")
        raise SystemExit(USAGE)

    if (
        arguments.datasource == "EA_survey_sites"
        and arguments.dataobject not in valid_EA_survey_sites_dataobject
    ):
        print(
            f"Invalid dataobject. Must be one of:\n{valid_EA_survey_sites_dataobject}."
        )
        raise SystemExit(USAGE)

    if (
        arguments.datasource == "EA_water_qual_archives"
        and arguments.dataobject not in valid_EA_water_qual_archives_dataobject
    ):
        print(
            f"Invalid dataobject. Must be one of:\n{valid_EA_water_qual_archives_dataobject}."
        )
        raise SystemExit(USAGE)

    return arguments


def create_f_service(args):
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
    elif args.datasource == "EA_survey_sites" and args.dataobject == "biosys_odm":
        geodata_obj = EASurveySitesBiosysODM(args.datasource, args.dataobject, user_obj)    
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
    elif (
        args.datasource == "EA_water_qual_archives"
        and args.dataobject == "sampling_history_1_yorkshire"
    ):
        geodata_obj = EAWaterQualSampleArchives1Yorkshire(args.datasource, args.dataobject, user_obj)
    elif (
        args.datasource == "EA_water_qual_archives"
        and args.dataobject == "sampling_history_2_yorkshire"
    ):
        geodata_obj = EAWaterQualSampleArchives2Yorkshire(args.datasource, args.dataobject, user_obj)
    elif args.datasource == "EA_hydrology" and args.dataobject == "flow":
        geodata_obj = EAHydrologyFlow(args.datasource, args.dataobject, user_obj)
    elif args.datasource == "EA_hydrology" and args.dataobject == "water_qual":
        geodata_obj = EAHydrologyWQ(args.datasource, args.dataobject, user_obj)
    elif args.datasource == "NBNatlas_occurrences":
        geodata_obj = NBNatlasOccurrences(args.datasource, args.dataobject, user_obj)
    else:
        print("Unable to create GeoData object\n")
        raise SystemExit()

    print(geodata_obj)
    print(f"Object name: {geodata_obj.name}")

    # Check feature service items of given name don't already exist
    if geodata_obj.check_item_already_exists() == True:
        print("\nEither delete items or run overwrite_feature_service.py instead")
        raise SystemExit()

    geodata_obj.get_data()
    geodata_obj.process_data()
    geodata_obj.create_geojson_file()
    geodata_obj.add_data_item()
    geodata_obj.publish_f_layer()
    geodata_obj.publish_f_layer_view()

    print("\nDone!\n")


def main() -> None:
    args = sys.argv[1:]
    if not args:
        raise SystemExit(USAGE)

    # Validate the arguments passed at the command line
    validated_args = validate(args)

    # Create a GeoData object and associated feature service
    create_f_service(validated_args)


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    runtime = round((end - start) / 60, 2)
    print(f"Function took {runtime} minutes to run.")

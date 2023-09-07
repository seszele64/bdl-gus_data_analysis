# import
from dataclasses import dataclass
import os

# create dataclass


@dataclass
class Paths:
    current_path: str = os.path.dirname(os.path.abspath(__file__))
    input_data_path: str = os.path.join(current_path, "data")
    output_data_path: str = os.path.join(input_data_path, "output")
    charts_path: str = os.path.join(current_path, "charts")


# create instance of dataclass
paths = Paths()

# if folder does not exist create it function


def create_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)


# create folders if they do not exist
for path in [paths.__dict__[key] for key in paths.__dict__]:
    create_folder(path)
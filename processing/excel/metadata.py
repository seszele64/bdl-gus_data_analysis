# -------------------------------- xlsx files -------------------------------- #

# get metadata from xlsx file

from datetime import datetime
from dataclasses import dataclass

## define metadata dataclass -> string is passed as argument
@dataclass
class Metadata:
    """Metadata for an excel file"""
    category: str = None
    indicator_code: str = None
    timestamp: datetime = None
    alternative_indicator_name: str = None
    
    def __str__(self) -> str:
        return f"category: {self.category}, indicator_code: {self.indicator_code}, timestamp: {self.timestamp}, alternative_indicator_name: {self.alternative_indicator_name}"
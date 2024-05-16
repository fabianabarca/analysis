from celery import shared_task

import logging
from datetime import datetime
import pytz
import zipfile
import io
import pandas as pd
import requests

from .models import *


@shared_task
def get_vehiclepositions():
    print("Fetching VehiclePositions")
    return "Fetching VehiclePositions"

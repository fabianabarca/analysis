from celery import shared_task

import logging
from datetime import datetime
import pytz
import zipfile
import io
import json
import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2 as gtfs_rt
from google.protobuf import json_format

from .models import *


@shared_task
def get_vehiclepositions():
    # GTFS configuration
    vehicle_positions = gtfs_rt.FeedMessage()
    vehicle_positions_url = "https://cdn.mbta.com/realtime/VehiclePositions.pb"
    vehicle_positions_response = requests.get(vehicle_positions_url)
    vehicle_positions.ParseFromString(vehicle_positions_response.content)
    vehicle_positions_json = json_format.MessageToJson(
        vehicle_positions, preserving_proto_field_name=True
    )
    vehicle_positions_json = json.loads(vehicle_positions_json)
    vehicle_positions_df = pd.json_normalize(vehicle_positions_json["entity"], sep="_")
    vehicle_positions_df.drop(columns=["vehicle_multi_carriage_details"], inplace=True)
    vehicle_positions_df.rename(columns={"id": "entity_id"}, inplace=True)
    vehicle_positions_df["vehicle_trip_start_date"] = vehicle_positions_df["vehicle_trip_start_date"].astype(str)
    objects = [VehiclePosition(**row) for row in vehicle_positions_df.to_dict(orient="records")]
    VehiclePosition.objects.bulk_create(objects)

    return "Hola"


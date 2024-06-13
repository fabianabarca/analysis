from celery import shared_task
from datetime import datetime, timedelta
import pytz
import json
import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2 as gtfs_rt
from google.protobuf import json_format

from .models import *


@shared_task
def get_vehiclepositions():
    providers = Provider.objects.filter(is_active=True)
    for provider in providers:
        vehicle_positions = gtfs_rt.FeedMessage()
        vehicle_positions_response = requests.get(provider.vehicle_positions_url)
        vehicle_positions.ParseFromString(vehicle_positions_response.content)

        feed_message = FeedMessage(
            feed_message_id=f"{provider.code}-vehicle-{vehicle_positions.header.timestamp}",
            provider=provider,
            entity_type="vehicle",
            timestamp=datetime.fromtimestamp(
                int(vehicle_positions.header.timestamp),
                tz=pytz.timezone(provider.timezone),
            ),
            incrementality=vehicle_positions.header.incrementality,
            gtfs_realtime_version=vehicle_positions.header.gtfs_realtime_version,
        )
        feed_message.save()
        # Create JSON object
        vehicle_positions_json = json_format.MessageToJson(
            vehicle_positions, preserving_proto_field_name=True
        )
        vehicle_positions_json = json.loads(vehicle_positions_json)
        # Normalize JSON object with Pandas in a DataFrame
        vehicle_positions_df = pd.json_normalize(
            vehicle_positions_json["entity"], sep="_"
        )
        # Process DataFrame to comply with database schema
        vehicle_positions_df.rename(columns={"id": "entity_id"}, inplace=True)
        vehicle_positions_df["feed_message"] = feed_message
        # Drop unnecessary columns
        try:
            vehicle_positions_df.drop(
                columns=["vehicle_multi_carriage_details"],
                inplace=True,
            )
        except:
            pass
        # Fix entity timestamp
        vehicle_positions_df["vehicle_timestamp"] = pd.to_datetime(
            vehicle_positions_df["vehicle_timestamp"].astype(int), unit="s", utc=True
        )
        # Fix trip start date
        vehicle_positions_df["vehicle_trip_start_date"] = pd.to_datetime(
            vehicle_positions_df["vehicle_trip_start_date"], format="%Y%m%d"
        )
        vehicle_positions_df["vehicle_trip_start_date"].fillna(
            datetime.now().date(), inplace=True
        )
        # Fix trip start time
        vehicle_positions_df["vehicle_trip_start_time"] = pd.to_timedelta(
            vehicle_positions_df["vehicle_trip_start_time"]
        )
        vehicle_positions_df["vehicle_trip_start_time"].fillna(
            timedelta(hours=0, minutes=0, seconds=0), inplace=True
        )
        # Fix trip direction
        vehicle_positions_df["vehicle_trip_direction_id"].fillna(-1, inplace=True)
        # Fix current stop sequence
        vehicle_positions_df["vehicle_current_stop_sequence"].fillna(-1, inplace=True)
        # Create vehicle position point
        vehicle_positions_df["vehicle_position_point"] = vehicle_positions_df.apply(
            lambda x: f"POINT ({x.vehicle_position_longitude} {x.vehicle_position_latitude})",
            axis=1,
        )
        # Save to database
        objects = [
            VehiclePosition(**row)
            for row in vehicle_positions_df.to_dict(orient="records")
        ]
        VehiclePosition.objects.bulk_create(objects)

    return "VehiclePositions saved to database"


@shared_task
def get_tripupdates():
    providers = Provider.objects.filter(is_active=True)
    for provider in providers:
        try:
            trip_updates_response = requests.get(provider.trip_updates_url, timeout=10)
            trip_updates_response.raise_for_status()  
        except requests.RequestException as e:
            print(f"Error fetching trip updates from {provider.trip_updates_url}: {str(e)}")
            continue  

        try:
            trip_updates = gtfs_rt.FeedMessage()
            trip_updates.ParseFromString(trip_updates_response.content)

            feed_message = FeedMessage(
                feed_message_id=f"{provider.code}-trip_updates-{trip_updates.header.timestamp}",
                provider=provider,
                entity_type="trip_update",
                timestamp=datetime.fromtimestamp(
                    int(trip_updates.header.timestamp),
                    tz=pytz.timezone(provider.timezone),
                ),
                incrementality=trip_updates.header.incrementality,
                gtfs_realtime_version=trip_updates.header.gtfs_realtime_version,
            )
            feed_message.save()

            trip_updates_json = json_format.MessageToJson(trip_updates, preserving_proto_field_name=True)
            trip_updates_json = json.loads(trip_updates_json)
            trip_updates_df = pd.json_normalize(trip_updates_json["entity"], sep="_")
            trip_updates_df.rename(columns={"id": "entity_id"}, inplace=True)
            trip_updates_df["feed_message"] = feed_message

            
            trip_updates_df.to_csv("trip_updates.csv")  

          
            objects = [
                TripUpdate(**row)
                for row in trip_updates_df.to_dict(orient="records")
            ]
            TripUpdate.objects.bulk_create(objects)
        except Exception as e:
            print(f"Error processing trip updates for provider {provider.code}: {str(e)}")

    return "TripUpdates saved to database"
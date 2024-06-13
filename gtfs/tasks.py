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
            print(
                f"Error fetching trip updates from {provider.trip_updates_url}: {str(e)}"
            )
            continue

        # Parse FeedMessage object from Protobuf
        trip_updates = gtfs_rt.FeedMessage()
        trip_updates.ParseFromString(trip_updates_response.content)

        # Build FeedMessage object
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
        # Save FeedMessage object
        feed_message.save()

        # Build TripUpdate DataFrame
        trip_updates_json = json_format.MessageToJson(
            trip_updates, preserving_proto_field_name=True
        )
        trip_updates_json = json.loads(trip_updates_json)
        trip_updates_df = pd.json_normalize(trip_updates_json["entity"], sep="_")
        trip_updates_df.rename(columns={"id": "entity_id"}, inplace=True)
        trip_updates_df["feed_message"] = feed_message

        # Fix entity timestamp
        trip_updates_df["trip_update_timestamp"].fillna(
            datetime.now().timestamp(), inplace=True
        )
        trip_updates_df["trip_update_timestamp"] = pd.to_datetime(
            trip_updates_df["trip_update_timestamp"].astype(int), unit="s", utc=True
        )
        # Fix trip start date
        trip_updates_df["trip_update_trip_start_date"] = pd.to_datetime(
            trip_updates_df["trip_update_trip_start_date"], format="%Y%m%d"
        )
        trip_updates_df["trip_update_trip_start_date"].fillna(
            datetime.now().date(), inplace=True
        )
        # Fix trip start time
        trip_updates_df["trip_update_trip_start_time"] = pd.to_timedelta(
            trip_updates_df["trip_update_trip_start_time"]
        )
        trip_updates_df["trip_update_trip_start_time"].fillna(
            timedelta(hours=0, minutes=0, seconds=0), inplace=True
        )
        # Fix trip direction
        trip_updates_df["trip_update_trip_direction_id"].fillna(-1, inplace=True)

        for i, trip_update in trip_updates_df.iterrows():
            this_trip_update = TripUpdate(
                entity_id=trip_update["entity_id"],
                feed_message=trip_update["feed_message"],
                trip_update_trip_trip_id=trip_update["trip_update_trip_trip_id"],
                trip_update_trip_route_id=trip_update["trip_update_trip_route_id"],
                trip_update_trip_direction_id=trip_update[
                    "trip_update_trip_direction_id"
                ],
                trip_update_trip_start_time=trip_update["trip_update_trip_start_time"],
                trip_update_trip_start_date=trip_update["trip_update_trip_start_date"],
                trip_update_trip_schedule_relationship=trip_update[
                    "trip_update_trip_schedule_relationship"
                ],
                trip_update_vehicle_id=trip_update["trip_update_vehicle_id"],
                trip_update_vehicle_label=trip_update["trip_update_vehicle_label"],
                # trip_update_vehicle_license_plate=trip_update["trip_update_vehicle_license_plate"],
                # trip_update_vehicle_wheelchair_accessible=trip_update["trip_update_vehicle_wheelchair_accessible"],
                trip_update_timestamp=trip_update["trip_update_timestamp"],
                # trip_update_delay=trip_update["trip_update_delay"],
            )
            # Save this TripUpdate object
            this_trip_update.save()

            # Build StopTimeUpdate DataFrame
            stop_time_updates_json = str(trip_update["trip_update_stop_time_update"])
            stop_time_updates_json = stop_time_updates_json.replace("'", '"')
            stop_time_updates_json = json.loads(stop_time_updates_json)
            stop_time_updates_df = pd.json_normalize(stop_time_updates_json, sep="_")
            stop_time_updates_df["trip_update"] = this_trip_update

            # Fix arrival time timestamp
            if "arrival_time" in stop_time_updates_df.columns:
                stop_time_updates_df["arrival_time"].fillna(
                    datetime.now().timestamp(), inplace=True
                )
                stop_time_updates_df["arrival_time"] = pd.to_datetime(
                    stop_time_updates_df["arrival_time"].astype(int), unit="s", utc=True
                )
            # Fix departure time timestamp
            if "departure_time" in stop_time_updates_df.columns:
                stop_time_updates_df["departure_time"].fillna(
                    datetime.now().timestamp(), inplace=True
                )
                stop_time_updates_df["departure_time"] = pd.to_datetime(
                    stop_time_updates_df["departure_time"].astype(int),
                    unit="s",
                    utc=True,
                )
            # Fix arrival uncertainty
            if "arrival_uncertainty" in stop_time_updates_df.columns:
                stop_time_updates_df["arrival_uncertainty"].fillna(0, inplace=True)
            # Fix departure uncertainty
            if "departure_uncertainty" in stop_time_updates_df.columns:
                stop_time_updates_df["departure_uncertainty"].fillna(0, inplace=True)
            # Fix arrival delay
            if "arrival_delay" in stop_time_updates_df.columns:
                stop_time_updates_df["arrival_delay"].fillna(0, inplace=True)
            # Fix departure delay
            if "departure_delay" in stop_time_updates_df.columns:
                stop_time_updates_df["departure_delay"].fillna(0, inplace=True)

            # Save to database
            objects = [
                StopTimeUpdate(**row)
                for row in stop_time_updates_df.to_dict(orient="records")
            ]
            StopTimeUpdate.objects.bulk_create(objects)

    return "TripUpdates saved to database"

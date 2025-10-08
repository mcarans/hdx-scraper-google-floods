#!/usr/bin/python
"""Google Floods scraper"""

import logging
from os.path import join
from typing import Dict, List, Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        tempdir: str,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self._base_url = self._configuration["base_url"]

    def get_events(self) -> List:
        severe_events = []
        page_token = None

        n = 0
        while True:
            url = f"{self._base_url}/severeEvents:searchLatestSevereEvents"
            parameters = {"pageToken": page_token}
            severe_events_response = self._retriever.download_json(
                url,
                filename=f"severe_events_{n}.json",
                post=True,
                parameters=parameters,
            )
            if "error" in severe_events_response:
                logger.error(severe_events_response)
                return severe_events
            severe_events.extend(severe_events_response.get("severeEvents", []))
            nextPageToken = severe_events_response.get("nextPageToken")
            if not nextPageToken:
                break
            page_token = nextPageToken
            n += 1
        return severe_events

    def get_kml(self, event: Dict, filename: str) -> str:
        polygon_id = event["eventPolygonId"]
        url = f"{self._base_url}/serializedPolygons/{polygon_id}"
        serialized_polygon_response = self._retriever.download_json(
            url, filename=f"polygons_{polygon_id}.json"
        )
        if "error" in serialized_polygon_response:
            logger.error(
                f"Error fetching polygon: {serialized_polygon_response['error']}"
            )
            return None
        path = join(self._tempdir, filename)
        with open(path, "w") as f:
            f.write(serialized_polygon_response["kml"])
        return path

    def generate_dataset(self, event: Dict) -> Optional[Dataset]:
        interval = event["eventInterval"]
        start_date = parse_date(interval["startTime"])
        endtime = interval.get("endTime")
        if not endtime:
            endtime = interval["minimumEndTime"]
        end_date = parse_date(endtime)
        countryiso2s = event["affectedCountryCodes"]
        countryiso3s = [
            Country.get_iso3_from_iso2(countryiso2) for countryiso2 in countryiso2s
        ]

        countryiso3s_str = "-".join(countryiso3s)
        dataset_name = (
            f"flood-{countryiso3s_str}-{iso_string_from_datetime(start_date)}".lower()
        )

        countrynames_list = [
            Country.get_country_name_from_iso3(countryiso3)
            for countryiso3 in countryiso3s
        ]
        countrynames = ", ".join(countrynames_list)
        formatted_date = (
            start_date.strftime("%B") + f" {start_date.day} {start_date.year}"
        )
        dataset_title = f"Flood in {countrynames} starting on {formatted_date}"

        formatted_enddate = end_date.strftime("%B") + f" {end_date.day} {end_date.year}"
        affected = event["affectedPopulation"]
        area = event["areaKm2"]
        parts = countrynames.rsplit(",", 1)
        countrynames = " and".join(parts)
        dataset_description = f"Severe flood event during {formatted_date}-{formatted_enddate} affecting {affected} people and covering {area} square km in {countrynames}."
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
                "notes": dataset_description,
            }
        )
        dataset.add_country_locations(countryiso2s)
        dataset.set_time_period(start_date, end_date)
        dataset_tags = ["flooding", "geodata"]
        dataset.add_tags(dataset_tags)
        dataset.set_subnational(True)

        filename = dataset_name.replace("-", "_") + ".kml"
        resource = Resource(
            {
                "name": filename,
                "description": dataset_title,
            }
        )
        resource.set_format("kml")
        kml_file = self.get_kml(event, filename)
        resource.set_file_to_upload(kml_file)
        dataset.add_update_resource(resource)

        return dataset

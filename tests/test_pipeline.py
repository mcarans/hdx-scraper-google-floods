from os.path import join

from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.google.floods.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestGoogleFloods",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)
                events = pipeline.get_events()
                dataset = pipeline.generate_dataset(events[0])
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "caveats": None,
                    "data_update_frequency": 365,
                    "dataset_date": "[2025-09-10T00:00:00 TO 2025-10-13T23:59:59]",
                    "dataset_source": "Google",
                    "groups": [{"name": "gin"}, {"name": "mli"}],
                    "license_id": "cc-by",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "methodology": "Other",
                    "methodology_other": "methodology",
                    "name": "flood-gin-mli-2025-09-10",
                    "notes": "Severe flood event during September 10 2025-October 13 2025 "
                    "affecting 4176560 people and covering 8368.11 square km in Guinea "
                    "and Mali.",
                    "owner_org": "hdx",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "flooding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Flood in Guinea, Mali starting on September 10 2025",
                }
                resource = dataset.get_resource()
                assert resource == {
                    "description": "Flood in Guinea, Mali starting on September 10 2025",
                    "format": "kml",
                    "name": "flood_gin_mli_2025_09_10.kml",
                }

                dataset = pipeline.generate_dataset(events[1])
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "caveats": None,
                    "data_update_frequency": 365,
                    "dataset_date": "[2025-10-07T00:00:00 TO 2025-10-10T23:59:59]",
                    "dataset_source": "Google",
                    "groups": [{"name": "vnm"}],
                    "license_id": "cc-by",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "methodology": "Other",
                    "methodology_other": "methodology",
                    "name": "flood-vnm-2025-10-07",
                    "notes": "Severe flood event during October 7 2025-October 10 2025 affecting "
                    "4774261 people and covering 29651.152 square km in Viet Nam.",
                    "owner_org": "hdx",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "flooding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Flood in Viet Nam starting on October 7 2025",
                }
                resource = dataset.get_resource()
                assert resource == {
                    "description": "Flood in Viet Nam starting on October 7 2025",
                    "format": "kml",
                    "name": "flood_vnm_2025_10_07.kml",
                }

                dataset = pipeline.generate_dataset(events[2])
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "caveats": None,
                    "data_update_frequency": 365,
                    "dataset_date": "[2025-10-09T00:00:00 TO 2025-10-14T23:59:59]",
                    "dataset_source": "Google",
                    "groups": [{"name": "mex"}],
                    "license_id": "cc-by",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "methodology": "Other",
                    "methodology_other": "methodology",
                    "name": "flood-mex-2025-10-09",
                    "notes": "Severe flood event during October 9 2025-October 14 2025 affecting "
                    "8059613 people and covering 31878.002 square km in Mexico.",
                    "owner_org": "hdx",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "flooding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Flood in Mexico starting on October 9 2025",
                }
                resource = dataset.get_resource()
                assert resource == {
                    "description": "Flood in Mexico starting on October 9 2025",
                    "format": "kml",
                    "name": "flood_mex_2025_10_09.kml",
                }

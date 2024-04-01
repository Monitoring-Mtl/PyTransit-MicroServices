import unittest

from pydantic import ValidationError

from BIXI_Services.BIXI_Historical_Data_Processor.main import Config


class TestConfigModel(unittest.TestCase):
    def test_config_model_creation(self):
        config_data = {
            "ATLAS_URI": "mongodb+srv://yourcluster",
            "MONGO_DATABASE_NAME": "exampleDB",
            "BIXI_CDN": "https://cdn.example.com",
            "BIXI_DEFAULT_EXTRACT_PATH": "/path/to/extract",
            "BIXI_LOCATION_COLLECTION": "locations",
            "BIXI_TRIP_COLLECTION": "trips",
            "BIXI_QUEUE_SIZE": 10,
            "BIXI_CHUNK_SIZE": 5,
            "BIXI_CONCURRENCY": 2,
            "BIXI_URL_COLLECTION": "urls",
            "additional": "whatever",
        }
        config = Config(**config_data)
        self.assertIsInstance(config, Config)

    def test_config_model_validation_error(self):
        config_data = {
            # "ATLAS_URI": "mongodb+srv://yourcluster",
            "MONGO_DATABASE_NAME": "exampleDB",
            "BIXI_CDN": "https://cdn.example.com",
            "BIXI_DEFAULT_EXTRACT_PATH": "/path/to/extract",
            "BIXI_LOCATION_COLLECTION": "locations",
            "BIXI_TRIP_COLLECTION": "trips",
            "BIXI_QUEUE_SIZE": 10,
            "BIXI_CHUNK_SIZE": 5,
            "BIXI_CONCURRENCY": 2,
            "BIXI_URL_COLLECTION": "urls",
        }
        with self.assertRaises(ValidationError):
            Config(**config_data)


if __name__ == "__main__":
    unittest.main()

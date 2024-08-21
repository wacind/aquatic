from unittest import TestCase
from unittest.mock import patch
from io import StringIO
import json
from interview.weather import Sample, Control
from . import weather

# Notes: I used black to format the code to keep lines under 100 characters


class TestWeather(TestCase):
    """
    Class for conduting test of Weather Samples
    """
    # Patching sys.stdout with StringIO where output is needed

    @patch("sys.stdout", new_callable=StringIO)
    def test_process_events_and_snapshot(self, mock_stdout):
        """
        Testing the processing of events and generating an output snapshot
        Passed in 4 samples from 2 stations
        The output should update the High or Low of each station and 
        generate an output when the snapshot control message is called
        """
        events = [
            {
                "type": "sample",
                "stationName": "Station1",
                "timestamp": 1672531200000,
                "temperature": 37.1,
            },
            {
                "type": "sample",
                "stationName": "Station2",
                "timestamp": 1672531200000,
                "temperature": 37.4,
            },
            {
                "type": "sample",
                "stationName": "Station1",
                "timestamp": 1672531300000,
                "temperature": 37.3,
            },
            {
                "type": "sample",
                "stationName": "Station2",
                "timestamp": 1672531400000,
                "temperature": 37,
            },
            {"type": "control", "command": "snapshot"},
        ]
        for result in weather.process_events(events):
            print(json.dumps(result))
        expected_metrics = {
            "Station1": {"high": 37.3, "low": 37.1},
            "Station2": {"high": 37.4, "low": 37.0},
        }
        expected_snapshot = json.dumps(
            {"type": "snapshot", "asOf": 1672531400000, "stations": expected_metrics}
        )
        output = mock_stdout.getvalue().strip()
        self.assertEqual(output, expected_snapshot)

    @patch("sys.stdout", new_callable=StringIO)
    def test_process_events_and_reset(self, mock_stdout):
        """
        Tests for the reset message and ensures it gnerates the correct output
        """
        events = [
            {
                "type": "sample",
                "stationName": "Station1",
                "timestamp": 1672531200000,
                "temperature": 37.1,
            },
            {
                "type": "sample",
                "stationName": "Station2",
                "timestamp": 1672531200000,
                "temperature": 37.4,
            },
            {
                "type": "sample",
                "stationName": "Station1",
                "timestamp": 1672531300000,
                "temperature": 37.3,
            },
            {
                "type": "sample",
                "stationName": "Station2",
                "timestamp": 1672531400000,
                "temperature": 37,
            },
            {"type": "control", "command": "reset"},
        ]
        for result in weather.process_events(events):
            print(json.dumps(result))
        expected_output = json.dumps({"type": "reset", "asOf": 1672531400000})
        output = mock_stdout.getvalue().strip()
        self.assertEqual(output, expected_output)

    def test_process_weather_sample_new_station(self):
        """ 
        Tests what happens when the first sample for a station is passed in
        """
        metrics = {}
        Sample.process_weather_sample(metrics, "Station1", 25.1)
        self.assertEqual(metrics, {"Station1": {"high": 25.1, "low": 25.1}})

    def test_process_weather_sample_update_high_and_low(self):
        """
        Tests if the high and low values of a station are updated when new
        events are passed
        """
        metrics = {"Station1": {"high": 20.0, "low": 10.0}}
        Sample.process_weather_sample(metrics, "Station1", 25.0)
        Sample.process_weather_sample(metrics, "Station1", 5.0)
        self.assertEqual(metrics, {"Station1": {"high": 25.0, "low": 5.0}})

    @patch("sys.stdout", new_callable=StringIO)
    def test_process_snapshot(self, mock_stdout):
        """
        Tests the snapshot control messsage to see if it generates the right output
        """
        metrics = {"Station1": {"high": 25.0, "low": 15.0}}
        result = Control.process_snapshot(metrics, 1672531200000)
        print(json.dumps(result))
        expected_output = json.dumps(
            {"type": "snapshot", "asOf": 1672531200000, "stations": metrics}
        )
        self.assertEqual(mock_stdout.getvalue().strip(), expected_output)

    @patch("sys.stdout", new_callable=StringIO)
    def test_process_reset(self, mock_stdout):
        """
        Tests the reset control message to see if it generates the correct output
        """
        result = Control.process_reset(1672531200000)
        print(json.dumps(result))
        expected_output = json.dumps({"type": "reset", "asOf": 1672531200000})
        self.assertEqual(mock_stdout.getvalue().strip(), expected_output)

    def test_process_events_invalid_sample(self):
        """
        Test for bad sample data
        Tthe following sample is missing temperature
        """
        with self.assertRaises(ValueError):
            list(
                weather.process_events(
                    [
                        {
                            "type": "sample",
                            "stationName": "Station1",
                            "timestamp": 1672531200000,
                        }
                    ]
                )
            )

    def test_process_events_invalid_control(self):
        """
        Tests for a bad control message
        We should either have a reset or snapshot command
        Any other command should be met with a meaningful error
        """
        with self.assertRaises(ValueError):
            list(
                weather.process_events(
                    [
                        {
                            "type": "control",
                            "command": "BadCommand",
                            "timestamp": 1672531200000,
                        }
                    ]
                )
            )

    def test_process_events_invalid_type(self):
        """
        Tests for a bad message type
        We only support Control or Sample messages
        Any other event type should be met with an error
        """
        with self.assertRaises(ValueError):
            list(
                weather.process_events(
                    [
                        {
                            "type": "BadType",
                            "stationName": "Station1",
                            "timestamp": 1,
                            "temperature": 20.0,
                        }
                    ]
                )
            )

from typing import Any, Iterable, Generator
from typing import Dict

# Notes: I used black to format the code to keep lines under 100 characters

"""
This module provides a function to take in events and process them based on the event type.
The event types are either Samples or Control
Samples will be used to update the Weather Metrics by storing the high and low temperatures
by each Weather Station
Control will be used to either output the result or reset the Weather Metrics
"""

class Sample:
    """
    This Class deals with processing the Sample Weather Data
    """
    @staticmethod
    def process_weather_sample(
        metrics: Dict[str, Dict[str, float]], station_name: str, temperature: float
    ) -> None:
        """
        If station name is not in dict, create new entry and set high and low to the temperature
        If station name is in dict take the max of the existing temp or new temp for the max value
        If station name is in dict take the min of the existing temp or new temp for the min value
        Updateds the dict of station name, high, low
       
        Args:
            metrics: dictionary of metrics of station name, high, low
            stationName: key in dict
            temperature: value to compare
        """
        if station_name not in metrics:
            metrics[station_name] = {"high": temperature, "low": temperature}
        else:
            metrics[station_name]["high"] = max(metrics[station_name]["high"], temperature)
            metrics[station_name]["low"] = min(metrics[station_name]["low"], temperature)


class Control:
    """
    This Class deals with processing Control Messages.
    These can be either Snapshot or Reset
    """
    @staticmethod
    def process_snapshot(metrics: Dict[str, Dict[str, float]], timestamp: int) -> Dict[str, Any]:
        """
        Output as a json, metrics for all stations as of the latest timestamp of any sample
        Args:
            metrics: dictionary of metrics of station name, high, low
            timestamp: latest timestamp of any sample
        
        Returns a dictionary of the snapshot output
        """

        output = {"type": "snapshot", "asOf": timestamp, "stations": metrics}
        return output


    @staticmethod
    def process_reset(timestamp: int) -> Dict[str, int]:
        """
        Output as a message indicating the metrics have been reset as of the latest timestamp

        Args:
            timestamp: latest timestamp of any sample.
        Returns a dictionary of the reset message and timestamp
        """
        output = {"type": "reset", "asOf": timestamp}
        return output


def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    """
    Processes the inputed json events. Using match to determine if the event type is a Sample
    or Control

    Args:
        events: A stream of events passed as dicts which is the json input.
    Returns:
        Dictionry with desired output whether its a snapshot or reset message
    """
    weather_metrics = {}
    latest_timestamp = 0
    for event in events:
        event_type = event.get("type")
        match event_type:
            case "sample":
                # Because timestamp is guaranteed never to decrease,
                # we can assume that the timestamp in the event is the latest
                # Ensure that the sample json contains all relevant fields
                # Ensure temperature is a float
                sample_keys = ["stationName", "timestamp", "temperature"]
                if set(sample_keys).issubset(event.keys()):
                    latest_timestamp = event.get("timestamp")
                    station_name = event.get("stationName")
                    try:
                        temperature = float(event.get("temperature"))
                    except ValueError as ex:
                        raise ValueError("Temperature value is not valid") from ex

                    Sample.process_weather_sample(weather_metrics, station_name, temperature)
                else:
                    raise ValueError("Not all keys are present in json")
            case "control":
                command = event.get("command")
                match command:
                    case "snapshot":
                        if weather_metrics:
                            yield Control.process_snapshot(weather_metrics, latest_timestamp)
                    case "reset":
                        # Clearing out the metrics dict
                        # latest timestamp is not being reset as it is guaranteed not to decrease
                        weather_metrics.clear()
                        yield Control.process_reset(latest_timestamp)
                    case _:
                        raise ValueError(
                            "Unknown control. Please provide either a snapshot or reset control"
                        )
            case _:
                raise ValueError(
                    "Unknown input type. Please provide either a sample or control message"
                )

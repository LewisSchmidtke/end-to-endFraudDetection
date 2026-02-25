import math
from datetime import datetime, timedelta
import random


def unpack_weighted_dict(distribution_data: dict) -> tuple[list, list]:
    """
    Takes a weighted distribution dictionary and returns (keys, weights).
    Expected Format: {key : {"weight" : float, ...}}

    Args:
        distribution_data (dict): Dictionary of weighted distribution data.
    Returns:
        tuple[list, list] : List of keys and list of weights.
    Raises:
        ValueError: If weights don't sum up to 1.
    """
    keys = list(distribution_data.keys())
    weights = [value["weight"] for value in distribution_data.values()]
    confirm_weights(weights)

    return keys, weights


def confirm_weights(weight_list: list, tolerance: float=1e-9) -> None:
    """
    Takes a list of weights and confirms that they sum up to 1.
    Has a small tolerance build in to account for rounding errors in floating point numbers.
    Args:
        weight_list (list): List of weights.
        tolerance (float): Value to be used as absolute tolerance by math.isclose function.
    Returns:
        None
    Raises:
        ValueError: If weights don't sum up to 1.
    """
    total_weight = sum(weight_list)
    # use math.isclose to account for potential rounding errors with floating point numbers.
    if not math.isclose(total_weight, 1.0, abs_tol=tolerance):
        raise ValueError(f"Weighting must sum to 1, got {total_weight}")


def generate_random_past_timestamp(months: int=12) -> datetime:
    """
    Generates a random past timestamp in the past x months.
    Args:
        months (int): Number of months prior that will be considered for the generation of the past timestamp.
    Returns:
        datetime: The generated past timestamp.
    """
    current_stamp = datetime.now()
    start_stamp = current_stamp - timedelta(days=months * 30)
    return start_stamp + timedelta(seconds=random.randint(0, int((current_stamp - start_stamp).total_seconds())))


def generate_random_timestamp_in_range(start: datetime, end: datetime) -> datetime:
    """
    Generates a random timestamp in a defined range.
    Args:
        start (datetime): Datetime object of the lower bound
        end (datetime): Datetime object of the upper bound
    Returns:
        datetime: The generated timestamp in range.
    """
    random_seconds = random.randint(0, int((end - start).total_seconds()))
    return start + timedelta(seconds=random_seconds)
import math

def confirm_weights(weight_list, tolerance=1e-9):
    total_weight = sum(weight_list)
    # use math.isclose to account for potential rounding errors with floating point numbers.
    if not math.isclose(total_weight, 1.0, rel_tol=tolerance):
        raise ValueError(f"Weighting must sum to 1, got {total_weight}")

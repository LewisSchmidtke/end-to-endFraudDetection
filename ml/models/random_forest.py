from sklearn.ensemble import RandomForestClassifier
from src.constants import RF_PARAMS

def get_model(class_weight: str | None = None) -> RandomForestClassifier:
    """
    Builds a Random Forest classifier from the dictionary RF_PARAMS in src/constants.
    Args:
        class_weight (str | None): Pass "balanced" for class weight imbalance handling. Sklearn will then automatically compute weights inversely proportional to class frequency. Set to None when using SMOTE as imbalance is handled in the data.
    Returns:
        RandomForestClassifier: Random Forest classifier instance ready for training.
    """
    params = RF_PARAMS.copy()
    params["class_weight"] = class_weight # Calculated dynamically
    return RandomForestClassifier(**params)
from xgboost import XGBClassifier
from src.constants import XGB_PARAMS

def get_model(scale_pos_weight: float | None = None) -> XGBClassifier:
    """
    Builds an XGBoost classifier from XGB_PARAMS.
    Args:
        scale_pos_weight (float | None): Ratio of negative to positive samples (n_normal / n_fraud). Pass this for class weight imbalance handling. Set to None when using SMOTE as imbalance is handled in the data.
    Returns:
        XGBClassifier: XGBoost classifier instance ready for training.
    """
    params = XGB_PARAMS.copy()
    params["scale_pos_weight"] = scale_pos_weight # Calculated dynamically
    return XGBClassifier(**params)
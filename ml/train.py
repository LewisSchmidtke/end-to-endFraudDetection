import argparse
import numpy as np
from pathlib import Path
import joblib

from ml.datasets import FraudDataset, TorchFraudDataset
import ml.models.xgb as xgb_model
import ml.models.random_forest as rf_model
import ml.models.pytorch_wrapper as pytorch_wrapper

import src.constants as const


ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / const.FEATURE_PATH
MODEL_OUTPUT_DIR = ROOT / const.MODEL_OUTPUT_DIR

# To add a new model: Create a file in ml/models/ with a get_model() function that returns an object with a .fit()
# method, then add it here.
MODEL_LIB = {
    "xgb":     xgb_model,
    "rf":      rf_model,
    "pytorch": pytorch_wrapper,
}

# Functions for class imbalances
def _class_ratio(y_train: np.ndarray) -> tuple[int, int, float]:
    """
    Calculates the fraud ratio of fraudulent and non-fraudulent transaction labels.

    Args:
        y_train (np.ndarray): Training label array
    Returns:
        tuple[int, int, float]: Nr of normal transactions, nr of fraudulent transactions, ratio
    """
    n_normal = int((y_train == 0).sum())
    n_fraud  = int((y_train == 1).sum())
    ratio = n_normal / n_fraud if n_fraud > 0 else 1.0
    return n_normal, n_fraud, ratio


def _imbalance_kwargs(model_name: str, y_train: np.ndarray, smote: bool) -> dict:
    """
    Builds the model-specific keyword arguments for handling class imbalance.
    When SMOTE is active the dataset is already balanced, so all weights are set to None.

    Args:
        model_name (str): Model name for which to build the keyword arguments, must be present in MODEL_LIB.
        y_train (np.ndarray): Training labels, used to compute class ratios
        smote (bool): Whether SMOTE was applied to the training data
    Returns:
        dict: Keyword arguments to pass to get_model().
    Raises:
        ValueError: If `model_name` is not recognized.
    """
    if smote:
        return {}  # Imbalance already handled in the data

    n_normal, n_fraud, ratio = _class_ratio(y_train)
    print(f"Class ratio: {ratio:.4f}, Normal Samples: {n_normal}, Fraud Samples: {n_fraud}")

    if model_name not in MODEL_LIB:
        raise ValueError(f"Invalid model name: {model_name}. Needs to be one of {MODEL_LIB.keys()}")

    if model_name == "xgb":
        return {"scale_pos_weight": ratio}
    elif model_name == "rf":
        return {"class_weight": "balanced"}
    elif model_name == "pytorch":
        return {"pos_weight": ratio}
    else:
        return {}


def train(model_name: str, smote: bool = False, include_tx_status: bool = False, pytorch_kwargs: dict | None = None) -> None:
    """
    Loads the dataset, builds the requested model, and runs training.

    For sklearn models (xgb, rf), a FraudDataset, whereas for PyTorch the TorchFraudDataset is used.
    Sklearn models use fit(X_train, y_train) and the PyTorch model uses fit(dataset).

    Args:
        model_name (str): Key from MODEL_LIB identifying which model to train.
        smote (bool): Whether to apply SMOTE oversampling to the training data.
        include_tx_status (bool): Whether to include transaction_status as a feature.
        pytorch_kwargs (dict | None): Extra keyword arguments forwarded to pytorch_wrapper.get_model() (epochs, batch_size, lr). Ignored for non-PyTorch models.
    Raises:
        ValueError: If model_name is not in MODEL_LIB.
    """
    if model_name not in MODEL_LIB:
        raise ValueError(f"Invalid model name: {model_name}. Needs to be one of {MODEL_LIB.keys()}")

    print(f"Training: {model_name.upper()}")

    module = MODEL_LIB[model_name]

    is_pytorch = model_name == "pytorch"
    dataset_class = TorchFraudDataset if is_pytorch else FraudDataset

    dataset = dataset_class(DATA_PATH, smote=smote, include_transaction_status=include_tx_status)
    X_train, _, y_train, _ = dataset.fetch_dataset()
    class_imbalance_kwargs = _imbalance_kwargs(model_name, y_train, smote=smote)

    if is_pytorch:
        model = module.get_model(input_size=X_train.shape[1], **class_imbalance_kwargs, **(pytorch_kwargs or {}))
        model.fit(dataset)
    else:
        model = module.get_model(**class_imbalance_kwargs)
        model.fit(X_train, y_train)

    # Build correct path and save
    extension = ".pt" if is_pytorch else ".joblib"
    path = f"{MODEL_OUTPUT_DIR}/{model_name}{extension}"
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    if is_pytorch:
        model.save(path)
    else:
        joblib.dump(model, path)
    print(f"Model saved to {path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train fraud detection models.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--model",
        type=str,
        choices=[*MODEL_LIB.keys(), "all"],
        default="all",
        help="Model to train, or 'all' to train every registered model.",
    )
    parser.add_argument(
        "--smote",
        action="store_true",
        help="Apply SMOTE oversampling to handle class imbalance.",
    )
    parser.add_argument(
        "--include-tx-status",
        action="store_true",
        help="Include transaction_status as a feature (post-transaction inference only).",
    )

    pytorch_group = parser.add_argument_group("PyTorch options")
    pytorch_group.add_argument("--epochs",     type=int,   default=50,   help="Training epochs.")
    pytorch_group.add_argument("--batch-size", type=int,   default=256,  help="Mini-batch size.")
    pytorch_group.add_argument("--lr",         type=float, default=1e-3, help="Adam learning rate.")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    models_to_train = list(MODEL_LIB.keys()) if args.model == "all" else [args.model]
    pytorch_kwargs  = {"epochs": args.epochs, "batch_size": args.batch_size, "lr": args.lr}

    for model_name in models_to_train:
        train(
            model_name=model_name,
            smote=args.smote,
            include_tx_status=args.include_tx_status,
            pytorch_kwargs=pytorch_kwargs,
        )

    print("Training Complete")


if __name__ == "__main__":
    main()
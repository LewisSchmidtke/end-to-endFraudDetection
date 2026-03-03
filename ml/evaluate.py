import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import joblib
from pathlib import Path

import torch
from sklearn.base import BaseEstimator
import sklearn.metrics as skm

from ml.datasets import FraudDataset, TorchFraudDataset
from ml.models.pytorch_wrapper import FraudNetWrapper
from ml.models.model_lib import MODEL_LIB
import src.constants as const


ROOT        = Path(__file__).resolve().parent.parent
DATA_PATH   = ROOT / const.FEATURE_PATH
MODEL_DIR   = ROOT / const.MODEL_OUTPUT_DIR
REPORT_DIR  = ROOT / const.EVALUATION_OUTPUT_DIR

######################## "Private" Functions ########################

def _load_sklearn_model(path: str | Path) -> BaseEstimator:
    """
    Loads a sklearn model from specified path.

    Args:
        path (str | Path): Path to sklearn model.
    Returns:
        BaseEstimator: Loaded sklearn model.
    """
    path = Path(path)
    return joblib.load(path)


def _load_pytorch_model(path: str | Path) -> FraudNetWrapper:
    """
    Reconstructs a FraudNetWrapper and loads the saved weights.
    pos_weight is not needed at inference time.

    Args:
        path (str | Path): Path to saved pytorch model.
    Returns:
        FraudNetWrapper: Loaded pytorch model inside a FraudNetWrapper instance.
    """
    path = Path(path)
    state_dict = torch.load(path, map_location="cpu") # We load into cpu
    # We read input size from first weight block instead of having it as a function param.
    input_size = state_dict["model_block1.0.weight"].shape[1]
    wrapper = FraudNetWrapper(input_size=input_size, pos_weight=None)
    wrapper.model.load_state_dict(state_dict)
    wrapper.model.eval()
    return wrapper


def _load_model(model_name: str) -> FraudNetWrapper | BaseEstimator:
    """
    Load a saved model by name.

    Args:
        model_name (str): One of 'xgb', 'rf', 'pytorch'.
    Returns:
        Loaded model object.
    Raises:
        FileNotFoundError: If no saved model file is found for the given name.
        ValueError: If model name is not recognized.
    """
    if model_name not in MODEL_LIB:
        raise ValueError(f"{model_name} is not a valid model name.")

    ext  = ".pt" if model_name == "pytorch" else ".joblib"
    path = MODEL_DIR / f"{model_name}{ext}"

    if not path.exists():
        raise FileNotFoundError(f"No saved model found at {path}")

    if model_name == "pytorch":
        return _load_pytorch_model(path)

    return _load_sklearn_model(path)


def _compute_metrics(y_true: np.ndarray, y_prob: np.ndarray, threshold: float = 0.5) -> dict:
    """
    Computes a full set of evaluation metrics for a binary fraud classifier.

    Args:
        y_true (np.ndarray): Ground-truth labels (0/1)
        y_prob (np.ndarray): Predicted fraud probabilities
        threshold (float): Decision threshold applied to probabilities
    Returns:
        dict: Key value pairs of metric name and calculated metric value.
    """
    y_pred = (y_prob >= threshold).astype(int)

    tn, fp, fn, tp = skm.confusion_matrix(y_true, y_pred).ravel()

    metric_dict = {
        # Receiver Operating Characteristic Area Under the Curve
        "roc_auc": skm.roc_auc_score(y_true, y_prob),
        "avg_precision": skm.average_precision_score(y_true, y_prob),   # Area under PR curve
        "f1": skm.f1_score(y_true, y_pred, zero_division=0), # F1 score: 2 x (Precision x Recall / Precision + Recall)
        "precision": skm.precision_score(y_true, y_pred, zero_division=0), # Precision: TP / (TP + FP)
        "recall": skm.recall_score(y_true, y_pred, zero_division=0), # Recall: TP / (TP + FN)
        # False positive rate: how often do we flag legitimate transactions?
        "fpr": fp / (fp + tn) if (fp + tn) > 0 else 0.0,
        "threshold": threshold, # decision threshold
        "tp": int(tp), # True positives
        "fp": int(fp), # False positives
        "tn": int(tn), # True negatives
        "fn": int(fn), # False negatives
    }

    return metric_dict


def _find_optimal_threshold(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """
    Finds the threshold that maximises F1 score on the precision-recall curve.

    Args:
        y_true (np.ndarray): Ground-truth labels
        y_prob (np.ndarray): Predicted fraud probabilities
    Returns:
        float: Threshold value that maximises F1
    """
    precisions, recalls, thresholds = skm.precision_recall_curve(y_true, y_prob)

    precisions = precisions[:-1] # We drop the final boundary point so lengths match thresholds
    recalls = recalls[:-1]

    # We avoid division by zero where both precision and recall are 0
    f1_denominator = precisions + recalls
    f1_scores = np.where(f1_denominator > 0, 2 * precisions * recalls / f1_denominator, 0)

    # Find indices of best f1_scores and then return their corresponding thresholds
    best_threshold_index = np.argmax(f1_scores)
    return float(thresholds[best_threshold_index])


def _plot_precision_recall(ax_i: plt.axes, model_name: str, y_true: np.ndarray, y_prob: np.ndarray) -> None:
    """
    Plots precision and recall curve.

    Args:
        ax_i (plt.axes): Axes on which to plot
        model_name (str): Model for which to plot precision and recall
        y_true (np.ndarray): Ground-truth labels
        y_prob (np.ndarray): Predicted labels
    Returns:
        None
    Raises:
        ValueError: If model_name is not recognized
    """
    if model_name not in MODEL_LIB:
        raise ValueError(f"{model_name} is not a valid model name.")

    precisions, recalls, _ = skm.precision_recall_curve(y_true, y_prob)
    average_precision_scr = skm.average_precision_score(y_true, y_prob)
    color = const.COLORS.get(model_name, "black")
    ax_i.plot(
        recalls,
        precisions,
        color,
        lw=2,
        label=f"{model_name.upper()}, AP={average_precision_scr:.3f}"
    )


def _plot_confusion_matrix(
        ax_i: plt.axes,
        model_name: str,
        y_true: np.ndarray,
        y_prob: np.ndarray,
        threshold: float
) -> None:
    """
    Visualizes a confusion matrix for a selected model type.

    Args:
        ax_i (plt.axes): Axes object from matplotlib
        model_name (str): Model name for which the confusion matrix should be visualized
        y_true (np.ndarray): Ground-truth labels
        y_prob (np.ndarray): Predicted labels
        threshold (float): Decision threshold
    Returns:
        None
    Raises:
        ValueError: If 'model_name' is not recognized
    """
    if model_name not in MODEL_LIB:
        raise ValueError(f"{model_name} is not a valid model name.")

    y_pred = (y_prob >= threshold).astype(int)
    confusion_mat = skm.confusion_matrix(y_true, y_pred)

    im = ax_i.imshow(confusion_mat, interpolation="nearest", cmap="Blues")
    ax_i.figure.colorbar(im, ax=ax_i)
    classes = ["Normal", "Fraud"]

    ax_i.set(
        xticks=np.arange(2),
        yticks=np.arange(2),
        xticklabels=classes,
        yticklabels=classes,
        title=f"{model_name.upper()}, threshold={threshold:.2f}",
        ylabel="True label",
        xlabel="Predicted label",
    )

    thresh = confusion_mat.max() / 2

    for i in range(2):
        for j in range(2):
            ax_i.text(j, i, f"{confusion_mat[i, j]:,}", ha="center", va="center",
                      color="white" if confusion_mat[i, j] > thresh else "black")


def _plot_feature_importance(
        ax_i: plt.axes,
        model_name: str,
        model: BaseEstimator | FraudNetWrapper,
        feature_names: list
) -> None:
    """
    Plots the feature importance for a selected scikit model. Skips PyTorch model.

    Args:
        ax_i (plt.axes): Axes object from matplotlib to plot on
        model_name (str): Model name for which the feature importance should be plotted
        model (BaseEstimator | FraudNetWrapper): Model object
        feature_names (list): List of feature names
    Returns:
        None
    """
    if model_name == "pytorch":
        ax_i.set_visible(False)
        return

    feat_importances = model.feature_importances_

    indices = np.argsort(feat_importances)[-20:]  # Top 20 features
    color = const.COLORS.get(model_name, "black")

    ax_i.barh(range(len(indices)), feat_importances[indices], color=color, alpha=0.8)
    ax_i.set_yticks(range(len(indices)))
    ax_i.set_title(f"{model_name.upper()}: Feature Importances")
    ax_i.set_xlabel("Importance")
    ax_i.set_yticklabels([feature_names[i] for i in indices], fontsize=6)


def _build_eval_plots_and_calc_metrics(
    model_name: str,
    model: BaseEstimator | FraudNetWrapper,
    X_test:  np.ndarray,
    y_test:  np.ndarray,
    feature_names: list,
    report_dir: Path,
) -> dict:
    """
    Runs the evaluation visualization for a single model and calculates the metrics.

    Args:
        model_name (str): Name of the model to be evaluated, used for plot titles and export path.
        model (BaseEstimator | FraudNetWrapper): Loaded model with .predict_proba() method.
        X_test (np.ndarray): Input data from test set
        y_test (np.ndarray): Labels from test set
        feature_names (list): List of feature names, matching X_test columns
        report_dir (Path): Directory where plots are saved
    Returns:
        dict: Evaluation metrics at the optimal threshold.
    """
    y_prob = model.predict_proba(X_test)[:, 1]

    optimal_threshold = _find_optimal_threshold(y_test, y_prob)
    metrics_at_05 = _compute_metrics(y_test, y_prob, threshold=0.5)
    metrics_optimal = _compute_metrics(y_test, y_prob, threshold=optimal_threshold)

    # Figure layout: PR Curve | Confusion Matrix | Feature Importance
    fig = plt.figure(figsize=(25, 6))
    fig.suptitle(f"{model_name.upper()} Eval", fontsize=14, fontweight="bold")
    gs = gridspec.GridSpec(1, 3, figure=fig, wspace=0.35)

    ax_prc = fig.add_subplot(gs[0])
    ax_conf_mat = fig.add_subplot(gs[1])
    ax_feat_imp = fig.add_subplot(gs[2])

    # PR curve with both thresholds marked
    _plot_precision_recall(ax_prc, model_name, y_test, y_prob)
    precisions, recalls, thresholds_pr = skm.precision_recall_curve(y_test, y_prob)
    # Default threshold
    idx_05 = np.argmin(np.abs(thresholds_pr - 0.5))
    ax_prc.scatter(
        recalls[idx_05],
        precisions[idx_05],
        marker="o",
        s=80,
        color="red",
        zorder=5,
        label=f"t=0.50 --- F1={metrics_at_05['f1']:.3f}"
    )
    # Optimal threshold
    idx_opt = np.argmin(np.abs(thresholds_pr - optimal_threshold))
    ax_prc.scatter(
        recalls[idx_opt],
        precisions[idx_opt],
        marker="*",
        s=120,
        color="green",
        zorder=5,
        label=f"t={optimal_threshold:.2f} --- F1={metrics_optimal['f1']:.3f}"
    )
    # Baseline (random classifier)
    baseline = y_test.mean()
    ax_prc.axhline(baseline, linestyle="--", color="grey", alpha=0.6, label=f"Baseline ({baseline:.3f})")
    ax_prc.set(xlabel="Recall", ylabel="Precision", title="Precision-Recall Curve", xlim=[0, 1], ylim=[0, 1.05])
    ax_prc.legend(fontsize=8)


    # Confusion matrix at optimal threshold
    _plot_confusion_matrix(ax_conf_mat, model_name, y_test, y_prob, optimal_threshold)

    # Feature importances
    _plot_feature_importance(ax_feat_imp, model_name, model, feature_names)

    plot_path = report_dir / f"{model_name}_evaluation.png"
    fig.savefig(plot_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved plot at {plot_path}")

    return metrics_optimal


def _print_metrics(model_name: str, metrics: dict) -> None:
    """
    Prints the given metrics from a metrics dictionary for a given model name.

    Args:
        model_name (str): Name of the model to which the metrics dictionary corresponds to
        metrics (dict): Dict consisting of the metrics
    Returns:
        None
    """
    separator = "-" * 50
    print(f"\n{separator}")
    print(f"{model_name.upper()}")
    print(separator)
    print(f"ROC-AUC          : {metrics['roc_auc']:.4f}")
    print(f"Avg Precision    : {metrics['avg_precision']:.4f}")
    print(f"F1  (optimal t)  : {metrics['f1']:.4f}")
    print(f"Precision        : {metrics['precision']:.4f}")
    print(f"Recall           : {metrics['recall']:.4f}")
    print(f"FPR              : {metrics['fpr']:.4f}")
    print(f"Threshold        : {metrics['threshold']:.4f}")
    print(f"TP={metrics['tp']}  FP={metrics['fp']}  TN={metrics['tn']}  FN={metrics['fn']}")
    print(separator)


def _save_metrics_csv(all_metrics: dict[str, dict], report_dir: str | Path) -> None:
    """
    Saves the given metrics to a csv file.

    Args:
        all_metrics (dict): Dict consisting of the metrics
        report_dir (str | Path): Location where the csv file gets saved to
    Returns:
        None
    """
    report_dir = Path(report_dir)
    rows = []
    for name, m in all_metrics.items():
        rows.append({"model": name, **m})
    path = report_dir / "metrics_summary.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    print(f"Saved metrics as CSV at: {path}")


######################## "Public" Functions ########################
def run_evaluation(
    model_name: str,
    smote: bool = False,
    include_tx_status: bool = False,
) -> dict:
    """
    Loads the dataset, loads a single model, runs evaluation and saves reports.

    Args:
        model_name (str): Model name to evaluate
        smote (bool): Must match the flag used during training so the dataset preprocessing is identical, default False.
        include_tx_status (bool): Must match the flag used during training, default False.
    Returns:
        dict: Evaluation metrics at the optimal threshold.
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    dataset_class = TorchFraudDataset if model_name == "pytorch" else FraudDataset
    dataset = dataset_class(DATA_PATH, smote=smote, include_transaction_status=include_tx_status)
    _, X_test, _, y_test = dataset.fetch_dataset()

    feature_names = [c for c in dataset.df.columns if c != "is_fraudulent"] # That is just our ground truth label

    model = _load_model(model_name)
    metrics = _build_eval_plots_and_calc_metrics(model_name, model, X_test, y_test, feature_names, REPORT_DIR)
    _print_metrics(model_name, metrics)

    return metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate trained fraud detection models.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--model",
        type=str,
        choices=[*MODEL_LIB, "all"],
        default="all",
        help="Model to evaluate, or 'all' to evaluate every saved model.",
    )
    parser.add_argument(
        "--smote",
        action="store_true",
        help="Must match the --smote flag used during training.",
    )
    parser.add_argument(
        "--include-tx-status",
        action="store_true",
        help="Must match the --include-tx-status flag used during training.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    models = list(MODEL_LIB.keys()) if args.model == "all" else [args.model]

    all_metrics: dict[str, dict] = {}

    for model_name in models:
        print(f"Evaluating: {model_name.upper()}")

        try:
            metrics = run_evaluation(
                model_name=model_name,
                smote=args.smote,
                include_tx_status=args.include_tx_status,
            )
        except (FileNotFoundError, ValueError) as e:
            print(f"Error, skipping model: {e}")
            continue

        all_metrics[model_name] = metrics

    if all_metrics:
        _save_metrics_csv(all_metrics, REPORT_DIR)

    print("Evaluation complete.")


if __name__ == "__main__":
    main()
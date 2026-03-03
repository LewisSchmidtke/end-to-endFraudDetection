import time
import numpy as np
from pathlib import Path
import torch
import torch.nn as nn
import torch.optim as optim
import torch.utils.data as tud

from ml.models.pytorch_model import get_model as get_fraud_net
from ml.datasets import TorchFraudDataset


class FraudNetWrapper:
    """
    Sklearn-compatible wrapper around FraudNet. Exposes .fit() and .predict_proba()
    so it can be used interchangeably with XGBoost and Random Forest in train.py.
    """
    def __init__(
        self,
        input_size: int,
        pos_weight: float | None = None,
        epochs: int = 50,
        batch_size: int = 256,
        lr: float = 1e-3,
    ) -> None:
        """
        Args:
            input_size (int): Number of input features.
            pos_weight (float | None): Weight applied to the positive (fraud) class in BCEWithLogitsLoss. Pass n_normal / n_fraud for imbalance handling, or None when using SMOTE, default None.
            epochs (int): Number of training epochs, default 50.
            batch_size (int): Mini-batch size for the DataLoader, default 256.
            lr (float): Adam learning rate, default 1e-3.
        """
        self.epochs = epochs
        self.batch_size = batch_size
        self.lr = lr
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.model = get_fraud_net(input_size=input_size).to(self.device)

        pos_weight_tensor = torch.tensor([pos_weight], dtype=torch.float32).to(self.device) if pos_weight else None
        self.criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight_tensor)
        self.optimizer = optim.Adam(self.model.parameters(), lr=lr)

    def fit(self, dataset: TorchFraudDataset) -> "FraudNetWrapper":
        """
        Runs the training loop using a TorchFraudDataset.

        Args:
            dataset (TorchFraudDataset): The prepared dataset. Used directly as the DataLoader source.
        Returns:
            FraudNetWrapper: self, to allow chaining.
        """
        loader = tud.DataLoader(dataset, batch_size=self.batch_size, shuffle=True)

        print(f"Device: {self.device} | Epochs: {self.epochs} | Batch size: {self.batch_size}")
        t0 = time.time()

        self.model.train()

        for epoch in range(1, self.epochs + 1):
            epoch_loss = 0.0
            for X_batch, y_batch in loader:
                X_batch, y_batch = X_batch.to(self.device), y_batch.to(self.device)
                self.optimizer.zero_grad()
                loss = self.criterion(self.model(X_batch), y_batch)
                loss.backward()
                self.optimizer.step()
                epoch_loss += loss.item() * len(X_batch)

            if epoch % 5 == 0 or epoch == 1:
                # Right align epoch count and show epoch loss
                print(f"Epoch {epoch:>3}/{self.epochs} loss: {epoch_loss / len(dataset):.4f}")

        print(f"Training time: {time.time() - t0:.1f}s")

        return self

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """
        Returns class probabilities in sklearn's [n_samples, 2] format. Sets model to eval mode.

        Args:
            X (np.ndarray): Feature matrix.
        Returns:
            np.ndarray: Array of shape (n_samples, 2) with [P(normal), P(fraud)] per row.
        """
        self.model.eval()

        with torch.no_grad():
            logits = self.model(torch.tensor(X, dtype=torch.float32).to(self.device))
            fraud_prob = torch.sigmoid(logits).cpu().numpy().squeeze(1)

        return np.column_stack([1 - fraud_prob, fraud_prob])

    def save(self, path: str) -> None:
        """
        Saves the model to a specified path.

        Args:
            path (str): Path that the model should be saved to.
        Returns:
            None
        """
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        torch.save(self.model.state_dict(), path)
        print(f"Model saved to {path}")


def get_model(
    input_size: int,
    pos_weight: float | None,
    epochs: int,
    batch_size: int,
    lr: float
) -> FraudNetWrapper:
    """
    Builds a FraudNetWrapper instance, matching the get_model convention used by xgb.py and random_forest.py.

    Args:
        input_size (int): Number of input features
        pos_weight (float | None): Positive class weight for BCEWithLogitsLoss. None when using SMOTE
        epochs (int): Training epochs
        batch_size (int): Mini-batch size
        lr (float): Adam learning rate
    Returns:
        FraudNetWrapper: Wrapper instance ready for .fit().
    """
    return FraudNetWrapper(input_size=input_size, pos_weight=pos_weight, epochs=epochs, batch_size=batch_size, lr=lr)
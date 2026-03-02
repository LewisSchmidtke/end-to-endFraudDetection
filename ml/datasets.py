import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
import torch.utils.data as tud

import src.constants as const


class FraudDataset:
    def __init__(
            self,
            data_path: str,
            smote: bool = False,
            include_transaction_status: bool = False,
            test_size: float = const.DATASET_PARAMS["test_size"],
            drop_columns: list = None,
    ) -> None:
        """
        Loads and preprocesses the fraud detection dataset from a parquet file.
        Handles NaN filling, categorical encoding, train/test splitting, scaling and optional SMOTE resampling.

        Args:
            data_path (str): Path to the parquet file containing the feature engineered dataset.
            smote (bool): Whether to apply SMOTE oversampling to the training data. Default False.
            include_transaction_status (bool): Whether to include transaction_status as a feature. If True, limits model to post-transaction use only. Default False.
            test_size (float): Fraction of data to use for testing. Default from DATASET_PARAMS.
            drop_columns (list): List of columns to drop before training. Default None -> from DATASET_PARAMS["drop_columns"].
        Returns:
            None
        """
        if drop_columns is None:
            drop_columns = const.DATASET_PARAMS["drop_columns"]

        self.df = pd.read_parquet(data_path)
        self.df.drop(columns=drop_columns, inplace=True)
        # Nans for single transaction in window, so we set stddev to 0
        self._fill_nans("user_stddev_amount_24h", 0)
        # Nans if this was the users' first transaction, set very high value to signal no prior transaction
        self._fill_nans("seconds_since_last_transaction", 1e9)
        # We binary encode the transaction channel
        self._binary_encode_column(
            "transaction_channel",
            value_map={const.ONLINE_TX_CHANNEL: 1, const.LOCAL_TX_CHANNEL: 0}
        )
        # We one hot encode the merchant category
        self._one_hot_encode_column("merchant_category")

        # If we include transaction status, we limit model deployment to after transactions have been completed. However,
        # we are losing a valuable feature if we drop transaction_status
        if include_transaction_status:
            self._binary_encode_column("transaction_status", value_map={const.APPROVED : 1, const.DECLINED : 0})
        else:
            self.df.drop(columns="transaction_status", inplace=True)

        X = self.df.drop(columns=["is_fraudulent"]).to_numpy()
        y = self.df["is_fraudulent"].to_numpy()
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            X, y, test_size=test_size, stratify=y, random_state=23)

        self.X_train, self.X_test = self._apply_standard_scaler(self.X_train, self.X_test)

        if smote:
            self.X_train, self.y_train = self._apply_smote_resample(self.X_train, self.y_train)

    def _fill_nans(self, column_name: str, fill_value: int | float) -> None:
        """
        Fills the NaN values in a specified column with a specified value.

        Args:
            column_name (str): Name of the column to fill
            fill_value (int | float): Value used for the fill
        Returns:
            None
        """
        self.df[column_name] = self.df[column_name].fillna(fill_value)

    def _binary_encode_column(self, column_name: str, value_map: dict) -> None:
        """
        Applies binary encoding to a specified column.

        Args:
            column_name (str): Name of the column to encode
            value_map (dict): Encoding map
        Returns:
            None
        Raises:
            ValueError: If encoding map is invalid (Doesn't have 2 key/value pairs) or values aren't {0,1}
        """
        if len(value_map) != 2:
            raise ValueError("value_map must contain 2 key/value pairs with one value being 0 and the other being 1, "
                             "got {} instead".format(len(value_map)))
        if set(value_map.values()) != {0, 1}:
            raise ValueError(
                "value_map must contain exactly 0 and 1 as values, got {} instead".format(set(value_map.values())))

        self.df[column_name] = self.df[column_name].map(value_map)

    def _one_hot_encode_column(self, column_name: str) -> None:
        """
        Applies one hot encoding to a specified column.

        Args:
            column_name (str): Name of the column to encode
        Returns:
            None
        """
        self.df = pd.get_dummies(self.df, columns=[column_name], prefix=column_name, dtype=int)

    @staticmethod
    def _apply_standard_scaler(x_train: np.ndarray, x_test:np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """
        Applies standard scaling to x train and test data. Applies fit_transform to x_train and transform to x_test.

        Args:
            x_train (np.ndarray): Training data
            x_test (np.ndarray): Test data
        Returns:
            Tuple[np.ndarray, np.ndarray]: scaled x_train, x_test
        """
        scaler = StandardScaler()
        x_train = scaler.fit_transform(x_train)
        x_test = scaler.transform(x_test) # We don't want to influence our test data so transform with params calculated from x_train
        return x_train, x_test

    @staticmethod
    def _apply_smote_resample(X: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """
        Applies SMOTE resampling to the data to handle class imbalance. Should only be applied to the train data

        Args:
            X (np.ndarray): Features
            y (np.ndarray): Label
        Returns:
            Tuple[np.ndarray, np.ndarray]: X_SMOTE, y_smote after resampling
        """
        smote = SMOTE(random_state=23)
        return smote.fit_resample(X, y)

    def fetch_dataset(self) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Returns the processed train/test splits ready for model training.

        Returns:
            tuple: X_train, X_test, y_train, y_test as numpy arrays
        """
        return self.X_train, self.X_test, self.y_train, self.y_test


class TorchFraudDataset(FraudDataset, tud.Dataset):
    def __init__(
            self,
            data_path: str,
            smote: bool = False,
            include_transaction_status: bool = False,
            test_size: float = const.DATASET_PARAMS["test_size"],
            drop_columns: list = None,
    ) -> None:
        super().__init__(data_path, smote, include_transaction_status, test_size, drop_columns)

    def __len__(self) -> int:
        pass

    def __getitem__(self, item):
        pass
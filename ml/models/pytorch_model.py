import torch
import torch.nn as nn
from src.constants import PYTORCH_PARAMS


class FraudNet(nn.Module):
    def __init__(self, input_size: int):
        super().__init__()

        self.model_block1 = self.create_model_block_type1(input_size, 64)
        self.model_block2 = self.create_model_block_type1(64, 32)
        self.model_block3 = self.create_model_block_type1(32, 16)
        self.output = nn.Linear(16, 1)

    @staticmethod
    def create_model_block_type1(input_neurons: int, output_neurons: int) -> nn.Sequential:
        """
        Function that creates a nn.Sequential model block with the following structure:
        nn.Linear -> BatchNorm1d -> ReLU -> Dropout
        Args:
            input_neurons (int): Number of input neurons for this model block
            output_neurons (int): Number of desired output neurons for this model block
        Returns:
            nn.Sequential: The built model block
        """
        model_block1 = nn.Sequential(
            nn.Linear(input_neurons, output_neurons),
            nn.BatchNorm1d(output_neurons),
            nn.ReLU(),
            nn.Dropout(PYTORCH_PARAMS["dropout_val"]),
        )
        return model_block1

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """
        A single forward pass through the model
        Args:
            x (torch.Tensor): input tensor
        Returns:
            torch.Tensor: Prediction y_hat the output tensor
        """
        x = self.model_block1(x)
        x = self.model_block2(x)
        x = self.model_block3(x)
        y_hat = self.output(x)

        return y_hat


def get_model(input_size: int) -> FraudNet:
    """
    Builds a FraudNet instance.
    Args:
        input_size (int): Number of input features for the model. Based on the features of the training data.
    Returns:
        FraudNet: FraudNet Instance
    """
    return FraudNet(input_size=input_size)
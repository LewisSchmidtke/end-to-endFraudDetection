import ml.models.xgb as xgb_model
import ml.models.random_forest as rf_model
import ml.models.pytorch_wrapper as pytorch_wrapper

# To add a new model: Create a file in ml/models/ with a get_model() function that returns an object with a .fit()
# method, then add it here.
MODEL_LIB = {
    "xgb": xgb_model,
    "rf": rf_model,
    "pytorch": pytorch_wrapper,
}

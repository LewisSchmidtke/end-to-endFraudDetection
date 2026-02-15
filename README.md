# E2E Fraud Detection

## Overview
This project implements a fully self developed end to end fraud detection pipeline spanning from synthetic transaction 
pattern generation, over data streaming and feature engineering to model training and inference. The core development 
was done in <span style="color:red">Python</span>. <span style="color:red">SQL</span> is being run through 
<span style="color:red">Docker</span> for data storage and database operations. <span style="color:red">Apache Kafka</span> 
and <span style="color:red">Apache Spark</span> are used for data ingestion and feature engineering. The machine learning 
aspect is handled through <span style="color:red">PyTorch</span> and <span style="color:red">Scikit-Learn</span> for a mix 
between custom and out-of-the-box models. Finally, <span style="color:red">NVIDIA Triton</span> and <span style="color:red">ONNX</span> 
are used to deploy the model for inference.

The system design can be seen in the figure 1.1:

![System Design](data/images/system_architecture.png?raw=true)

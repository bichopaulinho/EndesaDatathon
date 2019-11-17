# README #

Source code of **Paulino Tardáguila's** project **"Capturing Energy Consumption Patterns in ENDESA’s
customer network"** for the Endesa Datathon competition.


For reproducibility, a `R/datos` directory containing the file `DATATHON_secuencial.csv` must be added to the local copy of the material included here. A copy of this file can be downloaded from this [link](http://d1u7z8sxy0j3jc.cloudfront.net/DATATHON_secuencial.csv.bz2)

Requirements: `spark/makeclusters.py` is written to run from the  master node of a Spark cluster on AWS ec2 instances, reading information from a S3 bucket.

* **`R/genera_matriz_cluster_secuencial.R`**: Preprocessing and upload of the client's consumption time series to the corresponding S3 bucket
* **`spark/makeclusters.py`**: pySpark job to cluster time series
* **`report`**: Final report scripts
* **`report/ClusterViz`**: Shiny web app for result visualization
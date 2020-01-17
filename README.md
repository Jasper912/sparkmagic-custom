# sparkmagic

### This project is base on [jupyter-incubator/sparkmagic@0.12.9](https://github.com/jupyter-incubator/sparkmagic/tree/0.12.9)

### What differernts

* Auto create livy session after kernel initialized. Every spark or pyspark notebook will bind a livy session by specific session name

* PySpark kernel both support python2.7 and python3.6

* SparkSQL kernel supported

* PySpark and Spark kernel can use code completion in jupyterlab notebook by `Tab` like python kernel

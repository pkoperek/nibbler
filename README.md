=======
nibbler
=======

Service for processing large datasets with use of Scala/Apache Spark

Available services:
===================

Test query for status:
```
GET: http://149.156.10.32:9198/status
```

Evaluation (computes symbolical derivative by specified variable, differentiates numerically given data set, compares
both results with use of specific evaluation function):

Test query:
```
POST /evaluate
{
"inputFile": "hdfs://master/tmp/test1.txt"
}
```

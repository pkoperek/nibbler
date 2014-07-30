=======
nibbler
=======

service for processing large datasets with use of Scala/Apache Spark

Test query for status:
```
GET: http://149.156.10.32:9198/status
```

Test query for evaluation:
```
POST /evaluate
{
"inputFile": "hdfs://master/tmp/test1.txt"
}
```

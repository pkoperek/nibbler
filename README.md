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
  "numdiff": "backward",
  "inputFile": "/tmp/input.csv",
  "function": 	{
    "function": "sin",
    "operands": [{
      "function": "var_0"
    }]
  }
}
```

Parameters:
===========

  * `--local` - runs spark in "local" mode (with master URI set to `local`)

Useful scripts:
===============

* One-liner for rebuilding and starting nibbler
```
rm -rf nibbler && ./gradlew distZip && unzip build/distributions/nibbler.zip  && ./nibbler/bin/nibbler --local
```

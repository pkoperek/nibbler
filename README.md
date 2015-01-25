nibbler
=======

Service for processing large datasets with use of Scala/Apache Spark.

Notes:
======

* Building requires Java 7! _(Scala 2.10.4 isn't able to work with Java 8!)_
* After upgrading to `hadoop-client` 2.5.0 the ugly exception about missing `InputSplit...` class

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

Useful one-liners:
===============

Rebuilds project and launches current dev version (`localhost:8080`)
```
./gradlew clean jettyRun
```

License
=======

```
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
```
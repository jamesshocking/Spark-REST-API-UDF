# How to execute a REST API on Apache Spark - Python

## Introduction

The thing about Apache Spark is that you can run code on either the driver or the worker.  In the case of executing HTTP requests to a REST endpoint for example, using a Python library like Requests greatly simplifies the process of consuming JSON, or XML from such a web service.  The catch is that simply running something like

```python
s = "Python syntax highlighting"
print s
```
will result in the REST call being executed on the driver.  No parallelism, and no scaling, leaving Apache Spark no better than a single threaded Python program.  This may not be a problem but if you're integrating with a web service that requires you to page through it's data, then an alternative solution must be sought.

The solution is to use a UDF coupled to a withColumn statement.  This example, demonstrates how one can create a DataFrame whereby each row represents a single request to the REST service.  A UDF (User Defined Function) is used to encapsulate the HTTP request, returning a structured column that represents the REST API response, which can then be sliced and diced using the likes of explode and other built-in DataFrame functions.

## The Solution

For the sake of brevity I am assuming that a SparkSession has been created and assigned to a variable called spark.  In addition, for this example I will be used the Python Requests HTTP library.

The solution assumes that you need to consume data from a REST API, which you will need to call multiple times to get the data that you need.  In order to take advantage of the parallelism that Apache Spark offers, each REST API call will be owned by a UDF, which is bound to a DataFrame via a withColumn statement.  Each row in the DataFrame will represent a single call to the REST API service.  Once an action is executed on the DataFrame, the value assigned to each row in the new column, will be the result from each individual REST API call, albeit a stuctured value defined by a schema of our design.

For this example, I will be using a free REST API service that returns the makes and models of USA vehicles.

### Start by declaring your imports:

```python
import requests
import json
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import Row
```

### Now declare a function that will execute our REST API call

Use the Requests library to execute either an HTTP get or a post.  There is nothing special about this function, except that the REST service response will be passed back as a JSON object.

```python
def executeRestApi(verb, url, headers, body):
  #
  headers = {
      'content-type': "application/json"
  }

  res = None
  # Make API request, get response object back, create dataframe from above schema.
  try:
    if verb == "get":
      res = requests.get(url, data=body, headers=headers)
    else:
      res = requests.post(url, data=body, headers=headers)
  except Exception as e:
    return e

  if res != None and res.status_code == 200:
    return json.loads(res.text)

  return None
```

### Define the response schema and the UDF

This is one of the parts of Apache Spark that I really like.  I can pick and chose what values I want from the JSON returned by the REST API call.  All I have to do is when declaring the schema, I only need to identify what parts of the JSON I want.  

```python
schema = StructType([
  StructField("Count", IntegerType(), True),
  StructField("Message", StringType(), True),
  StructField("SearchCriteria", StringType(), True),
  StructField("Results", ArrayType(
    StructType([
      StructField("Make_ID", IntegerType()),
      StructField("Make_Name", StringType())
    ])
  ))
])
```

Next I declare the UDF, making sure to identify the return type as my schema.  This will ensure that the new column, which is used to execute the UDF, will eventually contain data as a structured object rather than plain JSON formatted text.

```python
udf_executeRestApi = udf(executeRestApi, schema)
```

### Create the Request DataFrame and Execute

The final piece is to create a DataFrame whereby each row represents a single REST API call.  


| Tables        | Are           | Cool  |
| ------------- |:-------------:| -----:|
| col 3 is      | right-aligned | $1600 |
| col 2 is      | centered      |   $12 |
| zebra stripes | are neat      |    $1 |


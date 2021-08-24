# How to execute a REST API on Apache Spark the Right Way - Python

## Introduction

Apache Spark is a wonderful invention that can solve a great many problems.  Its flexibility and adaptability gives great power but also the opportunity for big mistakes.  One such mistake is executing code on the driver, which you thought would run in a distributed way on the workers.  One such example is when you execute Python code outside of the context of a Dataframe.

For example, when you execute code similar to:

```python
s = "Python syntax highlighting"
print s
```

Apache Spark will execute the code on the driver, and not a worker.  This isn't a problem with such a simple command, but what happens when you need to download large amounts of data via a REST API service?

```python
import requests
import json

res = None

try:
  res = requests.get(url, data=body, headers=headers)
    
except Exception as e:
  print(e)

if res != None and res.status_code == 200:
 print(json.loads(res.text))
```

If we execute the code above, it will be executed on the Driver.  If I were to create a loop with multiple of API requests, there would be no parallelism, no scaling, leaving a huge dependency on the Driver.  This approach criples Apache Spark and leaves it no better than a single threaded Python program.  To take advantage of Apache Spark's scaling and distribution, an alternative solution must be sought.

The solution is to use a UDF coupled to a withColumn statement.  This example, demonstrates how one can create a DataFrame whereby each row represents a single request to the REST service.  A UDF (User Defined Function) is used to encapsulate the HTTP request, returning a structured column that represents the REST API response, which can then be sliced and diced using the likes of explode and other built-in DataFrame functions (Or collapsed, see [https://github.com/jamesshocking/collapse-spark-dataframe]().

## The Solution

For the sake of brevity I am assuming that a SparkSession has been created and assigned to a variable called spark.  In addition, for this example I will be used the Python Requests HTTP library.

The solution assumes that you need to consume data from a REST API, which you will be calling multiple times to get the data that you need.  In order to take advantage of the parallelism that Apache Spark offers, each REST API call will be encapsulated by a UDF, which is bound to a DataFrame.  Each row in the DataFrame will represent a single call to the REST API service.  Once an action is executed on the DataFrame, the result from each individual REST API call will be appended to each row as a Structured data type.

To demonstrate the mechanism, I will be using a free US Government REST API service that returns the makes and models of USA vehicles [https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json]().

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

Next I declare the UDF, making sure to set the return type as the schema that I declared.  This will ensure that the new column, which is used to execute the UDF, will eventually contain data as a structured object rather than plain JSON formatted text.  The action is similar to using the from_json function, which takes a schema as it's second parameter.

```python
udf_executeRestApi = udf(executeRestApi, schema)
```

### Create the Request DataFrame and Execute

The final piece is to create a DataFrame where each row represents a single REST API call.  The number of columns in the Dataframe are up to you but you will need at least one, which will host the URL and/or parameters required to execute the REST API call.  I am going to use four to reflect the number of individual parameters that the REST API call function needs.  

Using the US Goverments free-to-access vehicle make REST service, we would create a Dataframe as follows:

```python
from pyspark.sql import Row

headers = {
    'content-type': "application/json"
}

body = json.dumps({
})

RestApiRequestRow = Row("verb", "url", "headers", "body")
request_df = spark.createDataFrame([
            RestApiRequestRow("get", "https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json", headers, body)
          ])
```

The Row class is used to define the columns of the Dataframe, and using the createDataFrame method of the spark object, an instance of RestApiRequestRow is declared for each individual API call that we want to make.

All being well, the Dataframe will look like:

| verb        | url           | headers  | body |
| ------------- |-------------| -----|-----|
| get      | https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json | {'content-type': "application/json"} | {}

Finally we can use withColumn on the Dataframe to execute the UDF and REST API.

```python

result_df = request_df \
             .withColumn("result", udf_executeRestApi(col("verb"), col("url"), col("headers"), col("body"))) 
```

As Spark is lazy, the UDF will execute once an action like count() or show() is executed against the Dataframe.  Spark will distribute the API calls amongst all the workers, before returning the results such as:

| verb        | url           | headers  | body | result |
| ------------- |-------------| -----|-----|-----|
| get      | https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json | {'content-type': "application/json"} | {} | [9773, Response r...] |

The REST service results a number of attributes and we're only interested in the one identified as Results (i.e. result.Results).  If we use my collapse_columns function ([https://github.com/jamesshocking/collapse-spark-dataframe]()):

```python
df = request_df.select(explode(col("result.Results")).alias("results"))
df.select(collapse_columns(df.schema)).show()
```

you would see:

|results_Make_ID|   results_Make_Name|
|---------------|--------------------|
|            440|        ASTON MARTIN|
|            441|               TESLA|
|            442|              JAGUAR|
|            443|            MASERATI|
|            444|          LAND ROVER|
|            445|         ROLLS ROYCE|

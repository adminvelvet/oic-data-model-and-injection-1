### oic-data-model

This post decribes somes features about oci-project data.
- Data source and pipelines
- Data model
- Databases creation
- Table creation
- Data upload
- Data preparation


> Data source and pipelines

![Data Pipelines](https://github.com/agambov/oic-data-model/blob/master/img/data__pipeline.png)

> Data model

![Data Pipelines](https://github.com/agambov/oic-data-model/blob/master/img/data_model.png)


> Databases creation  
```sql
# Go to Hive console (from HUE or Jupyter terminal)
CREATE DATABASE oic;

``` 


> Table creation  

First, we create a temporaries tables which contain row data in primitive format (all fields are in STRING format) and files are stored as text file. 
From temporary tables, we create 2 categories of functional tables:
We create to category of tables:
- full table: tables which contain full data
- sample: tables which contain 10% of data

For query optimization, all functional table store data in Avro format and we add a partition on each table (based on date field). We cast all field in right format.

```sh
# Run script to create full temporary table 


# Run script to create 10pct temporary table


# Run script to create full fonctional table 


# Run script to create 10pct fonctional table


```


> Data upload


>

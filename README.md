### oic-data-model

This post decribes somes features about oci-project data.
- Data source and pipelines
- Data model
- Databases creation
- Table creation
- Data upload
- Data preparation

> Download git repo

```sh
# log as data_tech user
sudo du data_tech

# Clone git repo which contains script
git clone https://github.com/agambov/oic-data-model-and-injection.git

# go to script folder
cd oic-data-model-and-injection/script/

# add execution right on all sql script
chmod +x *.sql

# show files
ls -l
```

> Data source and pipelines

![Data Pipelines](https://github.com/agambov/oic-data-model/blob/master/img/data__pipeline.png)

> Data model

![Data Pipelines](https://github.com/agambov/oic-data-model/blob/master/img/data_model.png)


> Databases creation  
```sql
# Create database
hive -e "DROP DATABASE OIC; CREATE DATABASE OIC"

``` 


> Temporary table creation  

First, we create a temporaries tables which contain row data in primitive format. All fields are in STRING format type and files are stored as text file. 

```sh
# Run script to create temporaries tables 
hive -f 01_create_tmp_table.sql
```

> Functional table creation  

For query optimization, all functional table store data in Avro format and we add a partition on each table (based on date field). We cast all field in right format.

```sh
# Run script to create functional tables 
hive -f 02_create_table.sql
```

> Sample table creation

For query optimization, all sample table store data in Avro format and we add a partition on each table (based on date field). We cast all field in right format and select MSIDN which terminate by 0.

```sh
# Run script to create sample tables 
hive -f 03_create_10pct_table.sql
```


> Load data into temporay table

```sh
# Run script to load data
hive -f 04_load_data_into_tmp_tables.sql

```

> Load data into functional and sample table and drop temporary table

From temporary tables, we create 2 categories of functional tables:
We create to category of tables:
- full table: tables which contain full data
- sample: tables which contain 10% of data



```sh
# Run script to load data
hive -f 05_insert_into_cleaned_tables.sql
```

> Load data into functional and sample table and drop temporary table

From temporary tables, we create 2 categories of functional tables:
We create to category of tables:
- full table: tables which contain full data
- sample: tables which contain 10% of data



```sh
# Run script to load data
hive -f 06_insert_into_cleaned_10pct_tale
```

> Drop temporaries tables

```sh
# Run script to drop tmp table
hive -f 07_drop_tmp_tables.sql
````




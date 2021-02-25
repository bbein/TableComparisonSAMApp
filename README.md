# Table Comparison API
The Table Comparison API was created to help developers and data scientists to understand the differences between two versions of the same data. These differences include table schema difference l and changes in the data itselfe.

Schema difference are for example renamed columns, changed column data types.
Examples in changes of the data include difference in available categories for categorical columns and changes of the distribution for numerical values.

This document first describes how the API is called and responds for a succsefull call. After that some of the implementation details are explained for future development and improvement.


## API inputs
The API can be called using a POST request with a JSON payload specifying four parameters:
The `connection_string` parameters are [SQLAlchemy Databse Urls](https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls) and the `table` parameters are  


Example payload using a public database:
```
{
    "connection_string_1": "postgres://reader:NWDMCE5xdipIjRrp@hh-pgsql-public.ebi.ac.uk:5432/pfmegrnargs",
    "connection_string_2": "postgres://reader:NWDMCE5xdipIjRrp@hh-pgsql-public.ebi.ac.uk:5432/pfmegrnargs",
    "table_1": "rnc_database",
    "table_2": "rnc_database",
```

## API output
This section will describe in detail a successfull API response in the JSON format.
On the first level the JSON object will have five portions:
```
{
    inputs: {}
    top_level: {}
    schema_comparison: {}
    profiling: {}
    diff: {}
    diff_highlights: {}
}
```
The following sub sections describe the `value` dictionary of each of the first level `keys`.

### inputs
This section returns the exact input parameters for reference when the resulting differences are unexpected.
```
{   
    'connection_string_1': <SQLAlchemy connection string>,
    'connection_string_2': <SQLAlchemy connection string>,
    'table_1': <string>,
    'table_2': <string>
}
```

### top level
The top level section can help with a quick view if the input tables have the expected boundery values before exploring more detailed differences.
```
 {
    'table_1': {'n_columns': <int>, 'n_rows': <int>},
    'table_2': {'n_columns': <int>, 'n_rows': <int>}
}
```

### schema_comparison
As a first step the schema of both tables will be compared. **Only columns with the same column name and column data type are added to the `columns_both` key and are profiled in the `profiling section`**

The `only_table1` and `only_table2` keys can help identify spelling mistakes and unintended changes of column data types.
```
{
    'columns_both': {
        <column_name>: <column_data_type>,
        <column_name>: <column_data_type>,
        ...                                
    },
    'only_table1': {
        <column_name>: <column_data_type>,
        <column_name>: <column_data_type>,
        ...
    },
    'only_table2': {
        <column_name>: <column_data_type>,
        <column_name>: <column_data_type>,
        ...
    }
}
```

### profiling
The `profiling` portion of the respons contains an entry for every column that exists in both tables that is structured like the following:
```
{
    <column_name>: {
        'table_1': {},
        'table_2': {}
    }
}
```

each of the `table` keys reference another dictionary that always contains the count of non Null values within the column and the detected column type. Currently there are four different posible values for the column type:
- not supported
- categorical
- numerical
- date

**These four types are also used to calculate the column diff and create the column diff highlights**

#### not supported
Any column that is not detected as one of the other three types is treated as a `not supported` type and only contains the contains the count of non Null values within the column.
```
{
    'count': <int>,
    'type': 'not supported',
}
```

#### categorical
Any column that has less than 101 unique values is treated as a `categorical` column. For each categorical column there is a `uniques` key with a dictionary of all the categories and the count of each of the categories:
```
{
    'count': <int>,
    'type': 'categorical',
    'uniques': {
        <category_name>: <count_of_category>,
        <category_name>: <count_of_category>,
        ...
}
```

#### numerical

#### date

### diff
the diff portion of the results calculates the differences between the two tables. This includes the top level and the columns that exist in both tables. Each of these are dictionary themselfs:
```
{
    'top_level': {}
    'columns': {}
}
```

**In the diff positive numbers always mean that there are more values in table_1, while negative numbers mean there are more values in table_2.**

#### top_level
The top level differences only include if one table has more columns than the other table and if there where rows added or removed.
```
{ 
    'columns': <int>, 
    'rows': <int>
}
```
for example if columns > 0 that means that there are more `columns` in table_1 compared to table_2. On the other hand if `columns` < 0 there are overall more columns in table_2 than in table_1.
#### columns

##### categorical

```
{
    <column_name>: {
        'has_null': <int>,
        'null': <int>,
        'pct_null': <float>,
        'only_table_1': [
            <category_name>, 
            <category_name>,
            ...
            ],
        'only_table_2': [
            <category_name>, 
            <category_name>,
            ...
            ],
        'pct_difference': {
            <category_name>: <float>,
            <category_name>: <float>,
            ...
            },
        'count_difference': {
            <category_name>: <int>,
            <category_name>: <int>,
            ...
            },
    }

}
```
##### numerical

##### date

### diff highlights
Similar to the diffs `diff_highlights` include the top level and the columns that exist in both tables. Each of these are dictionary themselfs:
```
{
    'top_level': {}
    'columns': {}
}
```

The diff highlights try to select relevant information from the diff and turn it into readable text. Both of these dictionaries always exist even if they are empty. The reason for the diff highlights is that the diffs themselfs are very long and if the columns are vey similar or identical not interesting to the user. The highlights try to draw the attention to the relevant portions of the diff.

#### top_level
the top level can highlight two parts of the diff `columns` and `rows`. Neither of these entries exist if both tables contain the same number of columns and rows. If there are more `columns` or `rows` the diff number is turned into a more easily readable form for display.

```
{
    'columns': <str>
    'rows': <str>
}
```

#### categorical


## API implementation
Currently the API is implemented in a monolithic and procedural fashion.
All of the subheadings are implemented in the `lambda_handler` function and call subfunctions.

Within the `lambda_handler` the two most important variables are `table_1` and `table_2` which are SQLAlchemy Table objects that contain all the information to querry the databases and the schema information for the input tables.

In the subheadings some of the implementation assumptions and shortcomings are described.
A general problem at the moment is that there are still many names implemented as strings instead of being defined as Constants at the beginning of the code.

### top level
The top level uses the column count from the SQLAlchemy loaded `Table` object to create the column count.
The row count employes a `count(*)` statment on the table.

There might be a way to use meta data SQL tables to get a faster row count than the `count(*)` operation.

### schema
For the Schema comparison obly exact column name and column data type matches are added to the `columns_both` dictionary. To compare the SQL data types the SQLAlchemy data type representations are used.

One of the additional features for the schema should be to identify columns which have similar names and similar datatypes to sugest that these columns should be compared. This could be even further advanced to include a comparison of the column profiles to identify similar columns with different names.

### columns profiling
The profiling portion is creating a profile of every column that was identivied as being present in both tables during the schema assesment.

#### categorical
The profiling first tries to identify if a column is likley to be a categorical value. This is currently implemented by checking if there are less than 101 unique values in the column. The `unique_count` function is used to determine the number of unique values in a column. If a column is categorical the `unique_counts` function will create a dictionary with the record count for each unique value/category. One problem that could arise here is that both functions are very closely named and could easily be mistaken for one another. A more general problem arises for very small tables (100 or less rows) in which every column is a categorical column. 

#### numerical

#### date

### diff

### diff highlights


## API improvements
This is a summery of API improvements that would not improve the API's functionality but it's maintainability and extendability. Implementing these improvements would also make it easier for other developers to help with the development of parts of the API. The image below shows how the different parts/problems within the API are connected.

![current flow chart](./img/current_flow_chart.png)

### error handeling
currently there is no error handling implemented and a generic error response is given if the API code fails in any way. Some of the error handeling should include a meaningfull error message if:
- the API could not connect to either of the databses
- either of the specified tables does not exist in the corresponding database
- there is a problem profiling one column in one (or both) table(s)
- there is a problem with one aspect of profiling one column in one (or both) table(s)
- there is a problem creating the diff for one column
- there is a problem creating one of the diff highlights 

Depending on the error all other meaningfull evaluation should still return thier responses.
The details about the error should be accessable from the writen [logs](#logging)

### <a name="logging">logging</a>
Logging is esential for any application. Log's will enable developers to understand and reproduce specific errors within the application. Python has very usefull logging libary for this. 

### From functional to Package and classes
Currently every part of the application is implemented in functions and functional groups are seperated by comments. These functional groups should converted into classes. For example the `Column profiling` could be it's own python class. One of it's functionaleties would be to create a JSON object for the API to transmit. The Class should also be able to recrate it's instance from a JSON object. This would allow front end developers working in python to easily injest the API JSON.

In addition this would make it easier for multiple developers to develop parts of the API because they know where the new functionality has to be implemented.

### testing
to enable faster development one would need a testing enviorment which would include databases with test cases for end to end testing and unit tests for individual functions and classes. 

### breaking the monolith into multiple API's
Breaking up the API from one monolithic call to multiple smaller calls would allow the UI to be more reactive and interactive. The UI could already show the column names and row count while other parts of the UI are still profiling columns.

In addition the User could make selections about how they would like to profile each of the columns. In addition the user could also select additional column comparisons in case columni names changed. 

This breaking out would be much easier if each API is implemented as a seperate class since the different API's could share the python classes and exchange the instances using the JSON format.

![broken up flow chart](./img/broken_up_flow_chart.png)

## Used technolegies

### API Service
The API Service takes POST request with the input parameters ["connection_string_1", "connection_string_2", "table_1", "table_2"] in JSON in the body of the POST request
It then performs internal operations to compare both tables and returns a JSON with details about both tables and differences between them,
 
Implementation Technologies: AWS SAM (Lambda, API Gateway, ECR, Cloud Formation)
Tech stack Pros:
- Cost effective (FREE so far)
- Highly scalable (up to 1000 parallel requests, could also be increased if needed)
- Infrastructure is defined in the code
- Fast to deploy a REST API
 
Tech Stack Cons
- Dev environment setup takes a while (environment to be able to deploy the code)
- Vendor bound (AWS)
- Difficult to setup local test environment (I didn’t solve this yet)
- Lambda run time is limited to 15minutes!

### Data Connector
I use the SQLAlchemy data connector which makes it easy to connect to different SQL based data-bases, warehouses and lakes.

Tech stack Pros:
- easy add additional SQL based databses that are supported by SQLAlchemy
- implemt a querry once using the ORM and it can get translated into many different SQL dialects

Tech Stack Cons:
- It is often difficult to implement SQL querries using the ORM
- SQLAlchemies documentation is not the best
- The same functionality is implemented at different places using different syntax (sqlalchemy.case vs sqlalchemy.func.case)
 
### Profiling

Instead of implementing profiling wourselfs we should also explore already implemented packages like [pandas-profiling](https://github.com/pandas-profiling) or [great_expectations](https://github.com/great-expectations/great_expectations)
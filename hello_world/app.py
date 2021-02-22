# Main python
import json
from datetime import datetime

# 3rd part
import numpy as np
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker

# constants
DATE_FORMAT = '%Y-%m-%d'

###
# Helper Functions
###

def create_table(connection_string, table_name):
    engine = db.create_engine(connection_string)
    table = db.Table(table_name, db.MetaData(bind=engine), autoload=True, autoload_with=engine)
    return table

###
# Schema profiling
###
def table_row_count(table):
    n_rows = db.select([db.func.count()]).select_from(table).execute().scalar()
    return n_rows

def column_names_types(table):
    column_name_type = {}
    for c in table.columns:
        column_name_type[c.name] = c.type
    return column_name_type

def overlapping_columns(column_names_types_1, column_names_types_2):
    overlap = {} 
    for name_1, type_1 in column_names_types_1.items():
        if name_1 in column_names_types_2.keys():
            type_2 = type(column_names_types_2[name_1])
            if isinstance(type_1, type_2):
                overlap[name_1] = type_1  
    return overlap

def columns_only_in_table(column_names_types, overlap):
    return {c: column_names_types[c] for c in column_names_types if c not in overlap}

###
# Column profiling
###

### Uniques
def unique_count(column, limit=10000):
    try:
        Session = sessionmaker()
        session = Session(bind=column.table.metadata.bind)
        subq1 = session.query(column).limit(limit).subquery()
        uniques = db.select([db.func.count(db.distinct(subq1.columns[column.name]))]).execute().scalar()
        if uniques > 100:
            return '100+'
        else:
            uniques = db.select([db.func.count(db.distinct(column))]).execute().scalar()
            if uniques > 100:
                return '100+'
            else: 
                return str(uniques)
    except TypeError:
        return None
    
def unique_counts(column):
    data = db.select([column, db.func.count('*')]).group_by(column).select_from(column.table).execute()
    u_counts = {}
    for name, count in data:
        if name != None:
            u_counts[str(name)] = count
    return u_counts

### Numeric
def numeric_profiling(column, ntile=10, hist_bins=10):
    Session = sessionmaker()
    session = Session(bind=column.table.metadata.bind)
    # min, max
    minimum = session.query(db.func.min(column)).scalar()
    maximum = session.query(db.func.max(column)).scalar()
    
    # n-tile
    subq1 = session.query(column).filter(column.isnot(None)).subquery()
    subq2 = session.query(db.select([subq1.columns[column.name], db.func.ntile(ntile).over(order_by=subq1.columns[column.name]).label('ntile')])).subquery()
    ntile_result = session.query(subq2.columns['ntile'], db.func.max(subq2.columns[column.name])).group_by('ntile')
    
    ntiles = {}
    for tile, limit in ntile_result:
        ntiles[str(round(tile/ntile*100, 0))+'%'] = limit
        
    # histogram counts
    # counts of records >= upper and < lower
    data_range = (maximum - minimum)
    buckets = np.arange(minimum, maximum, data_range/hist_bins)[::-1]
    case = db.case([(column > x, y+1) for y , x in enumerate(buckets)]).label('hist_bucket')
    subq1 = session.query(column, case).filter(column.isnot(None)).subquery()
    sub_column = subq1.columns['hist_bucket']
    hist_results = session.query(sub_column, db.func.count('*')).group_by(sub_column).order_by(sub_column)
    
    upper = maximum
    histogram = {}
    for (bucket, count), lower in zip(hist_results, buckets):
        histogram[bucket] = {}
        histogram[bucket]['upper'] = upper
        histogram[bucket]['count'] = count
        histogram[bucket]['lower'] = lower
        upper = lower
    
    session.close()
    # generate result dict
    result = {}
    result['ntiles'] = ntiles
    result['histogram'] = histogram
    result['min'] = minimum
    result['max'] = maximum
    result['type'] = 'numerical'
    return result


### Dates
def date_profiling(column):
    Session = sessionmaker()
    session = Session(bind=column.table.metadata.bind)
    # min, max
    minimum = session.query(db.func.min(column)).scalar()
    maximum = session.query(db.func.max(column)).scalar()
    # number of days covered
    days = (maximum - minimum).days
    session.close()
    result = {}
    result['min'] = minimum.strftime(DATE_FORMAT)
    result['max'] = maximum.strftime(DATE_FORMAT)
    result['days'] = days
    result['type'] = 'date'
    # categorical if days < 1000
    if days < 1000:
        result['daily_records'] = unique_counts(column)
    return result

###
# Column Diff
###

def categorical_diff(cat_profile):
    cat_1 = cat_profile['table_1']['uniques']
    cat_2 = cat_profile['table_2']['uniques']
    count_1 = cat_profile['table_1']['count']
    count_2 = cat_profile['table_2']['count']
    
    only_1 = []
    diff = {}
    pct_diff = {}
    for key, value in cat_1.items():
        if key in cat_2:
            diff[key] = cat_1[key] - cat_2[key]
            pct_diff[key] = 100*round(float(cat_1[key])/count_1 - float(cat_2[key])/count_2, 3)
        else: 
            only_1.append(key)
    only_2 = list(set(cat_2) - set(diff))
    result = {}
    result['count_difference'] = diff
    result['pct_difference'] = pct_diff
    result['only_table_1'] = only_1
    result['only_table_2'] = only_2
    return result

def numerical_diff(num_profile):
    minimum = num_profile['table_1']['min'] - num_profile['table_2']['min']
    maximum = num_profile['table_1']['max'] - num_profile['table_2']['max']
    # ntiles
    ntiles = {}
    for tile in num_profile['table_1']['ntiles']:
        ntiles[tile] = num_profile['table_1']['ntiles'][tile] - num_profile['table_2']['ntiles'][tile]
    # histogram
    histogram = {}
    for bucket in num_profile['table_1']['histogram']:
        histogram[bucket] = num_profile['table_1']['histogram'][bucket]['count'] - num_profile['table_2']['histogram'][bucket]['count']
    result = {}
    result['ntiles'] = ntiles
    result['histogram'] = histogram
    result['min'] = minimum
    result['max'] = maximum
    return {}

def date_diff(date_profile):
    minimum = (datetime.strptime(date_profile['table_1']['min'], DATE_FORMAT).date() - datetime.strptime(date_profile['table_2']['min'], DATE_FORMAT).date()).days
    maximum = (datetime.strptime(date_profile['table_1']['max'], DATE_FORMAT).date() - datetime.strptime(date_profile['table_2']['max'], DATE_FORMAT).date()).days
    days = date_profile['table_1']['days'] - date_profile['table_2']['days']
    
    result = {}
    result['min'] = f'{minimum} days'
    result['max'] = f'{maximum} days'
    result['days'] = str(days)
    # TODO: add records per day diff and missing days
    return result

###
# Diff Highlights
###

def categorical_diff_highlights(cat_diff, table_1, table_2):
    # category only in one table
    pct_diff = cat_diff['pct_difference']
    # This happens when the column has only nulls
    if len(pct_diff) == 0:
        return {}
    pct_diff_abs = {key: abs(value) for key, value in pct_diff.items()}
    only_1 = cat_diff['only_table_1']
    only_2 = cat_diff['only_table_2']
    
    # get the highest change in count percent
    pct_max_key = max(pct_diff_abs, key=pct_diff_abs.get)
    pct_max = pct_diff[pct_max_key]
    
    result = {}
    if len(only_1) > 0:
        result['only_table_1'] = f"{table_1.name} has {len(only_1)} categories not present in {table_2.name}"
    if len(only_2) > 0:
        result['only_table_2'] = f"{table_2.name} has {len(only_2)} categories not present in {table_1.name}"
    result = {}
    if pct_max > 0:
        result['pct_max'] = f"{pct_max_key} has the largest categorical change of {pct_max}% more records in {table_1.name} than {table_2.name}"
    if pct_max < 0:
        result['pct_max'] = f"{pct_max_key} has the largest categorical change of {pct_max}% more records in {table_2.name} than {table_1.name}"
    return result


###
# Lambda Implementation
###
def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }
    
def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    ### get variables - needs checking for thier existence otherwise throw error
    print(event['body'])
    body = json.loads(event['body'])
    connection_string_1 = body['connection_string_1']
    connection_string_2 = body['connection_string_2']
    table_name_1 = body['table_1']
    table_name_2 = body['table_2']
    result = {'inputs': body}

    ### Setup SQLAlchemy tables
    table_1 = create_table(connection_string_1, table_name_1)
    table_2 = create_table(connection_string_2, table_name_2)

    ### Top Level
    # count columns
    n_columns1 = len(table_1.columns)
    n_columns2 = len(table_2.columns)
    # count rows
    n_rows1 = table_row_count(table_1)
    n_rows2 = table_row_count(table_2)

    top_level = {'table_1': {'n_columns': n_columns1, 'n_rows': n_rows1}, 'table_2': {'n_columns': n_columns2, 'n_rows': n_rows2}}
    result['top_level'] = top_level

    ### Schema proviling and comparing
    column_names_types_1 = column_names_types(table_1)
    column_names_types_2 = column_names_types(table_2)
    overlap = overlapping_columns(column_names_types_1, column_names_types_2)
    only_table_1 = columns_only_in_table(column_names_types_1, overlap)
    only_table_2 = columns_only_in_table(column_names_types_2, overlap)

    schema_comparison = {
        'columns_both': {c[0]: str(c[1]) for c in overlap.items()}, 
        'only_table1': {c[0]: str(c[1]) for c in only_table_1.items()}, 
        'only_table2': {c[0]: str(c[1]) for c in only_table_2.items()},
    }
    result['schema_comparison'] = schema_comparison

    ### profiling
    profiling = {}
    for column in overlap:
        profiling[column] = {}
        for lable, table in (('table_1', table_1), ('table_2', table_2)):
            profiling[column][lable] = {}
            sql_column = table.columns[column]
            # check if it is a categorical column
            n_uniques = unique_count(sql_column)
            if n_uniques != '100+':
                profiling[column][lable]['type'] = 'categorical'
                profiling[column][lable]['uniques'] = unique_counts(sql_column)
            # Numeric column
            elif isinstance(overlap[column], (db.BIGINT, db.DECIMAL, db.FLOAT, db.INTEGER, db.NUMERIC, db.REAL, db.SMALLINT)):
                profiling[column][lable] = numeric_profiling(sql_column)
            # Date column
            elif isinstance(overlap[column], (db.DATE)): #, db.DATETIME, db.TIMESTAMP)):
                profiling[column][lable] = date_profiling(sql_column)
            # attributes for all columns
            count = db.select([db.func.count(sql_column)]).execute().scalar()
            profiling[column][lable]['count'] = count
            if 'type' not in profiling[column][lable]:
                profiling[column][lable]['type'] = 'not supported'

    result['profiling'] = profiling

    ### diff
    diff = {}
    # top level diff
    diff['top_level'] = {}
    diff['top_level']['rows'] = result['top_level']['table_1']['n_rows'] - result['top_level']['table_2']['n_rows']
    diff['top_level']['columns'] = result['top_level']['table_1']['n_columns'] - result['top_level']['table_2']['n_columns']
    # column level diff
    diff['columns'] = {}
    for c in result['profiling']:
        diff['columns'][c] = {}
        # Categorical
        if result['profiling'][c]['table_1']['type'] == 'categorical':
            diff['columns'][c] = categorical_diff(result['profiling'][c])
        # Numerical
        if result['profiling'][c]['table_1']['type'] == 'numerical':
            diff['columns'][c] = numerical_diff(result['profiling'][c])
        # Date
        if result['profiling'][c]['table_1']['type'] == 'date':
            diff['columns'][c] = date_diff(result['profiling'][c])
        # Nulls - for all types
        diff['columns'][c]['null'] = (result['top_level']['table_1']['n_rows'] - result['profiling'][c]['table_1']['count']) - (result['top_level']['table_2']['n_rows'] - result['profiling'][c]['table_2']['count'])
        diff['columns'][c]['pct_null'] = round(100*(result['profiling'][c]['table_2']['count']/result['top_level']['table_2']['n_rows'] - result['profiling'][c]['table_1']['count']/result['top_level']['table_1']['n_rows']), 1)
        diff['columns'][c]['has_null'] = int(bool(result['top_level']['table_1']['n_rows'] - result['profiling'][c]['table_1']['count'])) - int(bool(result['top_level']['table_2']['n_rows'] - result['profiling'][c]['table_2']['count']))
    result['diff'] = diff

    ### Diff Highlights
    diff_highlights = {}
    # top level highlights
    diff_highlights['top_level'] = {}
    for key in diff['top_level']:
        if diff['top_level'][key] > 0:
            diff_highlights['top_level'][key] = f"{table_1.name} has {diff['top_level'][key]} more {key} than {table_2.name}"
        elif diff['top_level'][key] < 0:
            diff_highlights['top_level'][key] = f"{table_2.name} has {-diff['top_level'][key]} more {key} than {table_1.name}"
    # columns
    diff_highlights['columns'] = {}
    for c in diff['columns']:
        diff_highlights['columns'][c] = {}
        ## categorical
        if result['profiling'][c]['table_1']['type'] == 'categorical':
            diff_highlights['columns'][c] = categorical_diff_highlights(diff['columns'][c], table_1, table_2)
        ## Nulls
        # has null
        if diff['columns'][c]['has_null'] > 0:
            diff_highlights['columns'][c]['has_null'] = f"{table_1.name} has Nulls and {table_2.name} doesn't"
        elif diff['columns'][c]['has_null'] < 0:
            diff_highlights['columns'][c]['has_null'] = f"{table_2.name} has Nulls and {table_1.name} doesn't"
        # null pct
        if diff['columns'][c]['pct_null'] > 0:
            diff_highlights['columns'][c]['pct_null'] = f"{table_1.name} has {diff['columns'][c]['pct_null']}% more Nulls than {table_2.name}"
        elif diff['columns'][c]['pct_null'] < 0:
            diff_highlights['columns'][c]['pct_null'] = f"{table_2.name} has {-diff['columns'][c]['pct_null']}% more Nulls than {table_1.name}"
        
        ## remove columns without highlights
        if len(diff_highlights['columns'][c]) == 0:
            del diff_highlights['columns'][c]
    result['diff_highlights'] = diff_highlights
    print(result)

    return respond(None, result)

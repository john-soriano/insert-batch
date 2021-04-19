import glob
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine


db_param_dic = {
    "host": "localhost",
    "port": "5432",
    "database": "tagai",
    "user": "dbuser",
    "password": "salipoako"
}

# postgresql connection
engine = create_engine(
    'postgresql+psycopg2://%s:%s@%s:%s/%s' % (db_param_dic["user"], db_param_dic["password"], db_param_dic["host"], db_param_dic["port"], db_param_dic["database"]))

# table name
table = 'sample1'

# destination folder
path = 'data'

df = pd.DataFrame()
filenames = glob.glob(path + "/*.xlsx")
for f in filenames:
    data = pd.read_excel(f, engine='openpyxl')
    df = df.append(data)


def dict_types():
    # copy df but object is string only to compute text length for string and find max value and add 10 to evaluate varchar()
    df_copy = df.copy().select_dtypes(include="object")
    # columns of copied df
    columns = df_copy.columns
    # instanciate column type
    column_dict = {}
    # need to compute character length of column data
    for col in columns:
        df_copy[col] = df_copy[col].astype(str).apply(lambda x: len(x))

    # create column dict type and add 10 to max value of column length
    for col in columns:
        max_value = int(df_copy[col].max())
        column_dict.update({
            col: sqlalchemy.types.VARCHAR(max_value + 10)
        })

    return column_dict


if __name__ == "__main__":
    outputdict = dict_types()
    df.to_sql(table, engine, dtype=outputdict)

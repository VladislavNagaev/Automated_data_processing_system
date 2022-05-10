import argparse
from pyspark.sql import SparkSession



def csv_to_parquet(spark, path_to_csv_file, path_to_parquet_file):

    # Read data
    dataRaw = spark.read.load(
        path_to_csv_file, 
        format='csv',
        header=True, 
        sep=';',
        encoding="utf-8",
        delimiter=',',
        index=False,
    )

    # Copy data
    data = dataRaw

    # Remove unsupported symbols
    for column in data.columns:
        new_column = column.replace("(","").replace(")","").replace(" ","_")
        data = data.withColumnRenamed(column, new_column)

    # Save data
    data.write.save(
        path=path_to_parquet_file,
        format='parquet',
        mode='overwrite',
        compression='snappy',
        encoding='utf-8',
        engine='pyarrow',
        index=False,
    )


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="SparkJob csv_to_parquet")

    parser.add_argument(
        '-c', '--path_to_csv_file',
        metavar='CSV-file',
        dest='path_to_csv_file',
        type=str,
        default=None,
        help='Input path to source file in CSV-format',
        required=True
    )
    parser.add_argument(
        '-p', '--path_to_parquet_file',
        metavar='parquet-file',
        dest='path_to_parquet_file',
        type=str,
        default=None,
        help='Input path to target file in parquet-format',
        required=True
    )
    args = parser.parse_args()

    # Create SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Start function
    csv_to_parquet(
        spark=spark,
        path_to_csv_file=args.path_to_csv_file,
        path_to_parquet_file=args.path_to_parquet_file
    )

    spark.stop()




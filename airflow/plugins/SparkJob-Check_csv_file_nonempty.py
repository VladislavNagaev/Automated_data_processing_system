import argparse
from pyspark.sql import SparkSession


def check_csv_file_nonempty(spark, path_to_csv_file):

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

    if data.count() == 0:
        raise ValueError('Файл данных пуст!')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="SparkJob check_csv_file_nonempty")

    parser.add_argument(
        '-c', '--path_to_csv_file',
        metavar='CSV-file',
        dest='path_to_csv_file',
        type=str,
        default=None,
        help='Input path to source file in CSV-format',
        required=True
    )

    args = parser.parse_args()

    # Create SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Start function
    status = check_csv_file_nonempty(
        spark=spark,
        path_to_csv_file=args.path_to_csv_file,
    )

    spark.stop()




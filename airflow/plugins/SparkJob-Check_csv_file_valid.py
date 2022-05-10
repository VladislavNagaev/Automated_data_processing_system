import argparse
from pyspark.sql import SparkSession


def check_csv_file_valid(spark, path_to_csv_file):

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
    columns = data.columns

    required_columns = ['Time', 'Vпр']

    missing_columns = []

    for column in required_columns:
        if column not in columns:
            missing_columns.append(column)

    if len(missing_columns) >= 1:
        missing_columns_str = ", ".join([f"<{i}>" for i in missing_columns])
        raise ValueError(f'Файл данных не валиден: отсуствует один или более столбец данных: {missing_columns_str}')



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="SparkJob check_csv_file_valid")

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
    status = check_csv_file_valid(
        spark=spark,
        path_to_csv_file=args.path_to_csv_file,
    )

    spark.stop()




"""
Adapter for writing and reading Avro data files as per a convention
"""
import os

import avro
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
from contextlib import contextmanager


def dags_path() -> str:
    """
    Absolute path at which the dags are stored in Airflow
    :return:
    """
    return os.path.abspath(os.path.dirname(__file__))


def avro_path() -> str:
    return os.path.join(dags_path(), "avro")


@contextmanager
def open_avro_writer(identifier: str) -> DataFileWriter:
    """
    Opens an avro file for writing and closes it automatically.
    :param identifier:
    :return:
    """
    path_ = avro_path()
    schema = avro.schema.parse(open(f"{path_}/{identifier}.avsc", "rb").read())
    writer = DataFileWriter(open(f"{path_}/{identifier}.avro", "wb"), DatumWriter(), schema)
    try:
        yield writer
    finally:
        writer.close()


@contextmanager
def open_avro_reader(identifier: str) -> DataFileReader:
    """
    Opens an avro file for reading and auto closes it.
    :param identifier:
    """
    reader = DataFileReader(open(f"{avro_path()}/{identifier}.avro", "rb"), DatumReader())
    try:
        yield reader
    finally:
        reader.close()

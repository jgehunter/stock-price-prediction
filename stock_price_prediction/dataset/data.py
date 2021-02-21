from pathlib import Path
import pandas as pd
import numpy as np


class DataSeries:
    def __init__(
        self,
        file_location=None,
    ):
        df = pd.read_parquet(file_location)
        self._df = df
        self._variables = list(df.columns)
        self._sample_size = df.shape[0]

    @property
    def variables(self):
        return self._variables

    @variables.setter
    def variables(self, variable_list):
        self._variables = variable_list

    @property
    def sample_size(self):
        return self._sample_size

    @sample_size.setter
    def sample_size(self, ssize):
        self._sample_size = ssize

    @property
    def data(self):
        return self._df

    @data.setter
    def data(self, data):
        self._df = data

class DataSet:
    def __init__(
        self,
        files_directory=None,
    ):

        self.files_directory = files_directory
        self.data_series = [DataSeries(file) for file in Path(files_directory).expanduser().glob("*.parquet")]

    def create_numpy_dataset(
        self,
        variable=None,
    ):

        series = 0
        for ds in self.data_series:
            if (variable in ds.variables):
                series += 1

        length = np.min([ds.sample_size for ds in self.data_series])
        self.np_data = np.zeros((series,length),dtype = np.float32)

        
        for i, ds in enumerate(self.data_series):
            data = ds.data[[variable]]
            data = np.array(data)
            data = np.transpose(data)
            data = data[0]
            data = data[:length]
            self.np_data[i] = data

    def normalize_numpy_data(
        self,
    ):

        data = self.np_data
        data = data[:, :]
        max_data = data.max(axis=1)
        min_data = data.min(axis=1)
        max_data = np.reshape(max_data, (max_data.shape[0], 1))
        min_data = np.reshape(min_data, (min_data.shape[0], 1))
        data = ( 2 * data - (max_data + min_data)) / (max_data - min_data)
        self.np_normalized_data = data

    def split_train_val_test(
        self,
    ):

        data = self.np_normalized_data

        train_split = round(0.8 * data.shape[1])
        val_split = round(0.9 * data.shape[1])

        x_train = data[:, :train_split]
        y_train = data[: 1:train_split+1]
        x_val =  data[:, :val_split]
        y_val = data[:, 1:val_split+1]
        x_test = data[:, :-1]
        y_test = data[:, 1:]

        x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], 1))
        x_val = np.reshape(x_val, (x_val.shape[0], x_val.shape[1], 1))
        x_test = np.reshape(x_test, (x_test.shape[0], x_test.shape[1], 1))

        y_train = np.reshape(y_train, (y_train.shape[0], y_train.shape[1], 1))
        y_val = np.reshape(y_val, (y_val.shape[0], y_val.shape[1], 1))
        y_test = np.reshape(y_test, (y_test.shape[0], y_test.shape[1], 1))

        return x_train, x_val, x_test, y_train, y_val, y_test



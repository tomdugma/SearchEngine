import pandas as pd
import os

class ReadFile:

    last_para = False

    def __init__(self, corpus_path):
        self.corpus_path = corpus_path

    def read_file(self, file_name):
        """
        This function is reading a parquet file contains several tweets
        The file location is given as a string as an input to this function.
        :param file_name: string - indicates the path to the file we wish to read.
        :return: a dataframe contains tweets.
        """

        if self.corpus_path not in file_name:
            file_name = os.path.join(self.corpus_path, file_name)
        df = pd.read_parquet(file_name)
        df = df[:1000]
        return df.values.tolist()


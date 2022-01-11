import sys
import os
sys.path.append('../../../util')
import csv
import pickle
import concurrent.futures
from imdb import IMDb
from graphdb_base import GraphDBBase
from string_util import strip
from tqdm import tqdm

class DataGetter:
    def __init__(self):
        self.__ia = IMDb()

    def __get(self, imdb_id):
        try:
            return self.__ia.get_movie(imdb_id)
        except Exception:
            print("exception throw")

    def get(self, file):
        all_ids = list()
        with open(file, 'r+') as fd:
            reader = csv.reader(fd, delimiter=',')
            next(reader, None)
            for row in reader:
                if row:
                    imdb_id = strip(row[1])
                    all_ids.append(imdb_id)

        results = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            results = list(tqdm(executor.map(self.__get, all_ids), total=len(all_ids)))

        data = dict()
        for i in range(len(all_ids)):
            imdb_id = all_ids[i]
            result = results[i]
            data[imdb_id] = result

        return data

def main():
    base_path = '../../data/ml-latest-small'
    links_path = os.path.join(base_path, 'links.csv')
    data_ouput_path = os.path.join(base_path, 'details.pkl')

    getter = DataGetter()
    data = getter.get(links_path)
    with open(data_ouput_path, 'wb') as fd:
        pickle.dump(data, fd)

if __name__ == '__main__':
    main()

import os
import logging
import math
import zipfile
from contextlib import contextmanager
from tempfile import TemporaryDirectory
from tqdm import tqdm
import gzip
import pandas as pd
import json

def load_gzip_file(file_path, cols_to_keep, chunk_size=100000):
    """Load zip file
    Args:
        file_path: đường dẫn zip file
        cols_to_keep (list): Các cột cần thiết
        chunk_size : kích thước khối
    """
    df_list = []
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        
        chunk_iterator = pd.read_json(f, lines=True, chunk_size=chunk_size)
        
        for i, chunk in enumerate(chunk_iterator):
            chunk = chunk[cols_to_keep]
            
            df_list.append(chunk)
            
    df = pd.concat(df_list, ignore_index=True)
    return df
    
    
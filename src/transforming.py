import pathlib

import pandas as pd
import numpy as np

src_dir = pathlib.Path(__file__).parent
data_dir = src_dir.parent / 'data'

df = pd.read_parquet(data_dir / "api_data.parquet")
print(df[df['city'] == 'Bangkok'])
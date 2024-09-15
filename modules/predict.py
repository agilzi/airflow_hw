import json
import os
from datetime import datetime

import dill
import pandas as pd

path = os.environ.get('PROJECT_PATH', '.')

def load_df() -> pd.DataFrame:
    data = []
    for item in os.scandir(f'{path}/data/test'):
        if item.is_file():
            with open(item.path, 'r') as f:
                data.append(json.load(f))
    df = pd.DataFrame.from_dict(data)
    return df

def load_model():
    with open(f'{path}/data/models/cars_pipe.pkl', 'rb') as f:
        model = dill.load(f)
    return model

def save_predicts(predicts):
    df_predicts = pd.DataFrame.from_dict(predicts)
    file_name = f'{path}/data/predictions/preds_{datetime.now().strftime("%Y%m%d%H%M")}.csv'
    df_predicts.to_csv(file_name, index=False)


def predict():
    df_in = load_df()
    model = load_model()
    y = model.predict(df_in)

    predicts = []
    for index, predict_price_cat in enumerate(y):
        car_id = df_in.iloc[index]['id']
        price_cat = y[index]
        predicts.append({'car_id': car_id,
                         'pred': price_cat})
    save_predicts(predicts)

if __name__ == '__main__':
    predict()

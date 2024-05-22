""" Miscellaneous helper functions for data preprocessing """
from datetime import datetime

import pandas as pd

from sklego.preprocessing import RepeatingBasisFunction
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, accuracy_score
from sklearn.model_selection import train_test_split

# pyright: reportArgumentType=false

def repeating_basis_day(df: pd.DataFrame) -> pd.DataFrame:
    """ Adds repeating basis function for day of year to the DataFrame """

    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['nday'] = df['Timestamp'].dt.dayofyear

    X = df[['nday']]

    rbf = RepeatingBasisFunction(n_periods=12, column="nday", input_range=(1,365), remainder="drop")
    rbf.fit(X)

    X = pd.DataFrame(index=X.index, data=rbf.transform(X))

    df = pd.concat([df, X], axis=1)

    months = {i: datetime(2000, i+1, 1).strftime("%b") for i in range(12)}
    df.rename(columns=months, inplace=True)    # pyright: ignore[reportCallIssue, reportUnhashable]

    return df

def make_dummies(df: pd.DataFrame) -> pd.DataFrame:
    """ Makes dummies for EventType and Location columns """
    df = pd.get_dummies(df, columns=['EventType', 'Location'], dtype=int)
    return df

def eval_model(model, data, train_ratio=0.75, validation_ratio=0.15, test_ratio=0.10) -> None:
    """ Evaluates the model using the test set """

    X = data.drop(columns=['Is_Anomaly'])
    Y = data['Is_Anomaly']

    # Split the data into training and test sets
    x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size=1-train_ratio, random_state=530)

    # Further split the test set into validation and test sets
    x_val, x_test, y_val, y_test = train_test_split(x_test, y_test, test_size=test_ratio/(test_ratio+validation_ratio), random_state=530)

    model.fit(x_train, y_train, eval_set=[(x_val, y_val)], verbose=False)
    y_pred = model.predict(x_test)

    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    acc = accuracy_score(y_test, y_pred)
    
    print(f"Mean Absolute Error: {mae}")
    print(f"Mean Squared Error: {mse}")
    print(f"R2 Score: {r2}")
    print(f"Accuracy: {acc}")

    df = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})

    true_pos = df[(df['Actual'] == 1) & (df['Predicted'] == 1)].shape[0]
    false_neg = df[(df['Actual'] == 1) & (df['Predicted'] == 0)].shape[0]
    actual_pos = df[df['Actual'] == 1].shape[0]

    print(f"Total number of anomalies: {actual_pos}")
    print(f"True positives: {true_pos} ({true_pos / actual_pos:.2%})")
    print(f"False negatives: {false_neg} ({false_neg / actual_pos:.2%})")

    return None

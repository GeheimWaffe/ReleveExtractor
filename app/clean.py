import pandas as pd
import datetime as dt


def concat_frames(frame_list: list) -> pd.DataFrame:
    result = pd.concat(frame_list)
    return result


def filter_by_date(df: pd.DataFrame) -> pd.DataFrame:
    begin_of_week = dt.date.today()
    begin_of_week = begin_of_week - dt.timedelta(days=begin_of_week.isoweekday() - 1)

    # filter
    df = df[df['Date'] >= begin_of_week]

    # end
    return df

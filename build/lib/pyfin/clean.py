import pandas as pd
import datetime as dt


def concat_frames(frame_list: list) -> pd.DataFrame:
    result = pd.concat(frame_list)
    return result


def filter_by_date(df: pd.DataFrame, interval_type: str, interval_count: int) -> pd.DataFrame:
    first_date = dt.date.today()
    if interval_type == 'week':
        first_date = first_date - dt.timedelta(days=(first_date.isoweekday() - 1) +
                                               7*(interval_count-1))

    # filter
    df = df[df['Date'] >= first_date]

    # end
    return df

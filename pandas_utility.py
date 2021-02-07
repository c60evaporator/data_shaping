from typing import List, Dict
import pandas as pd

def grby_rename_agg(df: pd.DataFrame, grby_keys: List[str], agg_dict: Dict[str, List[str]], as_index: bool=True) -> pd.DataFrame:
    """
    グルーピングしてagg集計し、フィールド名をaggで指定した処理を追加してリネーム

    Parameters
    ----------
    df : pd.DataFrame
        集計対象のデータ
    grby_keys: str or List[str]
        グルーピングのkey（フィールド名で指定 or フィールド名のListで複数指定）
    agg_dict: Dict[str, List[str]]
        agg集計内容を表すDict（使用法はgroupby集計のaggと同じ）
    as_index: bool
        grby_keysの内容をindexとして使用するか（使用法はgroupby集計のas_indexと同じ）
    """
    df_grby = df.groupby(grby_keys, as_index=as_index).agg(agg_dict)
    # agg_dictのvaluesが1階層（リスト指定なし）のとき、agg_dictからリネーム
    if df_grby.columns.nlevel == 1:
        rename_dict = {k: k + '_' + v for k, v in agg_dict.items()}
        df_grby = df_grby.rename(columns = rename_dict)
    # agg_dictのvaluesが2階層（リスト指定）のとき、マルチインデックスのカラム名を結合
    elif df_grby.columns.nlevel == 2:
        df_grby.columns = [c1 + '_' + c2 for c1, c2 in df_grby.columns]
    # agg_dictのvaluesが3階層以上のとき、エラーを返す
    else:
        Exception('values of agg_dict must be str or list[str]')
    return df_grby

def grby_merge_agg(df: pd.DataFrame, keys: List[str], agg_dict: Dict[str, List[str]]) -> pd.DataFrame:
    """
    グルーピングしてagg集計した結果を元のデータフレームに結合（フィールド名はaggで指定した処理を追加してリネーム）

    Parameters
    ----------
    df : pd.DataFrame
        集計対象のデータ
    grby_keys: str or List[str]
        グルーピングのkey（フィールド名で指定 or フィールド名のListで複数指定）
    agg_dict: Dict[str, List[str]]
        agg集計内容を表すDict（使用法はgroupby集計のaggと同じ）
    """
    df_grby = grby_rename_agg(df, keys, agg_dict)
    df_merge = pd.merge(df, df_grby, left_on=keys, right_index=True, how='left')
    return df_merge
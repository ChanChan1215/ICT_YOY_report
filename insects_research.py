import re
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pymysql

def clean_column_name(col):
    """清理欄位名稱：去前後空白、換行、BOM、重複空白"""
    col = str(col).replace("\ufeff", "")   # 去 BOM
    col = col.replace("\n", " ").replace("\r", " ")
    col = re.sub(r"\s+", " ", col).strip()
    return col

def clean_text_value(x):
    """清理字串內容：去前後空白、換行、重複空白"""
    if pd.isna(x):
        return x
    x = str(x).replace("\n", " ").replace("\r", " ")
    x = re.sub(r"\s+", " ", x).strip()
    return x
def clean_customer_name(x):
    """
    客戶名稱清理規則：
    1. 空白 -> NaN
    2. 只要含有 ? 或 ？ -> NaN
    """
    if pd.isna(x):
        return pd.NA

    x = str(x).replace("\n", " ").replace("\r", " ")
    x = re.sub(r"\s+", " ", x).strip()

    # 空白字串 -> NaN
    if x == "":
        return pd.NA

    # 含有半形 ? 或全形 ？ -> NaN
    if "?" in x or "？" in x:
        return pd.NA

    return x


def try_convert_numeric(series):
    """
    嘗試把欄位轉成數字：
    1. 移除逗號
    2. 移除百分比符號
    3. 轉成 numeric
    """
    s = series.astype(str).str.strip()
    s = s.str.replace(",", "", regex=False)
    s = s.str.replace("%", "", regex=False)
    s = s.replace(["", "nan", "None", "null"], pd.NA)

    converted = pd.to_numeric(s, errors="coerce")

    # 如果整欄幾乎都能轉成功，就回傳數字欄
    if converted.notna().sum() >= max(1, int(len(series) * 0.7)):
        return converted
    return series

def main():
    #----------------讀檔案E
    filename = Path(r"C:\Users\user\Desktop\DataEnglneers20260307\python_insects\csv_files\11401-09【GA、EA_YOY】AccountNumber_cleaned.csv")
    df = pd.read_csv(filename, encoding="utf-8-sig")
    #df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    #print(df)
    #----------------清理(轉換)T
    print("=== 原始資料 ===")
    print(df.head(10))
    print("\n資料筆數：", len(df))
    #list為串列資料型態
    print("欄位名稱：", list(df.columns))
    print(list(df.columns)[0])
    # ---------- 1. 清理欄位名稱 ----------
    df.columns = [clean_column_name(col) for col in df.columns]

    # ---------- 2. 清理所有字串欄位 ----------
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(clean_text_value)
    
    # ---------- 3. 將空字串轉成缺失值 ----------
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    # ---------- 4. 刪除整列全空 ----------
    df = df.dropna(how="all")

    # ---------- 5. 刪除重複資料 ----------
    df = df.drop_duplicates()

    # ---------- 6. 嘗試把欄位轉成數字 ----------
    for col in df.columns:
        df[col] = try_convert_numeric(df[col])

    # ---------- 7. 若有常見日期欄位，自動轉日期 ----------
    possible_date_cols = [col for col in df.columns if ("日期" in col or "date" in col.lower())]
    for col in possible_date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # ---------- 8. 檢查缺失值 ----------
    print("\n=== 缺失值統計 ===")
    print(df.isna().sum())

    # ---------- 9. 看清理後資料型態 ----------
    print("\n=== 清理後資料型態 ===")
    print(df.dtypes)

    print("\n=== 清理後資料 ===")
    print(df.head(10))

    # ---------- 10. 輸出清理後檔案 ----------
    output_file = filename.parent / f"{filename.stem}_cleaned.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print("\n清理完成，已輸出檔案：")
    print(output_file)

# ----------------寫入資料庫 L
    print("\n=== 開始寫入資料庫 ===")
    # 只取前 4 欄對應資料表欄位
    db_df = df.iloc[:, :4].copy()
    db_df.columns = ['county', 'customer_name', 'amount', 'note']

    # 移除客戶名稱為空的列
    db_df = db_df.dropna(subset=['customer_name'])

    # 設定資料庫連線資訊
    host = 'localhost'
    port = 3306
    user = 'root'
    passwd = 'abc912321'
    db = 'mydb'
    charset = 'utf8mb4'

    conn = None
    cursor = None
    try:
        conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset=charset)
        print('Successfully connected!')
        cursor = conn.cursor()

        #create_table(conn)
        #insert_dataframe(conn, db_df)
        #query_top5(conn)

    except Exception as e:
        print(f"❌ 資料庫錯誤：{e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("✅ 資料庫連線已關閉")

    

if __name__ == "__main__":
    print("===================================")
    main()
    print("===================================")
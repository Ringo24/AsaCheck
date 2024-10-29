from simple_salesforce import Salesforce
import pandas as pd
import csv
import sys

def read_file(file_path):
    try:
        df = pd.read_csv(file_path, sep=',', header=0, dtype=str, encoding='utf-8')

        if df.empty:
            print("エラーメッセージ：ファイルが空です。")
            sys.exit(1)
        else:
            print(f"{file_path}を正常に読み込みました。")
            return df
    except FileNotFoundError:
        print("エラーメッセージ：ファイルが見つかりませんですた。")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print("エラーメッセージ：ファイルが空の状態で読み込みを実行したため失敗しました。")
        sys.exit(1)
    except pd.errors.ParserError:
        print("エラーメッセージ：ファイルが誤った形式で読み込みを実行したため失敗しました。")
        sys.exit(1)

def login_salesforce(username, password, security_token, domain):
    """
    Salesforceにログイン
    """
    return Salesforce(username=username, password=password, security_token=security_token, domain=domain)

if __name__ == "__main__":

    # Salesforce 接続情報
    username = 'cw000013@crm.east.ntt.co.jp.it'
    password = 'pass@word1'
    security_token = 'HANkadJwpO7BPZdhLP1YB03z'
    domain = 'test' #商用環境は login

    sf = login_salesforce(username, password, security_token, domain)

    #アクティブ化対象取引先事業所
    office__c_file = './取引先事業所.csv'
    office__c_df = read_file(office__c_file)

    chunk_size = 300
    total_records = len(office__c_df)

    for start in range(0, total_records, chunk_size):
        end = min(start + chunk_size, total_records)
        accidaddresscode__c_values = office__c_df['accidaddresscode__c'].iloc[start:end].astype(str).apply(lambda x: f"'{x}'").tolist()
        comma_accidaddresscode__c = ','.join(accidaddresscode__c_values)

        result = sf.query(f"SELECT id, AccIDAddressCode__c FROM Office__c WHERE AccIDAddressCode__c in ({comma_accidaddresscode__c})")
        
        result_df = pd.DataFrame(result['records']) if start == 0 else pd.concat([result_df, pd.DataFrame(result['records'])], ignore_index=True)
         
    result_df.to_csv('.取引先事業所検索結果.csv', header=True, index=False, mode='w', encoding='utf-8', quoting=csv.QUOTE_ALL)


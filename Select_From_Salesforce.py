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
    except


if __name__ == "__main__":

    # Salesforce 接続情報
    username = 'cw000013@crm.east.ntt.co.jp.it'
    password = 'pass@word1'
    security_token = 'HANkadJwpO7BPZdhLP1YB03z'
    domain = 'test' #商用環境は login

    sf = Salesforce(username=username, password=password, security_token=security_token, domain=domain)

    #アクティブ化対象取引先事業所
    activateOffice__c_file = './data/アクティブ化対象取引先事業所.csv'
    activateOffice__c_df = pd.read_csv(activateOffice__c_file, sep=',', header=0, dtype=str, encoding='utf-8')

    chunk_size = 300
    total_records = len(activateOffice__c_df)

    for start in range(0, total_records, chunk_size):
        end = min(start + chunk_size, total_records)
        accidaddresscode__c_values = activateOffice__c_df['accidaddresscode__c'].iloc[start:end].astype(str).apply(lambda x: f"'{x}'").tolist()
        comma_accidaddresscode__c = ','.join(accidaddresscode__c_values)

        result = sf.query(f"SELECT id, AccIDAddressCode__c FROM Office__c WHERE AccIDAddressCode__c in ({comma_accidaddresscode__c})")
        #検索結果が存在する場合、対象から除外
        if result['totalSize'] > 0:
            matching_accidaddresscode__c = [record['AccIDAddressCode__c'] for record in result['records']]
            activateOffice__c_df = activateOffice__c_df[~activateOffice__c_df['accidaddresscode__c'].isin(matching_accidaddresscode__c)]

    activateOffice__c_df.to_csv('./data/出力データ/アクティブ化対象取引先事業所.csv', header=True, index=False, mode='w', encoding='utf-8', quoting=csv.QUOTE_ALL)

    #非アクティブ化対象取引先事業所
    deactivateOffice__c_file = './data/非アクティブ化対象取引先事業所.csv'
    deactivateOffice__c_df = pd.read_csv(deactivateOffice__c_file, sep=',', header=0, dtype=str, encoding='utf-8')

    chunk_size = 300
    total_records = len(deactivateOffice__c_df)

    for start in range(0, total_records, chunk_size):
        end = min(start + chunk_size, total_records)
        ids_values = deactivateOffice__c_df['id'].iloc[start:end].astype(str).apply(lambda x: f"'{x}'").tolist()
        comma_ids = ','.join(ids_values)

        result = sf.query(f"SELECT id,recordtype.name,OpportunityActiveRegistDate__c,(select id from rel_OF_PhoneNumber__r) FROM Office__c WHERE id in ({comma_ids})")
        #検索結果のレコードタイプが手動でない又は、商談活動等登録日時がNull以外又は、電話番号が存在する場合、対象から除外
        if result['totalSize'] > 0:
            matching_ids = [record['id'] for record in result['records'] if record['recordtype.name'] != '手動' or record['OpportunityActiveRegistDate__c'] is not None or record['rel_OF_PhoneNumber__r'] is not None]
            deactivateOffice__c_df = deactivateOffice__c_df[~deactivateOffice__c_df['id'].isin(matching_ids)]

    deactivateOffice__c_df.to_csv('./data/出力データ/非アクティブ化対象取引先事業所.csv', header=True, index=False, mode='w', encoding='utf-8', quoting=csv.QUOTE_ALL)


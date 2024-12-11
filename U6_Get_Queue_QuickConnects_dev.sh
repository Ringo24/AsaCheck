#!/bin/bash
####################################################################################################
# 
# Amazon Connect環境キューに登録されているクイック接続の出力
# 
####################################################################################################

#####################################################################
# AWS環境情報
#####################################################################

#-------------------------------------------------------------------------
# AWSアカウント
#-------------------------------------------------------------------------
Target_Aws_Account=992967614638

#-------------------------------------------------------------------------
# 作成リージョン
#-------------------------------------------------------------------------
Target_Region=ap-northeast-1

#-------------------------------------------------------------------------
# AmazonConnectインスタンス名（エイリアス）
#-------------------------------------------------------------------------
Name_AmazonConnect_Instance=biz-merge-cti-it




#-------------------------------------------------------------------------
# 認証方法
#  SAML               ：SAML2.0ベース認証
#  CONNECT_MANAGED    ：Amazon Connectでユーザーを作成および管理
#  EXISTING_DIRECTORY ：既存のディレクトリへのリンク
#-------------------------------------------------------------------------
Identity_Management_Type=SAML

#-------------------------------------------------------------------------
# キューリストファイル名
#-------------------------------------------------------------------------
INPUT_FILE1=U6_amakone_Queues_INPUT.txt

#-------------------------------------------------------------------------
# ログファイル名
#-------------------------------------------------------------------------
OUTPUT_FILE1=U6_OUTPUT_log.txt
>${OUTPUT_FILE1}

#-------------------------------------------------------------------------
# ワークファイル名
#-------------------------------------------------------------------------
Work_List=Work_List.work
Work_Data1=Work_Data1.work
Work_Data2=Work_Data2.work
Work_Data3=Work_Data3.work
Work_Data4=Work_Data4.work
Work_Data5=Work_Data5.work
Work_Data6=Work_Data6.work

#-------------------------------------------------------------------------
# 環境変数設定
#-------------------------------------------------------------------------
export AWS_MAX_ATTEMPTS=50


echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇
echo ◇
echo ◇  Amazon Connect環境キューに登録されているクイック接続の出力を開始します。
echo ◇
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇

echo ◇　前処理
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇
echo ◇

#		#-------------------------------------------------------------------------
#		# 引数チェック
#		#-------------------------------------------------------------------------
#		if [ $# != 1 ]; then
#		  echo ◆　指定された引数は $# 個です。
#		  echo ◆　実行するにはユーザーファイルを１つ指定する必要があります。
#		  unset AWS_MAX_ATTEMPTS
#		  echo ◆　指定された引数は $# 個です。>>${OUTPUT_FILE1}
#		  echo ◆　実行するにはユーザーファイルを１つ指定する必要があります。>>${OUTPUT_FILE1}
#		  exit 10
#		else
#		  File_users_list=$1
#		  echo ◇　ユーザーファイル（${File_users_list}）が指定されました。
#		fi
#
#		#-------------------------------------------------------------------------
#		# 変数ファイルの読み込み
#		#-------------------------------------------------------------------------
#		if ! test -r ./${File_users_list} ;then
#		  echo ◆　ユーザーファイル（${File_users_list}）が読めません。
#		  echo ◆　処理を終了します。
#		  unset AWS_MAX_ATTEMPTS
#		  echo ◆　ユーザーファイル（${File_users_list}）が読めません。>>${OUTPUT_FILE1}
#		  echo ◆　処理を終了します。>>${OUTPUT_FILE1}
#		  exit 10
#		fi


#-------------------------------------------------------------------------
# 環境チェック：アカウントチェック
#-------------------------------------------------------------------------
My_Aws_Account=`aws --region ${Target_Region} sts get-caller-identity --output json | jq -r '.Account'`
if [ "${My_Aws_Account}" != "${Target_Aws_Account}" ] ; then
  echo ◆　環境変数で指定のアカウントと実行環境に相違があります。
  echo ◆　　設定アカウント：${Target_Aws_Account}
  echo ◆　　実行アカウント：${My_Aws_Account}
  echo ◆　処理を終了します。
  unset AWS_MAX_ATTEMPTS
  echo ◆　環境変数で指定のアカウントと実行環境に相違があります。>>${OUTPUT_FILE1}
  echo ◆　　設定アカウント：${Target_Aws_Account}>>${OUTPUT_FILE1}
  echo ◆　　実行アカウント：${My_Aws_Account}>>${OUTPUT_FILE1}
  echo ◆　処理を終了します。>>${OUTPUT_FILE1}
  exit 10
else
  echo ◇　環境変数で指定のアカウントと実行環境に相違なし。
  echo ◇　　設定アカウント：${Target_Aws_Account}
  echo ◇　　実行アカウント：${My_Aws_Account}
  echo ◇　
fi

#-------------------------------------------------------------------------
# 環境チェック：実行ユーザー表示
#-------------------------------------------------------------------------
My_Aws_User=`aws --region ${Target_Region} sts get-caller-identity --output json | jq -r '.Arn'`
echo ◇　実行ユーザーは（${My_Aws_User}）です。


#-------------------------------------------------------------------------
# インスタンス有無チェック
#-------------------------------------------------------------------------
aws --region ${Target_Region} connect list-instances  --output json | grep InstanceAlias | grep \"${Name_AmazonConnect_Instance}\" >/dev/null
RC=$?
if [ ${RC} -ne 0 ] ; then
  echo ◆　AmazonConnectのインスタンス（${Name_AmazonConnect_Instance}）が存在しません。
  echo ◆　処理を終了します。
  unset AWS_MAX_ATTEMPTS
  echo ◆　AmazonConnectのインスタンス（${Name_AmazonConnect_Instance}）が存在しません。>>${OUTPUT_FILE1}
  echo ◆　処理を終了します。>>${OUTPUT_FILE1}
  exit 10
else
  echo ◇　AmazonConnectのインスタンス（${Name_AmazonConnect_Instance}）が存在することを確認
  echo ◇　
fi
# インスタンスID取得
ID_AmazonConnect=`aws --region ${Target_Region} connect list-instances  --output text | grep $'\t'${Name_AmazonConnect_Instance}$'\t' | sed -e "s/^[^\t]*\t[^\t]*\t[^\t]*\t\([^\t]*\)\t.*$/\1/"`


User_Error_CNT=0
Queue_Error_CNT=0
User_CNT=0
Queue_CNT=0


Error_CNT=0
Queue_START_TIME=`TZ=JST-9 date +"%Y/%m/%d %H:%M:%S"`

echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇
echo ◇　キューに登録されているクイック接続の出力処理
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇
echo ◇
echo ◇　キューIDの取得処理を開始します。

#ID_Queues=(`aws --region ${Target_Region} connect list-queues  --instance-id  ${ID_AmazonConnect} --output json | jq -r '.QueueSummaryList[] | [.Id, .Name] | @csv'`) > ${Work_List}
#if [ "${ID_Queues[0]}" = "" ];then
#  echo ◆　　キューのIDが取得できません。
#  echo ◆　　処理を終了します。
#  unset AWS_MAX_ATTEMPTS
#  echo ◆　　キューのIDが取得できません。>>${OUTPUT_FILE1}
#  echo ◆　　処理を終了します。>>${OUTPUT_FILE1}
#  exit 10
#else
#  echo ◇　キューIDの取得処理が完了しました。
#  echo ◇　
#fi

echo ◇　キューリストの取得処理を開始します。
aws --region ${Target_Region} connect list-queues  --instance-id  ${ID_AmazonConnect} --output text > ${Work_List}
echo ◇　キューリストの取得処理が完了しました。


echo ◇
echo ◇　キューに対するクイック接続の出力処理を開始します。

#while read line
while read line || [ -n "${line}" ]
do
  line_1=`echo ${line} | cut -b 1`
  if [ "${line_1}" != "#" ] && [ "${line_1}" != "" ] ;then
    # 先頭と末尾のダブルクォート（"）を削除
    Def_Queue_Name=`echo ${line} |  sed -e "s/^\"\(.*\)\"$/\1/"`
    Queue_CNT=`expr ${Queue_CNT} + 1`
    echo ◇　　対象キュー名：${Def_Queue_Name}
    
    ##################################################
    # 変換後キュー情報取得
    ##################################################
#    ID_Queue=`aws --region ${Target_Region} connect list-queues  --instance-id  ${ID_AmazonConnect} --output text | grep $'\t'"${Def_Queue_Name}"$'\t'  |  sed -e "s/^[^\t]*\t[^\t]*\t\([^\t]*\)\t.*$/\1/"`
    ID_Queue=`cat ${Work_List} | grep $'\t'"${Def_Queue_Name}"$'\t'  |  sed -e "s/^[^\t]*\t[^\t]*\t\([^\t]*\)\t.*$/\1/"`
    if [ "${ID_Queue}" = "" ] ;then 
      echo ◆　　　対象のキュー（${Def_Queue_Name}）が存在しません。処理をスキップします。
      Error_CNT=`expr ${Error_CNT} + 1`
      echo ◆　　　対象のキュー（${Def_Queue_Name}）が存在しません。処理をスキップします。>>${OUTPUT_FILE1}
      continue
    fi
    
    ##################################################
    # クイック接続の関連付け取得
    ##################################################
    # CNT1: 現在キューの関連付けられているクイック接続のカウント。
    ##################################################
    CNT1=0
    aws --region ${Target_Region} connect list-queue-quick-connects  --instance-id  ${ID_AmazonConnect} --queue-id ${ID_Queue}  | jq -r '(.QuickConnectSummaryList | [.Name,.QuickConnectType]) | @csv' > ${Work_Data1}
    RC=$?
    if [ ${RC} -ne 0 ] ; then
      echo ◆　　キュー（${Def_Queue_Name}）の関連付けられているクイック接続取得でエラーが発生しました。（RC=${RC}）
      echo ◆　　キュー（${Def_Queue_Name}）の処理を中断します。
      echo ◆　　キュー（${Def_Queue_Name}）の関連付けられているクイック接続取得でエラーが発生しました。（RC=${RC}）>>${OUTPUT_FILE1}
      echo ◆　　キュー（${Def_Queue_Name}）の処理を中断します。>>${OUTPUT_FILE1}
      
      exit 10
    fi
  
    CNT1=$(wc -l < "${Work_Data1}")
    echo ◆　　キュー（${Def_Queue_Name}）の関連付けられているクイック接続数は（${CNT1}）件です。
    echo ◆　　キュー（${Def_Queue_Name}）の関連付けられているクイック接続数は（${CNT1}）件です。>>${OUTPUT_FILE1}

    # Todo: ファイル名：${Def_Queue_Name}、内容：${Work_Data1}のテキストファイルを出力。
    
  fi
done < ./${INPUT_FILE1}

echo ◇
echo ◇　キューに関連付けられているクイック接続出力処理を終了しました。
echo ◇

rm -f ${Work_List}
Queue_Error_CNT=${Error_CNT}
Queue_END_TIME=`TZ=JST-9 date +"%Y/%m/%d %H:%M:%S"`

echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇
echo ◇
echo ◇　Amazon Connect環境ユーザー設定（クイック接続登録）が完了しました。
echo ◇
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇
echo ◇
echo ◇　クイック接続登録処理
echo ◇　　登録キュー数　： ${Queue_CNT} 件
echo ◇　　エラー件数　　： ${Queue_Error_CNT} 件
echo ◇　　処理時間　　　： ${Queue_START_TIME} ～ ${Queue_END_TIME}
echo ◇
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇

echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇ >>${OUTPUT_FILE1}
echo ◇ >>${OUTPUT_FILE1}
echo ◇　クイック接続登録処理 >>${OUTPUT_FILE1}
echo ◇　　登録キュー数　： ${Queue_CNT} 件 >>${OUTPUT_FILE1}
echo ◇　　エラー件数　　： ${Queue_Error_CNT} 件 >>${OUTPUT_FILE1}
echo ◇　　処理時間　　　： ${Queue_START_TIME} ～ ${Queue_END_TIME} >>${OUTPUT_FILE1}
echo ◇ >>${OUTPUT_FILE1}
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇ >>${OUTPUT_FILE1}

unset AWS_MAX_ATTEMPTS
exit 0


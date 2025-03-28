#!/bin/bash
####################################################################################################
# 
# Amazon Connect環境クイック接続出力
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
# 継続ファイル名
#-------------------------------------------------------------------------
OUTPUT_FILE1=U4_OUTPUT_log.txt
OUTPUT_FILE2=U4_OUTPUT_QuickConnectsData.csv
OUTPUT_FILE3=U4_INPUT_QuickConnectList.txt
OUTPUT_FILE4=U4_INPUT_Counter.txt

#-------------------------------------------------------------------------
# ワークファイル名
#-------------------------------------------------------------------------
Work_UsersList=U4_Work_UsersList.work
Work_R_Pro=U4_Work_R_Pro.work
Work_S_Pro=U4_Work_S_Pro.work
Work_U_Pro=U4_Work_U_Pro.work
Work_UserData=U4_Work_UserData.work
Work_OutputData=U4_Work_OutputData.work

#-------------------------------------------------------------------------
# 環境変数設定
#-------------------------------------------------------------------------
export AWS_MAX_ATTEMPTS=50


echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇
echo ◇
echo ◇  Amazon Connect環境クイック接続出力を開始します。
echo ◇
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇

echo ◇　前処理
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇
echo ◇

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
  exit 10
else
  echo ◇　AmazonConnectのインスタンス（${Name_AmazonConnect_Instance}）が存在することを確認
  echo ◇　
fi
# インスタンスID取得
ID_AmazonConnect=`aws --region ${Target_Region} connect list-instances  --output text | grep $'\t'${Name_AmazonConnect_Instance}$'\t' | sed -e "s/^[^\t]*\t[^\t]*\t[^\t]*\t\([^\t]*\)\t.*$/\1/"`


#User_START_TIME=`TZ=JST-9 date +"%Y/%m/%d %H:%M:%S"`


#-------------------------------------------------------------------------
# クイック接続管理
#-------------------------------------------------------------------------
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇
echo ◇　実行状態の確認
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇
echo ◇

if [ -s ./${OUTPUT_FILE3} ] && [ -s ./${OUTPUT_FILE4} ] ; then
  . ./${OUTPUT_FILE4}
  echo ◇　仕掛かり中のクイック接続情報が存在します。
  echo ◇　　処理済クイック接続数　：${User_CNT}
  echo ◇　　総クイック接続数　　　：${Max_CNT}
  echo ◇　処理を継続しますか？（Y/N）
  Check_INPUT=
  while true
  do
    read  Check_INPUT
    
    if [ "${Check_INPUT}" = "Y" -o "${Check_INPUT}" = "y" ];then
      echo ◇　処理を継続します。
      echo ◇
      
      echo ◇◇◇◇◇◇◇◇ >>${OUTPUT_FILE1}
      echo ◇　処理再開　◇ >>${OUTPUT_FILE1}
      echo ◇◇◇◇◇◇◇◇ >>${OUTPUT_FILE1}
      
      break
    fi
    
    if [ "${Check_INPUT}" = "N" -o "${Check_INPUT}" = "n" ];then
      echo ◆　処理を終了します。
      echo ◆　一から処理する場合、以下のファイルを削除して下さい。
      echo ◆　　削除対象：${OUTPUT_FILE3}
      echo ◆　　　　　　　${OUTPUT_FILE4}
      unset AWS_MAX_ATTEMPTS
      exit 10
    fi
    echo ◇　もう一度入力してください。
    echo ◇　処理を継続しますか？（Y/N）
  done
  
  
  
else
  echo ◇　仕掛かり中のクイック接続情報が存在しません。
  
  echo ◇　一から処理を実施しますか？（Y/N）
  Check_INPUT=
  while true
  do
    read  Check_INPUT
    
    if [ "${Check_INPUT}" = "Y" -o "${Check_INPUT}" = "y" ];then
      echo ◇　処理を実施します。
      echo ◇
      
      break
    fi
    
    if [ "${Check_INPUT}" = "N" -o "${Check_INPUT}" = "n" ];then
      echo ◆　処理を終了します。
      unset AWS_MAX_ATTEMPTS
      exit 10
    fi
    echo ◇　もう一度入力してください。
    echo ◇　処理を実施しますか？（Y/N）
  done
  
  # 引継ぎファイルの初期化
  >${OUTPUT_FILE1}
  >${OUTPUT_FILE2}
  
  echo ◇　クイック接続一覧を取得します。
  aws --region ${Target_Region} connect list-quick-connects  --instance-id  ${ID_AmazonConnect} --output text  > ${Work_UsersList}
  RC=$?
  if [ "${RC}" != "0" ] ;then
    rm -f ${Work_UsersList}
    echo ◆
    echo ◆　クイック接続一覧の取得に失敗しました。（RC=${RC}）
    echo ◆　処理を中断します。
    unset AWS_MAX_ATTEMPTS
    exit 10
  fi

  echo ◇
  echo ◇　クイック接続一覧をクイック接続名でソートします。
  cat ${Work_UsersList} | sort -k 6 > ${OUTPUT_FILE3}
  rm -f ${Work_UsersList}
  
  
  User_CNT=0
  Max_CNT=`cat ${OUTPUT_FILE3} | wc -l`
  
  echo User_CNT=${User_CNT}> ${OUTPUT_FILE4}
  echo Max_CNT=${Max_CNT}>> ${OUTPUT_FILE4}
  
  echo ◇
  echo ◇　総クイック接続数は${Max_CNT}件です。
  
fi

echo ◇
echo ◇　ユーザー一覧を取得します。
aws --region ${Target_Region} connect list-users  --instance-id  ${ID_AmazonConnect} --output text  > ${Work_U_Pro}
RC=$?
if [ "${RC}" != "0" ] ;then
  rm -f ${Work_U_Pro}
  echo ◆
  echo ◆　ユーザー一覧の取得に失敗しました。（RC=${RC}）
  echo ◆　処理を中断します。
  unset AWS_MAX_ATTEMPTS
  exit 10
fi

echo ◇
echo ◇　キュー一覧を取得します。
aws --region ${Target_Region} connect list-queues  --instance-id  ${ID_AmazonConnect} --output text  > ${Work_R_Pro}
RC=$?
if [ "${RC}" != "0" ] ;then
  rm -f ${Work_U_Pro}
  rm -f ${Work_R_Pro}
  echo ◆
  echo ◆　キュー一覧の取得に失敗しました。（RC=${RC}）
  echo ◆　処理を中断します。
  unset AWS_MAX_ATTEMPTS
  exit 10
fi

echo ◇
echo ◇　フロー一覧を取得します。
aws --region ${Target_Region} connect list-contact-flows  --instance-id  ${ID_AmazonConnect} --output text  > ${Work_S_Pro}
RC=$?
if [ "${RC}" != "0" ] ;then
  rm -f ${Work_U_Pro}
  rm -f ${Work_R_Pro}
  rm -f ${Work_S_Pro}
  echo ◆
  echo ◆　フロー一覧の取得に失敗しました。（RC=${RC}）
  echo ◆　処理を中断します。
  unset AWS_MAX_ATTEMPTS
  exit 10
fi



  echo ◇
  echo ◇　クイック接続の出力を開始します。
  echo ◇
  ############################################
  # ヘッダを出力ファイルへ書き込み
  ############################################
  echo \"Id\",\"Name\",\"QuickConnectType\",\"UserName\",\"ContactFlow\",\"LastModifiedTime\",\"LastModifiedRegion\" > ${OUTPUT_FILE2}

  while [ -s ./${OUTPUT_FILE3} ]
  do
    line=`head -n 1 ./${OUTPUT_FILE3}`
    
    Def_01=`echo ${line}              | awk '{print $1}'  `
    Def_02=`echo ${line}              | awk '{print $2}'  `
    Def_Id=`echo ${line}              | awk '{print $3}'  `
    Def_Name=`echo ${line}            | awk '{print $6}'  `
    Def_Type=`echo ${line}            | awk '{print $7}'  `
    
    User_CNT=`expr ${User_CNT} + 1`
    echo ◇　対象クイック接続（${Def_Name}）の処理を開始します。　（${User_CNT}件目）
    echo ◇　対象クイック接続（${Def_Name}）の処理を開始します。　（${User_CNT}件目）>>${OUTPUT_FILE1}
    
    ############################################
    # レコード間の区切り
    ############################################
    > ${Work_OutputData}

    ############################################
    # クイック接続情報取得（ユーザー）
    ############################################
    if [ "${Def_Type}" = "USER" ] ; then
      aws --region ${Target_Region} connect describe-quick-connect  --instance-id  ${ID_AmazonConnect} --quick-connect-id ${Def_Id}  | jq -r '(.QuickConnect | [.QuickConnectId,.Name,.QuickConnectConfig.QuickConnectType,.QuickConnectConfig.UserConfig.UserId,.QuickConnectConfig.UserConfig.ContactFlowId,.LastModifiedTime,.LastModifiedRegion]) | @csv' > ${Work_UserData}
      RC=$?
      if [ ${RC} -ne 0 ] ; then
        echo ◆　　クイック接続（${Def_Name}）の情報取得でエラーが発生しました。（RC=${RC}）
        echo ◆　　クイック接続（${Def_Name}）の処理を中断します。
        echo ◆　　クイック接続（${Def_Name}）の情報取得でエラーが発生しました。（RC=${RC}）>>${OUTPUT_FILE1}
        echo ◆　　クイック接続（${Def_Name}）の処理を中断します。>>${OUTPUT_FILE1}
        
        exit 10
      fi
    
    ############################################
    # ユーザー情報の取得
    # ユーザー名の置換
    ############################################
      WK_UserId=$(cut -d "," -f 4 ${Work_UserData} | tr -d '"')
      WK_UserName=$(grep "${WK_UserId}" ${Work_U_Pro} | cut -f 6)
      echo ◇　ユーザーID（${WK_UserId}）のユーザー名は（${WK_UserName}）>>${OUTPUT_FILE1}
      sed -i "s/${WK_UserId}/${WK_UserName}/g" ${Work_UserData}
    
    ############################################
    # クイック接続情報取得（キュー）
    ############################################
    elif [ "${Def_Type}" = "QUEUE" ] ; then
      aws --region ${Target_Region} connect describe-quick-connect  --instance-id  ${ID_AmazonConnect} --quick-connect-id ${Def_Id}  | jq -r '(.QuickConnect | [.QuickConnectId,.Name,.QuickConnectConfig.QuickConnectType,.QuickConnectConfig.QueueConfig.QueueId,.QuickConnectConfig.QueueConfig.ContactFlowId,.LastModifiedTime,.LastModifiedRegion]) | @csv' > ${Work_UserData}
      RC=$?
      if [ ${RC} -ne 0 ] ; then
        echo ◆　　クイック接続（${Def_Name}）の情報取得でエラーが発生しました。（RC=${RC}）
        echo ◆　　クイック接続（${Def_Name}）の処理を中断します。
        echo ◆　　クイック接続（${Def_Name}）の情報取得でエラーが発生しました。（RC=${RC}）>>${OUTPUT_FILE1}
        echo ◆　　クイック接続（${Def_Name}）の処理を中断します。>>${OUTPUT_FILE1}
        
        exit 10
      fi

    ############################################
    # キュー情報の取得
    # キュー名の置換
    ############################################
      WK_QueueId=$(cut -d "," -f 4 ${Work_UserData} | tr -d '"')
      WK_QueueName=`cat ${Work_R_Pro} | grep "${WK_QueueId}" | cut -f 6`
      echo ◇　キューID（${WK_QueueId}）のキュー名は（${WK_QueueName}）>>${OUTPUT_FILE1}
      sed -i "s/${WK_QueueId}/${WK_QueueName}/g" ${Work_UserData}

    ############################################
    # クイック接続情報取得（電話番号）
    ############################################
    elif [ "${Def_Type}" = "PHONE_NUMBER" ] ; then
      aws --region ${Target_Region} connect describe-quick-connect  --instance-id  ${ID_AmazonConnect} --quick-connect-id ${Def_Id}  | jq -r '(.QuickConnect | [.QuickConnectId,.Name,.QuickConnectConfig.QuickConnectType,.QuickConnectConfig.PhoneConfig.PhoneNumber,"-",.LastModifiedTime,.LastModifiedRegion]) | @csv' > ${Work_UserData}
      RC=$?
      if [ ${RC} -ne 0 ] ; then
        echo ◆　　クイック接続（${Def_Name}）の情報取得でエラーが発生しました。（RC=${RC}）
        echo ◆　　クイック接続（${Def_Name}）の処理を中断します。
        echo ◆　　クイック接続（${Def_Name}）の情報取得でエラーが発生しました。（RC=${RC}）>>${OUTPUT_FILE1}
        echo ◆　　クイック接続（${Def_Name}）の処理を中断します。>>${OUTPUT_FILE1}
        
        exit 10
      fi
    fi
    
    ############################################
    # ContactFlow情報の取得
    # ContactFlow名の置換
    ############################################
    WK_FlowId=$(cut -d "," -f 5 ${Work_UserData} | tr -d '"')
    if [ ${WK_FlowId} != "-" ] ; then
      WK_FlowName=`cat ${Work_S_Pro} | grep "${WK_FlowId}" | cut -f 7`
      echo ◇　ContactFlowID（${WK_FlowId}）のContactFlow名は（${WK_FlowName}）>>${OUTPUT_FILE1}
      sed -i "s/${WK_FlowId}/${WK_FlowName}/g" ${Work_UserData}
    fi
        
    ############################################
    # クイック接続情報の取得
    ############################################
    cat ${Work_UserData} >> ${Work_OutputData}
    
    ############################################
    # クイック接続データを出力ファイルへ書き込み
    ############################################
    cat ${Work_OutputData} >>${OUTPUT_FILE2}
    
    ############################################
    # カウンターファイル更新
    ############################################
    echo User_CNT=${User_CNT}> ${OUTPUT_FILE4}
    echo Max_CNT=${Max_CNT}>> ${OUTPUT_FILE4}
    
    ############################################
    # クイック接続一覧から1行削除処理
    ############################################
    sed -e '1d' ${OUTPUT_FILE3} > ${Work_UsersList}
    cp -p ${Work_UsersList} ${OUTPUT_FILE3}
    rm -f ${Work_UsersList}
    
    echo ◇　対象クイック接続（${Def_Name}）の処理が終了しました。（${User_CNT}件目）
    echo ◇　対象クイック接続（${Def_Name}）の処理が終了しました。（${User_CNT}件目）>>${OUTPUT_FILE1}
    
  done
  
  echo ◇
  echo ◇　クイック接続の出力を終了しました。
  echo ◇

rm -f ${Work_U_Pro}
rm -f ${Work_R_Pro}
rm -f ${Work_S_Pro}
rm -f ${Work_UserData}
rm -f ${Work_OutputData}
rm -f ${OUTPUT_FILE3}
rm -f ${OUTPUT_FILE4}
#User_END_TIME=`TZ=JST-9 date +"%Y/%m/%d %H:%M:%S"`



echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇
echo ◇
echo ◇　Amazon Connect環境クイック接続出力が完了しました。
echo ◇
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇
echo ◇
echo ◇　クイック接続出力処理
echo ◇　　出力件数　　　　　： ${User_CNT} 件
echo ◇　　総クイック接続数　： ${Max_CNT} 件
echo ◇
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇

echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇ >>${OUTPUT_FILE1}
echo ◇ >>${OUTPUT_FILE1}
echo ◇　クイック接続出力処理 >>${OUTPUT_FILE1}
echo ◇　　出力件数　　　　　： ${User_CNT} 件 >>${OUTPUT_FILE1}
echo ◇　　総クイック接続数　： ${Max_CNT} 件 >>${OUTPUT_FILE1}
echo ◇ >>${OUTPUT_FILE1}
echo ◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇◇ >>${OUTPUT_FILE1}

unset AWS_MAX_ATTEMPTS
exit 0

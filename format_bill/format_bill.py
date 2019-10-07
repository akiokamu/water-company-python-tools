#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
from dateutil.relativedelta import relativedelta
import os
import csv
from dbfpy import dbf

class Constants:
    # SRC_DIR = r'\\BILLING-NARWASS\NRK_Watr\Data'
    # DIST_DIR = r'\\BILLING-NARWASS\NRK_Watr\SMS'
    SRC_DIR = r'/Users/okamuraakinori/prog/python/format_bill/Data'
    DIST_DIR = r'/Users/okamuraakinori/prog/python/format_bill/SMS'
    USE_HEADER = ['VNO','MOBILE','CON','ZONE','NAMES','ARR1','MREAD1','MREAD2','STATUS','CONS','TBILL','ADJ','ARR2']
    PAYBILL = '526560'
    CALLNO = '741878292'



# 最適なヌルデータの除去方法がわからないので、とりあえずSNOがnullのものにする
# こんなのあった　01,03172,ZONE,CON,NAME,,,,VILLA,METER SERIAL,METE,0,0,STAT,0,0,0,0,0,0,0,0,0,0,2014-11-07,,F,,,,,,,0,0,0,0,0,0,0,0,,,,F,0,0,0,0,F,0,0,0,
def remove_nulldata(row):
    if len(row[8]) != 0 and row[3] != "ZONE": 
        return True
    else:
        return False

def normalize_boolean(val):
    if val in [False,True]:
        val = str(val)[0]
    return val

# Add decimal
def format_float(val,length):
    if length < 1:
        return val
    return format(int(val),"." + str(length) + "f")


def format_sms(row,index_array,footer):
    rtn = []
    for i in index_array:
        rtn.append(row[i])
    conzone = rtn[2]+rtn[3]
    rtn2 = [rtn[0],rtn[1],conzone]
    rtn2.extend(rtn[3:])
    rtn2.extend(footer)
    return rtn2


def convert_dbf2csv(dbfpath,csvpath,footer):
    print "Converting %s to csv" % dbfpath
    writingddata = []
    
    in_db = dbf.Dbf(dbfpath)
    decimalCounts = []
    index_array = [0] * len(Constants.USE_HEADER)

    src_index = 0
    for field in in_db.header.fields:
        if field.name in Constants.USE_HEADER:
            index = Constants.USE_HEADER.index(field.name)
            index_array[index] = src_index
        decimalCounts.append(field.decimalCount)
        src_index = src_index + 1

    for rec in in_db:
        row = [ normalize_boolean(x) for x in rec.fieldData ]
        row = [ format_float(row[x],decimalCounts[x]) for x in xrange(len(row)) ]
        row = format_sms(row,index_array,footer)
        if remove_nulldata(row):
            writingddata.append(row)
    in_db.close()


    #並び替える
    writingddata.sort(key=lambda x: x[3]+x[0]+x[2])

    with open(csvpath,'wb') as csvfile:
        out_csv = csv.writer(csvfile)
        for row in writingddata:
            out_csv.writerow(row)


    print " Done..."





def get_paths(target_year,target_month):
    #出力先を確認
    if not os.path.exists(Constants.DIST_DIR):
        os.makedirs(Constants.DIST_DIR)
    #ファイルパスを作成
    target_date = datetime.date(int(target_year),int(target_month),1)
    target_date_str = target_date.strftime("%b%Y").upper()
    target_dbf = target_date_str + ".DBF"
    target_path = os.path.join(Constants.SRC_DIR,target_dbf)
    dist_csv = target_date_str + ".csv"
    dist_path = os.path.join(Constants.DIST_DIR,dist_csv)

    #csv用に対象年月の文字列を変換
    issue_month_str = target_date.strftime('%y-%b')
    #今日の日付+7日が締め切りになるので、”26th Feb”の形式にする
    due_date = datetime.date.today() + relativedelta(days=7)
    due_date_str = due_date.strftime('%dth %b')
    print "Due date -> " + due_date_str
    #末尾用の配列を生成
    footer = [issue_month_str,due_date_str,Constants.PAYBILL,Constants.CALLNO]
    return target_path,dist_path,footer


if __name__ == "__main__":
    #開くファイルを指定させる？
    #変換したい年月を入力させる→ファイルの場所は変わらないので、そのファイルを
    print "Input target year. Like this '2019'"
    target_year = raw_input(">> ")
    print "Input target month. Like this '2"
    print "-----------------------------------------"
    target_month = raw_input(">> ").zfill(2)
    print "Target date -> " +target_month+"-"+ target_year 

    target_path,dist_path,footer = get_paths(target_year,target_month)

    convert_dbf2csv(target_path,dist_path,footer)

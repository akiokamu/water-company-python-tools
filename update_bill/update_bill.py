#!/usr/bin/env python
# -*- coding: utf-8 -*-


# Set TaskScheduler, Create Basic Task


import os
import sys
import csv
import requests
from dbfpy import dbf
import datetime
import shutil
import filecmp
from dateutil.relativedelta import relativedelta


class Constants:
#    UPLOAD_URL = 'http://192.168.1.240:8080/gisapp/rest/BillingSync'
    UPLOAD_URL = 'http://localhost:8080/gisapp/rest/BillingSync'
    DIST_DBF_DIR = r'D:\documents\05_Billing Data\01_Raw Data\Consumption Data'
    DIST_CSV_DIR = r'D:\documents\05_Billing Data\02_CSV Data\Consumption Data'
    SRC_DIR = r'\\BILLING-NARWASS\NRK_Watr\Data'


# @yearmonth "201812"
def upload_csv(csvpath,yearmonth):
    print "Uploading %s , as %s" % ( csvpath, yearmonth )
    payload = { "yearmonth": yearmonth }
    with open(csvpath,"rb") as f:
        files = {'file': (os.path.basename(csvpath), f, 'text/csv')}
        result = requests.post(Constants.UPLOAD_URL, files=files, data=payload)
    print " Upload result %s ..." % result.text


def normalize_boolean(val):
    if val in [False,True]:
        val = str(val)[0]
    return val


# 最適なヌルデータの除去方法がわからないので、とりあえずSNOがnullのものにする
# こんなのあった　01,03172,ZONE,CON,NAME,,,,VILLA,METER SERIAL,METE,0,0,STAT,0,0,0,0,0,0,0,0,0,0,2014-11-07,,F,,,,,,,0,0,0,0,0,0,0,0,,,,F,0,0,0,0,F,0,0,0,
def remove_nulldata(row):
    if len(row[1]) != 0 and row[2] != "ZONE": 
        return True
    else:
        print "removed =>" + str(row)
        return False

# Add decimal
def format_float(val,length):
    if length < 1:
        return val
    return format(int(val),"." + str(length) + "f")


def convert_dbf2csv(dbfpath,csvpath):
    print "Converting %s to csv" % dbfpath
    with open(csvpath,'wb') as csvfile:
        in_db = dbf.Dbf(dbfpath)
        out_csv = csv.writer(csvfile)
        names = []
        decimalCounts = []
        for field in in_db.header.fields:
            names.append(field.name)
            decimalCounts.append(field.decimalCount)
        out_csv.writerow(names)
        for rec in in_db:
            row = [ normalize_boolean(x) for x in rec.fieldData ]
            row = [ format_float(row[x],decimalCounts[x]) for x in xrange(len(row)) ]
            if remove_nulldata(row):
                out_csv.writerow(row)
        in_db.close()
    print " Done..."



def check_billingsystem(src_dir):
    print "Checking %s" % src_dir
    rtnlst = []
    now = datetime.datetime.now()
    last = now - relativedelta(months=1)
    before_last = now - relativedelta(months=2)
    timelst = [now,last,before_last]
    # [ ["2019","01"],["2018","12"],["2018","11"] ]
    ymlstlst = [ [ x.strftime("%Y"), x.strftime("%m") ] for x in timelst ]
    # [ "JAN2019","DEC2018","NOV2018" ]
    MONylst = [ x.strftime("%b%Y").upper() for x in timelst ]
    for n in xrange(len(ymlstlst)):
        ym = ymlstlst[n]
        Mony = MONylst[n]
        dbfname = Mony + ".DBF"
        dbfpath = os.path.join(src_dir,dbfname)
        if os.path.exists(dbfpath) :
            rtnlst.append([ym,dbfname])
    print " Billing System has %s" % rtnlst
    return rtnlst



if __name__ == "__main__":
    # ビリングシステムに何月のデータがあるか確認する
    # 今月と、先月と先々月のみ更新を確認する？
    # そもそも向こうのPCが起動していない状態もある
    # まだ一度もデータがコピーされていないパターン忘れてた
    print "========== "+ str(datetime.datetime.now())+" =========="
    datelist = check_billingsystem(Constants.SRC_DIR)
    for ym,dbfname in datelist:
        print "Processing %s" % dbfname
        # copy dbf from BillingSystem to local
        srcdbfpath = os.path.join(Constants.SRC_DIR,dbfname)
        distdbfpath = os.path.join(Constants.DIST_DBF_DIR,dbfname)
        if os.path.exists(distdbfpath) and filecmp.cmp(srcdbfpath,distdbfpath):
            print " End process : Same files %s" % dbfname
            continue
        shutil.copy(srcdbfpath,distdbfpath)
        # convert dbf to csv
        csvname = ym[0] + "_" + ym[1] + ".csv"
        csvpath = os.path.join(Constants.DIST_CSV_DIR,csvname)
        convert_dbf2csv(distdbfpath,csvpath)
        # upload csv
        yearmonth = ym[0]+ym[1]
        upload_csv(csvpath,yearmonth)



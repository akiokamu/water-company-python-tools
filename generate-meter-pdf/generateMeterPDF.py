#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ① Narok Town
# 1.first generate csv by using psql
# \COPY (select distinct m.zonecd as zone,s.name as route,v.name as village,b.names as customer,m.connno as con,m.serialno,b.mobile from schemearea s,village v,meter m  INNER JOIN (select * from billing_bkup bo where bo.yearmonth = '201904' ) b on m.zonecd = b.zone and m.connno = to_number(b.con,'9999') where ST_intersects(m.geom,v.geom) AND ST_intersects(m.geom,s.geom) order by zone,village,route,con) TO '/Users/okamuraakinori/prog/postgres/meterread_d.csv' WITH CSV DELIMITER E',' FORCE QUOTE * NULL AS '' HEADER;

# ② Ololulunga　ルートのない地域の場合、①からrouteの列を除いたデータを出力する
# \COPY (select distinct m.zonecd as zone, v.name as village,b.names as customer,m.connno as con,m.serialno,b.mobile from village v,meter m  INNER JOIN (select * from billing_bkup bo where bo.yearmonth = '201904' ) b on m.zonecd = b.zone and m.connno = to_number(b.con,'9999') where ST_intersects(m.geom,v.geom) AND v.area = 'Ololulunga'  order by zone,village,con) TO '/Users/okamuraakinori/prog/postgres/noroute_meter.csv' WITH CSV DELIMITER E',' FORCE QUOTE * NULL AS '' HEADER;

import psycopg2

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4, portrait
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.units import mm
from reportlab.platypus import Table
import reportlab.lib.colors as colors

import csv
from collections import defaultdict
import datetime
import pathlib

import os
import configparser


MAXROW = 45


class PSQL():
    CONFIG_ARRAY = ['host','port','dbname','user','password']
    narok_sql = "select distinct m.zonecd as zone,s.name as route,v.name as village,b.names as customer,m.connno as con,m.serialno,b.mobile from schemearea s,village v,meter m  INNER JOIN (select * from billing_bkup bo where bo.yearmonth = '201904' ) b on m.zonecd = b.zone and m.connno = to_number(b.con,'9999') where ST_intersects(m.geom,v.geom) AND ST_intersects(m.geom,s.geom) order by zone,village,route,con"
    ololulunga_sql = "select distinct m.zonecd as zone, v.name as village,b.names as customer,m.connno as con,m.serialno,b.mobile from village v,meter m  INNER JOIN (select * from billing_bkup bo where bo.yearmonth = '201904' ) b on m.zonecd = b.zone and m.connno = to_number(b.con,'9999') where ST_intersects(m.geom,v.geom) AND v.area = 'Ololulunga'  order by zone,village,con"

    @staticmethod
    def load_config():
        inifile = configparser.ConfigParser()
        inifile.read('./psql.ini')
        return ' '.join([k + '=' + inifile.get('PSQL',k) for k in PSQL.CONFIG_ARRAY])

    @staticmethod
    def getSQLData(mode):
        sql = PSQL.narok_sql if mode == "narok" else PSQL.ololulunga_sql
        with psycopg2.connect(PSQL.load_config()) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()


# すべてのデータをルートごとにまとめる
# あるルートの中で最も多いヴィレッジをそのルートの代表として使用
# その後ビレッジごとにまとめる
# ルートごとにデータをまとめて、更にヴィレッジで見れるようにする
# ルート設定してあるところとしてないところで、ルートごとシートか、ヴィレッジごとシートに分ける
class TableCsv():
    def __init__(self,csvpath,iscsv):
        self.csvpath = csvpath
        if iscsv:
            self._parseData(csvpath)
        else:
            self._execSQLData(csvpath)
        self.routelist = TableRoute.loadTableRoutelist(self.rowlist)

    def _parseData(self,csvpath):
        self.rowlist = Row.loadCsv(csvpath)

    def _execSQLData(self,areaflg):
        self.rowlist = Row.loadData(PSQL.getSQLData(areaflg))


    # 各列における最大文字数を数える
    def maxStrCounts(self):
        rtn = [0]*6
        for row in self.rowlist:
            for i in range(6):
                l = len(row.row[i])
                if l > rtn[i]:
                    rtn[i] = l
        return rtn

    def exportPDFALL(self,outpath):
        print("Route Count = " + str(len(tableCsvData.routelist)))
        pathlib.Path(outpath).mkdir(parents=True, exist_ok=True)
        for route in tableCsvData.routelist:
            print(route.route)
            outfilepath = os.path.join(outpath,route.route + ".pdf")
            pdfmanager = PDFManaager(route,outfilepath)
            pdfmanager.export()


class TableRoute():
    def __init__(self,route):
        self.route = route
        self.rowlist = []

    @property
    def village(self):
        if len(self.rowlist) < 1:
            return ""
        else:
            d = defaultdict(int)
            for n in [x.village for x in self.rowlist]:
                d[n] += 1
            return max(d,key=d.get)

    @staticmethod
    def loadTableRoutelist(rowlist):
        rtn = []
        routedict = defaultdict(list)
        for row in rowlist:
            routedict[row.route].append(row)
        for key in routedict:
            tr = TableRoute(key)
            tr.rowlist = routedict[key]
            rtn.append(tr)
        return rtn

# A,AP_R1,AP Line Area,0235,H025511,0723720409
class Row():
    # mode=0 routeの列がある
    # mode=1 routeの列がない 
    def __init__(self,row,mode = 0):
        self.row = row
        self.zone = row[0] # 1
        self.route = row[1] # 12
        self.village = row[2 - mode] # 19
        self.name = row[3 - mode] 
        self.conno = row[4 - mode] # 4
        self.serialno = row[5 - mode] # 12
        self.mobile = row[6 - mode] # 15

    @staticmethod
    def loadCsv(csvpath):
        with open(csvpath,'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            return Row.loadData(reader)

    @staticmethod
    def loadData(data):
        rtn = []
        for line in data:
            mode = 1 if len(line) == 6 else 0
            rtn.append(Row(line,mode))
        return rtn


class PDFwriter():
    TableLeftup = 280
    colWidths = [7 * mm, 50 * mm,12 * mm,6 * mm,30 * mm,35 * mm]

    def __init__(self,filepath):
        self.filepath = filepath
        self.c = self._setPDFcontext(filepath)
        self._setDefaultFont()

    def _setDefaultFont(self):
        self.c.setFont('Futura', 10)

    def _setPDFcontext(self,exportPath):
        pdfmetrics.registerFont(TTFont('Futura', "/Library/Fonts/Futura.ttc"))
        c = canvas.Canvas(exportPath, pagesize = A4)
        return c

    def printHeader(self,data,pageNo,pageAll):
        Title = "<METER READING SHEET>"
        VillageName = data["village"]
        RouteName = data["route"]
        width, height = A4  # A4用紙のサイズ

        self.c.setFont('Futura', 15)
        self.c.drawString(10, height - 15 , Title)
        self._setDefaultFont()
        self.c.drawRightString(width, height - 15,"METER READER: __________________________________")
        self.c.setFont('Futura', 13)
        val = "Village: "+VillageName + "  |  " +"Route: "+RouteName+"  |  " +"Page: "+str(pageNo)+"/"+str(pageAll)
        self.c.drawCentredString(width / 2, height - 30 , val)
        self._setDefaultFont()

    def printFooter(self):
        width, height = A4  # A4用紙のサイズ
        today = datetime.date.today()
        year = str(today.year)
        self.c.setFont('Futura', 8)
        copyrightstr = "(C) "+ year + " Narok Water and Sewerage Services Co., Ltd."
        self.c.drawRightString(width, 0,copyrightstr)
        self.c.drawString(0,0,"Printed: "+str(today))

    #TODO 客の名前欄がはみ出るので、切り落とし処理を入れたい
    def printMainTable(self,data):
        rowHeights = 6
        t = Table(data, colWidths = self.colWidths, rowHeights = rowHeights * mm,  vAlign = 'MIDDLE', hAlign = 'CENTER')
        t.setStyle(
                [
                    ('TEXTCOLOR', (1, 1), (-2, -2), colors.black),
                    ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black), 
                    ('BOX', (0, 0), (-1, -1), 0.25, colors.black), 
                ]
        )
        tableHeight = rowHeights * len(data)
        tablePosition = self.TableLeftup + 5 - tableHeight
        t.wrapOn(self.c, 0, tableHeight * mm) 
        t.drawOn(self.c, 0, tablePosition * mm) 

    # 次のページへ移動する
    def nextPage(self):
        self.c.showPage()

    def exportPDF(self):
        self.c.save()



# Routeごとにこのインスタンスを作成する
# 入ってきたデータを適切に分割して1ファイルのpdfを作成する
class PDFManaager():
    header = ["No","Name","Con","Z","Meter Serial No.","Mobile","Meter Reading","Remarks"]
    # 出力先を設定
    def __init__(self,tableRouteData,outpdfpath):
        self.pdfWriter = PDFwriter(outpdfpath)
        self.tableRouteData = tableRouteData

    #TODO csvから生成する
    # 紙自体のヘッダーデータを生成
    @property
    def headerdata(self):
        #ルート名称とか作成
        headerdata = {"village": self.tableRouteData.village,"route":self.tableRouteData.route}
        return headerdata

    def omit(self,val):
        v = '{:.20}'.format(val)
        if len(val) > 20:
            v += "..."
        return v

    #TODO csvから生成する
    # TODO TableCsvの全体がやってくるので、これを各village, 各ルートごとに出力できるようにここのデータをセットしないといけない
    # ↑　これはその上の役割　ここではtableRouteクラスを扱う
    def setTabledata(self):
        #ここから加工しないと
        rtn = []
        i = 1
        for row in self.tableRouteData.rowlist:
            current = [""]*8
            current[0] = str(i)
            current[1] = self.omit(row.name)
            current[2] = row.conno
            current[3] = row.zone
            current[4] = row.serialno
            current[5] = row.mobile
            rtn.append(current)
            i += 1
        self.tabledata = rtn

    # 最大までとれないことがあるので、その範囲での持ちうる最大数を返す
    def getTableDate(self,pageNo):
        rtn = []
        endindex = pageNo * MAXROW - 1
        maxindex = len(self.tabledata) - 1
        if maxindex >= endindex:
            rtn = self.tabledata[(pageNo - 1) * MAXROW :endindex + 1]
        else:
            rtn = self.tabledata[(pageNo - 1) * MAXROW :maxindex + 1]
        return rtn

    def setheader(self,data):
        data.insert(0,self.header)
        data.append(self.header)
        return data  

    #TODO 複数データに対応したい→その上の役割
    def export(self):
        self.setTabledata()
        l = len(self.tabledata)
        pageAll = l // MAXROW + 1
        if l % MAXROW == 0:
            pageAll -= 1
        for pageNo in range(1,pageAll+1):
            currentTableData = self.setheader(self.getTableDate(pageNo))
            self.pdfWriter.printHeader(self.headerdata,pageNo,pageAll)
            self.pdfWriter.printMainTable(currentTableData)
            self.pdfWriter.printFooter()
            self.pdfWriter.nextPage()
        self.pdfWriter.exportPDF()


if __name__ == "__main__":
#    ORGCSV = "meterread_d.csv"
#    ORGCSV = "noroute_meter.csv"

    print("Which area do you need?")
    print("narok or ololulunga")
    area = input(">>")
    print("Output folder path")
    print("current-> . or specified folder, pdf")
    print(". or pdf")
    outpath = input(">>")

    tableCsvData = TableCsv(area,False)

    tableCsvData.exportPDFALL(outpath)

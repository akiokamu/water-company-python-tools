import os
import csv
from pathlib import Path


# rowindex = 13 # for WTP file
# path = "/Users/okamuraakinori/Documents/協力隊/NARWASCCO/erectronical_data/WTP/DB/DAY"

rowindex = 4 # for WIF file
path = "/Users/okamuraakinori/Documents/協力隊/NARWASCCO/erectronical_data/WIF/DB/DAY"
 


def getcsv(path):
    path_files = Path(path)
    csvlist = sorted(path_files.glob("*_RE*"))
    return list(csvlist)


def datafilter(time,voltage,morning_list,night_list):
    if len(time) < 2:
        return
    if time[1] in ["14:00:00","15:00:00"]:
        morning_list.append(voltage)
    elif time[1] in ["19:00:00","20:00:00"]:
        night_list.append(voltage)


def calc(csvpath):
    with open(csvpath,'r') as csvfile:    
        reader = csv.reader(csvfile)
        morning_list = []
        night_list = []
        t = ""
        for row in reader:
            time = row[0].split(" ")
            voltstr = row[rowindex]
            if not voltstr.isdigit():
                continue
            voltage = int(voltstr)
            t = time[0] if len(time[0]) > 20 or len(time[0]) > len(t) else t
            # if voltage is 0, take it missing.
            if voltage == 0:
                continue
            datafilter(time,voltage,morning_list,night_list)

        # if it could not get eigher timezone, return 0 as a missing value
        if len(morning_list) == 0 or len(night_list) == 0:
            return t,0

        # manipulate each substracted value. get max absolute value.
        maximum = 0
        absmax = 0
        for m in morning_list:
            for n in night_list:
                dif = m - n
                absdif = abs(dif)
                if absdif > absmax:
                    maximum = dif
                    absmax = absdif
        return t,maximum


if __name__ == "__main__":
    csvlist = getcsv(path)
    outlist = []
    for csvfile in csvlist:
        outlist.append(calc(csvfile))
    outpath = os.path.join(path,"summary.csv")
    with open(outpath,"w") as out:
        writer = csv.writer(out)
        writer.writerows(outlist)


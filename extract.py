import os  # provides functions to interact with OS
import cx_Oracle  # Oracle connection
import pandas as pd
import time as clocktime
from threading import Thread, active_count
import math


class extract:

    def __init__(self, countQ, exeQ, thrd, oradb, orapass, orahost, datadirpath, filename):
        self.exeQ = exeQ
        self.countQ = countQ
        self.thrd = thrd
        self.oradb = oradb
        self.orapass = orapass
        self.orahost = orahost
        self.datadirpath = datadirpath
        self.inc_file_nm = filename
        self.totalrows = self.__totalrows__()
        self.cusrsorlist = self.__createcursorlist__()
        self.headers = self.__headers__()
        self.filelist = self.__filelist__()
        self.__mergefiles__()

    def __mergefiles__(self):
        combined_csv = pd.concat([pd.read_csv(f) for f in self.filelist])
        csv_file = open(os.path.join(self.datadirpath, '{}.csv').format(self.inc_file_nm), "w")

        combined_csv.to_csv(csv_file, header=False, index=False, line_terminator='\n')
        csv_file.close()
        for file in self.filelist:
            os.remove(os.path.join(self.datadirpath, '{}.csv').format(file))

    def __headers__(self):
        cur = self.__oraclecursor__()
        cur[0].execute(self.exeQ)
        col_names = [row[0] for row in cur[0].description]
        return col_names

    def __filelist__(self):
        outlist = []
        fileincr = 0
        thread_listone = []
        for cursor in self.cusrsorlist:
            fileincr += 1
            filename = self.inc_file_nm + str(fileincr)
            filetem = os.path.join(self.datadirpath, '{}.csv').format(filename)
            outlist.append(filetem)
            t = Thread(target=self.__threadfetch__, args=(cursor,filetem,))
            thread_listone.append(t)
            t.start()
            while active_count() > self.thrd:  # max thread count (includes parent thread)
                print('\n == Current active threads ==: ' + str(active_count() - 1))
                clocktime.sleep(1)
        for ex in thread_listone:  # wait for all threads to finish
            ex.join()
        return outlist

    def __threadfetch__(self,cursor,filetem):
        csv_file = open(filetem, "w")
        n = 0
        while True:
            n = n + 1
            df = pd.DataFrame(cursor[0].fetchmany(1000))
            if len(df) == 0:
                break
            else:
                df.to_csv(csv_file, header=False, index=False, line_terminator='\n')
        csv_file.close()
        cursor[0].close()
        cursor[1].close()


    def __createcursorlist__(self):
        returnlist = []
        offset = 0
        mainchunks = math.ceil(self.totalrows / self.thrd)
        executedchunks = 0
        while executedchunks != mainchunks:
            print('mainchunk: ' + str(mainchunks))
            print('offset: ' + str(offset))
            print('totalrows: ' + str(self.totalrows))
            print('executedchunks: ' + str(executedchunks))
            executedchunks += 1
            conn = cx_Oracle.connect(self.oradb, self.orapass, self.orahost, encoding="UTF-8")
            cur = conn.cursor()
            print(self.exeQ + ' offset ' + str(offset) + ' rows fetch next ' + str(mainchunks) + ' rows only;')
            cur.execute(self.exeQ + ' offset ' + str(offset) + ' rows fetch next ' + str(mainchunks) + ' rows only')
            returnlist.append([cur, conn])
            if offset < self.totalrows:
                offset = offset + mainchunks
            else:
                offset = self.totalrows
        return returnlist

    def __totalrows__(self):
        ora_connection = cx_Oracle.connect(self.oradb, self.orapass, self.orahost, encoding="UTF-8")
        cur = ora_connection.cursor()
        print('count statement: ' + str(self.countQ))

        cur.execute(self.countQ)
        return (cur.fetchone()[0])

    def __oraclecursor__(self):
        ora_connection = cx_Oracle.connect(self.oradb, self.orapass, self.orahost, encoding="UTF-8")
        return ([ora_connection.cursor(), ora_connection])

### exeQ is the execution statement of query and countQ is the count statement of the exeQ .###
### oradb is oracle db , orapass is oracle password , orahost is oracle db host , datadirpath is the path in which the file has to be saved, filename is the final csv filename
#exeQ = """ select %s from %s  """ % (col_nm, full_tab_nm)
#countQ = """ select count(*) from %s  """ % (full_tab_nm)
#extract(countQ=countQ, exeQ=exeQ, thrd=4, oradb=oradb, orapass=orapass, orahost=orahost, datadirpath=datadirpath, filename=filename)



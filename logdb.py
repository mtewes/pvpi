import sqlite3
import csv
from datetime import datetime, timedelta
import os

import logging
logger = logging.getLogger(__name__)

class LogDB:
    def __init__(self, name="test", path=None, cols=None):
        """
        cols is a list of column names to store the values.
        """
        self.name = name

        self.path = path
        if path is None:
            self.path = "/home/pi/Databases/{}.db".format(name)

        self.cols = cols
        #if cols is None:
        #    self.cols = ["temp", "hum"]

        self.create()
    
    def __str__(self):
        return "Table {} at {} with cols {}".format(self.name, self.path, self.cols)

    def create(self):

        self.con = sqlite3.connect(self.path)  #, check_same_thread=False)
        self.cur = self.con.cursor()
    
        if self.cols is not None:
            cmd = "CREATE TABLE IF NOT EXISTS {}(datetime, {})".format(self.name, ",".join(self.cols))
            self.cur.execute(cmd)
            self.con.commit()
    
        #self.con.close()

        logger.info("Connected to table {}".format(str(self)))


    def log(self, d):
        """
        insert dict d data with datetime and commit.
        Only 
        """
        #con = sqlite3.connect(self.path)
        #cur = con.cursor()
        if self.cols is None:
            raise RuntimeError("Cannot log without specifying cols!")
            
        placeholder = ", ".join(["?" for c in self.cols])
        cmd = "INSERT INTO {} values(datetime('now'), {})".format(self.name, placeholder)
        self.cur.execute(cmd, [d[k] for k in self.cols])
        self.con.commit()
        #con.close()

    def print(self):
        #self.con = sqlite3.connect(self.path)
        #self.cur = con.cursor()
        for row in self.cur.execute("SELECT * FROM {}".format(self.name)):
            print(row)
        #con.close()

    def export_last_day(self, workdir="/home/pi/Databases/", testmode=False):
        """
        export, close, reopen, clean old inputs ?
        """

        # Date of yesterday for filename:
        yesterday = datetime.now() - timedelta(days=1)
        
        dbdirname = self.name
        yeardirname = yesterday.strftime('%Y')
        
        dbdir = os.path.join(workdir, dbdirname, yeardirname)
        os.makedirs(dbdir, exist_ok=True)
        
        filename = yesterday.strftime('%Y-%m-%d') + ".csv"
        filepath = os.path.join(dbdir, filename)
        logger.info("Exporting {} to {}...".format(self.name, filepath))

        cmd = """SELECT * FROM {} WHERE 
        datetime > DATETIME('NOW', 'start of day', '-1 day') 
        and 
        datetime < DATETIME('NOW', 'start of day') 
        ORDER BY datetime""".format(self.name)

        if testmode:
            cmd = """SELECT * FROM {} WHERE 
            datetime > DATETIME('NOW', 'start of day', '-1 day')  
            ORDER BY datetime""".format(self.name)

        self.cur.execute(cmd)

        with open(filepath, 'w') as csv_file: # We intentionally overwrite, as we might have new data
            csv_writer = csv.writer(csv_file, delimiter="\t")
            csv_writer.writerow([i[0] for i in self.cur.description]) 
            csv_writer.writerows(self.cur)

    def delete_old(self):

        logger.info("Deleting old entries from {} ...".format(self.name))

        cmd = "DELETE FROM {} WHERE datetime < DATETIME('NOW', 'start of day', '-7 day')".format(self.name)
        self.cur.execute(cmd)
        self.con.commit()

    def close(self):
        self.con.close()
        logger.info("Closed connection to {}.".format(self.name))


        
        

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    
    ldb = LogDB(name="ola", path=":memory:", cols=("a", "b"))

    data = {"a":23.4, "b":56.2}
    ldb.log(data)
    data["a"] = 444.5
    ldb.log(data)
    ldb.print()
    
    ltest = LogDB(name="S1", path=":memory:", cols=("temp", "hum", "gas"))


"""
import logging
import clock.logdb

logging.basicConfig(level=logging.DEBUG)


ldb1 = clock.logdb.LogDB(name="S1", path=":memory:", cols=("temp", "hum"))
ldb2 = clock.logdb.LogDB(name="S2", path=":memory:", cols=("temp", "hum", "inttemp"))


d = {"temp":0.0, "hum":45, "inttemp":23.0}
ldb1.log(d)
#ldb2.log(d)
ldb1.log(d)


print("S1:")
ldb1.print()
print("S2:")
ldb2.print()

ldb1.export_last_day(workdir="./test")
ldb2.export_last_day(workdir="./test")


ldb1.delete_old()



#ldb.print()


"""
    


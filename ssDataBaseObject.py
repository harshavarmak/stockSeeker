import sqlite3 as sl

class dbManager:
    """
        A class that holds data and methods to create/manage a connection to a db
    
        Attributes
        ----------
        dbName: string
            Name of the database
        dbConn: string
            Connection to database

        Methods
        -------
        checkAndConnect(self)
            Checks if given db name in constructor exists. If exists, connects to it
        closeDB()
            Closes the connection to dbName given in constructor
    """
    def __init__(self, dbName):
        self.dbName = dbName
        self.dbConn = None
    
    def checkAndConnect(self):
        try:
            self.dbConn = sl.connect(self.dbName)
            with self.dbConn as con:
                dbtSSD = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SubRedditStockData';")
                try:
                    if(len(dbtSSD.fetchone()) > 0):
                        print('Table exists, not creating')
                        pass
                    else:
                        self.generateInitialSchema()
                except TypeError as te:
                    print('Type error occured.', te)
                    self.generateInitialSchema()
            return True
        except sl.OperationalError as slErr:
            print('Local DB doesn\'t exist, creating it.', slErr)
            return False
    
    def closeDB(self):
        if(self.dbConn is not None):
            self.dbConn.close()
            
            print(f'Connection to {self.dbName} closed.')
        else:
            print(f'{self.dbName} connection was never created. Nothing to close')
    
    def generateInitialSchema(self):
        if(self.dbConn is not None):
            with self.dbConn as con:
                con.execute("CREATE TABLE SubRedditStockData (id TEXT NOT NULL PRIMARY KEY, sr TEXT, title TEXT, selftext TEXT, permalink TEXT, created TEXT, insertedAt TEXT, UNIQUE(id));")
                con.execute("CREATE TABLE SubRedditPostComments (id TEXT NOT NULL PRIMARY KEY, parent TEXT, commentbody TEXT, insertedAt TEXT, UNIQUE(id));")
                con.execute("CREATE TABLE Stocks (id TEXT NOT NULL PRIMARY KEY, counter INTEGER);")
                con.execute("CREATE TABLE StocksDaily (id TEXT NOT NULL PRIMARY KEY, stockCounters INTEGER);")
                con.execute("CREATE TABLE StockOnTheDay (id TEXT NOT NULL PRIMARY KEY, stockCounters INTEGER);")
                con.execute("CREATE TABLE StockOnTheQuarter (id TEXT NOT NULL PRIMARY KEY, stockCounters INTEGER);")
                print('Completed Initial Database Creation')
            return True
        else:
            return False

    def getTableSchema(self, table):
        if(table == 'SubRedditStockData'):
            return '(id, sr, title, selftext, permalink, created, insertedAt)'
        if(table == 'SubRedditPostComments'):
            return '(id, parent, commentbody, insertedAt)'
        if(table == 'Stocks'):
            return '(id, counter)'
        if(table == 'StocksDaily'):
            return '(id, stockCounters)'
        if(table == 'StockOnTheDay'):
            return '(id, stockCounters)'
        if(table == 'StockOnTheQuarter'):
            return '(id, stockCounters)'
        

    def insertData(self, table, data):
        if(self.dbConn is not None):
            with self.dbConn as con:
                insertQuery = f'INSERT OR IGNORE INTO {table} {self.getTableSchema(table)} VALUES ({", ".join(str("?") for x in data)})'
                con.execute(insertQuery, data)
    
    def readData(self, table):
        if(self.dbConn is not None):
            with self.dbConn as con:
                selectQuery = f'SELECT * FROM {table}'
                cur = con.cursor()
                cur.execute(selectQuery)
                data = cur.fetchall()
                return data

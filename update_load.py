import sqlite3



db = sqlite3.connect('earthquake.db')
#USED SQLITE FOR PORTABILITY

sql = "PRAGMA foreign_keys=1;"
cursor = db.cursor()
cursor.execute(sql)
db.commit()


def populatemaster(df):    
    sql = """
    CREATE TABLE IF NOT EXISTS EVENTS_MASTER(
        EVENT_ID TEXT NOT NULL PRIMARY KEY,
        MAGNITUDE REAL,
        MAGNITUDE_TYPE TEXT,
        MAX_INTENSITY REAL,
        FELT INTEGER,
        ALERT TEXT,
        MAX_EST_INTENSITY TEXT,
        SIGNIFICANCE INTEGER,
        TSUNAMI INTEGER,
        STATUS TEXT
        );
    """
        
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()

    for i, r in df.iterrows():
        sql = """INSERT OR IGNORE INTO EVENTS_MASTER(EVENT_ID,MAGNITUDE,MAGNITUDE_TYPE,MAX_INTENSITY,FELT,ALERT,MAX_EST_INTENSITY,SIGNIFICANCE,TSUNAMI,STATUS)
			VALUES(?,?,?,?,?,?,?,?,?,?);"""
        var = (r['ids'],r['mag'],r['magType'],r['cdi'],r['felt'],
                r['alert'],r['mmi'],r['sig'],r['tsunami'],r['status'])
        cursor.execute(sql,var)
        db.commit()
    
    
    

def populatedatetime(df):
    sql = """
    CREATE TABLE IF NOT EXISTS DATETIME_TEXT(
        ID TEXT NOT NULL,
        EVENT_TIME TEXT,
        EVENT_HOUR INTEGER,
        EVENT_DATE TEXT,
        UPDATE_TIME TEXT,
        FOREIGN KEY (ID)
            REFERENCES EVENTS_MASTER (EVENT_ID)
        );
    """
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    for i, r in df.iterrows():
        sql = """INSERT OR IGNORE INTO DATETIME_TEXT(ID,EVENT_TIME,EVENT_HOUR,EVENT_DATE,UPDATE_TIME)
                        VALUES(?,?,?,?,?);"""
        var = (r['ids'],str(r['event_time']),r['event_hour'],str(r['event_date']),str(r['updated']))
        cursor.execute(sql,var)
        db.commit()


def populatedes(df):
    sql = """
    CREATE TABLE IF NOT EXISTS DESCRIPTION(
        ID TEXT NOT NULL,
        TITLE TEXT,
        EVENT_TYPE TEXT NOT NULL,
        FOREIGN KEY (ID)
            REFERENCES EVENTS_MASTER (EVENT_ID)
        );
    """
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    for i, r in df.iterrows():
        sql = """INSERT OR IGNORE INTO DESCRIPTION(ID,TITLE,EVENT_TYPE)
                        VALUES(?,?,?);"""
        var = (r['ids'],r['title'],r['event_type'])
        cursor.execute(sql,var)
        db.commit()

def populateloc(df):
    sql = """
    CREATE TABLE IF NOT EXISTS LOCATION(
        ID TEXT NOT NULL,
        LATITUDE REAL,
        LONGITUDE REAL,
        DEPTH REAL,
        PLACE TEXT,
        TIMEZONE TEXT,
        GAP REAL,
        ROOT_MEAN_SQ REAL,
        NEAREST_STA_DIST REAL,
        FOREIGN KEY (ID)
            REFERENCES EVENTS_MASTER (EVENT_ID)
        );
    """
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    for i, r in df.iterrows():
        sql = """INSERT OR IGNORE INTO LOCATION(ID,LATITUDE,LONGITUDE,DEPTH,PLACE,TIMEZONE,GAP,ROOT_MEAN_SQ,NEAREST_STA_DIST)
                        VALUES(?,?,?,?,?,?,?,?,?);"""
        var = (r['ids'],r['LAT'],r['LONG'],r['DEPTH'],r['place'],r['tz'],r['gap'],r['rms'],r['dmin'])
        cursor.execute(sql,var)
        db.commit()

        
def populateprod(df):
    sql = """
    CREATE TABLE IF NOT EXISTS PRODUCTTYPES(
        ID TEXT NOT NULL,
        TYPES TEXT,
        PRIMARY KEY(ID,TYPES),
        FOREIGN KEY (ID)
            REFERENCES EVENTS_MASTER (EVENT_ID)
        );
    """
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    for i, r in df.iterrows():
        sql = """INSERT OR IGNORE INTO PRODUCTTYPES(ID,TYPES)
                        VALUES(?,?);"""
        var = (r['ids'],r['types'])
        cursor.execute(sql,var)
        db.commit()
        

def populatenet(df):
    sql = """
    CREATE TABLE IF NOT EXISTS NETWORK(
        ID TEXT NOT NULL,
        NETWORK_ID TEXT,
        STATION_NUMS INTEGER,
        PREFERRED TEXT,
        PRIMARY KEY(ID,NETWORK_ID),
        FOREIGN KEY (ID)
            REFERENCES EVENTS_MASTER (EVENT_ID)
        );
    """
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    for i, r in df.iterrows():
        sql = """INSERT OR IGNORE INTO NETWORK(ID,NETWORK_ID,STATION_NUMS,PREFERRED)
                        VALUES(?,?,?,?);"""
        var = (r['ids'],r['sources'],r['nst'],r['Preferred'])
        cursor.execute(sql,var)
        db.commit()

def populateurls(df):
    sql = """
    CREATE TABLE IF NOT EXISTS URLS(
        ID TEXT NOT NULL,
        URL TEXT UNIQUE,
        DETAIL TEXT UNIQUE,
        FOREIGN KEY (ID)
            REFERENCES EVENTS_MASTER (EVENT_ID)
        );
    """
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    for i, r in df.iterrows():
        sql = """INSERT OR IGNORE INTO URLS(ID,URL,DETAIL)
                        VALUES(?,?,?);"""
        var = (r['ids'],r['url'],r['detail'])
        cursor.execute(sql,var)
        db.commit()

import pandas as pd
import sqlite3

db = sqlite3.connect('earthquake.db')

#biggest earthquake of 2017
#assuming biggest means highest magnitude
sql_biggest = """SELECT * FROM 
                    (SELECT EVENTS_MASTER.*, DESCRIPTION.*,
                    DATETIME_TEXT.* ,
                    RANK() OVER (ORDER BY MAGNITUDE DESC) AS MYRANK
                    FROM EVENTS_MASTER, DESCRIPTION, DATETIME_TEXT
                    WHERE EVENTS_MASTER.EVENT_ID = DESCRIPTION.ID
                    AND EVENTS_MASTER.EVENT_ID = DATETIME_TEXT.ID
                    AND lower(DESCRIPTION.EVENT_TYPE) = 'earthquake') TEMP
                WHERE TEMP.MYRANK = 1
                """

Sql_probable_hour = """
                    SELECT  MAG_BUCKET,EVENT_HOUR,
                    RANK() OVER (PARTITION BY MAG_BUCKET, EVENT_HOUR
                                ORDER BY COUNT(DISTINCT EVENT_ID) DESC) AS MYRANK
                    FROM
                        (SELECT EVENTS_MASTER.EVENT_ID,
                        CASE 
                            WHEN MAGNITUDE >= 0 AND MAGNITUDE < 1 THEN "0-1"
                            WHEN MAGNITUDE >= 1 AND MAGNITUDE < 2 THEN "1-2"
                            WHEN MAGNITUDE >= 2 AND MAGNITUDE < 3 THEN "2-3"
                            WHEN MAGNITUDE >= 3 AND MAGNITUDE < 4 THEN "3-4"
                            WHEN MAGNITUDE >= 4 AND MAGNITUDE < 5 THEN "4-5"
                            WHEN MAGNITUDE >= 5 AND MAGNITUDE < 6 THEN "5-6"
                            WHEN MAGNITUDE >= 6 THEN ">6"
                            END AS MAG_BUCKET
                        DATETIME_TEXT.EVENT_HOUR 
                        FROM EVENTS_MASTER, DATETIME_TEXT
                        WHERE EVENTS_MASTER.EVENT_ID = DATETIME_TEXT.ID ) TEMP 
                    WHERE TEMP.MYRANK = 1
                    ORDER BY MAG_BUCKET                 
                    """

                

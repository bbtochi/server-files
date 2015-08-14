import ibm_db
from datetime import datetime
sqls = []

# connect to database
database = ("DATABASE=I8087215;HOSTNAME=192.155.240.174;PORT=50000;"
            "PROTOCOL=TCPIP;UID=gqxbnfjt;PWD=byrkrr79aie9;")
conn = ibm_db.connect( database, "gqxbnfjt", "byrkrr79aie9" )


# CREATE STATEMENTS
sql_create_tweet = ("CREATE TABLE tweet_temp (ID int not null GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),"
            " user varchar(255), tweet varchar(255), date varchar(255), time varchar(255))")

sql_create_feed = ("CREATE TABLE feed (ID int not null GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),"
            " user varchar(255), from varchar(255), to varchar(255), comment varchar(255), congestion float not null, date varchar(255), time varchar(255))")
sqls.append(sql_create_feed)

sql_create_live = ("CREATE TABLE live (ID int not null GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),"
            " from varchar(255), to varchar(255), congestion float, est_time varchar(255), est_density varchar(255), last_update varchar(255))")
sqls.append(sql_create_live)

sql_create_locations = ("CREATE TABLE locations (ID int not null GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),"
            " user varchar(255), address varchar(255), lat float, long float, date varchar(255), time varchar(255))")

sql_create_historical = ("CREATE TABLE historical (ID int not null GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),"
            " from varchar(255), to varchar(255), day varchar(255), start_time varchar(255), end_time varchar(255)"
            ", density float not null, speed float not null)")

sql_create_test = ("CREATE TABLE test (ID int not null GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),"
            " user varchar(255), comment varchar(255), float float not null, time timestamp not null generated always for each row on update as row change timestamp)")

sql_create_alt = sql_create_feed = ("CREATE TABLE alt (ID int not null GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),"
            " user varchar(255), from varchar(255), to varchar(255), comment varchar(255), congestion float not null, reportinglocation varchar(255), timestamp timestamp not null generated always for each row on update as row change timestamp)")

# ALTER STATEMENTS
sql_alter_live = ("ALTER TABLE GQXBNFJT.live add COLUMN percentage int")
sql_update_live = "UPDATE gqxbnfjt.live SET congestion=1.0 WHERE id=1"
sql_alter_tweet = ("ALTER TABLE GQXBNFJT.tweet_temp add COLUMN last_update "
                "timestamp not null generated always for each row on update as row change timestamp")


# INSERT STATEMENTS
sql_insert_live = ("INSERT INTO GQXBNFJT.live (from, to) VALUES ('a','b')")
sql_insert_feed = ("INSERT INTO GQXBNFJT.feed (from, to) VALUES ('A','B')")
sql_insert_test = ("INSERT INTO GQXBNFJT.test (user, comment, float, time) VALUES ('@jamie', 'It is about to get cray', 2.3, %s)")%("'"+str(datetime.now())+"'")


# SELECT STATEMENTS
sql_select_historical = "SELECT * FROM GQXBNFJT.historical WHERE day='Tuesday'"
sql_select_feed = "SELECT * FROM GQXBNFJT.feed"


# DROP STATEMENTS
sql_drop = "DROP TABLE GQXBNFJT.test"


# DELETE STATEMENTS
sql_delete = "DELETE FROM GQXBNFJT.feed where from='200 Market Street'"


# REORG STATEMENTS
sql_reorg = '''CALL SYSPROC.ADMIN_CMD('REORG TABLE "%(sName)s"."%(tName)s"')''' % {'sName': 'GQXBNFJT', 'tName': 'LIVE'}


# EXECUTION
stmt = ibm_db.exec_immediate(conn, sql_reorg)
# dictionary = ibm_db.fetch_both(stmt)
# i = 0
# while dictionary != False:
#     dictionary = ibm_db.fetch_both(stmt)
#     i +=1
#     print i


print "done"

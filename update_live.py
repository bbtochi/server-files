"""
This program keeps the live table in our db updated with current traffic congestion info
inferred from twitter and user data.

THOUGHTS
* Consider different time periods depending on the current congestion rating in live table
    i.e if live table says A-B is congested consider user ratings in the past 30 minutes
        else if light traffic only consider past 10 minutes
* Give weights to the congestion ratings. More recent ones would have a higher weight

"""


from datetime import datetime, timedelta
import ibm_db
import math

# write log data to a file
out_file = "log.txt"
with open(out_file, 'w') as out_f:
    note = "START TIME: "+str(datetime.now())+"\n"
    out_f.write(note)

# dict to hold routes, the sum of their user sourced congestion ratings, and number of users to give feedback
routes = {}
day = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# connect to database
database = ("DATABASE=I8087215;HOSTNAME=192.155.240.174;PORT=50000;"
            "PROTOCOL=TCPIP;UID=gqxbnfjt;PWD=byrkrr79aie9;")
conn = ibm_db.connect( database, "gqxbnfjt", "byrkrr79aie9" )

# sql statements
sql_select_feed = ("SELECT * FROM gqxbnfjt.feed")
sql_update_live = ("UPDATE gqxbnfjt.live SET congestion=?, est_time=?, est_density=?, cong_resp=?, tot_resp=?, percentage=? WHERE from=? and to=?")
sql_select_historical = ("SELECT * FROM gqxbnfjt.historical WHERE from=? and to=? and day=? and start_time=?")

# function to bind multiple parameters to one sql statement
def bind(stmt, num_params, params):
    for i in range(1,num_params+1):
        ibm_db.bind_param(stmt,i,params[i-1])

print
print "GOING THROUGH POSTS IN THE PAST HOUR..."

# Go through all the data from the feed table (in the past 60 mins)
res = ibm_db.exec_immediate(conn, sql_select_feed)
dictionary = ibm_db.fetch_assoc(res)
while dictionary != False:
    post_time = dictionary['TIMESTAMP']
    lastHourDateTime = datetime.now() - timedelta(hours = 1)

    # check if entry was within the last hour
    if (post_time > lastHourDateTime):
        route = (dictionary['FROM'],dictionary['TO'])
        if route not in routes:
            # get estimated time and density from historical data
            stmt_hist = ibm_db.prepare(conn, sql_select_historical)
            params = [route[0], route[1], day[post_time.weekday()], str(post_time.hour)+':00']
            bind(stmt_hist, 4, params)
            ibm_db.execute(stmt_hist)
            dct = ibm_db.fetch_assoc(stmt_hist)
            est_time, density = None, None
            while dct != False:
                est_time, density = str(float(dct['EST_TIME'])), str(float(dct['DENSITY']))
                dct = ibm_db.fetch_assoc(stmt_hist)

            routes[route] = {
                                'tot_resp': 1,
                                'cong_resp': -1,
                                'percentage': 0,
                                'ratings_sum': dictionary['CONGESTION'],
                                'est_time': est_time,
                                'density': density,
                                '0': 0, '1': 0, '2': 0, '3': 0, '4': 0}
            routes[route][str(int(dictionary['CONGESTION']))] = 1
        else:
            routes[route]['tot_resp']+=1
            routes[route]['ratings_sum']+=dictionary['CONGESTION']
            routes[route][str(int(dictionary['CONGESTION']))]+=1
    dictionary = ibm_db.fetch_both(res)

print "GOT ALL POSTS IN LAST HOUR"
print routes

print
print "CALCULATING AND UPDATING LIVE TRAFFIC CONGESTION INFO..."
# calculate averages for all routes and update live table (and maybe some other count table)
for route in routes:
    congestion_av = routes[route]['ratings_sum']/float(routes[route]['tot_resp'])
    print "AVERAGE FOR", (route), ":"
    print congestion_av
    cong = int(round(congestion_av))
    cong_resp, tot_resp = routes[route][str(cong)], routes[route]['tot_resp']
    p = float(cong_resp)/float(tot_resp)*100.
    percentage = int(round(p))

    stmt_live = ibm_db.prepare(conn, sql_update_live)
    params = [cong, routes[route]['est_time'], routes[route]['density'], cong_resp, tot_resp, percentage, route[0], route[1]]
    print "PARAMS:", params
    bind(stmt_live, 8, params)
    ibm_db.execute(stmt_live)
    print
print "DONE"

with open(out_file, 'a') as out_f:
    note = "END TIME: "+str(datetime.now())+"\n"
    out_f.write(note)

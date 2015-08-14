"""
* READ IN DATA WITH SPEED ESTIMATES FOR EACH HOUR BLOCK
* PREDICT SPEED INFO FOR EACH ROUTE ON SPECIFIC DAYS BY TAKING SIMPLE AVERAGES
* WRITE RESULTS TO HISTORICAL TRAFFIC DATA TABLE, AND UPLOAD ROUTES TO LIVE TABLE
"""

"""
"""

import csv
import json
import ibm_db


in_file = "speed_estimates"
routes = {}
week = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
limit = 90
absurd = 0

# given metric distance as string, returns distance in miles
def truedist(d):
    dst = d.split(" ")
    val, unit = float(dst[0]), dst[1]
    # convert to miles
    if unit == 'm':
        return val*0.000621371
    else:
        return val*0.621371

print "READING IN DATA..."
# read in data
with open(in_file, 'r') as in_f:
    # parse as csv file
    in_csv = csv.reader(in_f, delimiter=';', quotechar='"')

    # skip header
    next(in_csv,None)

    for entry in in_csv:
        # assign some useful entry info to variables
        route, dr, dst, dt, day = entry[1]+" TO "+entry[2], entry[3], entry[4], entry[5], entry[6]

        # update speed info for specific day
        i,j = 24,1
        while j != 25:
            time = str(i)+':00-'+str(j)+':00'
            spd_dty = entry[j+6].split(',')
            spd_dty = [spd_dty[0][1:],spd_dty[1][:-1]]

            speed = float(spd_dty[0])
            density = float(spd_dty[1])
            dst_float = truedist(dst)

            # check route has been seen
            if route not in routes:
                routes[route] =  {"distance": dst, "direction": dr, day: {}}
                routes[route][day][time] = {}
                routes[route][day][time]["est_time"] = (dst_float/speed)*60
                routes[route][day][time]["density"] = density
                routes[route][day]["count"] = 1.0
            # check if day has been seen for route
            elif day not in routes[route]:
                routes[route][day] = {"count": 1.0, time: {"est_time": (dst_float/speed)*60, "density": density}}
            # check if time has been seen on this day of week for this route
            elif time not in routes[route][day]:
                routes[route][day][time] = {}
                routes[route][day][time]["est_time"] = (dst_float/speed)*60
                routes[route][day][time]["density"] = density
            else:
                routes[route][day][time]["est_time"]+= (dst_float/speed)*60
                routes[route][day][time]["density"]+= density
            i=j
            j+=1

        routes[route][day]["count"]+= 1.0

# CALCULATE AVERAGES PER DAY
for route in routes:
    for day in routes[route]:
        if day != "distance" and day != "direction":
            for time in routes[route][day]:
                c = routes[route][day]["count"]
                if time!="count":
                    routes[route][day][time]["est_time"] /= c
                    routes[route][day][time]["density"] /= c


print "DONE READING IN DATA!"
print
print "NOW WRITING RESULTS TO DATABASE..."

# connect to database
database = ("DATABASE=I8087215;HOSTNAME=192.155.240.174;PORT=50000;"
            "PROTOCOL=TCPIP;UID=gqxbnfjt;PWD=byrkrr79aie9;")
conn = ibm_db.connect( database, "gqxbnfjt", "byrkrr79aie9" )

# prepare sql statements
sql_insert_historical = ("INSERT INTO GQXBNFJT.historical (from, to, day, start_time, end_time, density, distance, est_time) VALUES"
                " (?,?,?,?,?,?,?,?)")
sql_insert_live = ("INSERT INTO GQXBNFJT.live (from, to) VALUES (?,?)")

print "  preparing statements..."
stmt_hist = ibm_db.prepare(conn,sql_insert_historical)
stmt_live = ibm_db.prepare(conn,sql_insert_live)
print "  statements prepared!"
print

def bind(stmt,num_params,params):
    for i in range(1,num_params+1):
        ibm_db.bind_param(stmt,i,params[i-1])

print "going through all routes..."
for route in routes:
    print "Writing data for: ", route, "..."
    from_to = route.split(" TO ")
    # bind(stmt_live,2,params)
    # ibm_db.execute(stmt_live)
    # print "  successfully wrote to live table"

    for day in routes[route]:
        if day != "distance" and day != "direction":
            for time in routes[route][day]:
                if time!="count":
                    tm = time.split("-")
                    start = tm[0]
                    stop = tm[1]
                    density = routes[route][day][time]['density']
                    est_time = routes[route][day][time]['est_time']
                    distance = routes[route]["distance"]

                    # bind parameters to sql statement
                    params = from_to+[day,start,stop,density,distance,est_time]
                    bind(stmt_hist,8,params)
                    ibm_db.execute(stmt_hist)
    print "  successfully wrote to historical table"
    print


print "DONE WRITING RESULTS!"

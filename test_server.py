"""
* This file tests the functionality of our web server
* MAKE POSTS TO SERVER
* MAKE GET REQUESTS
"""
import requests
import random as rand
import urllib2
import simplejson
# important variables
usernames = ['@jamie', '@jessica', '@anjola', '@arakua', '@wale', '@jumai', '@nana', '@selorm']
routes = [('6801 4th Avenue, Brooklyn, NY 11220, USA', '400 Senator Street, Brooklyn, NY 11220, USA')]
routes.append(('6001 18th Avenue, Brooklyn, NY 11204, USA','5924 19th Avenue, Brooklyn, NY 11204, USA'))
routes.append(('81 Prince Street, New York, NY 10012, USA','528 Broadway, New York, NY 10012, USA'))
routes.append(('4 West 4th Street, New York, NY 10012, USA','388 Lafayette Street, New York, NY 10012, USA'))
routes.append(('433 1st Avenue, New York, NY 10010, USA','458 1st Avenue, New York, NY 10016, USA'))
lat, lng = 123.2222, 321.2222

# comments
closed = ["Couldn't get through, must be closed today.", "Looks like the road's closed today", "Closed! Don't take this route."]
no_traffic = ["Soo freeeee!", "I literally have never seen traffic this good here", "I love public holidays! Zero traffic here :)"]
moderate = ["Traffic is moving slowly but it's better than usual", "I've seen slower, this is moderate really", "Could be worse. Only took me 5 minutes to get through"]
heavy = ["Arrggh Don't come down this route!", "I've been stuck in the same spot for 8 minutes and counting.", "You shall not pass!!"]
stopngo = ["Stop and go traffic here. #theworst", "Trying to get home to watch the game and this stop and go traffic is driving me crazy!", "Moving like snails here!"]
comments = [closed, no_traffic, moderate, heavy, stopngo]

# prompt for number of posts to generate

print "get or post?"
req_type = (raw_input())

if req_type == "post":
    print "HOW MANY POSTS TO GENERATE:"
    num = int(raw_input())
    print "HOW MANY DISTINCT ROUTES TO CONSIDER (1-5):"
    uniq = int(raw_input())
    print "CONGESTION RATING DESIRED (0-4, 5 IF NONE):"
    cong = int(raw_input())

    print
    print "Great! Preparing posts..."
    rand_key = 0
    for i in range(num):
        print "  Post",i
        print "    generating params..."
        username = usernames[rand.randint(0,len(usernames)-1)]
        if uniq > 1:
            rand_key = rand.randint(0,uniq-1)
        origin, destination = routes[rand_key][0], routes[rand_key][1]
        if cong == 5:
            congestion = rand.randint(1,4)
        elif cong == 0:
            congestion = 0
        elif cong == 1:
            congestion = rand.randint(1,2)
        elif cong == 4:
            congestion = rand.randint(3,4)
        else:
            congestion = rand.randint(cong-1,cong+1)
        comment = comments[congestion][rand.randint(0,len(comments[congestion])-1)]

        post = {'username': username,
                'from': origin,
                'to': destination,
                'latitude': lat,
                'longitude': lng,
                'reporting_address': origin,
                'congestion_rating': congestion,
                'comment': comment}
        print post
        print "posting..."
        res = requests.post("http://localhost:1337/", data=post)
        print "posted! Status:", res.status_code, res.reason

elif req_type == "get":
    print
    print "specific or general?"
    feed = raw_input()
    url = "http://localhost:1337/?feed="
    if feed == "general":
        data = urllib2.urlopen(url+feed).read()
        response = simplejson.loads(data)
        print "FIRST 5 POSTS: "
        i = 0
        for post in response["posts"][:5]:
            print i, post
            print
            i+=1
        print "DETAILS: "
        print response["details"]
    elif feed == "specific":
        print
        print "OK, enter a route (FORMAT: from A to B is A->B ) or enter rand for a random one"
        route = raw_input()
        if route == "rand":
            route = routes[rand.randint(0,4)]
            print "RANDOM ROUTE: "
            print route
            print
        else:
            route = (route).split('->')
            print "ROUTE: ", route

        org, dest = route[0].replace(' ', '+'), route[1].replace(' ', '+')
        url = url+feed+'&from='+org+'&to='+dest
        data = urllib2.urlopen(url).read()
        response = simplejson.loads(data)
        print "POSTS: "
        i = 0
        for post in response["posts"]:
            print i, post
            print
            i+=1
        print "DETAILS: "
        print response["details"]
else:
    print "invalid response, start again loser :P"

print
print "DONE!"

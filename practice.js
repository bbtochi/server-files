// REQUIRE NECESSARY MODULES
var express = require('express');
var bodyParser = require('body-parser');
var multer = require('multer');
var ibmdb = require('ibm_db');
var http = require('http');

// CREATE APP
var app = express();

// configure app to be able to parse the body of a url request
app.use(bodyParser.json()); // for parsing application/json
app.use(bodyParser.urlencoded({ extended: true })); // for parsing application/x-www-form-urlencoded
app.use(multer().array()); // for parsing multipart/form-data

// Database credentials
credentials = "DRIVER={DB2};DATABASE=I8087215;HOSTNAME=192.155.240.174;UID=gqxbnfjt;PWD=byrkrr79aie9;PORT=50000;PROTOCOL=TCPIP"

// function to convert capitalized keys returned from db to lowercase
function decap(data) {
  for (var elt in data) {
    dict = data[elt];
    for (var key in dict) {
      if (dict.hasOwnProperty(key)) {
        var new_key = key.toLowerCase();
        dict[new_key] = dict[key];
        delete dict[key];
      }
    }
  }
  return data;
}

// function to get the street of an address in the googleapi address format
function get_street(address) {
  var num_street = (address.split(',')[0]).split(" ");
  var street = "";
  for (var i in num_street) {
    if (i !=0) {
      street = street+num_street[i];
      if (i != (num_street.length-1)) {
        street = street+" ";
      }
    }
  }
  return street;
};

// function to check if variable is
function isset(variable) {
  return (typeof(variable) != "undefined" && variable !== null);
};

// HANDLE GET REQUESTS
app.get('/', function(req, res) {
  var query = req.query;
  var resp = {feed: "general", posts: []};

  console.log('\n');
  console.log('REQUEST TYPE:', req.method);
  console.log('REQUEST URL:', req.originalUrl);
  console.log('REQUEST QUERY:', query);
  console.log('\n');

  // connect to database
  ibmdb.open(credentials, function(err,conn) {
    if (err) return console.log(err);

    // sql statement
    var sql_select_feeds = "select * from gqxbnfjt.feed";

    // if general feed request received
    if (query.feed == "general") {
      console.log("GENERAL FEED REQUEST: ");
    } // else if specific feed request received
    else if (query.feed == "specific") {
      console.log("SPECIFIC ROUTE FEED REQUEST: ");
      resp.feed = "specific";

      // initialize params of requests
      var origin = query.from;
      var destination = query.to;
      var street_from = get_street(origin);
      var street_to = get_street(destination);
      // var route_area
      var add_on = (" ").concat("where from='",origin,"' and to='",destination,"'");
      var sql_select_live = ("select * from gqxbnfjt.live").concat(add_on);
      sql_select_feeds = sql_select_feeds.concat(add_on);
      var sql_select_tweets = "select * from gqxbnfjt.tweet_temp";
      var sql_insert_tweet = "insert into gqxbnfjt.feed (user, from, to, comment, congestion) values (?,?,?,?,?);";
      var sql_delete_tweet = "delete from gqxbnfjt.tweet_temp where tweet=?"

      // check for related tweets
      console.log("Connecting to database...");
      console.log("Checking for tweets related to route...");
      data = conn.querySync(sql_select_tweets)
      console.log("TWEETS: ")
      for (var i in data) {
        post = data[i]
        var tweet = post.TWEET;

        // if tweet contains the street of either the start or end address
        if ((tweet.indexOf(street_from) > -1) || (tweet.indexOf(street_to) > -1)) {
          console.log("true");
          console.log(i, tweet);
          console.log("\n");

          var sql_if_exists = "select * from gqxbnfjt.feed where from=? and to=? and comment=?";
          params = [origin, destination, tweet];
          copies = conn.querySync(sql_if_exists, params);

          // if tweet has already been associated with this route
          if (copies.length == 0) {
            // write tweet to feed table
            params = [post.USER, origin, destination, tweet, post.CONGESTION];
            console.log("writing tweet to feed table...")
            conn.querySync(sql_insert_tweet, params);
            console.log("done")
          }
        }
      }
      console.log("\n");

      // check for live info
      console.log("Connecting to database...");
      console.log("Checking for live traffic details...");
      resp.details = decap(conn.querySync(sql_select_live));
      console.log("RESPONSE");
      console.log(resp.details);

    }

    // execute statement
    console.log('\n');
    console.log("Connecting to database...")
    conn.query(sql_select_feeds, function (err, data) {
      if (err) console.log(err);
      else console.log("Checking for feeds...");

      // convert keys returned from db to lowercase
      resp.posts = decap(data);
      console.log("RESPONSE");

      console.log(resp)
      res.json(resp);
    });
    conn.close(function() {
      console.log("DONE!");
    });
  });

});

// HANDLE POST REQUESTS
app.post('/', function(req, res, next) {
  var body = req.body;
  resp = {status: "OK", message: ""};

  // get body of post request and send it back to user as result
  // res.json(body);
  console.log('REQUEST TYPE:', req.method);
  console.log('REQUEST URL:', req.originalUrl);
  console.log('REQUEST BODY:', body);
  console.log('\n');

  // boolean to check parameters
  var present = (isset(body.username) && isset(body.from) && isset(body.to)
     && isset(body.congestion_rating) && isset(body.reporting_address) && isset(body.latitude)
      && isset(body.longitude));

  // if parameters are correct
  if (present) {
    // insert to table
    ibmdb.open(credentials, function (err,conn) {
      if (err) {
        resp.status = "FAIL"
        resp.message = err
        return console.log(err);
      }

      // sql statements
      var sql_insert_feed = 'insert into gqxbnfjt.feed (user, from, to, comment, congestion, reportinglocation) values (?,?,?,?,?,?)';
      var sql_insert_locations = 'insert into gqxbnfjt.locations (user, address, lat, long) values (?,?,?,?)';

      // values to insert to table
      comment = "";
      if (body.comment) {
        comment = body.comment;
      }
      var values_feed = [body.username, body.from, body.to, comment,
        parseFloat(body.congestion_rating), body.reporting_address];

      var values_locations = [body.username, body.reporting_address, parseFloat(body.latitude),
         parseFloat(body.longitude)];

      var sqls = [sql_insert_feed, sql_insert_locations]
      var vals = [values_feed, values_locations]

      for(i = 0; i < 2; i++) {
        conn.query(sqls[i], vals[i], function (err, data) {
          if (err) {
            resp.status = "FAIL"
            resp.message = err
            console.log(err);
          }
        });
      }
      conn.close(function () {
        console.log('POSTED !!');
        console.log('\n')
      });
    });
  }
  else {
    console.log('SOME PARAMETERS MISSING');
    resp.status = "FAIL"
    resp.message = "Some parameters are missing/invalid"
  }
  res.json(resp);
  next();
});


// PORT TO LISTEN TO
app.listen(1337,'127.0.0.1');

// app.set('port', process.env.PORT || 3000);
// app.set('host', process.env.HOST || '169.55.141.28');
//
// http.createServer(app).listen(app.get('port'), app.get('host'), function(){
// console.log("Express server listening on port " + app.get('port'));
// });

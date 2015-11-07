/*The app implements implements a REST api with the following endpoints:

POST  /queue,  JSON -> {'url': 'http://example.com'}  -  create a new task that will fetch data from the url

DELETE  /queue  -  if sent with no querystring, will delete all jobs, finished or otherwise. The 'id' and 'finished' querystrings can be used as filters and expect a mongo id and either 'true' or 'false', respectively.

GET  /queue  -  if sent with no querystring, will get all jobs, finished or otherwise. The 'id' and 'finished' querystrings can be used as filters and expect a mongo id and either 'true' or 'false', respectively. 
*/

var app = require('express')();
var bodyParser = require('body-parser');
var validator = require('validator');
var async = require('async');
var mongojs = require('mongojs');
var get = require('get');

app.use(bodyParser.json()); //for parsing application/json

var db = mongojs('test');
var jobs = db.collection("url_queues");

//this function is run everytime we call queue.push
//task_obj is the arg we call queue.push with
var queue = async.queue( function(task_obj) {

    setTimeout(function(){

        get(task_obj.url).asString(function(err, data) {

            var response;

            if (err)
                response = err;
            else
                response = data;

            //update the db with the response or error
            jobs.findAndModify({
                query: { '_id': task_obj['_id'] },
                update: { $set: { 'finished': true, 'response': response } }
            });
        });

    }, 10000);

//run 20 tasks concurrently
}, 20);


app.get('/queue', function (req, res) {

    //eventually this should be a validator middleware function to verify all query args
    var search_obj = {};

    if ( req.query.id && validator.isMongoId(req.query.id) )
        search_obj['_id'] = mongojs.ObjectId(req.query.id);

    if ( req.query.finished )
        search_obj['finished'] = validator.toBoolean(req.query.finished);

    //find the entries and send them
    jobs.find(search_obj, function(err, docs) {

        if (err)
            res.status(500).json(err);

        res.json(docs);
    });
});


app.post('/queue', function (req, res) {

    //send back an error if we didn't get a valid url
    if ( ! validator.isURL(req.body.url) ) {
        res.status(400).json({ error: 'Not a valid url' });
        return false;
    }

    jobs.findOne({'url': req.body.url}, function(err, doc) {

        //if the url already exists in mongo, return error
        if (doc || err) {
            res.status(400).json({'msg': 'Entity already exists'});
            return false;
        }

        //insert the new url into mongo
        jobs.insert( {'url': req.body.url, 'finished': false}, function(err, doc) {

            if (err) {
                res.status(500).json(err);
                return false;
            }

            //add the url to the job que. Give the _id for efficient lookup
            queue.push( {'url': req.body.url, '_id': mongojs.ObjectId(doc['_id']) } );

            res.json(doc);
        });
    });
});


app.delete('/queue', function (req, res) {

    //eventually this should be a validator middleware function to verify all query args
    var search_obj = {};

    if ( req.query.id && validator.isMongoId(req.query.id) )
        search_obj['_id'] = mongojs.ObjectId(req.query.id);

    if ( req.query.finished )
        search_obj['finished'] = validator.toBoolean(req.query.finished);

    jobs.remove( search_obj, function (err, num_removed) {

        if (err) {
            res.status(500).json(err);
            return false;
        }

        res.json(num_removed);
    });

});


var server = app.listen(3000, function () {

    var host = server.address().address;
    var port = server.address().port;

    console.log('Example app listening at http://%s:%s', host, port)

});

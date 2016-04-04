'use strict';

var MongoClient = require('mongodb').MongoClient;
var kafka = require('kafka-node');
var moment = require('moment');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.Client;

var topic = 'temperature';

var client = new Client('172.24.98.29:8080');
var topics = [
        {topic: topic, partition: 0}
    ],
    options = { autoCommit: true, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024*1024 };

var consumer = new Consumer(client, topics, options);
var offset = new Offset(client);

MongoClient.connect("mongodb://localhost:8081/meteor", function(err, db) {
  if(err) { return console.dir(err); }

  db.collection('temperatures', function(err, collection) {
        console.log("Ready to put data");
      
        consumer.on('message', function (message) {
            console.log("put message",message.value);
            var measure = JSON.parse(message.value);
	    console.log("Measure type", typeof measure);

	    var now = moment();
	    var ts = moment(measure.ts);

	    console.log('now',now.format());
	    console.log('ts',ts.format());
            var diff = now.diff(ts,'seconds');
	    console.log('Processed in', diff);
	    
            var mr = {
			id:measure.id,
			tmp:measure.tmp,
			ts:measure.ts,
			diff:diff
		};
	    console.log('Ready to save',mr);
	    collection.insert(mr);
        });

        consumer.on('error', function (err) {
            console.log('error', err);
        });      
  });
});


console.log('all up');

/*
* If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset
*/
consumer.on('offsetOutOfRange', function (topic) {
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offsets) {
        var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
        consumer.setOffset(topic.topic, topic.partition, min);
    });
});

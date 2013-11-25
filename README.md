dynamokv
========

Dynamo Key-Value
=======
DynamoKV - Dynamo as simple Key-Value
=====================================

Amazon Dynamo is a key value, but its API is a bit more cumbersome that it should
when all you need is just that: key and value.

This is a simple API to save and retrieve items based on a simple key.

In other words: this acts like a hash table on dynamo with a simple string hash key.
So far this module doesn't support non-string hash keys or range keys.

As a bonus, this module includes a DynamoHelper object that abstracts a running
dynamo instance. It can be used for testing to switch between a real dynamo instance
in the cloud, or a local fake_dynamo instance. See below for details.


DynamoKV
--------

How to get a DynamoKV instance:

    var DynamoKV = require('dynamokv').DynamoKV;

    DynamoKV.createDynamoKV("myTablePrefix", tableDescriptions, function (err, dynamoKv) {
        dynamoKV.xxxxx // here is the dynamoKV instance
    };   

What do you get depends on your environment variables. By default it will use the AWS environment
variables to connect to the real dynamo, but it can be redirected somewhere else. For example
to a local instance of [fake_dynamo](https://github.com/ananthakumaran/fake_dynamo) using 
the *FAKE_DYNAMO_ENDPOINT* env variable.

For example:

    // Run fake_dynamo --port=4567 on your machine
    $ export FAKE_DYNAMO_ENDPOINT=http://localhost:4567
   
    $ mycode.js // your code using DynamoKV, will now call fake_dynamo

Now:
    $ unset FAKE_DYNAMO_ENDPOINT
    $ mycode.js // this code will connect to Amazon Dynamo


Note that in the second case, AWS environment variables (like *AWS_ACCESS_KEY_ID*) must be set.



DynamoHelper
------------

Is great to get your software running on dynamo, but cumbersome to test. Tests usually need
clean tables (or in a specific state) and working on the real dynamo is slow (and consumes
bandwith).

This can be solved using [fake_dynamo](https://github.com/ananthakumaran/fake_dynamo), an 
implementation of dynamo API that can be run locally. Now the problem is to start/stop that
instance. Furthermore, code in the real cloud expect tables to exist, while in the tests
we need to create them.

DynamoHelper to the rescue. If you want to use the real dynamo for the tests, it will just
go there. If you prefer to use the fake version of dynamo, it will run an instance, create
the required tables and stop it when the tests are done.

This means that adjusting environment variables, the same tests can run on a local dynamo
or the real one.

This is how the code looks:

    var DynamoHelper = require('dynamokv').DynamoHelper;

    describe("my code with dynamo backend", function () {
        var dynamoInstance;
  
        before(function (done) {
            DynamoHelper.getFakeDynamo("test-tables", ["A", "B"], function (err, dynamo) {
                dynamoInstance = dynamo;
                done(err);
            }); 
        });     

        after(function (done) {
            dynamoInstance.stop(done);
        });     
    }); 

This code will create tables: *test-tables-A* and *test-tables-B* on dynamo.

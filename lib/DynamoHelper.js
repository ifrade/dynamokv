var fs = require('fs');
var assert = require('assert');
var url = require('url');

var createDynamoKV = require('./DynamoKV.js').createDynamoKV;
var fakeDynamoGem = require('fake-dynamo-gem');

/*
 * callback(err, TestDynamo instance) with err set if something went wrong
 * This wrapper on the fake_dynamo_gem module to create the tables.
 */
function FakeDynamo(tablePrefix, tableDescriptions, callback) {
    assert(typeof callback === 'function');

    var that = this;

    this.dbfile = "fakeDynamoDB.fdb";
    try {
        if (fs.existsSync(this.dbfile)) {
            fs.unlinkSync(this.dbfile);
        }
    } catch (e) {
        callback(new Error("Unable to prepare database file " + this.dbfile));
        return;
    }

    var dynamoUrl = url.parse(process.env.FAKE_DYNAMO_ENDPOINT);
    assert(dynamoUrl.port, 'Cannot read the port in FAKE_DYNAMO_ENDPOINT');

    var options = ['--port=' + dynamoUrl.port, '--db=' + this.dbfile];

    fakeDynamoGem(options, function (err, instance) {
        if (err) {
            callback(err);
            return;
        }

        that.dynamo = instance;
        that.createTablesInDB(tablePrefix, tableDescriptions, function (err) {
            callback(err, that);
        });
    });
}

FakeDynamo.prototype.stop = function (done) {
    this.dynamo.on('exit', function () {
        // Clean the database file, just in case
        if (fs.existsSync(this.dbfile)) {
            fs.unlinkSync(this.dbfile);
        }
        done();
    });
    this.dynamo.kill();
};

function createTablesInDB(tablePrefix, tableDescriptions, callback) {
    createDynamoKV(tablePrefix, tableDescriptions, function (err, dynamoKV) {
        dynamoKV.createTables(function (err) {
            callback(err);
        });
    });
}

FakeDynamo.prototype.createTablesInDB = createTablesInDB;

// Factory method for aesthetic purposes
function getFakeDynamo(tablePrefix, tableDescriptions, callback) {
    assert(tablePrefix, "Must specify table prefix to create a Dynamo DB");
    assert(typeof callback === 'function');

    var defaultFakeEndpoint = "http://localhost:4567";
    /*
    if (process.env.FAKE_DYNAMO_ENDPOINT &&
        process.env.FAKE_DYNAMO_ENDPOINT !== defaultFakeEndpoint) {
        // Warning
    }
    */
    process.env.FAKE_DYNAMO_ENDPOINT = defaultFakeEndpoint;

    return new FakeDynamo(tablePrefix, tableDescriptions, callback);
}


function RealDynamo(tablePrefix, tableDescriptions, callback) {
    assert(typeof callback === 'function');

    var that = this;
    createDynamoKV(tablePrefix, tableDescriptions, function (err, dynamoKV) {
        that.dynamo = dynamoKV;
        if (err) { callback(err, null); return; }
        dynamoKV.emptyTables(function (err) {
            callback(err, that);
        });
    });
}

RealDynamo.prototype.stop = function (done) {
    done();
};

RealDynamo.prototype.createTablesInDB = createTablesInDB;

function getRealDynamo(tablePrefix, tableDescriptions, callback) {
    assert(typeof callback === 'function');
    assert(tablePrefix, "Must specify table prefix to create a Dynamo DB");

    assert(process.env.AWS_ACCESS_KEY_ID &&
           process.env.AWS_SECRET_ACCESS_KEY &&
           process.env.AWS_DEFAULT_REGION,
          "AWS_ env variables required to use a real AWS");
    assert(!process.env.FAKE_DYNAMO_ENDPOINT,
           "FAKE_DYNAMO_ENDPOINT should not be set when trying to use a real Dynamo");
    return new RealDynamo(tablePrefix, tableDescriptions, callback);
}


/*
 * In the fake dynamo a new clean database is created with the table prefix.
 *
 * In the real dynamo, tables MUST exist. They will be emptied before the test,
 *   (but NOT created)
 */
module.exports = { getFakeDynamo: getFakeDynamo,
                   getRealDynamo: getRealDynamo,
                   // set getDynamo to the function you want to use in the test!
                   getDynamo: getFakeDynamo };

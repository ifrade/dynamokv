var fs = require('fs');
var assert = require('assert');
var url = require('url');

var createDynamoKV = require('./DynamoKV.js').createDynamoKV;
var dynalite = require('dynalite');

/*
 * callback(err, TestDynamo instance) with err set if something went wrong
 * This wrapper on the fake_dynamo_gem module to create the tables.
 */
function FakeDynamo(tableNames, tableDescriptions, callback) {
    assert(typeof callback === 'function');
    assert(typeof tableNames === 'object',
           'Table names must be an object with short-> dynamo name translations');

    var that = this;

    var dynamoUrl = url.parse(process.env.FAKE_DYNAMO_ENDPOINT);
    assert(dynamoUrl.port, 'Cannot read the port in FAKE_DYNAMO_ENDPOINT');


    var opts = {
        // In memory by default...
        //path: "./db",
        createTableMs: 0
    };
    this.dynaliteServer = dynalite(opts);
    this.dynaliteServer.listen(dynamoUrl.port, function (err) {
        if (err) {
            callback(err);
            return;
        }

        that.createTablesInDB(tableNames, tableDescriptions, function (err) {
            callback(err, that);
        });
    });
}

FakeDynamo.prototype.stop = function (done) {
    this.dynaliteServer.close(function () {
        // Clean the database file, just in case
        if (fs.existsSync(this.dbfile)) {
            fs.unlinkSync(this.dbfile);
        }
        done();
    });
};

function createTablesInDB(tableNames, tableDescriptions, callback) {
    createDynamoKV(tableNames, tableDescriptions, function (err, dynamoKV) {
        dynamoKV.createTables(function (err) {
            callback(err);
        });
    });
}

FakeDynamo.prototype.createTablesInDB = createTablesInDB;

// Factory method for aesthetic purposes
function getFakeDynamo(tableNames, tableDescriptions, callback) {
    assert(typeof callback === 'function');
    assert(typeof tableNames === 'object', "Must specify table names to create a Dynamo DB");

    var defaultFakeEndpoint = "http://localhost:4567";
    /*
    if (process.env.FAKE_DYNAMO_ENDPOINT &&
        process.env.FAKE_DYNAMO_ENDPOINT !== defaultFakeEndpoint) {
        // Warning
    }
    */
    process.env.FAKE_DYNAMO_ENDPOINT = defaultFakeEndpoint;
    return new FakeDynamo(tableNames, tableDescriptions, callback);
}


function RealDynamo(tableNames, tableDescriptions, callback) {
    assert(typeof callback === 'function');

    var that = this;
    createDynamoKV(tableNames, tableDescriptions, function (err, dynamoKV) {
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

function getRealDynamo(tableNames, tableDescriptions, callback) {
    assert(typeof callback === 'function');
    assert(typeof tableNames === 'object', "Must specify table names to create a Dynamo DB");

    assert(process.env.AWS_ACCESS_KEY_ID &&
           process.env.AWS_SECRET_ACCESS_KEY &&
           process.env.AWS_DEFAULT_REGION,
          "AWS_ env variables required to use a real AWS");
    assert(!process.env.FAKE_DYNAMO_ENDPOINT,
           "FAKE_DYNAMO_ENDPOINT should not be set when trying to use a real Dynamo");
    return new RealDynamo(tableNames, tableDescriptions, callback);
}


/*
 * In the fake dynamo a new clean database is created with the tables descriptions (and names).
 *
 * In the real dynamo, tables MUST exist. They will be emptied before the test,
 *   (but NOT created)
 */
module.exports = { getFakeDynamo: getFakeDynamo,
                   getRealDynamo: getRealDynamo,
                   // set getDynamo to the function you want to use in the test!
                   getDynamo: getFakeDynamo };

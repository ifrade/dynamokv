var assert = require('assert');
var async = require('async');
var AWS = require('aws-sdk');
var DynamoTables = require('./DynamoTables.js');

function DynamoKV(prefix, tableDefinitions, callback) {
    var that = this;
    assert(prefix, "Table prefix mandatory to create a DynamoKV");
    assert(typeof callback === 'function', "Third parameter must be a callback function");

    /* Guess which dynamo to use */
    if (process.env.FAKE_DYNAMO_ENDPOINT) {
        //logger.info("Using fake Dynamo on", process.env.FAKE_DYNAMO_ENDPOINT);
        AWS.config.update({
            region : "us-west-1",
            endpoint : process.env.FAKE_DYNAMO_ENDPOINT,
            sslEnabled : false,
            accessKeyId:     "xxx",
            secretAccessKey: "xxx",
        });
    } else {
        /*
        if (!process.env.AWS_ACCESS_KEY_ID ||
            !process.env.AWS_SECRET_ACCESS_KEY ||
            !process.env.AWS_DEFAULT_REGION) {
            logger.warn("No AWS credentials in the environment.");
        }
        */
        //logger.info("Using real Dynamo");
        AWS.config.update({
            region: process.env.AWS_DEFAULT_REGION
        });
    }

    //logger.silly("AWS config", AWS.config);
    this.dynamo = new AWS.DynamoDB();
    this.prefix = prefix;
    //logger.debug("Using '" + this.prefix + "' as table prefix");


    this.dynamo.listTables(function (err, data) {
        if (err) {
            callback(err);
            return;
        }

        that.tables = new DynamoTables(data.TableNames, that.prefix, tableDefinitions);
        callback(null, that);
    });
}


function createDynamoKV(prefix, tableDefinitions, callback) {
    var x = new DynamoKV(prefix, tableDefinitions, callback);
}

function formatKey(key) {
    var formattedKey;

    if (typeof key === 'string') {
        formattedKey = { hash: key };
    } else if (typeof key === 'object') {
        assert(typeof key.hash !== 'undefined', "Key must have a 'hash' field");
        formattedKey = key;
    }

    return formattedKey;
}

function assertKeyFormat(expectedFormat, incomingKey) {
    // Hash is always mandatory
    assert(incomingKey.hash, "Key must have a 'hash' field");

    if (expectedFormat.rangeField) {
        assert(incomingKey.range, "Key must have a 'range' field");
    }
}

function setKeysInOperation(operation, obj, keyFields, formattedKey) {
    operation[obj][keyFields.hashField] = { "S": formattedKey.hash};
    if (keyFields.rangeField) {
        operation[obj][keyFields.rangeField] = { "S": formattedKey.range};
    }
    return operation;
}

/*
 * Sets @key with @value in table @tableName.
 *
 * @key can be an string (for backward compatibility)
 *   or an object { hash: x, range: y }
 *
 * If overwrite is false, then it writes only if the key didnt exist before.
 *
 * @value doesnt need to be an string. Internally will be stringified for storage
 *  and parsed on getFromTable.
 *
 * When done, callback(err) is called, with the err coming from dynamo.
 */
DynamoKV.prototype.putOnTable = function (tableName, key, value, overwrite, callback) {
    assert(typeof key === 'string' || typeof key === 'object',
           "Key must be string or object, not " + (typeof key));
    assert(typeof callback === 'function', "Callback not a function. Incorrect # of arguments?");
    assert(typeof overwrite !== 'undefined', "Should set overwrite to true or false");

    // Backwards compatibility
    var formattedKey = formatKey(key);

    var fullTableName = this.tables.fullTableName(tableName);
    var keyFields = this.tables.getKeyFieldForTable(tableName);

    assertKeyFormat(keyFields, formattedKey);

    var operation = {
        TableName: fullTableName,
        Item: {}
    };
    setKeysInOperation(operation, "Item", keyFields, formattedKey);
    operation.Item.content = { "S": JSON.stringify(value) };

    if (!overwrite) {
        operation.Expected = {};
        if (keyFields.rangeField) {
            operation.Expected[keyFields.rangeField] = { "Exists": false };
        } else {
            operation.Expected[keyFields.hashField] = { "Exists": false };
        }
    }

    this.dynamo.putItem(operation, function (err, data) {
        // Ignore data. It is not used bu any client.
        callback(err);
    });
};

/*
 * Get @key from the table @tableName.
 * The returned value is the same as passed on the putOnTable operation.
 */
DynamoKV.prototype.getFromTable = function (tableName, key, callback) {
    var fullTableName = this.tables.fullTableName(tableName);
    var keyFields = this.tables.getKeyFieldForTable(tableName);

    var formattedKey = formatKey(key);
    assertKeyFormat(keyFields, formattedKey);

    var operation = {
        TableName: fullTableName,
        Key: {}
    };

    setKeysInOperation(operation, "Key", keyFields, formattedKey);
    this.dynamo.getItem(operation, function (err, data) {
        if (err) {
            callback(err, data);
            return;
        }

        if (!data.Item) {
            var ops = new Error("Item " + key + " not found on " + tableName);
            ops.notFound = true;
            callback(ops);
            return;
        }

        if (!data.Item.content ||
            !data.Item.content.S) {
            callback(new Error("Item " + key + " has a weird format in the database"));
            return;
        }

        callback(err, JSON.parse(data.Item.content.S));
    });
};

/*
 * The value of @key is a list of items. This method appends @item to that list.
 * If the key doesnt exit, a new one is created with @item and set on @key.
 *
 * callback(err) if something went wrong,
 */
DynamoKV.prototype.appendToListOnKey = function (tableName, key, item, callback) {
    var that = this;
    this.getFromTable(tableName, key, function (err, listOnDb) {
        var previousList = listOnDb || [];

        if (err && !err.notFound) {
            callback(err);
            return;
        }
        previousList.push(item);
        that.putOnTable(tableName, key, previousList, true, callback);
    });

};

/*
 * The value of @key is a list of things. This method takes the list, looks for @item
 * (using the custom @comparator if provided). If found removes it and saves the list back to @key
 *
 * callback(err, current number of elements on the list);
 */
DynamoKV.prototype.removeFromListOnKey = function (tableName, key, item, comparator, callback) {

    assert(typeof callback === 'function');
    var that = this;
    var comparationFunction = comparator || function (a, b) { return a === b; };

    function keyInList(theList) {
        // item and comparationFunction in the closure... too clever
        for (var i = 0; i < theList.length; i++) {
            if (comparationFunction(theList[i], item)) {
                return i;
            }
        }
        return -1;
    }

    this.getFromTable(tableName, key, function (err, listOnDb) {
        var previousList = listOnDb || [];

        if (err) {
            callback(err);
            return;
        }

        var pos = keyInList(previousList);
        if (pos != -1) {
            previousList.splice(pos, 1);
            if (previousList.length > 0) {
                that.putOnTable(tableName, key, previousList, true, callback);
            } else {
                that.deleteFromTable(tableName, key, callback);
            }
        } else {
            // Nothing to remove (error?)
            callback();
        }
    });
};

/*
 * Check if an @item is in the list under @key, using the @comparator function or === by default
 */
DynamoKV.prototype.isInListOnKey = function (tableName, key, item, comparator, callback) {
    assert(typeof callback === 'function', "Callback should be a function. Wrong number of arguments?");
    var that = this;
    var comparationFunction = comparator || function (a, b) { return a === b; };

    function keyInList(theList) {
        // item and comparationFunction in the closure... too clever
        for (var i = 0; i < theList.length; i++) {
            if (comparationFunction(theList[i], item)) {
                return i;
            }
        }
        return -1;
    }

    this.getFromTable(tableName, key, function (err, listOnDb) {
        var previousList = listOnDb || [];

        if (err && !err.notFound) {
            callback(err);
            return;
        }

        var pos = keyInList(previousList);
        callback(null, pos !== -1);
    });
};

/*
 * Delete @key from table @tableName.
 * If @key doesnt exists, it reports an error
 *
 * callback(err, content_of_deleted_key)
 */
DynamoKV.prototype.deleteFromTable = function (tableName, key, callback) {
    var fullTableName = this.tables.fullTableName(tableName);
    var keyFields = this.tables.getKeyFieldForTable(tableName);
    var formattedKey = formatKey(key);

    var operation = {
        TableName: fullTableName,
        Key: {}
    };

    setKeysInOperation(operation, "Key", keyFields, formattedKey);

    // Ask for old value, to know if we really delete anything
    operation.ReturnValues = "ALL_OLD";

    this.dynamo.deleteItem(operation, function (err, data) {
        if (err) {
            callback(err);
            return;
        }

        if (!err && (typeof data.Attributes === 'undefined')) {
            err = new Error("Deleting unexistent item in DB");
            err.notFound = true;
            callback(err);
            return;
        }

        if (!data.Attributes.content ||
            !data.Attributes.content.S) {
            console.log("data", data);
            callback(new Error("Item " + key + " has a weird format in the database"));
            return;
        }

        callback(err, JSON.parse(data.Attributes.content.S));
    });
};



/* callback (err, [{ range: x, content: y},...] */
DynamoKV.prototype.listOnKey = function (tableName, hashKey, callback) {
    var fullTableName = this.tables.fullTableName(tableName);
    var keyFields = this.tables.getKeyFieldForTable(tableName);

    var operation = {
        TableName: fullTableName,
        KeyConditions: {
        }
    };
    operation.KeyConditions[keyFields.hashField] = {
        "AttributeValueList" : [{ "S": hashKey }],
        "ComparisonOperator" : "EQ"
    };

    this.dynamo.query(operation, function (err, data) {
        if (err) {
            callback(err, data);
            return;
        }

        /*
        var results = data.Items.map(function (item) {
            return { range: item.range, content: item.content };
        });
        */
        callback(err, data.Items || []);
    });
};




/*
 * Table management functions. For tests and tools.
 */
DynamoKV.prototype.createTables = function (callback) {
    var that = this;

    async.eachSeries(this.tables.getTableDescriptions(), function (tableDesc, cb) {
        that.dynamo.createTable(tableDesc, cb);
    }, function (err, results) {
        callback(err);
    });
};


/*
 * callback(error, allOk, results)
 *
 * error usually indicates a network problem
 * allOk is a convenience, saying that all tables are ACTIVE
 * results is a list of objects with table name and table status:
 *   [{ TableName: tableName, TableStatus: 'ACTIVE|PENDING|...' }]
 *
*/
DynamoKV.prototype.checkTables = function (callback) {
    var that = this;

    var tableNames = this.tables.getFullTableNames();

    async.map(tableNames, function (tableName, cb) {
        that.dynamo.describeTable({TableName: tableName}, cb);
    }, function (err, results) {
        if (err) {
            callback(err, false, results);
            return;
        }

        var allOk = true;
        var processedResults = [];
        if (results) {
            results.forEach(function (tableResult) {
                processedResults.push({TableName: tableResult.Table.TableName,
                                       TableStatus: tableResult.Table.TableStatus });
                if (tableResult.Table.TableStatus !== 'ACTIVE') {
                    allOk = false;
                }
            });
        }
        callback(null, allOk, processedResults);
    });
};

/*
 * Returns the list of tables ONLINE in Dynamo.
 * The names are "full", with the prefix.
 *
 * callback(err, ['table-name1', 'table-name2'...]);
 */
DynamoKV.prototype.listTables = function (callback) {
    var that = this;

    this.dynamo.listTables({}, function (err, data) {
        if (err || !data.TableNames) {
            callback(err, data);
            return;
        }
        callback(err, data.TableNames.filter(function (tableName) {
            return (tableName.indexOf(that.prefix) === 0);
        }));
    });
};

/*
 * Goes through all online tables, deleting their contents.
 *
 * callback(err), with err set if something went wrong.
 */
DynamoKV.prototype.emptyTables = function (callback) {
    var that = this;
    this.listTables(function (err, tableNames) {
        if (err) {
            callback(err);
            return;
        }

        async.each(tableNames, function (tableName, cb) {
            that.emptyTable(tableName, cb);
        }, function (err) {
            if (err) {
                console.log(err);
            }
            callback(err);
        });
    });
};

/*
 * Internal function.
 * Delete all contents from @tableName, which is a fully qualified
 *  table name.
 */
DynamoKV.prototype.emptyTable = function (tableName, callback) {
    var that = this;

    this.dynamo.scan({TableName: tableName}, function (err, data) {
        if (err) {
            callback(err);
            return;
        }

        if (data.Items.length === 0) {
            callback();
            return;
        }
        // BatchWriteItem...
        // http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
        var tableKeyField = that.tables.getKeyFieldForTable(tableName);

        var deleteRequests = data.Items.map(function (item) {
            var itemKey = that.tables.getKeyForItem(item, tableName);

            var itemRequest = {};
            itemRequest.Key = {};
            itemRequest.Key[tableKeyField.hashField] = itemKey.hash;

            if (tableKeyField.rangeField) {
                itemRequest.Key[tableKeyField.rangeField] = itemKey.range;
            }

            return { "DeleteRequest": itemRequest };
        });

        var batchRequest = {};
        batchRequest.RequestItems = {};
        batchRequest.RequestItems[tableName] = deleteRequests;

        that.dynamo.batchWriteItem(batchRequest, function (err, data) {
            callback(err, data);
        });

    });
};

DynamoKV.prototype.scanTable = function (tableName, callback) {
    var fullTableName = this.tables.fullTableName(tableName);
    this.dynamo.scan({TableName: fullTableName}, callback);
};

module.exports = {createDynamoKV: createDynamoKV };

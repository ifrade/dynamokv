var assert = require('assert');

/* tablesIdx: object with short table name and full dynamo table name { "TableA" : "a-xyz-123-TableA-123123" }
   rawTables: our table definitions (with undefined TableNames)
   callback: (err, dynamotables instance);
*/
function DynamoTables(tablesIdx, rawTables) {
    var that = this;
    assert(typeof tablesIdx === 'object',
          "DynamoTables receives now an object with short -> dynamo table name");

    this.rawTables = JSON.parse(JSON.stringify(rawTables)); // Keep a copy
    this.tablesIdx = JSON.parse(JSON.stringify(tablesIdx)); // Shortname => Dynamo name
    this.tablesRIdx = {};                                   // Dynamo name -> shortname
    this.tables = [];

    Object.keys(this.tablesIdx).forEach(function (k) {
        var v = that.tablesIdx[k];
        that.tablesRIdx[v] = k;
    });

    for (var expectedTable in this.rawTables) {
        if (this.rawTables.hasOwnProperty(expectedTable)) {
            assert(this.tablesIdx[expectedTable],
                   "Cannot find table " + expectedTable + " in dynamo");

            var tableDescription = this.rawTables[expectedTable];

            //logger.info("Translating", tableName, "=>", fullTableName);
            tableDescription.TableName = this.tablesIdx[expectedTable];
            this.tables.push(tableDescription);
        }
    }
}

/*
 * List of table descriptions ready to be send to dynamo (create)
 */
DynamoTables.prototype.getTableDescriptions = function () {
    return this.tables;
};

/*
 * List of table names as in dynamo (prefix-TableName-suffix)
 */
DynamoTables.prototype.getFullTableNames = function () {
    return this.tables.map(function (e) { return e.TableName; });
};


/*
 * Functions to manipulate tableNames
 */
DynamoTables.prototype.fullTableName = function (tableName) {
    return this.tablesIdx[tableName];
};

DynamoTables.prototype.shortTableName = function (tableName) {
    if (this.tablesIdx.hasOwnProperty(tableName)) {
        return tableName;
    }
    return this.tablesRIdx[tableName];
};


function keySchemaToOurObject(keySchema) {
    // TODO: Use type and not positioning to assign these keys
    var keys = { hashField: keySchema[0].AttributeName };
    if (keySchema.length > 1) {
        keys.rangeField = keySchema[1].AttributeName;
    }

    return keys;
}

/*
 * Returns an object with { hashField: keyAttributeName,
 *                          rangeField: keyAttributeName }
 */
DynamoTables.prototype.getKeyFieldForTable = function (tableName) {
    var shortName = this.shortTableName(tableName);

    var tableDesc = this.rawTables[shortName];
    assert(tableDesc, "No table named '" + tableName + "'?");

    return keySchemaToOurObject(tableDesc.KeySchema);
};


/*
 * Returns an object with { hashField: keyAttributeName,
 *                          rangeField: keyAttributeName }
 */
DynamoTables.prototype.getKeyFieldForIndex = function (tableName, idxName) {
    var shortName = this.shortTableName(tableName);
    assert(shortName, "Cannot find a table named '" + tableName + "'?");

    var tableDesc = this.rawTables[shortName];
    assert(tableDesc, "No table named '" + tableName + "'?");

    var indexes = tableDesc.GlobalSecondaryIndexes;
    if (!indexes) {
        return null;
    }

    for (var i = 0; i < indexes.length; i++) {
        if (indexes[i].IndexName === idxName) {
            return keySchemaToOurObject(indexes[i].KeySchema);
        }
    }
    return null;
};

/*
 * Returns a list ["property1", "property2",...] with properties
 * that we expect in the object but are not keys. For example because
 * those properties are used in indexes.
 */
DynamoTables.prototype.getOtherFieldsForTable = function (tableName) {
    var shortName = this.shortTableName(tableName);

    var tableDesc = this.rawTables[shortName];
    assert(tableDesc, "No table named '" + tableName + "'?");

    var allAttributes = tableDesc.AttributeDefinitions;
    var keyAttributes = tableDesc.KeySchema;

    function attrNotInKeyAttr(attr) {
        for (var i = 0; i < keyAttributes.length; i++) {
            if (keyAttributes[i].AttributeName === attr.AttributeName) {
                return false;
            }
        }
        return true;
    }

    var result = allAttributes.filter(attrNotInKeyAttr);
    return result.map(function (e) { return e.AttributeName; });
};

/*
 * Given an dynamo item, returns the value for the key field
 *  as { hash: "X", range: "Y" }
 */
DynamoTables.prototype.getKeyForItem = function (item, tableName) {
    var keyFields = this.getKeyFieldForTable(tableName);
    var key = { hash: item[keyFields.hashField] };

    if (keyFields.rangeField) {
        key.range = item[keyFields.rangeField];
    }

    return key;
};

module.exports = DynamoTables;

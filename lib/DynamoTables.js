var assert = require('assert');

/* tablesInDB: list of table names as coming from dynamo.listTables
   rawTables: our table definitions (with undefined TableNames)
   prefix: prefix we want to use for the tables
   callback: (err, dynamotables instance);
*/
function DynamoTables(tablesInDB, prefix, rawTables) {

    assert(typeof prefix === 'string');

    this.prefix = prefix;
    this.rawTables = rawTables;
    this.tables = [];
    this.tablesIdx = {};

    this.initTablesStructure(tablesInDB, prefix);
}

function findByPrefix(fullNameList, prefixedTableName) {
    for (var i = 0; i < fullNameList.length; i++) {
        if (fullNameList[i].indexOf(prefixedTableName) === 0) {
            return fullNameList[i];
        }
    }
    return null;
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
 * To be called in the constructor
 *
 * this.dynamo, this.tables and this.tablesIdx must be defined before calling
 * this function.
 *
 * It sets TableName in every object in tables. Needs to check dynamo because
 * they have the form of prefix-TableName-randomID
 */
DynamoTables.prototype.initTablesStructure = function (tablesInDB, prefix) {

    for (var tableName in this.rawTables) {
        if (this.rawTables.hasOwnProperty(tableName)) {
            var tableDescription = this.rawTables[tableName];

            /* tableName:                ServiceDescription
             * prefixedTableName: prefix-ServiceDescription
             * fullTableName:     prefix-ServiceDescription-123123123
             */
            var prefixedTableName = prefix + "-" + tableName;
            var fullTableName = findByPrefix(tablesInDB, prefixedTableName);
            if (!fullTableName) {
                // This is OK for tests, so do not throw error.
                // Potentially this is a big error in production... table doesnt exist!?!
                //logger.error("Unable to resolve " + prefixedTableName + "to a dynamo table");
                fullTableName = prefixedTableName;
            }

            //logger.info("Translating", tableName, "=>", fullTableName);
            tableDescription.TableName = fullTableName;
            this.tables.push(tableDescription);
            this.tablesIdx[tableName] = fullTableName;
        }
    }
};


/*
 * Functions to manipulate tableNames
 */
DynamoTables.prototype.fullTableName = function (tableName) {
    if (tableName.indexOf(this.prefix) === 0) {
        //logger.warn("Asking for full table name of", tableName, "but it is already fully defined.");
        return tableName;
    }
    return this.tablesIdx[tableName];
};

DynamoTables.prototype.shortTableName = function (tableName) {
    var shortName = tableName;
    if (tableName.indexOf(this.prefix) === 0) {
        shortName = tableName.substr(this.prefix.length + 1);
    }

    var dashPos = shortName.lastIndexOf("-");
    if (dashPos !== -1) {
        shortName = shortName.substr(0, dashPos);
    }

    assert(this.rawTables[shortName], "The shortened name " + shortName + " is not a valid table");

    return shortName;
};


/*
 * Returns an object with { hashField: keyAttributeName,
 *                          rangeField: keyAttributeName }
 */
DynamoTables.prototype.getKeyFieldForTable = function (tableName) {
    var shortName = this.shortTableName(tableName);

    var tableDesc = this.rawTables[shortName];
    assert(tableDesc, "No table named '" + tableName + "'?");

    var keys = { hashField: tableDesc.KeySchema[0].AttributeName };
    if (tableDesc.KeySchema.length > 1) {
        keys.rangeField = tableDesc.KeySchema[1].AttributeName;
    }

    return keys;
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

/* global describe, it, before, after */
/* global afterEach, beforeEach */
/* jshint node: true*/

var should = require('should');

var DynamoTables = require('../../lib/DynamoTables.js');

var testTables = {
    "A" : {
        "TableName" : null,
        "AttributeDefinitions": [
            { "AttributeName" : "id",
              "AttributeType" : "S" }
        ],
        "KeySchema": [
            { "AttributeName": "id",
              "KeyType" : "HASH" }
        ],
        "ProvisionedThroughput": {
            "ReadCapacityUnits" : 10,
            "WriteCapacityUnits" : 5
        }
    },

    "B" : {
        "TableName" : null,
        "AttributeDefinitions": [
            { "AttributeName": "id",
              "AttributeType": "S" },
            { "AttributeName": "itemType",
              "AttributeType": "S" }
        ],
        "KeySchema": [
            { "AttributeName": "id",
              "KeyType" : "HASH" },
            { "AttributeName": "itemType",
              "KeyType" : "RANGE" }
        ],
        "ProvisionedThroughput": {
            "ReadCapacityUnits" : 10,
            "WriteCapacityUnits" : 5
        }
    },


    "C" : {
        "TableName" : null,
        "AttributeDefinitions": [
            { "AttributeName": "id",
              "AttributeType": "S" },
            { "AttributeName": "itemType",
              "AttributeType": "S" },
            { "AttributeName": "itemColor",
              "AttributeType": "S" }
        ],
        "KeySchema": [
            { "AttributeName": "id",
              "KeyType" : "HASH" },
            { "AttributeName": "itemType",
              "KeyType" : "RANGE" }
        ],
        "GlobalSecondaryIndexes" : [{
            "IndexName": "CIndex",
            "KeySchema": [
                { "AttributeName": "itemColor",
                  "KeyType": "HASH" },
                { "AttributeName": "id",
                  "KeyType": "RANGE" }
            ],
            "Projection": {
                "ProjectionType": "KEYS_ONLY"
            },
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 10,
                "WriteCapacityUnits": 5
            }
        }],
        "ProvisionedThroughput": {
            "ReadCapacityUnits" : 10,
            "WriteCapacityUnits" : 5
        }
    }
};


describe("DynamoTables", function () {

    var tablesInDB = ["prefix-A-123", "prefix-B-234"];
    var tables = new DynamoTables(tablesInDB, "prefix", testTables);

    it("Full name correctly resolved", function () {
        var fullName = tables.fullTableName("A");
        should.exist(fullName);
        fullName.should.equal("prefix-A-123");

        fullName = tables.fullTableName("B");
        should.exist(fullName);
        fullName.should.equal("prefix-B-234");
    });

    it("Full name of unknown table", function () {
        var fullName = tables.fullTableName("meh");
        should.not.exist(fullName);
    });


    it("Short name", function () {
        var shortName = tables.shortTableName("prefix-A-123");
        should.exist(shortName);
        shortName.should.equal("A");
    });

    it("Short name of a non-existing table name", function () {
        (function () { var shortName = tables.shortTableName("ha-ha-ha"); }).should.throwError();
    });

    describe("Only hash key", function () {
        it("Get key fields for a table", function () {
            var keyField = tables.getKeyFieldForTable("A");
            should.exist(keyField);
            keyField.should.have.property("hashField");
            keyField.hashField.should.equal("id");
            keyField.should.not.have.property("keyField");
        });

        it("Get key fields for unknown table", function () {
            (function () { var keyField = tables.getKeyFieldForTable("X"); }).should.throwError();
        });

        it("Get key from dynamo item (internal)", function () {
            var dynamoObj = {
                "id": { "S": "1" },
                "a": { "S": "bla" },
                "b": { "S": "ble"}
            };
            var keyInObj = tables.getKeyForItem(dynamoObj, "prefix-A-123");
            should.exist(keyInObj);
            keyInObj.should.have.property("hash");
            keyInObj.hash.S.should.equal("1");
            keyInObj.should.not.have.property("range");
        });
    });

    describe("Hash and range key", function () {
        it("Get key fields for a table", function () {
            var keyField = tables.getKeyFieldForTable("B");
            should.exist(keyField);
            keyField.should.have.property("hashField");
            keyField.hashField.should.equal("id");
            keyField.should.have.property("rangeField");
            keyField.rangeField.should.equal("itemType");
        });

        it("Get key fields for unknown table", function () {
            (function () { var keyField = tables.getKeyFieldForTable("X"); }).should.throwError();
        });

        it("Get key from dynamo item (internal)", function () {
            var dynamoObj = {
                "id": { "S": "1" },
                "itemType" : { "S": "2" },
                "a": { "S": "bla" },
                "b": { "S": "ble"}
            };
            var keyInObj = tables.getKeyForItem(dynamoObj, "prefix-B-123");
            should.exist(keyInObj);
            keyInObj.should.have.property("hash");
            keyInObj.hash.S.should.equal("1");
            keyInObj.should.have.property("range");
            keyInObj.range.S.should.equal("2");
        });
    });

    describe("Indexes", function () {
        it("Get non-key fields", function () {
            var result = tables.getOtherFieldsForTable("C");
            should.exist(result);
            result.should.be.instanceOf(Array).and.have.lengthOf(1);
            result.indexOf("itemColor").should.equal(0);
        });

        it("Get keys for an index", function () {
            var result = tables.getKeyFieldForIndex("C", "CIndex");
            should.exist(result);
            result.should.have.property("hashField", "itemColor");
            result.should.have.property("rangeField", "id");
        });
    });

});

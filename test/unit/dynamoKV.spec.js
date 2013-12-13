/* global describe, it, before, after */
/* global afterEach, beforeEach */
/* jshint node: true*/
'use strict';

var should = require('should');
var fs = require('fs');
var async = require('async');

var DynamoHelper = require('../../lib/DynamoHelper.js');
var createDynamoKV = require('../../lib/DynamoKV.js').createDynamoKV;
var tables = JSON.parse(fs.readFileSync(__dirname + "/test.dynamo").toString());

describe("DynamoKV", function () {
    this.timeout(4000);

    var dynamo;
    var dynamokv;
    var TABLE_PREFIX = "test-dynamokv";

    before(function (done) {
        DynamoHelper.getFakeDynamo(TABLE_PREFIX, tables, function (err, dynamoInstance) {
            dynamo = dynamoInstance;
            dynamokv = createDynamoKV(TABLE_PREFIX, tables, function (err, dkv) {
                dynamokv = dkv;
                done();
            });
        });
    });

    it('listTables', function (done) {
        dynamokv.listTables(function (err, tables) {
            should.exist(tables);
            tables.should.have.length(2);
            tables.should.contain(TABLE_PREFIX + "-" + "TableA");
            tables.should.contain(TABLE_PREFIX + "-" + "TableB");
            done();
        });
    });

    var basicOperationTests = function () {
        it('Insert key', function (done) {
            var that = this;
            dynamokv.putOnTable(this.tableName, this.goodKey, { a: 1, b: 2}, false, function (err) {
                should.not.exist(err);
                dynamokv.getFromTable(that.tableName, that.goodKey, function (err, data) {
                    should.not.exist(err);
                    should.exist(data);
                    data.should.have.property("a").and.equal(1);
                    data.should.have.property("b").and.equal(2);
                    done();
                });
            });
        });

        it('Update key', function (done) {
            var that = this;
            dynamokv.putOnTable(this.tableName, this.goodKey, { a: 2, b: 3}, true, function (err) {
                should.not.exist(err);
                dynamokv.getFromTable(that.tableName, that.goodKey, function (err, data) {
                    should.not.exist(err);
                    should.exist(data);
                    data.should.have.property("a").and.equal(2);
                    data.should.have.property("b").and.equal(3);
                    done();
                });
            });
        });

        it('Failed insert (key exists and no overwrite)', function (done) {
            dynamokv.putOnTable(this.tableName, this.goodKey, { a: "fail"}, false, function (err) {
                should.exist(err);
                done();
            });
        });


        it('Delete key', function (done) {
            dynamokv.deleteFromTable(this.tableName, this.goodKey, function (err) {
                should.not.exist(err);
                done();
            });
        });

        it('Delete unexisting key', function (done) {
            dynamokv.deleteFromTable(this.tableName, this.unexistingKey, function (err) {
                // It is not reported
                should.not.exist(err);
                done();
            });
        });
    };

    describe('Basic operations (string key, backwards compatibility)', function () {
        before(function () {
            this.tableName = "TableA";
            this.goodKey = "keyA1";
            this.unexistingKey = "keyAX";
        });
        basicOperationTests();
    });

    describe('Basic operations (only hash key, as obj)', function () {
        before(function () {
            this.tableName = "TableA";
            this.goodKey = { hash: "keyA1" };
            this.unexistingKey = { hash: "keyAX" };
        });
        basicOperationTests();
    });

    describe('Basic operations (hash and range key)', function () {
        before(function () {
            this.tableName = "TableB";
            this.goodKey = { hash: "keyC1", range: "userC1" };
            this.unexistingKey = {hash: "keyCX", range: "whatever"};
        });
        basicOperationTests();
    });

    describe('Get all from hash', function () {

        before(function (done) {
            var count = 0;
            async.whilst(
                function () { return count < 5; },
                function (cb) {
                    var key = {
                        hash: "testHash",
                        range: "testRange" + (count++)
                    };
                    dynamokv.putOnTable("TableB", key, {}, false, cb);
                }, function (err) {
                    done(err);
                });
        });

        it("Multiple values", function (done) {
            dynamokv.listOnKey("TableB", "testHash", function (err, rangeValues) {
                should.not.exist(err);
                should.exist(rangeValues);
                rangeValues.should.have.length(5);
                // { range: x, content: Y}
                //rangeValues.should.contain("testRange1");
                //rangeValues.should.contain("testRange3");
                //rangeValues.should.contain("testRange5");
                done();
            });
        });

        it("No values", function (done) {
            dynamokv.listOnKey("TableB", "randomkey", function (err, rangeValues) {
                should.not.exist(err);
                should.exist(rangeValues);
                rangeValues.should.have.length(0);
                done();
            });
        });

    });



    var listOperationTests = function () {
        it('Insert first element', function (done) {
            var that = this;
            dynamokv.appendToListOnKey(this.tableName, "listThing", this.content1, function (err) {
                should.not.exist(err);
                dynamokv.getFromTable(that.tableName, "listThing", function (err, storedList) {
                    should.not.exist(err);
                    should.exist(storedList);
                    storedList.should.have.length(1);
                    done();
                });
            });
        });

        it('Insert second element', function (done) {
            var that = this;
            dynamokv.appendToListOnKey(this.tableName, "listThing", this.content2, function (err) {
                should.not.exist(err);
                dynamokv.getFromTable(that.tableName, "listThing", function (err, storedList) {
                    should.not.exist(err);
                    should.exist(storedList);
                    storedList.should.have.length(2);
                    done();
                });
            });
        });

        it('Is element in list (positive)', function (done) {
            dynamokv.isInListOnKey(this.tableName, "listThing", this.key1, this.comparator, function (err, itIs) {
                should.not.exist(err);
                should.exist(itIs);
                itIs.should.be.ok;
                done();
            });
        });

        it('Is element in list (negative)', function (done) {
            dynamokv.isInListOnKey(this.tableName, "listThing", this.keyNotSet, this.comparator, function (err, itIs) {
                should.not.exist(err);
                should.exist(itIs);
                itIs.should.not.be.ok;
                done();
            });
        });


        it('Remove unexisting element', function (done) {
            dynamokv.removeFromListOnKey(this.tableName, "listThing", this.keyNotSet, this.comparator, function (err) {
                // Should 'Element not in list' be reported as error?
                should.not.exist(err);
                done();
            });
        });

        it('Remove first element', function (done) {
            var that = this;
            dynamokv.removeFromListOnKey(this.tableName, "listThing", this.key1, this.comparator, function (err) {
                should.not.exist(err);
                dynamokv.getFromTable(that.tableName, "listThing", function (err, storedList) {
                    should.not.exist(err);
                    should.exist(storedList);
                    storedList.should.have.length(1);
                    //storedList.should.contain("string2");
                    done();
                });
            });
        });

        it('Remove first element (empty list)', function (done) {
            var that = this;
            dynamokv.removeFromListOnKey(this.tableName, "listThing", this.key2, this.comparator, function (err) {
                should.not.exist(err);
                dynamokv.getFromTable(that.tableName, "listThing", function (err, storedList) {
                    should.exist(err);
                    err.should.have.property("notFound");
                    done();
                });
            });
        });

        it('Remove from non-existing list', function (done) {
            dynamokv.removeFromListOnKey(this.tableName, "listThing", this.key1, this.comparator, function (err) {
                should.exist(err);
                err.should.have.property("notFound");
                done();
            });
        });

        it('Is element in non existing list', function (done) {
            dynamokv.isInListOnKey(this.tableName, "listThing", this.key1, this.comparator, function (err, itIs) {
                should.not.exist(err);
                should.exist(itIs);
                itIs.should.not.be.ok;
                done();
            });
        });
    };

    describe('List values (default comparator)', function () {
        before(function () {
            this.tableName = "TableA";

            this.content1 = "string1";
            this.key1 = "string1";

            this.content2 = "string2";
            this.key2 = "string2";

            this.contentNotSet = "stringX";
            this.comparator = null;
        });

        listOperationTests();
    });

    describe('List values (custom comparator)', function () {
        before(function () {
            this.tableName = "TableA";

            // Real content in the list
            this.content1 = { a: "string1"};
            // Key that we use to search in the list
            this.key1 = "string1";

            this.content2 = { a: "string2"};
            this.key2 = "string2";

            this.contentNotSet = { a: "stringX"};
            this.keyNotSet = "stringX";

            this.comparator = function (itemInList, value) {
                return itemInList.a === value;
            };
        });

        listOperationTests();
    });

    after(function (done) {
        dynamo.stop(done);
    });
});

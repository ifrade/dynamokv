/* global describe, it, before, after */
/* global afterEach, beforeEach */
/* jshint node: true*/
'use strict';

var should = require('should');
var fs = require('fs');

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


    it('Insert key', function (done) {
        dynamokv.putOnTable("TableA", "keyA1", { a: 1, b: 2}, false, function (err) {
            should.not.exist(err);
            dynamokv.getFromTable("TableA", "keyA1", function (err, data) {
                should.not.exist(err);
                should.exist(data);
                data.should.have.property("a").and.equal(1);
                data.should.have.property("b").and.equal(2);
                done();
            });
        });
    });

    it('Update key', function (done) {
        dynamokv.putOnTable("TableA", "keyA1", { a: 2, b: 3}, true, function (err) {
            should.not.exist(err);
            dynamokv.getFromTable("TableA", "keyA1", function (err, data) {
                should.not.exist(err);
                should.exist(data);
                data.should.have.property("a").and.equal(2);
                data.should.have.property("b").and.equal(3);
                done();
            });
        });
    });

    it('Failed insert (key exists and no overwrite)', function (done) {
        dynamokv.putOnTable("TableA", "keyA1", { a: "fail"}, false, function (err) {
            should.exist(err);
            done();
        });
    });


    it('Delete key', function (done) {
        dynamokv.deleteFromTable("TableA", "keyA1", function (err) {
            should.not.exist(err);
            done();
        });
    });

    it('Delete unexisting key', function (done) {
        dynamokv.deleteFromTable("TableA", "keyX1", function (err) {
            // It is not reported
            should.not.exist(err);
            done();
        });
    });



    describe('List values (default comparator)', function () {
        it('Insert first element', function (done) {
            dynamokv.appendToListOnKey("TableA", "listThing", "string1", function (err) {
                should.not.exist(err);
                dynamokv.getFromTable("TableA", "listThing", function (err, storedList) {
                    should.not.exist(err);
                    should.exist(storedList);
                    storedList.should.have.length(1);
                    done();
                });
            });
        });

        it('Insert second element', function (done) {
            dynamokv.appendToListOnKey("TableA", "listThing", "string2", function (err) {
                should.not.exist(err);
                dynamokv.getFromTable("TableA", "listThing", function (err, storedList) {
                    should.not.exist(err);
                    should.exist(storedList);
                    storedList.should.have.length(2);
                    done();
                });
            });
        });

        it('Is element in list (positive)', function (done) {
            dynamokv.isInListOnKey("TableA", "listThing", "string1", null, function (err, itIs) {
                should.not.exist(err);
                should.exist(itIs);
                itIs.should.be.ok;
                done();
            });
        });

        it('Is element in list (negative)', function (done) {
            dynamokv.isInListOnKey("TableA", "listThing", "stringX", null, function (err, itIs) {
                should.not.exist(err);
                should.exist(itIs);
                itIs.should.not.be.ok;
                done();
            });
        });


        it('Remove unexisting element', function (done) {
            dynamokv.removeFromListOnKey("TableA", "listThing", "string3", null, function (err) {
                // Should 'Element not in list' be reported as error?
                should.not.exist(err);
                done();
            });
        });

        it('Remove first element', function (done) {
            dynamokv.removeFromListOnKey("TableA", "listThing", "string1", null, function (err) {
                should.not.exist(err);
                dynamokv.getFromTable("TableA", "listThing", function (err, storedList) {
                    should.not.exist(err);
                    should.exist(storedList);
                    storedList.should.have.length(1);
                    storedList.should.contain("string2");
                    done();
                });
            });
        });

        it('Remove first element (empty list)', function (done) {
            dynamokv.removeFromListOnKey("TableA", "listThing", "string2", null, function (err) {
                should.not.exist(err);
                dynamokv.getFromTable("TableA", "listThing", function (err, storedList) {
                    should.exist(err);
                    err.should.have.property("notFound");
                    done();
                });
            });
        });

        it('Remove from non-existing list', function (done) {
            dynamokv.removeFromListOnKey("TableA", "listThing", "string3", null, function (err) {
                should.exist(err);
                err.should.have.property("notFound");
                done();
            });
        });

        it('Is element in non existing list', function (done) {
            dynamokv.isInListOnKey("TableA", "listThing", "stringX", null, function (err, itIs) {
                should.not.exist(err);
                should.exist(itIs);
                itIs.should.not.be.ok;
                done();
            });
        });

    });



    describe('List values (custom comparator)', function () {
        it('Insert first element', function (done) {
            dynamokv.appendToListOnKey("TableA", "listThing", {a: "string1"}, function (err) {
                should.not.exist(err);
                dynamokv.getFromTable("TableA", "listThing", function (err, storedList) {
                    should.not.exist(err);
                    should.exist(storedList);
                    storedList.should.have.length(1);
                    done();
                });
            });
        });

        it('Insert second element', function (done) {
            dynamokv.appendToListOnKey("TableA", "listThing", {a: "string2"}, function (err) {
                should.not.exist(err);
                dynamokv.getFromTable("TableA", "listThing", function (err, storedList) {
                    should.not.exist(err);
                    should.exist(storedList);
                    storedList.should.have.length(2);
                    done();
                });
            });
        });

        var comparator = function (itemInList, value) {
            return itemInList.a === value;
        };

        it('Is element in list (positive)', function (done) {
            dynamokv.isInListOnKey("TableA", "listThing", "string1", comparator, function (err, itIs) {
                should.not.exist(err);
                should.exist(itIs);
                itIs.should.be.ok;
                done();
            });
        });

        it('Is element in list (negative)', function (done) {
            dynamokv.isInListOnKey("TableA", "listThing", "stringX", comparator, function (err, itIs) {
                should.not.exist(err);
                should.exist(itIs);
                itIs.should.not.be.ok;
                done();
            });
        });

        it('Remove unexisting element', function (done) {
            dynamokv.removeFromListOnKey("TableA", "listThing", "string3", comparator, function (err) {
                // Element not in list should be an error?
                should.not.exist(err);
                done();
            });
        });

        it('Remove first element', function (done) {
            dynamokv.removeFromListOnKey("TableA", "listThing", "string1", comparator, function (err) {
                should.not.exist(err);
                dynamokv.getFromTable("TableA", "listThing", function (err, storedList) {
                    should.not.exist(err);
                    should.exist(storedList);
                    storedList.should.have.length(1);
                    storedList[0].should.have.property("a");
                    storedList[0]["a"].should.equal("string2");
                    done();
                });
            });
        });

        it('Remove first element (empty list)', function (done) {
            dynamokv.removeFromListOnKey("TableA", "listThing", "string2", comparator, function (err) {
                should.not.exist(err);
                dynamokv.getFromTable("TableA", "listThing", function (err, storedList) {
                    should.exist(err);
                    err.should.have.property("notFound");
                    done();
                });
            });
        });

        it('Remove from non-existing list', function (done) {
            dynamokv.removeFromListOnKey("TableA", "listThing", "string3", comparator, function (err) {
                should.exist(err);
                err.should.have.property("notFound");
                done();
            });
        });

        it('Is element in non existing list', function (done) {
            dynamokv.isInListOnKey("TableA", "listThing", "stringX", comparator, function (err, itIs) {
                should.not.exist(err);
                should.exist(itIs);
                itIs.should.not.be.ok;
                done();
            });
        });

    });



    after(function (done) {
        dynamo.stop(done);
    });
});

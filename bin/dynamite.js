#!/usr/bin/env node

"use strict";

var fs = require('fs');
var async = require('async');
var DynamoKV = require(__dirname + '/../lib/DynamoKV.js');

var optimist = require('optimist')
    .usage('Manage tables in a dynamo DB instance.\nUsage: $0')

    .boolean('l', 'list')
    .alias('l', 'list')
    .describe('l', 'List the tables in the dynamo (with the defined prefix)')
    .default('l', false)

    .boolean('s')
    .alias('s', 'status')
    .describe('s', 'Print the status of the tables (active, creating, deleting...)')
    .default('s', false)

    .boolean('c')
    .alias('c', 'create')
    .describe('c', 'Create tables in the dynamo')
    .default('c', false)

    .boolean('e')
    .alias('e', 'empty')
    .describe('e', 'Remove all contents in the databases')
    .default('e', false)

    .describe('t', 'file containing the table description')
    .alias('t', 'tables')
    .boolean('i')
    .alias('i', 'ignore')
    .describe('i', 'Ignore errors and always exit ok.')
    .default('i', false)

    .boolean('h')
    .alias('h', 'help')
    .describe('h', 'Print this help')

    .string('p')
    .describe('p', 'prefix for the tables')
    .alias('p', 'prefix')
    .default('p', 'test-te');


var argv = optimist.argv;


if (argv.h) {
    optimist.showHelp();
    process.exit(0);
}

if (!argv.l && !argv.s && !argv.c && !argv.e) {
    argv.l = true;
}

var tableDefinitions = {};
if (argv.c) {
    if (!argv.t) {
        console.log("[-t tableDesc] option mandatory for this operation");
        process.exit(argv.i ? 0 : -1);
    }
    tableDefinitions = JSON.parse(fs.readFileSync(argv.t).toString());
}

DynamoKV.createDynamoKV(argv.p, tableDefinitions, function (err, dynamo) {
    if (err) {
        console.log(err);
        return;
    }

    var dynamoKV = dynamo;

    if (argv.l) {
        dynamoKV.listTables(function (err, tableList) {
            if (err) {
                console.log(err);
                process.exit(argv.i ? 0 : -1);
            }

            console.log("Tables in dynamo:");
            if (!tableList || tableList.length === 0) {
                console.log("  (no tables)");
                process.exit(0);
            }

            tableList.forEach(function (i, pos) {
                console.log("  ", pos, ".", i);
            });
            process.exit(0);
        });
    }

    if (argv.s) {
        dynamoKV.checkTables(function (err, allOk, data) {
            if (err) {
                console.log(err);
                process.exit(argv.i ? 0 : -1);
            }
            
            if (allOk) {
                console.log("  All tables active");
                process.exit(0);
            }

            data.forEach(function (item) {
                console.log("  ", item.TableName, " - ", item.TableStatus);
            });

            process.exit(0);
        });
    }

    if (argv.c) {
        console.log("Creating tables");
        dynamoKV.createTables(function (err) {
            if (err) {
                console.log(err);
                process.exit(-1);
            }

            console.log("Ok");
            process.exit(0);
        });
    }

    if (argv.e) {
        console.log("Emptying tables");
        dynamoKV.emptyTables(function (err) {
            if (err) {
                console.log(err);
                process.exit(argv.i ? 0 : -1);
            }
            console.log("Ok");
            process.exit(0);
        });
    }
});

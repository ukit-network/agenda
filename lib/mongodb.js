'use strict';

var mongodb = require('mongodb'),
    util = require('util'),

    ignoreErrors = function () {
    },

    DataConnector = function (config) {

        var url, options, self;

        if (!(this instanceof DataConnector)) {
            return new DataConnector(config);
        }

        self = this;

        self.config = config ? config : {};


        self.collectionName = config.collection || 'agendaJobs';
        url = config.url || 'mongodb://localhost:27017/test';

        if (!url.match(/^mongodb:\/\/.*/)) {
            url = 'mongodb://' + url;
        }

        options = self.config.options || {w: 0, native_parser: true};

        self.mongo = 'initialising';

        mongodb.connect(url, options, function (err, db) {

            if (!err && db) {

                self.initialization(db);

            } else {

                throw err || new Error('Could not connect to Agenda DB');

            }
        });

    };


DataConnector.prototype.initialization = function (db) {

    this.mongo = 'initialised';

    this.db = db;

    this.config.mongo = this.collection = this.db.collection(this.collectionName);

    this.collection.ensureIndex("nextRunAt", ignoreErrors);
    this.collection.ensureIndex("lockedAt", ignoreErrors);
    this.collection.ensureIndex("name", ignoreErrors);
    this.collection.ensureIndex("priority", ignoreErrors);

    console.log('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');
    console.log('THIS-> ' + util.inspect(this, {showHidden: true, depth: 3, colors: true}));
    console.log('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');

};


// ------------------------------------------- AGENDA CALLS ------------------------------------------ //



DataConnector.prototype.insert = function () {
    this.collection.insert.apply(this.connection, arguments);
};
DataConnector.prototype.update = function () {
    this.collection.update.apply(this.connection, arguments);
};
DataConnector.prototype.remove = function () {
    var col;

    console.log('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');
    console.log('THIS-> ' + util.inspect(this, {showHidden: true, depth: 10, colors: true}));
    console.log('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');
    //this.collection.remove.apply(this.connection, arguments);

    //col = this.db.collection(this.collectionName);

    //console.log('COL-> ', inspect(col));
};
DataConnector.prototype.findItems = function () {
    this.collection.findItems.apply(this.connection, arguments);
};
DataConnector.prototype.findAndModify = function () {

    console.log('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');
    console.log('THIS-> ' + util.inspect(this, {showHidden: true, depth: 10, colors: true}));
    console.log('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');

    this.collection.findAndModify.apply(this.connection, arguments);
};


// ----------------------------------------- END AGENDA CALLS ---------------------------------------- //

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ //

// ----------------------------------------- AGENDA UI CALLS ----------------------------------------- //

DataConnector.prototype.count = function () {
    this.collection.count.apply(this.connection, arguments);
};

// --------------------------------------- END AGENDA UI CALLS --------------------------------------- //


module.exports = DataConnector;
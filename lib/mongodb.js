'use strict';

var mongodb = require('mongodb'),
    async = require('async'),
    util = require('util'),

    ignoreErrors = function () {
    },

    DataConnector = function (config, cb) {

        var url, options, self;

        cb = cb || function(){};

        if (!(this instanceof DataConnector)) {
            return new DataConnector(config);
        }

        self = this;

        self.cb = typeof cb === 'function' ? cb : function(){};

        self.pid = process.pid;

        self.config = config ? config : {};


        self.collectionName = config.db.collection || 'agendaJobs';
        url = config.db.url || 'mongodb://localhost:27017/test';

        if (!url.match(/^mongodb:\/\/.*/)) {
            url = 'mongodb://' + url;
        }

        options = self.config.options || {w: 0, native_parser: true};

        self.mongo = 'initialising';

        mongodb.connect(url, options, function (err, db) {

            if (!err && db) {

                self.initialization(db, cb);

            } else {

                throw err || new Error('Could not connect to Agenda DB');

            }
        });

    };


DataConnector.prototype.initialization = function (db, cb) {

    var self = this;

    self.mongo = 'initialised';

    self.db = db;

    inspect('DB OBJECT', self.db);

    self.collection = self.config.mongo = self.db.collection(self.collectionName);

    async.parallel([
        function(acb){self.collection.createIndex("nextRunAt", acb);},
        function(acb){self.collection.createIndex("lockedAt", acb);},
        function(acb){self.collection.createIndex("name", acb);},
        function(acb){self.collection.createIndex("priority", acb);}
    ], function(err, data){
        console.log('Response from creating index: ', util.inspect({err: err, data: data}, {showHidden: true, depth: 10, colors: true}));
        cb();
    });

};


// ------------------------------------------- AGENDA CALLS ------------------------------------------ //



DataConnector.prototype.insert = function () {
    return this.collection.insert.apply(this.collection, arguments);
};
DataConnector.prototype.update = function () {
    return this.collection.update.apply(this.collection, arguments);
};
DataConnector.prototype.remove = function () {
    return this.collection.remove.apply(this.collection, arguments);
};
DataConnector.prototype.findItems = function () {
    return this.collection.findItems.apply(this.collection, arguments);
};
DataConnector.prototype.findAndModify = function () {
    return this.collection.findAndModify.apply(this.collection, arguments);
};


// ----------------------------------------- END AGENDA CALLS ---------------------------------------- //

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ //

// ----------------------------------------- AGENDA UI CALLS ----------------------------------------- //

DataConnector.prototype.count = function () {
    return this.collection.count.apply(this.collection, arguments);
    /*
    console.log('[' + query.name + '] - Request for Count: ', util.inspect(query, {showHidden: true, depth: 10, colors: true}));
    // this.collection.count.apply(this.collection, arguments);
    this.collection.count(query, function(err, data){
        console.log('[' + query.name + '] - Response from Count: ', util.inspect({err: err, data: data}, {showHidden: true, depth: 10, colors: true}));
        cb(data);
    });
    */
};

// --------------------------------------- END AGENDA UI CALLS --------------------------------------- //


module.exports = DataConnector;
'use strict';

var mongoSkin = require('mongoskin'),
    util = require('util'),
    ignoreErrors = function () {
    },

    DataConnector = module.exports = function (config) {

        var url, collection, options, replSet;

        if (!(this instanceof DataConnector)) {
            return new DataConnector(config);
        }

        this.config = config ? config : {};

        if (this.config.mongo) {

            this.connection = this.config.mongo;

        } else {

            this.collection = config.collection || 'agendaJobs';
            url = config.url || 'mongodb://localhost:27017/test';

            if (!url.match(/^mongodb:\/\/.*/)) {
                url = 'mongodb://' + url;
            }

            options = this.config.options || {w: 0, native_parser:true};

            if(this.config.replSet){

                var replSet = new ReplSetServers(this.config.replSet);

                this.db = new mongoSkin.Db(url, replSet, options);

                this.config.mongo = this.connection = this.db.collection(this.collection);

            } else {

                this.db = mongoSkin.db(url, options);

                this.config.mongo = this.connection = this.db.collection(this.collection);

            }

            console.log('THIS->' + util.inspect(this));

            this.db.collection(this.collection).ensureIndex("nextRunAt", ignoreErrors)
                .ensureIndex("lockedAt", ignoreErrors)
                .ensureIndex("name", ignoreErrors)
                .ensureIndex("priority", ignoreErrors);


        }

    };

// ------------------------------------------- AGENDA CALLS ------------------------------------------ //

DataConnector.prototype.insert = function () {
    this.db.collection(this.collection).insert.apply(this.connection, arguments);
};
DataConnector.prototype.update = function () {
    this.db.collection(this.collection).update.apply(this.connection, arguments);
};
DataConnector.prototype.remove = function () {
    this.db.collection(this.collection).remove.apply(this.connection, arguments);
};
DataConnector.prototype.findItems = function () {
    this.db.collection(this.collection).findItems.apply(this.connection, arguments);
};
DataConnector.prototype.findAndModify = function () {
    this.db.collection(this.collection).findAndModify.apply(this.connection, arguments);
};


// ----------------------------------------- END AGENDA CALLS ---------------------------------------- //

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ //

// ----------------------------------------- AGENDA UI CALLS ----------------------------------------- //

DataConnector.prototype.count = function () {
    this.db.collection(this.collection).count.apply(this.connection, arguments);
};

// --------------------------------------- END AGENDA UI CALLS --------------------------------------- //

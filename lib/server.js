/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var assert = require('assert-plus');
var artedi = require('artedi');
var events = require('events');
var fast = require('fast');
var fs = require('fs');
var kang = require('kang');
var net = require('net');
var os = require('os');
var restify = require('restify');
var util = require('util');
var uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');

var boray_client = require('./boray_client');
var dtrace = require('./dtrace');
var errors = require('./errors');
var data_placement = require('./data_placement');

var InvocationError = errors.InvocationError;

var KANG_VERSION = '1.2.0';
var BORAY_LIMIT = 1000;

var CB_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'name', type: 'string' }
];

var DB_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'name', type: 'string' }
];

var LB_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'sorted', type: 'boolean' },
    { name: 'order_by', type: 'string' },
    { name: 'prefix', type: 'string' },
    { name: 'limit', type: 'number' }
];

var GB_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'name', type: 'string' }
];

var GO_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'bucket_id', type: 'string' },
    { name: 'name', type: 'string' }
];

var CO_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'bucket_id', type: 'string' },
    { name: 'name', type: 'string' },
    { name: 'content_length', type: 'number' },
    { name: 'content_md5', type: 'string' },
    { name: 'content_type', type: 'string' },
    { name: 'headers', type: 'object' },
    { name: 'sharks', type: 'object' },
    { name: 'properties', type: 'object' }
];

var LO_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'bucket_id', type: 'string' },
    { name: 'sorted', type: 'boolean' },
    { name: 'order_by', type: 'string' },
    { name: 'prefix', type: 'string' },
    { name: 'limit', type: 'number' }
];

util.inherits(LimitOffsetStream, events.EventEmitter);
function LimitOffsetStream(opts) {
    var self = this;

    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.number(opts.limit, 'opts.limit');
    assert.func(opts.getStream, 'opts.getStream');

    self.offset = 0;
    self.limit = opts.limit;
    self.done = false;
    self.log = opts.log;

    self.getStream = opts.getStream;
}

LimitOffsetStream.prototype._getNewStream = function _getNewStream(cb) {
    var self = this;

    assert.func(cb, 'cb');

    self.log.debug({
        offset: self.offset,
        limit: self.limit
    }, 'calling getStream(offset=%d, limit=%d)',
        self.offset,
        self.limit);

    if (self.res) {
        self.res.removeAllListeners();
    }

    self.res = self.getStream(self.offset, self.limit);
    self.numRecords = 0;
    self.resEnded = false;
    self.recordPending = false;

    self.res.on('end', function () {
        self.log.debug('getNewStream ended');
        self.resEnded = true;
    });

    self.res.on('error', function (err) {
        self.log.error(err, 'getNewStream error');
        self.emit('error', err);
    });
};

LimitOffsetStream.prototype.getNextRecord =
    function getNextRecord(cb, skipCheck) {

    var self = this;

    assert.func(cb, 'cb');
    assert.optionalBool(skipCheck, skipCheck);

    if (!skipCheck) {
        assert(!self.recordingPending, 'self.recordPending');
        self.recordPending = true;
    }

    if (!self.res) {
        // hasn't opened initial stream yet
        self.log.debug('requesting initial stream');
        self._getNewStream(cb);
        setImmediate(function () {
            self.getNextRecord(cb, true);
        });
        return;
    }

    var record = self.res.read();

    if (record) {
        self.log.trace({record: record}, 'record available - sending');
        sendRecord(record);
        return;
    }

    if (self.resEnded) {
        self.log.debug('self.resEnded is true');
        if (self.numRecords === self.limit) {
            self.log.debug('requesting new stream');
            self.offset += self.numRecords;
            self._getNewStream(cb);
            setImmediate(function () {
                self.getNextRecord(cb, true);
            });
            return;
        }

        self.log.debug('no more pagination required');
        self.done = true;
    }

    if (self.done) {
        self.log.debug('self.done is true, sending final event');
        cb(null, true);
        return;
    }

    self.log.debug('attaching to readable and end events');

    self.res.on('readable', tryRead);
    self.res.on('end', tryRead);
    var done = false;

    function tryRead() {
        if (done) {
            return;
        }

        self.log.debug('detaching readable and end events');

        done = true;
        self.removeListener('readable', tryRead);
        self.removeListener('end', tryRead);

        setImmediate(function () {
            self.getNextRecord(cb, true);
        });
    }

    function sendRecord(_record) {
        assert.object(_record, '_record');

        setImmediate(function () {
            self.numRecords++;
            self.recordPending = false;
            cb(_record, false);
        });
    }
};

function createServer(options, callback) {
    assert.object(options, 'options');
    assert.optionalObject(options.fast, 'options.fast');
    assert.optionalNumber(options.fast.client_crc_mode,
        'options.fast.client_crc_mode');
    assert.optionalNumber(options.fast.server_crc_mode,
        'options.fast.server_crc_mode');
    assert.func(callback, 'callback');

    var log = options.log;
    var opts = {
        log: options.log
    };

    // remove ready flag
    log.info('server.createServer: removing ready cookie on startup');
    try {
        fs.unlinkSync('/var/tmp/electric-boray-ready');
    } catch (e) {
        // ignore failures if file DNE
    }

    data_placement.createDataDirector({
        log: options.log
    }, function (err, dataDirector) {
        if (err) {
            throw new verror.VError(err, 'unable to instantiate data director');
        }

        opts.dataDirector = dataDirector;

        var client_crc_mode = options.fast.client_crc_mode ||
            fast.FAST_CHECKSUM_V1;
        var server_crc_mode = options.fast.server_crc_mode ||
            fast.FAST_CHECKSUM_V1;

        log.info('creating boray clients');
        boray_client.createBucketClient({
            pnodes: opts.dataDirector.getPnodes(),
            borayOptions: options.borayOptions,
            log: options.log,
            crc_mode: client_crc_mode
        }, function (cErr, clients) {
            if (cErr) {
                throw new verror.VError(cErr, 'unable to create boray clients');
            }

            opts.clients = clients;
            // opts.indexShards = options.ringCfg.indexShards;

            var labels = {
                datacenter: options.datacenter,
                server: options.server_uuid,
                zonename: os.hostname(),
                pid: process.pid
            };

            var collector = artedi.createCollector({
                labels: labels
            });

            collector.gauge({
                name: 'client_crc_mode',
                help: 'The node-fast CRC compatibilty mode of the Fast client'
            }).set(client_crc_mode);

            collector.gauge({
                name: 'server_crc_mode',
                help: 'The node-fast CRC compatibilty mode of the Fast server'
            }).set(server_crc_mode);

            var socket = net.createServer({ 'allowHalfOpen': true });
            var server = new fast.FastServer({
                collector: collector,
                log: log.child({ component: 'fast' }),
                server: socket,
                crc_mode: server_crc_mode
            });

            var methods = [
                { rpcmethod: 'getbucket', rpchandler: getBucket(opts) },
                { rpcmethod: 'createbucket', rpchandler: createBucket(opts) },
                { rpcmethod: 'deletebucket', rpchandler: deleteBucket(opts) },
                { rpcmethod: 'listbuckets', rpchandler: listBuckets(opts) },
                { rpcmethod: 'getobject', rpchandler: getObject(opts) },
                { rpcmethod: 'createobject', rpchandler: createObject(opts) },
                { rpcmethod: 'deleteobject', rpchandler: deleteObject(opts) },
                { rpcmethod: 'listobjects', rpchandler: listObjects(opts) }
            ];

            methods.forEach(function (rpc) {
                server.registerRpcMethod(rpc);
            });

            var kangOpts = {
                service_name: 'electric-boray',
                version: KANG_VERSION,
                uri_base: '/kang',
                ident: os.hostname + '/' + process.pid,
                list_types: server.kangListTypes.bind(server),
                list_objects: server.kangListObjects.bind(server),
                get: server.kangGetObject.bind(server),
                stats: server.kangStats.bind(server)
            };

            var monitorServer = restify.createServer({
                name: 'Monitor'
            });

            monitorServer.get('/kang/.*', kang.knRestifyHandler(kangOpts));

            monitorServer.get('/metrics',
                function getMetricsHandler(req, res, next) {
                    req.on('end', function () {
                        assert.ok(collector, 'collector');
                        collector.collect(artedi.FMT_PROM,
                            function (cerr, metrics) {
                                if (cerr) {
                                    next(new verror.VError(err));
                                    return;
                                }
                                res.setHeader('Content-Type',
                                    'text/plain; version 0.0.4');
                                res.send(metrics);
                        });
                        next();
                    });
                    req.resume();
            });

            monitorServer.listen(options.monitorPort, options.bindip,
                function () {
                    log.info('monitor server started on port %d',
                        options.monitorPort);
            });

            socket.on('listening', function () {
                log.info('boray listening on %d', options.port);
                callback(null, {
                    dataDirector: opts.dataDirector,
                    // ring: opts.ring,
                    clientList: Object.keys(opts.clients.map)
                });
            });

            socket.on('error', function (serr) {
                log.error(serr, 'server error');
            });

            socket.listen(options.port, options.bindip);
        });
    });
}

function invalidArgs(rpc, argv, types) {
    var route = rpc.methodName();
    var len = types.length;

    if (argv.length !== len) {
        rpc.fail(new InvocationError(
            '%s expects %d argument%s %d',
            route, len, len === 1 ? '' : 's', argv.length));
        return true;
    }

    for (var i = 0; i < len; i++) {
        var name = types[i].name;
        var type = types[i].type;
        var val = argv[i];

        // 'array' is not a primitive type in javascript, but certain
        // rpcs expect them. Since typeof ([]) === 'object', we need to
        // special case this check to account for these expectations.
        if (type === 'array') {
            if (!Array.isArray(val)) {
                rpc.fail(new InvocationError('%s expects "%s" (args[%d]) to ' +
                            'be of type array but received type %s instead',
                            route, name, i, typeof (val)));
                return true;
            }
            continue;
        }

        if (type === 'object' && val === null) {
            rpc.fail(new InvocationError('%s expects "%s" (args[%d]) to ' +
                        'be an object but received the value "null"', route,
                        name, i));
            return true;
        }

        if (typeof (argv[i]) !== types[i].type) {
            rpc.fail(new InvocationError('%s expects "%s" (args[%d]) to be ' +
                'of type %s but received type %s instead (%j)', route, name, i,
                type, typeof (val), val));
            return true;
        }
    }

    return false;
}


function createBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _createBucket(rpc) {
        var msgid = rpc.requestId();
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, CB_ARGS_SCHEMA)) {
            return;
        }

        var owner = argv[0];
        var bucket = argv[1];

        var id = options.req_id || uuid.v1();

        dtrace['createbucket-start'].fire(function () {
            return ([msgid, id, owner, bucket]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: owner,
            bucket: bucket
        }, 'createBucket: entered');

        options.dataDirector.getBucketLocation(owner, bucket,
            function (err, location) {
            if (err) {
                rpc.fail(err);
                return;
            }

            var vnode = location.vnode;
            var pnode = location.pnode;
            log.info('pnode: ' + pnode);
            var client = options.clients.map[pnode];

            log.info('client: ' + client);

            client.createBucket(owner, bucket, vnode, function (pErr, meta) {
                log.debug({
                    err: pErr,
                    meta: meta
                }, 'createBucket: returned');

                dtrace['createbucket-done'].fire(function () {
                    return ([msgid]);
                });

                if (pErr) {
                    rpc.fail(pErr);
                } else {
                    // Add shard information to the response.
                    meta._node = location;

                    rpc.write(meta);
                    rpc.end();
                }
            });
        });
    }

    return _createBucket;
}


function getBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _getBucket(rpc) {
        var msgid = rpc.requestId();
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, GB_ARGS_SCHEMA)) {
            return;
        }

        var owner = argv[0];
        var bucket = argv[1];

        var id = options.req_id || uuid.v1();

        dtrace['getbucket-start'].fire(function () {
            return ([msgid, id, owner, bucket]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: owner,
            bucket: bucket
        }, 'getBucket: entered');

        options.dataDirector.getBucketLocation(owner, bucket,
            function (err, location) {
            if (err) {
                rpc.fail(err);
                return;
            }

            var vnode = location.vnode;
            var pnode = location.pnode;

            options.clients.map[pnode].getBucket(owner, bucket, vnode,
                function (gErr, rbucket) {
                log.debug({
                    err: gErr,
                    bucket: rbucket
                }, 'getBucket: returned');

                dtrace['getbucket-done'].fire(function () {
                    return ([msgid]);
                });

                /*
                 * serialize the deserialized bucket response. To make this
                 * faster, we could:
                 * 1) modify the boray client to make deserializing optional.
                 * 2) directly hook up the streams by modifying the underlying
                 * node-fast stream.
                 */
                if (gErr) {
                    rpc.fail(gErr);
                } else {
                    rpc.write(rbucket);
                    rpc.end();
                }
            });
        });
    }

    return _getBucket;
}


function deleteBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _deleteBucket(rpc) {
        var msgid = rpc.requestId();
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, DB_ARGS_SCHEMA)) {
            return;
        }

        var owner = argv[0];
        var bucket = argv[1];

        var id = options.req_id || uuid.v1();

        dtrace['deletebucket-start'].fire(function () {
            return ([msgid, id, owner, bucket]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: owner,
            bucket: bucket
        }, 'deleteBucket: entered');

        options.dataDirector.getBucketLocation(owner, bucket,
            function (err, location) {
            if (err) {
                rpc.fail(err);
                return;
            }

            var vnode = location.vnode;
            var pnode = location.pnode;

            options.clients.map[pnode].deleteBucket(owner, bucket, vnode,
                function (gErr, rbucket) {
                log.debug({
                    err: gErr,
                    bucket: rbucket
                }, 'deleteBucket: returned');

                dtrace['deletebucket-done'].fire(function () {
                    return ([msgid]);
                });

                /*
                 * serialize the deserialized bucket response. To make this
                 * faster, we could:
                 * 1) modify the boray client to make deserializing optional.
                 * 2) directly hook up the streams by modifying the underlying
                 * node-fast stream.
                 */
                if (gErr) {
                    rpc.fail(gErr);
                } else {
                    // rpc.write(rbucket);
                    rpc.end();
                }
            });
        });
    }

    return _deleteBucket;
}

function listBuckets(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _listBuckets(rpc) {
        var msgid = rpc.requestId();
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, LB_ARGS_SCHEMA)) {
            return;
        }

        var owner = argv[0];
        var sorted = argv[1];
        var order_by = argv[2];
        var prefix = argv[3];
        var limit = argv[4];

        if (limit === 0) {
            limit = Infinity;
        }

        var id = options.req_id || uuid.v1();

        dtrace['listbuckets-start'].fire(function () {
            return ([msgid, id, owner, sorted, order_by, prefix, limit]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: owner,
            sorted: sorted,
            order_by: order_by,
            prefix: prefix,
            limit: limit
        }, 'listBuckets: entered');

        var nodes = options.dataDirector.getAllNodes();
        var vnodes = {};
        var totalVnodes = nodes.length;

        // Find an appropriate limit to use with boray
        var borayLimit = Math.ceil(limit / totalVnodes);
        borayLimit = Math.min(borayLimit, BORAY_LIMIT);

        log.debug('%d vnodes found total, want %d records, using limit of %d',
            totalVnodes, limit, borayLimit);

        // Create a mapping of vnodes to pnodes.
        nodes.forEach(function (node) {
            var client = options.clients.map[node.pnode];
            assert.object(client, 'client for pnode: ' + node.pnode);

            vnodes[node.vnode] = {
                lostream: new LimitOffsetStream({
                    limit: borayLimit,
                    log: log.child({vnode: node.vnode}),
                    getStream: function (offset, _limit) {
                        return client.listBuckets(owner, order_by,
                            prefix, _limit, offset, node.vnode);
                    }
                }),
                record: null
            };
        });

        var opts = {
            limit: limit,
            prefix: prefix,
            order_by: order_by,
            log: log,
            vnodes: vnodes,
            sorted: sorted
        };
        paginationStream(opts,
            function onRecord(record) {
                dtrace['listbuckets-record'].fire(function () {
                    return ([msgid]);
                });
                rpc.write(formatRecord(record));
            },
            function done(err) {
                if (err) {
                    log.error(err, 'listBuckets error');
                    rpc.fail(err);
                    return;
                }

                log.debug('listBuckets done');

                dtrace['listbuckets-done'].fire(function () {
                    return ([msgid]);
                });

                rpc.end();
            });

        function formatRecord(record) {
            assert.object(record, 'record');
            assert.string(record.name, 'record.name');
            assert.date(record.created, 'record.created');

            /*
             * name: r.key.split('/').pop(),
             * etag: r.value.etag,
             * size: r.value.contentLength,
             * type: r.value.type,
             * contentType: r.value.contentType,
             * contentMD5: r.value.contentMD5,
             * mtime: new Date(r.value.mtime).toISOString()
             */
            var obj = {
                key: record.name,
                value: {
                    type: 'bucket',
                    mtime: record.created
                }
            };
            return obj;
        }
    }

    return _listBuckets;
}


function createObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _createObject(rpc) {
        var msgid = rpc.requestId();
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, CO_ARGS_SCHEMA)) {
            return;
        }

        var owner = argv[0];
        var bucket = argv[1];
        var key = argv[2];
        var content_length = argv[3];
        var content_md5 = argv[4];
        var content_type = argv[5];
        var headers = argv[6];
        var sharks = argv[7];
        var props = argv[8];

        var id = options.req_id || uuid.v1();

        dtrace['createobject-start'].fire(function () {
            return ([msgid, id, owner, bucket, key]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: owner,
            bucket: bucket,
            key: key
        }, 'createObject: entered');

        options.dataDirector.getObjectLocation(owner, bucket, key,
            function (err, location) {
            if (err) {
                rpc.fail(err);
                return;
            }

            var vnode = location.vnode;
            var pnode = location.pnode;
            var client = options.clients.map[pnode];

            if (props.constructor === Object &&
                Object.keys(props).length === 0) {

                props = null;
            }

            client.createObject(owner, bucket, key, content_length,
                content_md5, content_type, headers, sharks, props, vnode,
                function (pErr, meta) {
                log.debug({
                    err: pErr,
                    meta: meta
                }, 'createObject: returned');

                dtrace['createobject-done'].fire(function () {
                    return ([msgid]);
                });

                if (pErr) {
                    rpc.fail(pErr);
                } else {
                    // Add shard information to the response.
                    meta._node = location;

                    rpc.write(meta);
                    rpc.end();
                }
            });
        });
    }

    return _createObject;
}


function getObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _getObject(rpc) {
        var msgid = rpc.requestId();
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, GO_ARGS_SCHEMA)) {
            return;
        }

        var owner = argv[0];
        var bucket = argv[1];
        var key = argv[2];

        var id = options.req_id || uuid.v1();

        dtrace['getobject-start'].fire(function () {
            return ([msgid, id, owner, bucket, key]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: owner,
            bucket: bucket,
            key: key
        }, 'getObject: entered');

        options.dataDirector.getObjectLocation(owner, bucket, key,
            function (err, location) {

            if (err) {
                rpc.fail(err);
                return;
            }

            var pnode = location.pnode;
            var vnode = location.vnode;
            var client = options.clients.map[pnode];

            client.getObject(owner, bucket, key, vnode, function (gErr, obj) {
                log.debug({
                    err: gErr,
                    obj: obj
                }, 'getObject: returned');

                // MANTA-1400: set the vnode info for debugging purposes
                if (obj) {
                    obj._node = location;
                }

                log.debug({
                    obj: obj
                }, 'sanitized object');

                dtrace['getobject-done'].fire(function () {
                    return ([msgid, obj]);
                });

                if (gErr) {
                    rpc.fail(gErr);
                } else {
                    rpc.write(obj);
                    rpc.end();
                }
            });
        });
    }

    return _getObject;
}

function deleteObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _deleteObject(rpc) {
        var msgid = rpc.requestId();
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, GO_ARGS_SCHEMA)) {
            return;
        }

        var owner = argv[0];
        var bucket = argv[1];
        var key = argv[2];

        var id = options.req_id || uuid.v1();

        dtrace['deleteobject-start'].fire(function () {
            return ([msgid, id, owner, bucket, key]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: owner,
            bucket: bucket,
            key: key
        }, 'deleteObject: entered');

        options.dataDirector.getObjectLocation(owner, bucket, key,
            function (err, location) {

            if (err) {
                rpc.fail(err);
                return;
            }

            var pnode = location.pnode;
            var vnode = location.vnode;
            var client = options.clients.map[pnode];

            client.deleteObject(owner, bucket, key, vnode,
                function (gErr, obj) {
                log.debug({
                    err: gErr,
                    obj: obj
                }, 'deleteObject: returned');

                // MANTA-1400: set the vnode info for debugging purposes
                if (obj) {
                    obj._node = location;
                }

                log.debug({
                    obj: obj
                }, 'sanitized object');

                dtrace['deleteobject-done'].fire(function () {
                    return ([msgid]);
                });

                if (gErr) {
                    rpc.fail(gErr);
                } else {
                    rpc.end();
                }
            });
        });
    }

    return _deleteObject;
}

function listObjects(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _listObjects(rpc) {
        var msgid = rpc.requestId();
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, LO_ARGS_SCHEMA)) {
            return;
        }

        var owner = argv[0];
        var bucket_id = argv[1];
        var sorted = argv[2];
        var order_by = argv[3];
        var prefix = argv[4];
        var limit = argv[5];

        if (limit === 0) {
            limit = Infinity;
        }

        var id = options.req_id || uuid.v1();

        dtrace['listobjects-start'].fire(function () {
            return ([msgid, id, owner, sorted, order_by, prefix, limit]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: owner,
            bucket_id: bucket_id
        }, 'listObjects: entered');

        var vnodes = {};
        var nodes = options.dataDirector.getAllNodes();
        var totalVnodes = nodes.length;

        // Find an appropriate limit to use with boray
        var borayLimit = Math.ceil(limit / totalVnodes);
        borayLimit = Math.min(borayLimit, BORAY_LIMIT);

        log.debug('%d vnodes found total, want %d records, using limit of %d',
            totalVnodes, limit, borayLimit);

        // Create a mapping of vnodes to pnodes.
        nodes.forEach(function (node) {
            var client = options.clients.map[node.pnode];
            assert.object(client, 'client for pnode: ' + node.pnode);

            vnodes[node.vnode] = {
                lostream: new LimitOffsetStream({
                    limit: borayLimit,
                    log: log.child({vnode: node.vnode}),
                    getStream: function (offset, _limit) {
                        return client.listObjects(owner, bucket_id,
                            order_by, prefix, _limit, offset, node.vnode);
                    }
                }),
                record: null
            };
        });

        var opts = {
            limit: limit,
            prefix: prefix,
            order_by: order_by,
            log: log,
            vnodes: vnodes,
            sorted: sorted
        };
        paginationStream(opts,
            function onRecord(record) {
                dtrace['listobjects-record'].fire(function () {
                    return ([msgid]);
                });
                rpc.write(formatRecord(record));
            },
            function done(err) {
                if (err) {
                    log.error(err, 'listObjects error');
                    rpc.fail(err);
                    return;
                }

                log.debug('listObjects done');

                dtrace['listobjects-done'].fire(function () {
                    return ([msgid]);
                });

                rpc.end();
            });

        function formatRecord(record) {
            assert.object(record, 'record');
            assert.uuid(record.id, 'record.id');
            assert.string(record.name, 'record.name');
            assert.date(record.created, 'record.created');
            assert.string(record.content_type, 'record.content_type');
            assert.string(record.content_md5, 'record.content_md5');
            assert.number(record.content_length, 'record.content_length');

            /*
             * name: r.key.split('/').pop(),
             * etag: r.value.etag,
             * size: r.value.contentLength,
             * type: r.value.type,
             * contentType: r.value.contentType,
             * contentMD5: r.value.contentMD5,
             * mtime: new Date(r.value.mtime).toISOString()
             */
            var obj = {
                key: record.name,
                value: {
                    type: 'bucketobject',
                    etag: record.id,
                    mtime: record.created,
                    contentType: record.content_type,
                    contentMD5: record.content_md5,
                    contentLength: record.content_length
                }
            };

            return obj;
        }
    }

    return _listObjects;
}

function paginationStream(opts, onRecord, done) {
    assert.object(opts, 'opts');
    assert.bool(opts.sorted, 'opts.sorted');
    assert.object(opts.vnodes, 'opts.vnodes');
    assert.object(opts.log, 'opts.log');
    assert.number(opts.limit, 'opts.limit');
    assert.string(opts.order_by, 'opts.order_by');
    assert.func(onRecord, 'onRecord');
    assert.func(done, 'done');

    var log = opts.log;
    var vnodes = opts.vnodes;
    var limit = opts.limit;

    var totalRecordsSent = 0;

    if (!opts.sorted) {
        log.debug('paginationStream unsorted');
        vasync.forEachParallel({
            inputs: Object.keys(vnodes),
            func: function (vnode, cb2) {
                var o = vnodes[vnode];

                function next() {
                    o.lostream.getNextRecord(function (record, isDone) {
                        if (isDone) {
                            cb2();
                            return;
                        }

                        if (totalRecordsSent >= limit) {
                            log.debug('limit hit (%d) - ending early',
                                limit);
                            cb2();
                            return;
                        }

                        assert.object(record, 'record');
                        assert.string(record.created, 'record.created');
                        record.created = new Date(record.created);

                        onRecord(record);
                        totalRecordsSent++;

                        setImmediate(next);
                    });
                }

                next();
            }
        }, done);
        return;
    }

    log.debug('paginationStream sorted');
    vasync.whilst(
        function () {
            return Object.keys(vnodes).length > 0;
        },
        function (cb) {
            vasync.forEachParallel({
                inputs: Object.keys(vnodes),
                func: function (vnode, cb2) {
                    var o = vnodes[vnode];

                    if (o.record) {
                        cb2();
                        return;
                    }

                    o.lostream.getNextRecord(function (record, isDone) {
                        if (isDone) {
                            delete vnodes[vnode];
                            cb2();
                            return;
                        }

                        assert.object(record, 'record');
                        assert.string(record.created, 'record.created');
                        record.created = new Date(record.created);
                        o.record = record;
                        cb2();
                    });
                }
            }, function (err) {
                if (totalRecordsSent >= limit) {
                    log.debug('limit hit (%d) - ending early', limit);
                    vnodes = {};
                    cb(err);
                    return;
                }

                processRecords();
                cb(err);
            });
        }, done);

    function processRecords() {
        var keys = Object.keys(vnodes);

        if (keys.length === 0) {
            log.debug('no more records to process, we are done');
            return;
        }

        keys.sort(function (a, b) {
            a = vnodes[a].record;
            b = vnodes[b].record;
            return a[opts.order_by] < b[opts.order_by] ? -1 : 1;
        });

        var vnode = parseInt(keys[0], 10);
        assert.number(vnode, 'vnode');

        var o = vnodes[vnode];
        assert.object(o, 'o');

        onRecord(o.record);
        totalRecordsSent++;

        o.record = null;
    }
}

module.exports = {
    createServer: createServer
};

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var dtrace = require('dtrace-provider');



///--- Globals

var DTraceProvider = dtrace.DTraceProvider;

var PROBES = {
    // msgid, req_id, owner, bucket
    'createbucket-start': ['int', 'char *', 'char *', 'char *'],

    // msgid
    'createbucket-done': ['int'],

    // msgid, req_id, owner, bucket
    'getbucket-start': ['int', 'char *', 'char *', 'char *'],

    // msgid, value
    'getbucket-done': ['int', 'json'],

    // msgid, req_id, owner, bucket
    'deletebucket-start': ['int', 'char *', 'char *', 'char *'],

    // msgid
    'deletebucket-done': ['int'],

    // msgid, req_id, owner, sorted, order_by, prefix, limit
    'listbuckets-start': ['int', 'char *', 'char *', 'json', 'char *', 'char *',
        'int'],

    // msgid
    'listbuckets-record': ['int'],

    // msgid, num_records
    'listbuckets-done': ['int'],

    // msgid, req_id, owner, bucket, key
    'createobject-start': ['int', 'char *', 'char *', 'char *', 'char *'],

    // msgid
    'createobject-done': ['int'],

    // msgid, req_id, owner, bucket, key
    'getobject-start': ['int', 'char *', 'char *', 'char *', 'char *'],

    // msgid, value
    'getobject-done': ['int', 'json'],

    // msgid, req_id, owner, bucket, key
    'deleteobject-start': ['int', 'char *', 'char *', 'char *', 'char *'],

    // msgid
    'deleteobject-done': ['int'],

    // msgid, req_id, owner, sorted, order_by, prefix, limit
    'listobjects-start': ['int', 'char *', 'char *', 'json', 'char *', 'char *',
        'int'],

    // msgid
    'listobjects-record': ['int'],

    // msgid, num_records
    'listobjects-done': ['int']
};
var PROVIDER;



///--- API

module.exports = function exportStaticProvider() {
    if (!PROVIDER) {
        PROVIDER = dtrace.createDTraceProvider('electric-boray');

        PROVIDER._fast_probes = {};

        Object.keys(PROBES).forEach(function (p) {
            var args = PROBES[p].splice(0);
            args.unshift(p);

            var probe = PROVIDER.addProbe.apply(PROVIDER, args);
            PROVIDER._fast_probes[p] = probe;
        });

        PROVIDER.enable();
    }

    return (PROVIDER);
}();

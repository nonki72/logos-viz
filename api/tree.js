// Copyright 2015-2016, Google, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

var express = require('express');
var bodyParser = require('body-parser');
const async = require('async');
const dataLib = require('./datalib');


var router = express.Router();

// Automatically parse request body as JSON
router.use(bodyParser.json());


/**
 * GET /api/tree
 *
 * Retrieve a entity.
 */
router.get('/', function get (req, res, next) {
  var tree = {};
  var availableChildMap = {};
  dataLib.readAll(tree, availableChildMap, function (err) {
   if (err) {
    return next(err);
   }
   res.json(tree);
 });
});


/**
 * Errors on "/api/entities/*" routes.
 */
router.use(function handleRpcError (err, req, res, next) {
  // Format error and forward to generic error handler for logging and
  // responding to the request
  err.response = {
    message: err.message,
    internalCode: err.code
  };
  next(err);
});

module.exports = router;

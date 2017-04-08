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

function addDefsToChildren(node) {
  if (!('children' in node)) { 
    node.children = [];
  }
  if (node.type == "sub") {
    node.name = node.type + "(" + node.styp + "):" + node.id.toString();
  } else if (node.type == "abs") {
    node.name = node.type + "(" + node.name + "):" + node.id.toString();
  } else if (node.type == "id") {
    node.name = node.type + "(" + node.indx + "):" + node.id.toString();
  } else {
    node.name = node.type + ":" + node.id.toString();
  }
  if ('def1' in node && node.def1 !== null && typeof node.def1 === 'object') {
    node.children.push(node.def1);
    addDefsToChildren(node.def1);
  } else if ('def1' in node && typeof node.def1 === 'string') {
    node.children.push({"name": node.def1});
  }
  if ('def2' in node && node.def2 !== null && typeof node.def2 === 'object') {
    node.children.push(node.def2);
    addDefsToChildren(node.def2);
  } else if ('def2' in node && typeof node.def2 === 'string') {
    node.children.push({"name": node.def2});
  }
}

function toDndTreeFormat(tree) {
  var dndTree = {"name": "root", "children":[]};
  var keys = Object.keys(tree);
  for(var i = 0; i < keys.length; i++) {
    var id = keys[i];
    var node = tree[id];
    addDefsToChildren(node);
    dndTree.children.push(node);
  }
  return dndTree;
}

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
   var dndTree = toDndTreeFormat(tree);
   res.json(dndTree);
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

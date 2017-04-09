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

function getExpressionFromDef(def, totalExpressionMap) {
  if (def !== null && typeof def === 'object') {
    if ('expression' in def) {
      return def.expression;
    } else {
      return totalExpressionMap[def.id].expression; // assume already made
    }
  } else {
    addNameAndDefsToChildren(totalExpressionMap[def], totalExpressionMap);
    return totalExpressionMap[def].expression;
  }
}

function addNameAndDefsToChildren(node, totalExpressionMap) {
  if (node.expression) return;
  if (!('children' in node)) { 
    node.children = [];
  }
  // add 'def's to children
  if ('def1' in node && node.def1 !== null && typeof node.def1 === 'object') {
    node.children.push(node.def1);
    addNameAndDefsToChildren(node.def1, totalExpressionMap);
  } else if ('def1' in node && typeof node.def1 === 'string') {
    node.children.push({"name": node.def1});
  }
  if ('def2' in node && node.def2 !== null && typeof node.def2 === 'object') {
    node.children.push(node.def2);
    addNameAndDefsToChildren(node.def2, totalExpressionMap);
  } else if ('def2' in node && typeof node.def2 === 'string') {
    node.children.push({"name": node.def2});
  }

  // add name and expression
  if (node.type == "sub") {
    node.expression = getExpressionFromDef(node.def2, totalExpressionMap);
    node.name = "sub(" + node.styp + "):" + node.id.toString();
  } else if (node.type == "abs") {
    node.expression = "λ" + node.name + "." + getExpressionFromDef(node.def2, totalExpressionMap);
    node.name = "abs(" + node.name + "):" + node.id.toString() + " => " + node.expression;
  } else if (node.type == "app") {
    node.expression = getExpressionFromDef(node.def1, totalExpressionMap) 
                    + getExpressionFromDef(node.def2, totalExpressionMap);
    node.name = "app:" + node.id.toString() + " => " + node.expression;
  } else if (node.type == "id") {
    node.expression = "[" + node.indx + "]";
    node.name = "id(" + node.indx + "):" + node.id.toString();
  } else {
    throw new Error("unknown expression type:" + node.type);
  }
  totalExpressionMap[node.id].expression = node.expression;

}

function toDndTreeFormat(tree, totalExpressionMap) {
  var dndTree = {"name": "root", "children":[]};
  var keys = Object.keys(tree);
  for(var i = 0; i < keys.length; i++) {
    var id = keys[i];
    var node = tree[id];
    addNameAndDefsToChildren(node, totalExpressionMap);
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
  var totalExpressionMap = {};
  dataLib.readAll(tree, availableChildMap, totalExpressionMap, function (err) {
   if (err) {
    return next(err);
   }
   var dndTree = toDndTreeFormat(tree, totalExpressionMap);
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

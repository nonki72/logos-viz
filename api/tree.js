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
  } else if (def in totalExpressionMap) {
    addNameAndDefsToChildren(totalExpressionMap[def], totalExpressionMap);
    return totalExpressionMap[def].expression;
  } else {
    return "[?]";
  }
}

function addNameAndDefsToChildren(node, totalExpressionMap) {
  if ('expression' in node && node.element.entity.expression !== null) return;
  if (!('children' in node)) { 
    node.element.children = [];
  }
  // add 'def's to children
  if ('def1' in node && node.element.entity.def1 !== null && typeof node.element.entity.def1 === 'object') {
    node.element.children.push(node.element.entity.def1);
    addNameAndDefsToChildren(node.element.entity.def1, totalExpressionMap);
  } else if ('def1' in node && typeof node.element.entity.def1 === 'string') {
    node.element.children.push({"name": node.element.entity.def1});
  }
  if ('def2' in node && node.element.entity.def2 !== null && typeof node.element.entity.def2 === 'object') {
    node.element.children.push(node.element.entity.def2);
    addNameAndDefsToChildren(node.element.entity.def2, totalExpressionMap);
  } else if ('def2' in node && typeof node.element.entity.def2 === 'string') {
    node.element.children.push({"name": node.element.entity.def2});
  }

  // add name and expression
  if (node.element.entity.type == "sub") {
    node.element.entity.expression = getExpressionFromDef(node.element.entity.def2, totalExpressionMap);
    node.element.entity.name = "sub(" + node.element.styp + "):" + node.element.id.toString();
  } else if (node.element.entity.type == "abs") {
    node.element.entity.expression = "λ" + node.element.entity.name + "." + getExpressionFromDef(node.element.entity.def2, totalExpressionMap);
    node.element.entity.name = "abs(" + node.element.entity.name + "):" + node.element.id.toString() + " => " + node.element.entity.expression;
  } else if (node.element.entity.type == "app") {
    node.element.entity.expression = getExpressionFromDef(node.element.entity.def1, totalExpressionMap) 
                    + getExpressionFromDef(node.element.entity.def2, totalExpressionMap);
    node.element.entity.name = "app:" + node.element.id.toString() + " => " + node.element.entity.expression;
  } else if (node.element.entity.type == "id") {
    node.element.entity.expression = "[" + node.element.indx + "]";
    node.element.entity.name = "id(" + node.element.indx + "):" + node.element.id.toString();
  } else if (node.element.entity.type == "free") {
    node.element.entity.expression = "[" + node.element.entity.name + "]";
    node.element.entity.name = "free(" + node.element.entity.name + "):" + node.element.id.toString();
  } else {
    throw new Error("unknown expression type:" + node.element.entity.type);
  }
  totalExpressionMap[node.element.id].expression = node.element.entity.expression;

}

function toDndTreeFormat(tree, totalExpressionMap) {
  var dndTree = {"name": "root", "children":[]};
  var keys = Object.keys(tree);
  for(var i = 0; i < keys.length; i++) {
    if (tree.depth <= 2) continue; // dont list shallow branches
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
  var limit = 50000;
  if (req.params.limit != null && Number.isInteger(req.params.limit)) limit = req.params.limit;
  var tree = {}; // a tree of the nodes (entities with children map)
  var totalExpressionMap = {}; // a flat map of the entities
  dataLib.readAll(tree, totalExpressionMap, limit, function (err) {
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

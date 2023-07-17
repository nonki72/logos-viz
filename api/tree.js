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

function addNameAndExpression(node, totalExpressionMap) {
  if ('expression' in node && node.expression !== null) return;
  // add name and expression
  if (node.entity.type == "sub") {
    node.expression = getExpressionFromDef(node.entity.def2, totalExpressionMap);
    node.entity.name = "sub(" + node.styp + "):" + node.id.toString();
  } else if (node.entity.type == "abs") {
    node.expression = "λ" + node.entity.name + "." + getExpressionFromDef(node.entity.def2, totalExpressionMap);
    node.entity.name = "abs(" + node.entity.name + "):" + node.id.toString() + " => " + node.expression;
  } else if (node.entity.type == "app") {
    node.expression = getExpressionFromDef(node.entity.def1, totalExpressionMap) 
                    + getExpressionFromDef(node.entity.def2, totalExpressionMap);
    node.entity.name = "app:" + node.id.toString() + " => " + node.expression;
  } else if (node.entity.type == "id") {
    node.expression = "[" + node.indx + "]";
    node.entity.name = "id(" + node.indx + "):" + node.id.toString();
  } else if (node.entity.type == "free") {
    node.expression = "[" + node.entity.name + "]";
    node.entity.name = "free(" + node.entity.name + "):" + node.id.toString();
  } else {
    throw new Error("unknown expression type:" + node.entity.type);
  }
}

function toDndTreeFormat(tree, totalExpressionMap) {
  var element = tree.element;
  if (element.depth <= 0) return ; // dont list shallow branches
  var node = {
    name: element.entity.name,
    children: [],
    entity: element.entity
  };
  addNameAndExpression(node, totalExpressionMap);
  for(var j = 0; j < node.children.length; j++) {
    const childNode = node.children[j];
    const childTree = toDndTreeFormat(childNode, totalExpressionMap);
    node.children.push(childTree);
  }
  return node;
}

/**
 * GET /api/tree
 *
 * Retrieve a entity.
 */
router.get('/', function get (req, res, next) {
  var limit = 10000;
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

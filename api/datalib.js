'use strict';

const datastore = require('./datastore');

function readOrCreateAbstraction (name, definition2, cb) {
	const query = datastore.ds.createQuery('Diary') // TODO: save the lookups for EC. Remove cat's
	 .filter('type', '=', 'abs')
//   .filter('name', '=', name)
   .filter('def2', '=', definition2)
   .limit(1);
  datastore.ds.runQuery(query)
   .then((results) => {
   	var abstractions = results[0];
  	if (abstractions.length) {
	   	var abstraction = abstractions[Object.keys(abstractions)[0]];
	   	abstraction.id = abstraction[datastore.ds.KEY]['id'];
	   	console.log(abstraction);
  		return cb(abstraction);
  	}

		// if not found
	  var data = {
	  	type: 'abs',
	  	name: name,
	  	def2: definition2
	  };
		datastore.create('Diary', data, function(err, entity){
	    return cb(entity);
		});
  }).catch((reason) => {
    console.log("Diary abstraction query error: " + reason);
  });
}

function readOrCreateApplication (definition1, definition2, cb) {
	const query = datastore.ds.createQuery('Diary')
	 .filter('type', '=', 'app')
   .filter('def1', '=', definition1)
   .filter('def2', '=', definition2)
   .limit(1);
  datastore.ds.runQuery(query)
   .then((results) => {
   	var applications = results[0];
  	if (applications.length) {
	   	var application = applications[Object.keys(applications)[0]];
	   	application.id = application[datastore.ds.KEY]['id'];
	   	console.log(application);
  		return cb(application);
  	}

		// if not found
	  var data = {
	  	type: 'app',
	  	def1: definition1,
	  	def2: definition2
	  };
		datastore.create('Diary', data, function(err, entity){
	    return cb(entity);
		});
  }).catch((reason) => {
    console.log("Diary application query error: " + reason);
  });


}


function readOrCreateIdentifier ( index, cb ) {
	const query = datastore.ds.createQuery('Diary')
	 .filter('type', '=', 'id')
   .filter('indx', '=', index)
   .limit(1);
  datastore.ds.runQuery(query)
   .then((results) => {
   	var identifiers = results[0];
  	if (identifiers.length) {
	   	var identifier = identifiers[Object.keys(identifiers)[0]];
	   	identifier.id = identifier[datastore.ds.KEY]['id'];
	   	console.log(identifier);
  		return cb(identifier);
  	}

		// if not found
	  var data = {
	  	type: 'id',
	  	indx: index
	  };
		datastore.create('Diary', data, function(err, entity){
			if (err) {
				console.log("diary err "+err);
			}
	    return cb( entity);
		});
	}).catch((reason) => {
    console.log("Diary identifier query error: " + reason);
  });
}

function readFreeIdentifier ( name, cb ) {
	const query = datastore.ds.createQuery('Diary')
	 .filter('type', '=', 'free')
   .filter('name', '=', name)
   .limit(1);
  datastore.ds.runQuery(query)
   .then((results) => {
   	var identifiers = results[0];
  	if (identifiers.length) {
	   	var identifier = identifiers[Object.keys(identifiers)[0]];
	   	identifier.id = identifier[datastore.ds.KEY]['id'];
	   	console.log(identifier);
  		return cb(identifier);
  	}

		// if not found
		console.log("Diary error: could not find free identifier '"+name+"'");
		return cb(undefined);
	});
}

function createFreeIdentifier (name, ast, fn, argn, argTypes, cb) {
  var data = {
  	type: 'free',
  	name: name,
    ast: ast, // location (id)
    fn: fn,
    argn: argn,
    argt: argTypes
  };
	datastore.create('Diary', data, function(err, entity){
		if (err) {
			console.log("diary err "+err);
		}
    return cb( entity);
	});
}

function createSubstitution (subType, location1, location2, cb) {
  if (subType == 'beta') {
    // check that action1 lhs is abstraction
  }

  // actually create the substitution
  var createSub = function(err,entity) {
	  var data = {
	  	type: 'sub',
	  	styp: subType,
	  	def1: location1,
	  	def2: location2,
	    rand: Math.random()
	  };
		datastore.create('Diary', data, function(err, newEntity){
  	  return cb( newEntity);
		});
	};

  // invalidate the old cat/sub anchored here
  datastore.read('Diary', location1, function(err,entity) {
  	entity.invalid = true;
  	datastore.update('Diary', location1, entity, createSub);
  });

}

// id of entity which matches either def1 or def2 of node in tree
function applyEntityToAvailableChild(id, entity, node) {
	// figure out if it matches def1 or def2
	if (node.def1 == id) {
		// apply the entity, replacing its reference
		node.def1 = entity;
		if (!node.def2) {
			delete availableChildMap[id];
		}
	} else if (node.def2 == id) {
		node.def2 = entity;
		if (node.type == 'abs' || !node.def1) {
			delete availableChildMap[id];
		}
	} else {
		throw new Error("Stale value in map: " + id);
	}	
}

function readAll (tree, availableChildMap, cb) {
	const query = datastore.ds.createQuery('Diary')
	.filter('type', '!=', 'id')
	.limit(100);
  datastore.ds.runQuery(query)
   .then((results) => {
   	var entities = results[0];
  	if (entities.length) {
  		Object.keys(entities).forEach(function(key) {
  			var entity = entities[key];
  			var id = entity[datastore.ds.KEY]['id'];
  			if (id in availableChildMap) {
  				// this entity's id was found in the listing of available references
  				// obtain the node(s) that advertise and replace the references with the entity
  				// and then erasing the advertisement
					var nodes = availableChildMap[id];
					for (var i = 0; i < nodes.length; i++) {
						applyEntityToAvailableChild(id, entity, nodes[i]);
					}
  			} else {
  				// simple case, add to root node
  				tree[id] = entity;
  			}
  			var def1pulled = false;
  			var def2pulled = false;
				// also check all root children to match def1 or def2
				// and pull them in
				if (entity.def1 && entity.def1 in tree) {
					var def1id = entity.def1;
					entity.def1 = tree[def1id];
					delete tree[def1id];
					def1pulled = true;
				}
				if (entity.def2 && entity.def2 in tree) {
					var def2id = entity.def2;
					entity.def2 = tree[def2id];
					delete tree[def2id];
					def2pulled = true;
				}
  			// advertise open references (if any)
				if ((entity.def1 && !def1pulled) || (entity.def2 && !def1pulled)) {
					availableChildMap[id] = entity;
				}
  		});
  		return cb(null);
  	}

		// if none found
		return cb(null);
	}).catch((reason) => {
    console.log("Diary readall query error: " + reason);
  });

}

module.exports = {
	readOrCreateAbstraction: readOrCreateAbstraction,
	readOrCreateApplication: readOrCreateApplication,
	readOrCreateIdentifier: readOrCreateIdentifier,
	readFreeIdentifier: readFreeIdentifier,
	createFreeIdentifier: createFreeIdentifier,
	createSubstitution: createSubstitution,
	readAll: readAll
};
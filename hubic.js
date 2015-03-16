var Async = require('async');
var Authentification = require('hubic-auth');
var Underscore = require('underscore');
var util = require('util');

var Swift = require('./swift.js');

var Hubic = function(options, callback) {
	var self = this;

	this._options = options || {};
	this._debug = this._options.hubicLog || false;
	this._loginDebug = this._options.hubicLoginLog || false;

	this._options.uploadingPrefix = this._options.uploadingPrefix || "__uploading ";

	this._callQueue = Async.queue(function(task, callback) {
		task(callback);
	}, this._options.maxRequest || 2);

	this._uploadQueue = Async.queue(function(task, callback) {
		task(callback);
	}, this._options.maxUpload || 2);

	this._containerName = this._options.containerName || "default";

	var auth = new Authentification({
		log: this._loginDebug
	});
	this._auth = auth;

	new Swift(function(callback2) {

		auth.load(null, function(error) {
			if (error) {
				return callback(error);
			}

			auth.getStorageInfos(null, function(error, tokens) {
				if (error) {
					return callback(error);
				}
				self.loginLog("[Login] Session opened", tokens);

				self.loginLog("[Login] Got HUBIC profile informations"); // "+util.inspect(hubic));

				callback2(null, {
					storageUrl: tokens.endpoint,
					id: tokens.token

				});
			});
		});
	}, self._options, function(error, swift) {
		self._swift = swift;

		callback(error, self);
	});
};

Hubic.prototype.log = function() {
	if (!this._debug) {
		return;
	}

	console.log.apply(console, arguments);
};

Hubic.prototype.loginLog = function() {
	if (!this._loginDebug) {
		return;
	}

	console.log.apply(console, arguments);
};

Hubic.prototype.select = function(containerName) {
	this._containerName = containerName;
};

function makeHierarchie(list, files) {
	if (!files.length) {
		return;
	}

	for (var i = 0; i < files.length; i++) {
		var file = files[i];

		if (!file.name) {
			continue;
		}

		var metas = {};

		metas.name = file.name;
		metas.lastModified = new Date(file.last_modified);
		if ("application/directory" == file.content_type) {
			metas.directory = true;
		} else {
			metas.length = file.bytes;
		}

		list.push(metas);
	}
}

Hubic.prototype.list = function(path, callback) {
	this.log("[hubic] list: path='" + path + "'");

	var options = Underscore.clone(this._options.request || {});
	if (path) {
		if (path.charAt(path.length - 1) != '/') {
			path += "/";
		}
		if (path == "/") {
			path = "";
		}
		options.prefix = path;
	}

	var total = 0;
	var self = this;
	var root = [];
	var files = [];
	function list(callback) {
		Async.parallel([ function(callback) {
			self._swift.getFiles(self._containerName, options, function(error, fs) {
				if (error) {
					return callback(error);
				}

				if (!fs || !fs.length) {
					return callback();
				}

				total += fs.length;

				// console.error("Receive '", fs, "' files total=", total);

				files = files.concat(fs);

				if (!options.limit || fs.length < options.limit) {
					return callback();
				}

				for (var i = fs.length - 1; i >= 0; i--) {
					if (!fs[i].name) {
						continue;
					}

					options.marker = fs[i].name;
					list(callback);
					return;
				}

				return callback();
			});

		}, function(callback) {
			var fs = files;
			files = [];

			// console.error("Make hierarchy of ", fs);

			makeHierarchie(root, fs);

			callback();

		} ], callback);
	}

	list(function(error) {
		if (error) {
			return callback(error);
		}

		makeHierarchie(root, files);

		// console.error("Make last hierarchy returns ", root);

		callback(null, root, total);
	});
};

Hubic.prototype.put = function(remotePath, localPath, size, hlist, callback) {
	// Pas de queue, ca se fait dedans !
	this.log("[hubic] put: name='" + remotePath + "' path='" + localPath + "' size=" + size);

	this._swift
			.put(this._containerName, remotePath, localPath, size, hlist, this._uploadQueue, this._callQueue, callback);
};

Hubic.prototype.$delete = function(remotePath, ignoreError, callback) {

	var self = this;
	this._callQueue.push(function(callback) {
		self.log("[hubic] delete: path='" + remotePath + "'");

		self._swift.$delete(self._containerName, remotePath, ignoreError, callback);
	}, callback);
};
Hubic.prototype['delete'] = Hubic.prototype.$delete;

Hubic.prototype.newDirectory = function(localPath, callback) {

	var self = this;
	this._callQueue.push(function(callback) {
		self.log("[hubic] newDir: path='" + localPath + "'");

		self._swift.mkdir(self._containerName, localPath, callback);
	}, callback);
};

Hubic.prototype.moveTo = function(dst, src, callback) {

	var self = this;
	this._callQueue.push(function(callback) {
		self.log("[hubic] moveTo: dst='" + dst + "' src='" + src + "'");

		self._swift.copy(self._containerName, dst, src, function(error) {
			if (error) {
				return callback(error);
			}

			self._swift.$delete(self._containerName, src, false, callback);
		});
	}, callback);
};

module.exports = Hubic;

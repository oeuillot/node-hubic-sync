var File = function(parent, path, lastModified, size, isDirectory) {
	this.parent = parent;
	this.path = path;

	if (lastModified) {
		this.lastModified = lastModified;
	}

	if (isDirectory === true) {
		this.isDirectory = true;

	} else if (size !== undefined) {
		this.size = size;
	}

	var idx = path.lastIndexOf('/');
	if (idx > 0) {
		this.name = path.substring(idx + 1);
	} else {
		this.name = path;
	}

	if (parent) {
		this._root = parent._root;

	} else {
		this._root = this;
	}
};

File.prototype.find = function(path, callback) {
	var fs = path.split("/");

	this.list(function(error, list) {
		if (error) {
			return callback(error);
		}

		var f = list[fs[0]];

		if (!f) {
			return callback(null);
		}

		fs.shift();

		if (fs.length) {
			if (!f.isDirectory) {
				return callback(null);
			}
			return f.find(fs.join('/'), callback);
		}

		callback(null, f);
	});
};

module.exports = File;

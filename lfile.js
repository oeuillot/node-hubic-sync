var Util = require("util");
var File = require('./file.js');
var Path = require("path");
var FS = require("fs");
var Async = require("async");

var LFile = function(parent, path, lastModified, size, isDirectory, localPath) {
  LFile.super_.call(this, parent, path, lastModified, size, isDirectory,
      localPath);

  this.localPath = localPath;
};

Util.inherits(LFile, File);

LFile.prototype.list = function(callback) {
  var self = this;

  var p = Path.join(this._root._base, this.path);

  // console.log("Readdir ", p);

  FS.readdir(p, function(error, files) {
    if (error) {
      return callback(error);
    }

    var list = {};
    Async.map(files, function(file, callback) {

      var path = Path.join(p, file);

      // console.log("Stat ", path);

      FS.stat(path, function(error, stats) {
        if (error) {
          return callback(error);
        }

        file = path.substring(self._root._base.length + 1);
        // if (Path.sep!='/') {
        file = file.replace(/\\/g, '/');
        // }

        // console.log("Create file '"+file+"' ("+path+")");

        var lf = new LFile(self, file, stats.mtime, stats.size, stats
            .isDirectory(), path);

        list[lf.name] = lf;
        callback();
      });
    }, function(error) {
      callback(error, list);
    });
  });
};

LFile.createRoot = function(path) {
  var root = new LFile(null, "/", null, undefined, true);

  root._base = path;

  return root;
};

module.exports = LFile;

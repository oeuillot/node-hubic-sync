var Util = require("util");
var File = require('./file.js');
var Async = require('async');

var HFile = function(parent, path, lastModified, size, isDirectory, options) {
  HFile.super_.call(this, parent, path, lastModified, size, isDirectory,
      options);

  this._moveToEachLimit = 4;
  this._deleteEachLimit = 4;

  if (!parent) {
    if (options && options.weakMap) {
      this._weakMap2 = {
        get : function(key) {
          return this[key];
        },
        set : function(key, value) {
          return this[key] = value;
        }
      };
    }
  }
};

Util.inherits(HFile, File);

HFile.prototype.list = function(callback) {
  if (this._root._weakMap) {
    var children = this._root._weakMap.get(this.path);
    if (children) {
      return callback(null, children);
    }
  }

  var self = this;
  this._root._hubic.list(this.path, function(error, list) {
    if (error) {
      return callback(error);
    }

    var children = {};

    for (var i = 0; i < list.length; i++) {
      var child = list[i];

      // console.log("Child "+child.name+"' lastModified="+child.lastModifier+" size="+child.length+"
      // "+Util.inspect(child));

      var f = new HFile(self, child.name, child.lastModified, child.length,
          child.directory);

      children[f.name] = f;
    }

    if (self._root._weakMap) {
      self._root._weakMap.set(this.path, children);
    }
    callback(null, children);
  });
};

HFile.prototype.put = function(localFile, hlist, callback) {
  var date = new Date();
  var self = this;

  if (this.isDirectory) {
    this._root._hubic.put(this.path + "/" + localFile.name,
        localFile.localPath, localFile.size, hlist, function(error) {
          if (error) {
            return callback(error);
          }

          var f = new HFile(self, self.path + "/" + localFile.name, date,
              localFile.size);

          callback(null, f);
        });
    return;
  }

  this._root._hubic.put(this.path, localFile.localPath, localFile.size, hlist,
      function(error) {
        if (error) {
          return callback(error);
        }

        var f = new HFile(self.parent, self.path, date, localFile.size);

        callback(null, f);
      });
};

HFile.prototype.newDirectory = function(name, callback) {
  var date = new Date();
  var self = this;
  this._root._hubic.newDirectory(this.path + "/" + name, function(error) {
    if (error) {
      return callback(error);
    }

    var f = new HFile(self, self.path + "/" + name, date, 0, true);

    callback(null, f);
  });
};

HFile.prototype.moveTo = function(dest, callback) {
  if (!dest.isDirectory) {
    return callback("dest(" + dest.path + ") is not a directory");
  }

  if (this.isDirectory) {
    var self = this;

    // Il faut faire à la main !
    dest.newDirectory(this.name, function(error, hnew) {
      if (error) {
        return callback(error);
      }

      self.list(function(error, list) {
        if (error) {
          return callback(error);
        }

        Async.eachLimit(Object.keys(list), self._moveToEachLimit, function(
            name, callback) {
          var item = list[name];

          item.moveTo(hnew, callback);

        }, function(error) {
          if (error) {
            return callback(error);
          }

          self.$delete(function(error) {
            if (error) {
              return callback(error);
            }

            callback(null, hnew);
          });
        });
      });
    });

    return;
  }

  this._root._hubic.moveTo(dest.path + "/" + this.name, this.path, callback);
};

HFile.prototype.rename = function(newName, callback) {
  if (this.isDirectory) {
    return callback("dest(" + this.path + ") is a directory");
  }
  this._root._hubic.moveTo(this.parent.path + "/" + newName, this.path,
      callback);
};

HFile.prototype.newFile = function(name) {
  if (!this.isDirectory) {
    return callback("dest(" + this.path + ") is not a directory");
  }

  var f = new HFile(this, this.path + "/" + name, new Date(), 0, false);

  return f;
};

HFile.prototype.$delete = function(ignoreError, callback) {
  if (typeof (ignoreError) == "function") {
    callback = ignoreError;
    ignoreError = undefined;
  }

  if (!this.isDirectory) {
    this._root._hubic.$delete(this.path, ignoreError, callback);
    return;
  }

  var self = this;

  // Suppression recursive
  this.list(function(error, files) {
    if (error) {
      return callback(error);
    }

    Async.eachLimit(files, self._deleteEachLimit, function(item, callback) {
      item.$delete(ignoreError, callback);

    }, function(error) {
      if (error) {
        return callback(error);
      }

      self._root._hubic.$delete(self.path, ignoreError, callback);
    });
  });
};

HFile.createRoot = function(hubic) {
  var root = new HFile(null, "/", null, undefined, true, hubic._options);
  root._hubic = hubic;

  return root;
};

module.exports = HFile;

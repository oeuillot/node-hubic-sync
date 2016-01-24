/*jslint node: true, plusplus:true, node: true, esversion: 6 */
"use strict";

var Async = require('async');
var Util = require("util");

var File = require('./file.js');

class HFile extends File {
  constructor(parent, path, lastModified, size, isDirectory, options) {
    super(parent, path, lastModified, size, isDirectory, options);

    this._moveToEachLimit = 4;
    this._deleteEachLimit = 4;

    if (!parent) {
      if (options && options.weakMap) {
        this._weakMap2 = {
            get: function(key) {
              return this[key];
            },
            set: function(key, value) {
              return this[key] = value;
            }
        };
      }
    }
  }

  list(callback) {
    if (this._root._weakMap) {
      var children = this._root._weakMap.get(this.path);
      if (children) {
        return callback(null, children);
      }
    }

    this._root._hubic.list(this.path, (error, list) => {
      if (error) {
        return callback(error);
      }

      var children = {};

      list.forEach((child) => {
        var f = new HFile(this, child.name, child.lastModified, child.length, child.directory);

        children[f.name] = f;
      });

      if (this._root._weakMap) {
        this._root._weakMap.set(this.path, children);
      }
      callback(null, children);
    });
  }

  put(localFile, hlist, callback) {
    var date = new Date();

    if (this.isDirectory) {
      this._root._hubic.put(this.path + "/" + localFile.name, localFile.localPath, localFile.size, hlist, (error) => {
        if (error) {
          return callback(error);
        }

        var f = new HFile(this, this.path + "/" + localFile.name, date, localFile.size);

        callback(null, f);
      });
      return;
    }

    this._root._hubic.put(this.path, localFile.localPath, localFile.size, hlist, (error) => {
      if (error) {
        return callback(error);
      }

      var f = new HFile(this.parent, this.path, date, localFile.size);

      callback(null, f);
    });
  }

  newDirectory(name, callback) {
    var date = new Date();
    this._root._hubic.newDirectory(this.path + "/" + name, (error) => {
      if (error) {
        return callback(error);
      }

      var f = new HFile(this, this.path + "/" + name, date, 0, true);

      callback(null, f);
    });
  }

  moveTo(dest, callback) {
    if (!dest.isDirectory) {
      return callback("dest(" + dest.path + ") is not a directory");
    }

    if (this.isDirectory) {
      // Il faut faire à la main !
      dest.newDirectory(this.name, (error, hnew) => {
        if (error) {
          return callback(error);
        }

        this.list((error, list) => {
          if (error) {
            return callback(error);
          }

          Async.eachLimit(Object.keys(list), this._moveToEachLimit, (name, callback) => {
            var item = list[name];

            item.moveTo(hnew, callback);

          }, (error) => {
            if (error) {
              return callback(error);
            }

            this.$delete((error) => {
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
  }

  rename(newName, callback) {
    if (this.isDirectory) {
      return callback("dest(" + this.path + ") is a directory");
    }
    this._root._hubic.moveTo(this.parent.path + "/" + newName, this.path, callback);
  }

  newFile(name) {
    if (!this.isDirectory) {
      return callback("dest(" + this.path + ") is not a directory");
    }

    var f = new HFile(this, this.path + "/" + name, new Date(), 0, false);

    return f;
  }

  $delete(ignoreError, callback) {
    if (typeof (ignoreError) == "function") {
      callback = ignoreError;
      ignoreError = undefined;
    }

    if (!this.isDirectory) {
      this._root._hubic.$delete(this.path, ignoreError, callback);
      return;
    }

    // Suppression recursive
    this.list((error, files) => {
      if (error) {
        return callback(error);
      }

      Async.eachLimit(files, this._deleteEachLimit, (item, callback) => {
        item.$delete(ignoreError, callback);

      }, (error) => {
        if (error) {
          return callback(error);
        }

        this._root._hubic.$delete(this.path, ignoreError, callback);
      });
    });
  }

  static createRoot(hubic) {
    assert(hubic, "Invalid hubic parameter");
    
    var root = new HFile(null, "/", null, undefined, true, hubic._options);
    root._hubic = hubic;

    return root;
  }
}
module.exports = HFile;

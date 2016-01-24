/*jslint node: true, plusplus:true, node: true, esversion: 6 */
"use strict";

var Async = require("async");
var FS = require("fs");
var Path = require("path");
var Util = require("util");

var File = require('./file.js');

class LFile extends File {
  constructor(parent, path, lastModified, size, isDirectory, localPath) {
    super(parent, path, lastModified, size, isDirectory, localPath);

    this.localPath = localPath;
  }

  list(callback) {

    var p = Path.join(this._root._base, this.path);

    // console.log("Readdir ", p);

    FS.readdir(p, (error, files) => {
      if (error) {
        return callback(error);
      }

      var list = {};
      Async.map(files, (file, callback) => {

        var path = Path.join(p, file);

        // console.log("Stat ", path);

        FS.stat(path, (error, stats) => {
          if (error) {
            return callback(error);
          }

          file = path.substring(this._root._base.length + 1);
          // if (Path.sep!='/') {
          file = file.replace(/\\/g, '/');
          // }

          // console.log("Create file '"+file+"' ("+path+")");

          var lf = new LFile(this, file, stats.mtime, stats.size, stats
              .isDirectory(), path);

          list[lf.name] = lf;
          callback();
        });
      }, (error) => {
        callback(error, list);
      });
    });
  }

  static createRoot(path) {
    var root = new LFile(null, "/", null, undefined, true);

    root._base = path;

    return root;
  }
}

module.exports = LFile;

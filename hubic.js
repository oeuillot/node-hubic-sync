/*jslint node: true, plusplus:true, node: true, esversion: 6 */
"use strict";

var Async = require('async');
var Authentification = require('hubic-auth');
var Underscore = require('underscore');
var util = require('util');
var debug=require('debug')('hsync:hubic');
var debugLog=require('debug')('hsync:log');

var Swift = require('./swift.js');

class Hubic {
  constructor(options, callback) {

    this._options = options || {};

    this._options.uploadingPrefix = this._options.uploadingPrefix || "__uploading ";

    this._callQueue = Async.queue((task, callback) => {
      task(callback);

    }, this._options.maxRequest || 2);

    this._uploadQueue = Async.queue((task, callback) => {
      task(callback);
    
    }, this._options.maxUpload || 2);

    this._containerName = this._options.containerName || "default";

    var auth = new Authentification(this._options);
    this._auth = auth;

    new Swift((callback) => {

      auth.load(null, (error) => {
        if (error) {
          return callback(error);
        }

        auth.getStorageInfos((callback) => {
          callback(null, this._options.username, this._options.password);

        }, (error, tokens) => {
          if (error) {
            return callback(error);
          }
          debugLog("[Login] Session opened", tokens);

          debugLog("[Login] Got HUBIC profile informations"); // "+util.inspect(hubic));

          callback(null, {
            storageUrl : tokens.endpoint,
            id : tokens.token
          });
        });
      });
    }, this._options, (error, swift) => {
      this._swift = swift;

      callback(error, this);
    });
  }

  /**
   * Specify the container name
   * 
   * @param {string}
   *            containerName
   */
  select(containerName) {
    this._containerName = containerName;
  }

  _makeHierarchie(list, files) {
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

  /**
   * List files of a remote directoy
   * 
   * @param {string}
   *            path Remote path
   * @param callback
   */
  list(path, callback) {
    debug("[hubic] list: path=", path);

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
    var root = [];
    var files = [];
    var list = (callback) => {
      Async.parallel([ (callback) => {
        this._swift.getFiles(this._containerName, options, (error, fs) => {
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

      }, (callback) => {
        var fs = files;
        files = [];

        // console.error("Make hierarchy of ", fs);

        this._makeHierarchie(root, fs);

        callback();

      } ], callback);
    };

    list((error) => {
      if (error) {
        return callback(error);
      }

      this._makeHierarchie(root, files);

      // console.error("Make last hierarchy returns ", root);

      callback(null, root, total);
    });
  }

  /**
   * Upload file to HUBIC server
   * 
   * @param {string}
   *            remotePath Remote path (The path, not the URL)
   * @param {string}
   *            localPath The path of the file which will be uploaded
   * @param {number}
   *            [size] Size of the file
   * @param {Object}
   *            [hlist] List of the remove directory
   * @param callback
   *            Called when done or if any error
   */
  put(remotePath, localPath, size, hlist, callback) {
    switch (arguments.length) {
    case 3:
      callback = size;
      size = undefined;
      break;

    case 4:
      callback = hlist;
      hlist = undefined;
      break
    }

    // Pas de queue, ca se fait dedans !
    debug("[hubic] put: name=", remotePath, "path=", localPath, "size=", size);

    this._swift.put(this._containerName, remotePath, localPath, size, hlist,
        this._uploadQueue, this._callQueue, callback);
  }

  /**
   * 
   */
  $delete(remotePath, ignoreError, callback) {
    this._callQueue.push((callback) => {
      debug("[hubic] delete: path=",remotePath);

      this._swift.$delete(this._containerName, remotePath, ignoreError, callback);
    }, callback);
  }

  /**
   * Create a remote directory
   * 
   * @param {string}
   *            localPath Remote path of new directory
   * @param callback
   */
  newDirectory(localPath, callback) {

    this._callQueue.push((callback) => {
      debug("[hubic] newDir: path=", localPath);

      this._swift.mkdir(this._containerName, localPath, callback);
    }, callback);
  }

  /**
   * Move a file
   * 
   * @param {string}
   *            dst Target remote path
   * @param {string}
   *            src Source remote path
   * @param callback
   */
  moveTo(dst, src, callback) {

    this._callQueue.push((callback) => {
      debug("[hubic] moveTo: dst=", dst, "src=", src);

      this._swift.copy(this._containerName, dst, src, (error) => {
        if (error) {
          return callback(error);
        }

        this._swift.$delete(this._containerName, src, false, callback);
      });
    }, callback);
  }

  /**
   * Flush all pending uploads
   * 
   * @param callback
   */
  flush(callback) {

    var callQueue = this._callQueue;
    var uploadQueue = this._uploadQueue;

    var waitUpload = () => {
      if (uploadQueue.idle()) {
        return callback();
      }

      uploadQueue.drain = () => {
        uploadQueue.drain = null;

        callback();
      }
    }

    if (callQueue.idle()) {
      return waitUpload();
    }
    callQueue.drain = () => {
      callQueue.drain = null;

      waitUpload();
    }
  }
}

module.exports = Hubic;

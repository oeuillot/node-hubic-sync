/*jslint node: true, plusplus:true, node: true, esversion: 6 */
"use strict";

var debug=require('debug')('hsync:swift')
var debugRequest=require('debug')('hsync:request')

var request = require('request');
var URL = require('url');
var util = require('util');
var fs = require('fs');
var stream = require('stream');
var Sprintf = require('sprintf');
var Async = require('async');
var Mime = require('mime');

class Swift {
  constructor(authFunction, options, callback) {

    this._options = options;
    this._dryRun = options.dryRun || false;
    this.maxRequestSize = options.maxUploadSize || 1024 * 1024;
    this._slots = {};

    this.authFunction = authFunction;
    this.tokens = {};
    this.authFunction((err, tokens) => {
      if (!err) {
        this.tokens = tokens;
      }
      callback(err, this);
    });
  }

  getFiles(containerName, options, callback) {
    if (!options) {
      options = {};
    }

    debug("[swift] getFiles: containerName='" + containerName + "' path='" +
        options.prefix + "'");

    options.delimiter = "/";
    if (options.prefix) {
      // options.prefix=normalizePath(options.prefix);
    }

    var targetURL = URL.parse(this.tokens.storageUrl + '/' + containerName);

    debugRequest("LIST '" + URL.format(targetURL) + "' prefix='" +
        options.prefix + "'");

    request({
      method : 'GET',
      uri : URL.format(targetURL),
      qs : options,
      json : {},
      headers : {
        "X-Auth-Token" : this.tokens.id,
        "Accept" : "application/json"
      }
    }, (err, res, body) => {
      debugRequest("LIST '" + URL.format(targetURL) + "' prefix='" +
          options.prefix + "' =>> " + (res && res.statusCode));

      if (!err && res && res.statusCode && res.statusCode >= 200 &&
          res.statusCode <= 204) {
        var files = body;

        return callback(null, files);
      }

      // console.log("Request '"+URL.format(targetURL)+"' => "+err);

      if (!err) {
        err = new Error("GET Request unsuccessful: path='" + options.prefix +
            "' statusCode=" + (res && res.statusCode));
      }
      callback(err);
    });
  }

  $delete(containerName, path, ignoreError, callback) {
    debug("[swift] delete: containerName=", containerName, "path=", path);

    var targetURL = URL.parse(this.tokens.storageUrl + '/' + containerName + '/' +
        normalizePath(path));

    debugRequest("DELETE",URL.format(targetURL));
    if (this._dryRun) {
      debugRequest("DELETE", URL.format(targetURL),"=> DRY RUN");
      return callback(null);
    }

    request({
      method : 'DELETE',
      uri : URL.format(targetURL),
      headers : {
        "X-Auth-Token" : this.tokens.id,
        "Accept" : "application/json"
      }
    }, (err, res, body) => {
      debugRequest("DELETE", URL.format(targetURL), "=>", (res && res.statusCode), "body=", body);

      if (!err && res && res.statusCode >= 200 && res.statusCode < 300) {
        return callback(null);
      }

      if (ignoreError && res && res.statusCode == 404) {
        return callback(null);
      }

      // console.log("Request '"+URL.format(targetURL)+"' => "+err);

      if (!err) {
        err = new Error("DELETE Request unsuccessful: path='" + path +
            "' statusCode=" + (res && res.statusCode) + " body='" + res.body +
        "'");
      }
      callback(err);
    });
  }

  mkdir(containerName, path, callback) {
    debug("[swift] mkdir: containerName=", containerName, "path=", path);

    var targetURL = URL.parse(this.tokens.storageUrl + '/' + containerName + "/" +
        normalizePath(path));

    debugRequest("MKDIR", URL.format(targetURL));
    if (this._dryRun) {
      debugRequest("MKDIR", URL.format(targetURL), "=> DRY RUN");
      return callback(null);
    }

    request({
      method : 'PUT',
      uri : URL.format(targetURL),
      headers : {
        "X-Auth-Token" : this.tokens.id,
        "Accept" : "application/json",
        "Content-Type" : "application/directory",
        "Content-Length" : 0
      }
    }, (err, res, body) => {
      debugRequest("MKDIR", URL.format(targetURL), "=>", (res && res.statusCode), "body=", body);

      if (!err && res && res.statusCode >= 200 && res.statusCode < 300) {
        return callback(null);
      }

      if (!err) {
        err = new Error("MKDIR Request unsuccessful: path='" + path +
            "' statusCode=" + (res && res.statusCode) + " body='" + body + "'");
      }
      callback(err);
    });
  }

  copy(containerName, destName, sourceName, callback) {
    debug("[swift] copy: destName=", destName, "sourceName=",sourceName);

    var targetURL = URL.parse(this.tokens.storageUrl + '/' + containerName + '/' +
        normalizePath(destName));
    var sourceURL = containerName + '/' + normalizePath(sourceName);

    debugRequest("COPY", URL.format(targetURL), "from=", sourceURL);
    if (this._dryRun) {
      debugRequest("COPY", URL.format(targetURL), "=> DRY RUN");
      return callback(null);
    }

    request({
      method : 'PUT',
      uri : URL.format(targetURL),
      headers : {
        "X-Auth-Token" : this.tokens.id,
        "X-Copy-From" : sourceURL,
        "Content-Type" : Mime.lookup(destName),
        "Content-Length" : 0,
      },
      body : ""

    }, (err, res, body) => {
      debugRequest("COPY '" + URL.format(targetURL) + "' => " +
          (res && res.statusCode) + " '" + body + "'");

      if (!err && res && res.statusCode >= 200 && res.statusCode < 300) {
        return callback(null);
      }

      // console.log("Request '"+URL.format(targetURL)+"' => "+err);

      if (!err) {
        err = new Error("COPY Request unsuccessful: path='" + sourceName +
            "' statusCode=" + (res && res.statusCode));
      }
      callback(err);
    });
  }

  put(containerName, remotePath, localPath, size, hlist, uploadQueue, cmdQueue, callback) {
    if (size === undefined || size < 0) {
      var stats = fs.statSync(localPath);

      size = stats.size;
    }

    var list = [];
    for (var i = 0; i * this.maxRequestSize < size; i++) {
      list.push(i);
    }

    var slash = remotePath.lastIndexOf('/');
    var rp = remotePath.substring(slash + 1);
    var prefix = remotePath.substring(0, slash + 1);

    var segmentName = this._options.uploadingPrefix + rp;

    var firstRP;
    Async.map(list, (partId, callback) => {
      var sz = size - partId * this.maxRequestSize;
      if (sz > this.maxRequestSize) {
        sz = this.maxRequestSize;
      }

      var rp = segmentName + " -" + Sprintf.sprintf("%08d", partId);
      var hr = hlist && hlist[rp];
      // console.log("#"+partId+" '"+rp+"' Found '"+hr+"' size="+sz);
      if (hr && hr.size == sz) {
        // TODO VERIFIER LA DATE AUSSI !
        return callback(null, prefix + rp);
      }

      uploadQueue.unshift((callback) => {
        this.putSegment(containerName, prefix + rp, localPath, size, partId,
            callback);

      }, (error) => {
        callback(error, prefix + rp);
      });

    }, (error, rps) => {
      if (error) {
        return callback(error);
      }

      var mergedPath = prefix + segmentName;

      cmdQueue.unshift((callback) => {
        if (rps.length == 1) {
          this.copy(containerName, remotePath, rps[0], (error) => {
            if (error) {
              return callback(error);
            }

            this.$delete(containerName, rps[0], false, callback);
          });
          return;
        }

        this.mergeSegments(containerName, mergedPath, prefix + segmentName + " -", size, (error) => {
          if (error) {
            return callback(error);
          }

          this.copy(containerName, remotePath, mergedPath, (error) => {
            if (error) {
              return callback(error);
            }

            this.$delete(containerName, mergedPath, false, (error) => {
              if (error) {
                return callback(error);
              }

              var deletePath = (partPath) => {
                cmdQueue.unshift((callback) => {
                  this.$delete(containerName, partPath, false, callback);
                });
              }

              for (var i = 0; i < rps.length; i++) {
                var rp = rps[i];

                deletePath(rp);
              }

              callback(null);
            });
          });
        });

      }, callback);
    });
  }

  mergeSegments(containerName, remotePath, segmentsPath, size, callback) {
    debug("[swift] mergeSegments: path=", remotePath, "segmentsPath=", segmentsPath);

    var targetURL = URL.parse(this.tokens.storageUrl + '/' + containerName + '/' +
        normalizePath(remotePath));

    var objectPath = containerName + '/' + normalizePath(segmentsPath);

    debugRequest("MERGE", URL.format(targetURL), "objectPath=", objectPath);
    if (this._dryRun) {
      debugRequest("MERGE", URL.format(targetURL), "=> DRY RUN");
      return callback(null);
    }

    var req = request({
      method : 'PUT',
      uri : URL.format(targetURL),
      headers : {
        "X-Auth-Token" : this.tokens.id,
        "Accept" : "application/json",
        "X-Object-Manifest" : objectPath,
        "Content-Type" : Mime.lookup(remotePath),
        "Content-Length" : 0
      }

    }, (err, res, body) => {
      debugRequest("MERGE", URL.format(targetURL), "=>", (res && res.statusCode), "body=", body);

      if (!err && res && res.statusCode >= 200 && res.statusCode < 300) {
        return callback(null);
      }

      if (!err) {
        err = new Error("MERGE Request unsuccessful: url='" + remotePath +
            "' statusCode=" + (res && res.statusCode) + " body='" + body + "'");
      }
      return callback(err);
    });
  }

  putSegment(containerName, remotePath, localPath, size, partId, callback) {
    debug("[swift] putSegment: remotePath=", remotePath, "localPath=", localPath, "size=",size, "partId=", partId);

    var targetURL = URL.parse(this.tokens.storageUrl + '/' + containerName + '/' +
        normalizePath(remotePath));

    var streamOptions = {
        start : 0,
        end : size - 1
    };

    var max = this.maxRequestSize;

    if (partId > 0) {
      streamOptions.start = partId * max;
    }

    if (streamOptions.end - streamOptions.start + 1 > max) {
      streamOptions.end = streamOptions.start + max - 1;
    }

    if (streamOptions.end >= size) {
      streamOptions.end = size - 1;
    }

    var realSize = streamOptions.end - streamOptions.start + 1;

    var now = Date.now();

    debugRequest("PUT", URL.format(targetURL), "local=", localPath, 
         "startOffset=", streamOptions.start, "endOffset=", streamOptions.end);
    if (this._dryRun) {
      debugRequest("PUT",URL.format(targetURL), "=> DRY RUN");
      return callback(null);
    }

    this._slots[remotePath] = {
        realSize : realSize,
        readBytes : 0,
        start : now
    };

    debug("S #" + partId + " " + localPath);

    var req = request({
      method : 'PUT',
      uri : URL.format(targetURL),
      headers : {
        "X-Auth-Token" : this.tokens.id,
        "Content-Type" : "application/octet-stream",
        "Content-Length" : realSize
      }

    }, (err, res, body) => {
      var dt = Date.now() - now;

      delete this._slots[remotePath];

      this._showSlots();

      debugRequest("PUT", URL.format(targetURL), "=>", (res && res.statusCode), "body=", body);

      if (!err && res && res.statusCode >= 200 && res.statusCode < 300) {
        debugRequest("Time", dt, "ms", Math.floor((realSize / 1024) / (dt / 1000)), "kb/s");

        return callback(null);
      }

      if (!err) {
        err = new Error("PUT Request unsuccessful: path='" + remotePath +
            "' statusCode=" + (res && res.statusCode) + " body='" + body + "'");
      }
      return callback(err);
    });

    var readStream = fs.createReadStream(localPath, streamOptions);

    if (this._options.progress) {
      readStream.on('data', (chunk) => {
        var slot = this._slots[remotePath];

        if (!slot) {
          return;
        }
        slot.readBytes += chunk.length;

        this._showSlots();
      });
      readStream.on('end', (chunk) => {
        var slot = this._slots[remotePath];

        if (!slot) {
          return;
        }

        slot.end = Date.now();
        slot.readBytes = slot.realSize;

        this._showSlots();
      });
    }
    readStream.pipe(req);
  }

  _showSlots() {

    var s = "";

    var now = Date.now();

    var total = 0;

    for ( var path in this._slots) {
      var infos = this._slots[path];

      if (infos.end) {
        s += " [ACK " +
        Math.floor((infos.readBytes / 1024) / ((infos.end - infos.start) / 1000)) + "kb/s]";
        continue;
      }

      if (!this._options.progress) {
        s += " [" + (now - infos.start) + "ms]";
        continue;
      }

      var bp = 0;
      if (now - infos.start > 3000) {
        bp = (infos.readBytes / 1024) / ((now - infos.start) / 1000);
      }
      total += bp;

      s += " [" + Math.floor(100 * infos.readBytes / infos.realSize) + "%";
      if (bp) {
        s += " " + Math.floor(bp) + "kb/s";
      }
      s += "]";
    }
    if (!s) {
      s += " []";
    }

    if (bp) {
      s += " (Tot: " + Math.floor(total) + "kb/s)";
    }

    process.stdout.write("Upload:" + s + "\r");
  }
}

module.exports = Swift;

function normalizePath(path) {
  if (!path || path == '/') {
    return path;
  }

  var ps = path.split('/');

  var ls = [];
  for (var i = 0; i < ps.length; i++) {
    ls.push(encodeURIComponent(ps[i]));
  }

  return ls.join('/');
}

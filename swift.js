var request = require('request');
var URL = require('url');
var util = require('util');
var fs = require('fs');
var stream = require('stream');
var Sprintf = require('sprintf');
var Async = require('async');
var Mime = require('mime');

var Swift = function(authFunction, options, callback) {
  var self = this;

  this._options = options;
  this._debug = options.swiftLog || false;
  this._logRequest = options.swiftRequestLog || false;
  this._dryRun = options.dryRun || false;
  this.maxRequestSize = options.maxUploadSize || 1024 * 1024;
  this._slots = {};

  this.authFunction = authFunction;
  this.tokens = {};
  this.authFunction(function(err, tokens) {
    if (!err) {
      self.tokens = tokens;
    }
    callback(err, self);
  });
};

Swift.prototype.log = function() {
  if (!this._debug) {
    return;
  }

  console.log.apply(console, arguments);
};

Swift.prototype.logRequest = function() {
  if (!this._logRequest) {
    return;
  }

  console.log.apply(console, arguments);
};

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

Swift.prototype.getFiles = function(containerName, options, callback) {
  if (!options) {
    options = {};
  }

  this.log("[swift] getFiles: containerName='" + containerName + "' path='" +
      options.prefix + "'");

  options.delimiter = "/";
  if (options.prefix) {
    // options.prefix=normalizePath(options.prefix);
  }

  var self = this;
  var targetURL = URL.parse(self.tokens.storageUrl + '/' + containerName);

  this.logRequest("LIST '" + URL.format(targetURL) + "' prefix='" +
      options.prefix + "'");

  request({
    method : 'GET',
    uri : URL.format(targetURL),
    qs : options,
    json : {},
    headers : {
      "X-Auth-Token" : self.tokens.id,
      "Accept" : "application/json"
    }
  }, function(err, res, body) {
    self.logRequest("LIST '" + URL.format(targetURL) + "' prefix='" +
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
    return callback(err);
  });
};

Swift.prototype.$delete = function(containerName, path, ignoreError, callback) {
  this.log("[swift] delete: containerName='" + containerName + "' path='" +
      path + "'");

  var self = this;
  var targetURL = URL.parse(self.tokens.storageUrl + '/' + containerName + '/' +
      normalizePath(path));

  this.logRequest("DELETE '" + URL.format(targetURL) + "'");
  if (this._dryRun) {
    self.logRequest("DELETE '" + URL.format(targetURL) + "' => DRY RUN");
    return callback(null);
  }

  request({
    method : 'DELETE',
    uri : URL.format(targetURL),
    headers : {
      "X-Auth-Token" : self.tokens.id,
      "Accept" : "application/json"
    }
  }, function(err, res, body) {
    self.logRequest("DELETE '" + URL.format(targetURL) + "' => " +
        (res && res.statusCode) + " '" + body + "'");

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
};

Swift.prototype.mkdir = function(containerName, path, callback) {
  this.log("[swift] mkdir: containerName='" + containerName + "' path='" +
      path + "'");

  var self = this;
  var targetURL = URL.parse(self.tokens.storageUrl + '/' + containerName + "/" +
      normalizePath(path));

  this.logRequest("MKDIR '" + URL.format(targetURL) + "'");
  if (this._dryRun) {
    self.logRequest("MKDIR '" + URL.format(targetURL) + "' => DRY RUN");
    return callback(null);
  }

  request({
    method : 'PUT',
    uri : URL.format(targetURL),
    headers : {
      "X-Auth-Token" : self.tokens.id,
      "Accept" : "application/json",
      "Content-Type" : "application/directory",
      "Content-Length" : 0
    }
  }, function(err, res, body) {
    self.logRequest("MKDIR '" + URL.format(targetURL) + "' => " +
        (res && res.statusCode) + " '" + body + "'");

    if (!err && res && res.statusCode >= 200 && res.statusCode < 300) {
      return callback(null);
    }

    if (!err) {
      err = new Error("MKDIR Request unsuccessful: path='" + path +
          "' statusCode=" + (res && res.statusCode) + " body='" + body + "'");
    }
    return callback(err);
  });
};

Swift.prototype.copy = function(containerName, destName, sourceName, callback) {
  this.log("[swift] copy: destName='" + destName + "' sourceName='" +
      sourceName + "'");

  var self = this;
  var targetURL = URL.parse(self.tokens.storageUrl + '/' + containerName + '/' +
      normalizePath(destName));
  var sourceURL = containerName + '/' + normalizePath(sourceName);

  this.logRequest("COPY '" + URL.format(targetURL) + "' from='" + sourceURL +
      "'");
  if (this._dryRun) {
    self.logRequest("COPY '" + URL.format(targetURL) + "' => DRY RUN");
    return callback(null);
  }

  request({
    method : 'PUT',
    uri : URL.format(targetURL),
    headers : {
      "X-Auth-Token" : self.tokens.id,
      "X-Copy-From" : sourceURL,
      "Content-Type" : Mime.lookup(destName),
      "Content-Length" : 0,
    },
    body : ""
  }, function(err, res, body) {
    self.logRequest("COPY '" + URL.format(targetURL) + "' => " +
        (res && res.statusCode) + " '" + body + "'");

    if (!err && res && res.statusCode >= 200 && res.statusCode < 300) {
      return callback(null);
    }

    // console.log("Request '"+URL.format(targetURL)+"' => "+err);

    if (!err) {
      err = new Error("COPY Request unsuccessful: path='" + sourceName +
          "' statusCode=" + (res && res.statusCode));
    }
    return callback(err);
  });
};

Swift.prototype.put = function(containerName, remotePath, localPath, size,
    hlist, uploadQueue, cmdQueue, callback) {
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
  var self = this;
  Async.map(list, function(partId, callback) {
    var sz = size - partId * self.maxRequestSize;
    if (sz > self.maxRequestSize) {
      sz = self.maxRequestSize;
    }

    var rp = segmentName + " -" + Sprintf.sprintf("%08d", partId);
    var hr = hlist && hlist[rp];
    // console.log("#"+partId+" '"+rp+"' Found '"+hr+"' size="+sz);
    if (hr && hr.size == sz) {
      // TODO VERIFIER LA DATE AUSSI !
      return callback(null, prefix + rp);
    }

    uploadQueue.unshift(function(callback) {
      self.putSegment(containerName, prefix + rp, localPath, size, partId,
          callback);

    }, function(error) {
      callback(error, prefix + rp);
    });

  }, function(error, rps) {
    if (error) {
      return callback(error);
    }

    var mergedPath = prefix + segmentName;

    cmdQueue.unshift(function(callback) {
      if (rps.length == 1) {
        self.copy(containerName, remotePath, rps[0], function(error) {
          if (error) {
            return callback(error);
          }

          self.$delete(containerName, rps[0], false, callback);
        });
        return;
      }

      self.mergeSegments(containerName, mergedPath,
          prefix + segmentName + " -", size, function(error) {
            if (error) {
              return callback(error);
            }

            self.copy(containerName, remotePath, mergedPath, function(error) {
              if (error) {
                return callback(error);
              }

              self.$delete(containerName, mergedPath, false, function(error) {
                if (error) {
                  return callback(error);
                }

                function deletePath(partPath) {
                  cmdQueue.unshift(function(callback) {
                    self.$delete(containerName, partPath, false, callback);
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
};

Swift.prototype.mergeSegments = function(containerName, remotePath,
    segmentsPath, size, callback) {
  this.log("[swift] mergeSegments: path='" + remotePath + "' segmentsPath='" +
      segmentsPath + "'");

  var self = this;
  var targetURL = URL.parse(self.tokens.storageUrl + '/' + containerName + '/' +
      normalizePath(remotePath));

  var objectPath = containerName + '/' + normalizePath(segmentsPath);

  this.logRequest("MERGE '" + URL.format(targetURL) + "' objectPath='" +
      objectPath + "'");
  if (this._dryRun) {
    self.logRequest("MERGE '" + URL.format(targetURL) + "' => DRY RUN");
    return callback(null);
  }

  var req = request({
    method : 'PUT',
    uri : URL.format(targetURL),
    headers : {
      "X-Auth-Token" : self.tokens.id,
      "Accept" : "application/json",
      "X-Object-Manifest" : objectPath,
      "Content-Type" : Mime.lookup(remotePath),
      "Content-Length" : 0
    }

  }, function(err, res, body) {
    self.logRequest("MERGE '" + URL.format(targetURL) + "' => " +
        (res && res.statusCode) + " '" + body + "'");

    if (!err && res && res.statusCode >= 200 && res.statusCode < 300) {
      return callback(null);
    }

    if (!err) {
      err = new Error("MERGE Request unsuccessful: url='" + remotePath +
          "' statusCode=" + (res && res.statusCode) + " body='" + body + "'");
    }
    return callback(err);
  });

};

Swift.prototype.putSegment = function(containerName, remotePath, localPath,
    size, partId, callback) {
  this.log("[swift] putSegment: remotePath='" + remotePath + "' localPath='" +
      localPath + "' size=" + size + " partId=" + partId);

  var self = this;
  var targetURL = URL.parse(self.tokens.storageUrl + '/' + containerName + '/' +
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

  this.logRequest("PUT '" + URL.format(targetURL) + "'  local='" + localPath +
      "' startOffset=" + streamOptions.start + " endOffset=" +
      streamOptions.end);
  if (this._dryRun) {
    self.logRequest("PUT '" + URL.format(targetURL) + "' => DRY RUN");
    return callback(null);
  }

  this._slots[remotePath] = {
    realSize : realSize,
    readBytes : 0,
    start : now
  };

  if (!this._debug) {
    console.log("S #" + partId + " " + localPath);
  }

  var req = request({
    method : 'PUT',
    uri : URL.format(targetURL),
    headers : {
      "X-Auth-Token" : self.tokens.id,
      "Content-Type" : "application/octet-stream",
      "Content-Length" : realSize
    }

  }, function(err, res, body) {
    var dt = Date.now() - now;

    delete self._slots[remotePath];

    self._showSlots();

    self.logRequest("PUT '" + URL.format(targetURL) + "' => " +
        (res && res.statusCode) + " '" + body + "'");

    if (!err && res && res.statusCode >= 200 && res.statusCode < 300) {
      self.logRequest("Time " + dt + "ms " +
          Math.floor((realSize / 1024) / (dt / 1000)) + "kb/s");

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
    readStream.on('data', function(chunk) {
      var slot = self._slots[remotePath];

      if (!slot) {
        return;
      }
      slot.readBytes += chunk.length;

      self._showSlots();
    });
    readStream.on('end', function(chunk) {
      var slot = self._slots[remotePath];

      if (!slot) {
        return;
      }

      slot.end = Date.now();
      slot.readBytes = slot.realSize;

      self._showSlots();
    });
  }
  readStream.pipe(req);
};

Swift.prototype._showSlots = function() {

  var s = "";

  var now = Date.now();

  var total = 0;

  for ( var path in this._slots) {
    var infos = this._slots[path];

    if (infos.end) {
      s += " [ACK " +
          Math.floor((infos.readBytes / 1024) /
              ((infos.end - infos.start) / 1000)) + "kb/s]";
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

module.exports = Swift;

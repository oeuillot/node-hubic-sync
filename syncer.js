var util = require('util');
var assert = require('assert');
var Hubic = require('./hubic.js');
var HFile = require('./hfile.js');
var LFile = require('./lfile.js');
var Underscore = require('underscore');
var Async = require('async');
var Sprintf = require('sprintf');

var Syncer = function(hubic, options) {

  this._options = options || {};

  this._debug = !!this._options.syncLog;
  this._versioning = !!this._options.versioning;
  var now = new Date();
  this._options.backupDirectoryName = this._options.backupDirectoryName ||
      "___backup";
  this.backupName = Sprintf.sprintf("%04d-%02d-%02d %02d:%02d", now
      .getFullYear(), now.getMonth() + 1, now.getDate(), now.getHours(), now
      .getMinutes());
  this._eachLimit = 4;

  this._backupDirs = {};

  var self = this;
  this.moveBackupDirQueue = Async.queue(function(task, callback) {
    var hfile = task.hfile;
    var backupBase = self._options.backupDirectoryName;

    // Ouverture du dossier !
    // => Non => Creer le dossier
    // => OK => Renomme le fichier

    var backupDir = self._backupDirs[hfile.parent.path];
    if (backupDir) {
      return hfile.moveTo(backupDir, callback);
    }

    function subBackup(bbase, callback) {
      bbase.find(self.backupName, function(error, backupDir) {
        if (error) {
          return callback(error);
        }
        if (!backupDir) {
          bbase.newDirectory(self.backupName, function(error, backupDir) {
            if (error) {
              return callback(error);
            }

            self._backupDirs[hfile.parent.path] = backupDir;

            hfile.moveTo(backupDir, callback);
          });

          return;
        }
        self._backupDirs[hfile.parent.path] = backupDir;

        hfile.moveTo(backupDir, callback);
      });
    }

    hfile.parent.find(backupBase, function(error, backupDir) {
      if (error) {
        return callback(error);
      }
      if (!backupDir) {
        hfile.parent.newDirectory(backupBase, function(error, backupDir) {
          if (error) {
            return callback(error);
          }

          subBackup(backupDir, callback);
        });

        return;
      }
      subBackup(backupDir, callback);
    });
  });
};

Syncer.prototype.log = function() {
  if (!this._debug) {
    return;
  }

  console.log.apply(console, arguments);
};

Syncer.prototype._syncHFile = function(lfile, hdir, hlist, callback) {

  this
      .log("[Sync ] file local='" + lfile.path + "' remote='" + hdir.path + "'");

  var self = this;
  if (lfile.isDirectory) {
    hdir.newDirectory(lfile.name, function(error, newhdir) {
      if (error) {
        return callback(error);
      }
      return self.sync(lfile, newhdir, callback);
    });

    return;
  }

  upfile = hdir.newFile(lfile.name);

  if (!this._debug) {
    console.log("U " + lfile.path + "       ");
  }

  upfile.put(lfile, hlist, callback);
};

Syncer.prototype.sync = function(ldir, hdir, callback) {

  assert(ldir, "Invalid local dir parameter " + ldir);
  assert(hdir, "Invalid remote dir parameter " + hdir);

  if (this._debug) {
    this.log("[Sync ] Directory local='" + ldir.path + "' remote='" +
        hdir.path + "'");
  } else {
    console.log("Synchronize directory: " + ldir.path + "         ");
  }

  var self = this;
  Async.parallel([ function(callback) {
    ldir.list(callback);

  }, function(callback) {
    hdir.list(callback);

  } ], function(error, result) {
    if (error) {
      // console.log("Error = "+error);
      return callback(error);
    }

    var llist = result[0];
    var hlist = Underscore.clone(result[1]);

    Async.eachLimit(Object.keys(llist), self._eachLimit, function(name,
        callback) {
      var lfile = llist[name];
      var hfile = hlist[name];

      self.log("[Sync ] lfile='" + ((lfile) ? lfile.path : null) + "' hfile='" +
          ((hfile) ? hfile.path : null) + "'");

      if (hfile) {
        delete hlist[name];

        if (lfile.isDirectory == hfile.isDirectory) {
          if (hfile.isDirectory) {
            return self.sync(lfile, hfile, callback);
          }
          if (hfile.lastModified.getTime() > lfile.lastModified.getTime() &&
              hfile.size == lfile.size) {
            return callback(null);
          }
        }

        self.log("[Sync ] File modified  directory=" + lfile.isDirectory + "/" +
            hfile.isDirectory + " date=" + hfile.lastModified + "/" +
            lfile.lastModified + " size=" + hfile.size + "/" + lfile.size +
            "       ");

        if (!this._debug) {
          console.log("D " + hfile.path + "       ");
        }

        if (this._versioning) {
          self.moveBackupDirQueue.push({
            hfile : hfile

          }, function(error) {
            if (error) {
              return callback(error);
            }

            return self._syncHFile(lfile, hdir, hlist, callback);
          });

          return;
        }

        hfile.$delete(function(error) {
          if (error) {
            return callback(error);
          }

          return self._syncHFile(lfile, hdir, hlist, callback);
        });

        return;
      }

      self._syncHFile(lfile, hdir, hlist, callback);

    }, function(error) {
      if (error) {
        return callback(error);
      }

      var uploadings = [];

      Async.eachLimit(Object.keys(hlist), self._eachLimit, function(name,
          callback) {
        var hfile = hlist[name];

        if (name.indexOf(self._options.backupDirectoryName) === 0) {
          return callback(null);
        }

        if (self._options.uploadingPrefix &&
            name.indexOf(self._options.uploadingPrefix) === 0) {
          uploadings.push(hfile);
          return callback(null);
        }

        if (!this._debug) {
          console.log("D " + hfile.path + "       ");
        }

        self.moveBackupDirQueue.push({
          hfile : hfile

        }, callback);

      }, function(error) {
        if (error) {
          return callback(error);
        }

        if (!self._options.purgeUploadingFiles) {
          return callback();
        }

        Async.eachLimit(uploadings, self._eachLimit, function(hfile, callback) {
          // TODO Ca peut faire doublons !
          hfile.$delete(true, callback);

        }, callback);
      });
    });
  });
};

module.exports = Syncer;

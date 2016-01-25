/*jslint node: true, plusplus:true, node: true, esversion: 6 */
"use strict";

var debug = require('debug')('hsync:syncer');
var util = require('util');
var assert = require('assert');
var Hubic = require('./hubic.js');
var HFile = require('./hfile.js');
var LFile = require('./lfile.js');
var Underscore = require('underscore');
var Async = require('async');
var Sprintf = require('sprintf');

class Syncer {
  constructor(hubic, options) {

    this._options = options || {};

    this._versioning = !!this._options.versioning;
    var now = new Date();
    this._options.backupDirectoryName = this._options.backupDirectoryName ||
    "___backup";
    this.backupName = Sprintf.sprintf("%04d-%02d-%02d %02d:%02d", now
        .getFullYear(), now.getMonth() + 1, now.getDate(), now.getHours(), now
        .getMinutes());
    this._eachLimit = 4;

    this._backupDirs = {};

    this.moveBackupDirQueue = Async.queue((task, callback) => {
      var hfile = task.hfile;
      var backupBase = this._options.backupDirectoryName;

      // Ouverture du dossier !
      // => Non => Creer le dossier
      // => OK => Renomme le fichier

      var backupDir = this._backupDirs[hfile.parent.path];
      if (backupDir) {
        return hfile.moveTo(backupDir, callback);
      }

      var subBackup = (bbase, callback) => {
        bbase.find(this.backupName, (error, backupDir) => {
          if (error) {
            return callback(error);
          }
          if (!backupDir) {
            bbase.newDirectory(this.backupName, (error, backupDir) => {
              if (error) {
                return callback(error);
              }

              this._backupDirs[hfile.parent.path] = backupDir;

              hfile.moveTo(backupDir, callback);
            });

            return;
          }
          this._backupDirs[hfile.parent.path] = backupDir;

          hfile.moveTo(backupDir, callback);
        });
      }

      hfile.parent.find(backupBase, (error, backupDir) => {
        if (error) {
          return callback(error);
        }
        if (!backupDir) {
          hfile.parent.newDirectory(backupBase, (error, backupDir) => {
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
  }

  _syncHFile(lfile, hdir, hlist, callback) {

    debug("file local='" + lfile.path + "' remote='" + hdir.path + "'");

    if (lfile.isDirectory) {
      hdir.newDirectory(lfile.name, (error, newhdir) => {
        if (error) {
          return callback(error);
        }
        this.sync(lfile, newhdir, callback);
      });

      return;
    }

    var upfile = hdir.newFile(lfile.name);

    if (!this._debug) {
      console.log("U " + lfile.path + "       ");
    }

    upfile.put(lfile, hlist, callback);
  }

  sync(ldir, hdir, callback) {

    assert(ldir, "Invalid local dir parameter " + ldir);
    assert(hdir, "Invalid remote dir parameter " + hdir);

    debug("Directory local=", ldir.path, "remote=", hdir.path);

    Async.parallel([ (callback) => {
      ldir.list(callback);

    }, (callback) => {
      hdir.list(callback);

    } ], (error, result) => {
      if (error) {
        // console.log("Error = "+error);
        return callback(error);
      }

      var llist = result[0];
      var hlist = Underscore.clone(result[1]);

      Async.eachLimit(Object.keys(llist), this._eachLimit, (name, callback) => {
        var lfile = llist[name];
        var hfile = hlist[name];

        debug("lfile=", ((lfile) ? lfile.path : null), "hfile=", ((hfile) ? hfile.path : null));

        if (hfile) {
          delete hlist[name];

          if (lfile.isDirectory == hfile.isDirectory) {
            if (hfile.isDirectory) {
              return this.sync(lfile, hfile, callback);
            }
            if (hfile.lastModified.getTime() > lfile.lastModified.getTime() &&
                hfile.size == lfile.size) {
              return callback(null);
            }
          }

          debug("File modified  directory=", lfile.isDirectory, "/",
              hfile.isDirectory, "date=", hfile.lastModified, "/",
              lfile.lastModified, "size=", hfile.size, "/", lfile.size, "       ");

          if (!this._debug) {
            console.log("D " + hfile.path + "       ");
          }

          if (this._versioning) {
            this.moveBackupDirQueue.push({
              hfile : hfile

            }, (error) => {
              if (error) {
                return callback(error);
              }

              return this._syncHFile(lfile, hdir, hlist, callback);
            });

            return;
          }

          hfile.$delete((error) => {
            if (error) {
              return callback(error);
            }

            this._syncHFile(lfile, hdir, hlist, callback);
          });

          return;
        }

        this._syncHFile(lfile, hdir, hlist, callback);

      }, (error) => {
        if (error) {
          return callback(error);
        }

        var uploadings = [];

        Async.eachLimit(Object.keys(hlist), this._eachLimit, (name, callback) => {
          var hfile = hlist[name];

          if (name.indexOf(this._options.backupDirectoryName) === 0) {
            return callback(null);
          }

          if (this._options.uploadingPrefix &&
              name.indexOf(this._options.uploadingPrefix) === 0) {
            uploadings.push(hfile);
            return callback(null);
          }

          if (!this._debug) {
            console.log("D " + hfile.path + "       ");
          }

          this.moveBackupDirQueue.push({
            hfile : hfile

          }, callback);

        }, (error) => {
          if (error) {
            return callback(error);
          }

          if (!this._options.purgeUploadingFiles) {
            return callback();
          }

          Async.eachLimit(uploadings, this._eachLimit, (hfile, callback) => {
            // TODO Ca peut faire doublons !
            hfile.$delete(true, callback);

          }, callback);
        });
      });
    });
  }
}

module.exports = Syncer;

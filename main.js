/*jslint node: true, plusplus:true, node: true, esversion: 6 */
"use strict";

var program = require('commander');
var util = require('util');
var async = require('async');
var debug = require('debug')('hsync');

var Hubic = require('./hubic.js');
var HFile = require('./hfile.js');
var LFile = require('./lfile.js');
var Syncer = require('./syncer.js');

module.exports = {
  Hubic : Hubic,
  Syncer : Syncer
};

program.option("--scenario <path>", "Scenario file");

program.option("-s, --source <path>", "Source directory");
program.option("-d, --destination <path>", "Destination directory");
program.option("--hubic-log", "Enable hubic log");
program.option("--hubic-login-log", "Enable hubic login log");
program.option("--swift-log", "Enable swift log");
program.option("--swift-request-log", "Enable swift request log");
program.option("--sync-log", "Enable sync processus log");
program.option("-n, --dry-run", "Perform a trial run with no changes made");
program.option("--max-request <number>",
    "Max simultaneous swift requests  (does not include upload requests)",
    parseInt);
program.option("--max-upload <number>", "Max simultaneous updload requests",
    parseInt);
program.option("--uploading-prefix <prefix>", "Upload filename prefix");
program.option("--backup-directory-name <name>", "Backup directory name");
program.option("--container-name <name>", "Swift container name");
program.option("--purge-uploading-files", "Delete not used uploading files");
program.option("--progress", "Show progress");
program.option("--versioning", "Move modified or deleted file to a backup folder");
program.option("--tokenPath <path>", "Specify path of the last authorized token");
program.option("--clientId <id>", "Specify the Hubic application Client ID");
program.option("--clientSecret <secret>", "Specify Hubic application Client Secret");
program.option("--username <username>", "Specify Hubic username");
program.option("--password <password>", "Specify Hubic password");
program.option("--redirectURI <redirectURI>", "Specify Hubic redirect URI");
program.option("--saveTokens", "Hubic tokens must be saved");
program.parse(process.argv);

function goHubic(hubic, source, destination, callback) {
  debug("Synchronize source=", source, "destination=", destination);
  
  var syncer = new Syncer(hubic, program);

  if (destination.charAt(0) === '/') {
    destination = destination.substring(1);
  }

  var hroot = HFile.createRoot(hubic);
  var lroot = LFile.createRoot(source);
  hroot.find(destination, function(error, hfile) {
    if (error) {
      console.error("Can not find destination: '" + destination + "' ", error);

      return callback(error);
    }

    if (!lroot) {
      callback(new Error("Invalid local root for source " + program.source));
      return;
    }
    if (!hfile) {
      callback(new Error("Invalid remote root for destination " + destination));
      return;
    }

    syncer.sync(lroot, hfile, function(error) {
      if (error) {
        return callback(error);
      }

      debug("Synchronize ended");
      callback();
    });
  });
}

if (program.scenario) {
  var scenario = require(program.scenario);
  
  debug("Scenarii loaded", scenario);

  var hubic = new Hubic(program, function(error, hubic) {
    if (error) {
      console.error("Can not create hubic context: ", error);
      process.exit(1);
      return;
    }
    
    debug("Hubic engine created");

    async.eachSeries(scenario, function(sc, callback) {
      var source = sc.source;
      var destination = sc.destination;

      if (!source || !destination) {
        console.error("Source or destination are not defined !");
        return callback("Source or destination are not defined !");
      }

      goHubic(hubic, source, destination, function(error) {
        if (error) {
          console.error("Can not sync: ", error);

          callback();
          return;
        }

        callback();
      });
    }, function(error) {
      debug("Waiting last uploads ...");

      hubic.flush(function() {
        debug("Uploads flushed !");
        process.exit(0);
      });
    });
  });
  return;
}

if (!program.source) {
  console.log("Source is not specified");
  process.exit(1);
}

if (!program.destination) {
  console.log("Destination is not specified");
  process.exit(1);
}

var hubic = new Hubic(program, (error, hubic) => {
  if (error) {
    console.error("Can not create hubic context: ", error);
    return;
  }

  goHubic(hubic, program.source, program.destination, (error) => {
    if (error) {
      console.error("Can not sync: ", error);
      return;
    }

    debug("Waiting last uploads ...");

    hubic.flush(() => {
      debug("Uploads flushed !");
      process.exit(0);
    });
  });
});

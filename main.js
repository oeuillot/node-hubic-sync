var program = require('commander');
var util = require('util');
var Hubic = require('./hubic.js');
var HFile = require('./hfile.js');
var LFile = require('./lfile.js');
var Syncer = require('./syncer.js');
var Authentification = require('./authentification.js');

program.option("-u, --username <login>", "Hubic username");
program.option("-p, --passwd <passwd>", "Hubic password");
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
program.option("--versioning",
    "Move modified or deleted file to a backup folder");
program.option("--certPath <path>",
    "Specify the path of the SSL certificate (for authentication process)");
program.option("--keyPath <path>",
    "Specify the path of the SSL key (for authentication process)");
program.option("--tokenPath <path>", "Specify the path of the last authorized token");
program.option("--clientID <id>", "Specify the Hubic application Client ID");
program.option("--clientSecret <secret>", "Specify the Hubic application Client Secret");
program.parse(process.argv);

if (!program.username) {
  console.log("Username is not specified");
  process.exit(1);
}

function goHubic() {

  if (!program.token) {
    console.log("Token is not specified");
    process.exit(1);
  }

  if (!program.source) {
    console.log("Source is not specified");
    process.exit(1);
  }

  if (!program.destination) {
    console.log("Destination is not specified");
    process.exit(1);
  }

  var hubic = new Hubic(program.username, program.passwd, program, function(
      error, hubic) {
    if (error) {
      console.error("Error: " + error);
      return;
    }

    var syncer = new Syncer(hubic, program);

    var hroot = HFile.createRoot(hubic);
    var lroot = LFile.createRoot(program.source);
    hroot.find(program.destination, function(error, hfile) {
      if (error) {
        console.error("ERROR: " + error);
        return;
      }

      syncer.sync(lroot, hfile, function(error) {
        if (error) {
          console.error("ERROR: " + error);
          return;
        }
      });
    });
  });

}

if (!program.token) {
  var auth = new Authentification();
  if (program.certPath) {
    auth.certPath = program.certPath;
  }
  if (program.keyPath) {
    auth.keyPath = program.keyPath;
  }
  if (program.clientID) {
    auth.clientID = program.clientID;
  }
  if (program.clientSecret) {
    auth.clientSecret = program.clientSecret;
  }
  auth.username = program.username;

  auth.process(function(error, password) {
    if (error) {
      console.error("Can not authenticate: " + error);
      Process.exit(1);
    }

    program.passwd = password;

    goHubic();
  });

} else {
  goHubic();
}

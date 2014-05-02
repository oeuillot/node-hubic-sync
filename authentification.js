var spawn = require('child_process').spawn;
var readline = require('readline');
var fs = require('fs');
var https = require('https');
var os = require('os');

var Authentification = function() {
  this.serverPort = 20443;
};

Authentification.prototype.process = function(callback) {

  if (this.keyPath && this.certPath) {
    if (!fs.existsSync(this.keyPath)) {
      return callback("Key file does not exist: " + this.keyPath);
    }

    if (!fs.existsSync(this.certPath)) {
      return callback("Cert file does not exist: " + this.certPath);
    }

    console.log("Use key and cert paths");

    return this.openServer(callback);
  }

  this.certPath = this.tmpFile("cert");
  this.keyPath = this.tmpFile("key");

  var params = [ "req", "-newkey", "rsa:2048", "-new", "-nodes", "-x509",
      "-days", "3650", "-keyout", this.keyPath, "-out", this.certPath, "-subj",
      "/CN=" + this.username + "/OU=hsync" ];

  console.log("Start openssl with parameters " + params);

  var openssl = spawn("openssl", params);

  var self = this;
  openssl.on('close',
      function(code) {
        console.log('Openssl process exited with code ' + code);

        if (code) {
          try {
            fs.unlinkSync(self.certPath);
          } catch (x) {

          }
          try {
            fs.unlinkSync(self.keyPath);
          } catch (x) {

          }
          return callback("Failed to create certificate (return code=" + code
              + ")");
        }

        return self.openServer(function(error, password) {

          fs.unlinkSync(self.certPath);
          fs.unlinkSync(self.keyPath);

          return callback(error, password);
        });
      });
};

Authentification.prototype.tmpFile = function(suffix) {
  var now = new Date();
  var name = [ now.getYear(), now.getMonth(), now.getDate(), '-', process.pid,
      '-', (Math.random() * 0x100000000 + 1).toString(36), '.', suffix ]
      .join('');

  return name;
};

Authentification.prototype.openServer = function(callback) {
  var options = {
    key : fs.readFileSync(this.keyPath),
    cert : fs.readFileSync(this.certPath)
  };

  var server = https.createServer(options, function(req, res) {
    console.log("Get request from " + req);
    res.writeHead(200);
    res.end("hello world\n");

  });

  var self = this;
  server.on("listening", function(error) {
    if (error) {
      console.error(error);
      return;
    }

    var rl = readline.createInterface({
      input : process.stdin,
      output : process.stdout
    });

    var ifaces = os.networkInterfaces();
    var firstHostName = "localhost";
    for ( var dev in ifaces) {
      ifaces[dev].forEach(function(details) {
        if (details.family == 'IPv4' && !details.internal) {
          firstHostName = details.address;
        }
      });
    }

    var address = server.address();
    process.stdout.write("Register a new personnal application.\n"
        + "Go to https://hubic.com/home/browser/developers/\n"
        + "Specify 'https://" + firstHostName + ":" + address.port
        + "/callback/' for the redirection domain.\n")

    rl.question("Enter ClientID=", function(answer) {
      self.clientID = answer;

      rl.question("Enter ClientSecret=", function(answer) {
        self.clientSecret = answer;

        process.stdout.write("Go to 'https://" + firstHostName + ":"
            + address.port + "/register?client=" + self.username);
        rl.close();
      });
    });
  });

  server.listen(this.serverPort);
};

module.exports = Authentification;

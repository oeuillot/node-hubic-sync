var hsync = require('hsync');

var params = {};

// client_id, client_secret can be retrieved from the hubic web interface: https://hubic.com/home/browser/developers/
params.client_id = "xxxxxxxxxxxx";
params.client_secret = "yyyyyyyyyyyyy";

// If there is a problem (refresh token not retrieved), node-hubic is compatible with https://github.com/TurboGit/hubicfuse
// see git pages of this project to generate refresh_token.

var hubic = new hsync.Hubic(params, function(error, hubic) {
  if (error) {
    console.error("Can not create hubic context: ", error);
    return;
  }
});

hubic.put("/remotedir/remote-filename", "/tmp/localPath", function(error) {
  if (error) {
    console.error("Error: " + error);
    return;
  }

  console.log("DONE !");
});

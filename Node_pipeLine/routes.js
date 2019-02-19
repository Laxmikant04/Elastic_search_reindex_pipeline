var express = require("express");
var router = express.Router();

var services=require("./services.js");

  router.get('/generaldata',services.generalData);
  router.get('/bcldata',services.bclData);
  module.exports = router
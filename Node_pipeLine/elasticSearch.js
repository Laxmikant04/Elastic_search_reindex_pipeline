var elasticsearch = require('elasticsearch');
var clientlocal = new elasticsearch.Client({
   hosts: [ 'http://localhost:9200']
});


module.exports=clientlocal;
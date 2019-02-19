var elasticDb= require('./elasticSearch.js');
var generalMapping=  require('./mapping_general.json');
var bclMapping=  require('./mapping_bcl.json');

var services={
    generalData: async function(req,res){
        let index="facile_db_regular";
        let sourcedIndex="facile_db";
        await services.createMapping(false,index);
        Promise.all([
            services.generalData_batch1(sourcedIndex,index),
            services.generalData_batch2(sourcedIndex,index)
        ]).then(function(resp){
            let respObj=[];

            resp.forEach(function (response) {
                respObj.push(response);
            })
            res.status(200).json(respObj);

        }).catch(function(err){
            res.status(500).json(err);
        })
    },
    generalData_batch1:function(sourcedIndex,index){
        return new Promise(function(resolve,reject){
            elasticDb.reindex({
                requestsPerSecond:10000,
                waitForCompletion:false,
                "body":{
                    "source": {
                    index: sourcedIndex,
                    query: {
                        "bool": {
                        "must_not": [
                            {
                            "term": {
                                "email_address.keyword": {
                                "value": ""
                                }
                            }
                            }
                        ]
                        }
                    }
                  },
                  "dest": {
                    index: index
                  }
                }
            }            
            ,function(err,resp){
                if(err){
                    reject(err);
                }else{
                    resolve(resp);
                }
            })
        })
    },
    
    generalData_batch2:function(sourcedIndex,index){
        return new Promise(function(resolve,reject){
            elasticDb.reindex({
                requestsPerSecond:10000,
                waitForCompletion:false,
                "body":{
                    "source": {
                    index: sourcedIndex,
                    query:{
                          "bool": {
                            "must": [
                              {
                                "term": {
                                  "email_address.keyword": {
                                    "value": ""
                                  }
                                }
                              }
                            ]
                          }
                      }
                  },
                  "dest": {
                    index: index
                  }
                }
            }            
            ,function(err,resp){
                if(err){
                    reject(err);
                }else{
                    resolve(resp);
                }
            })
        })
    },
    bclData: async function(req,res){
        try{
            let pilelineName="to_date_test";
            let index="facile_db_bcl";
            let sourcedIndex="facile_db";
            await services.createMapping(true,index);
            await services.createPipeline(pilelineName);
            elasticDb.reindex({
                requestsPerSecond:5000,
                waitForCompletion:false,
                "body":{
                    "source": {
                    "index": sourcedIndex,
                    "query": {
                        "bool": {
                        "must_not": [
                            {
                            "term": {
                                "dialing_date.keyword": {
                                "value": ""
                                }
                            }
                            }
                        ]
                        }
                    }
                },
                "dest": {
                    "index": index,
                    "pipeline": pilelineName
                }
                }
            }            
            ,async function(err,resp){
                console.log("tested");
                await services.deletePipeline(pilelineName);
                if(err){
                    res.status(500).json(err);
                }else{
                    res.status(200).json(resp);
                }
            })
        }catch(err){
            res.status(500).json(err);
        }
        
    },
    createPipeline:function(pilelineName){
        return new Promise(function(resolve,reject){
             elasticDb.ingest.putPipeline({
                "id":pilelineName,
                "requestTimeout":60000,
                "body":{
      
                    "description" : "convert to date",
                    
                    "processors" : [
                      {
                        "date" : {
                          "field" : "dialing_date",
                          "target_field" : "dialing_date",
                          "formats" : ["ISO8601"],
                          "timezone" : "{{my_timezone}}",
                          "locale" : "{{my_locale}}",
                          "ignore_failure" : true
                        }
                      },
                      {
                        "date" : {
                          "field" : "call_updated",
                          "target_field" : "call_updated",
                          "formats" : ["ISO8601"],
                          "timezone" : "{{my_timezone}}",
                          "locale" : "{{my_locale}}",
                          "ignore_failure" : true
                        }
                      },
                      {
                        "date" : {
                          "field" : "updated_on",
                          "target_field" : "updated_on",
                          "formats" : ["ISO8601"],
                          "timezone" : "{{my_timezone}}",
                          "locale" : "{{my_locale}}",
                          "ignore_failure" : true
                        }
                      },
                      {
                        "date" : {
                          "field" : "created_on",
                          "target_field" : "created_on",
                          "formats" : ["ISO8601"],
                          "timezone" : "{{my_timezone}}",
                          "locale" : "{{my_locale}}",
                          "ignore_failure" : true
                        }
                      }
                      
                    ]
                  }
            },function(err,resp,status){
                console.log("pileline query");
                resolve(resp);
            })
        })      
        
    },
    
    deletePipeline:function(pileline){
        return new Promise(function(resolve,reject){
            elasticDb.ingest.deletePipeline({
                "id":pileline
            },function(err,resp,status){
                resolve(resp)
                });
        })
    },
    createMapping:function(isBcl,index){
        return new Promise(function(resolve,reject){
           if(isBcl){
                elasticDb.indices.create({
                    "index":index,
                    "body":bclMapping
                },function(err,resp,status){
                    resolve(resp);
                })
           }else{
            elasticDb.indices.create({
                "index":index,
                "body":generalMapping
            },function(err,resp,status){
                resolve(resp);
            })
           }
        })
    }
}
module.exports=services;
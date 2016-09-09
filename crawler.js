'use strict'
var track_tool = require('./tool/track_tool.js');
var express = require('express');
var crawler = express.Router();
crawler.post('/mission',function(req,res){
    var err_flag=0;
    var err_msg='';
    try{
        mission_token = req.body['mission_token'];
        var mission = req.body['mission'];
        graph_request_interval = mission['graph_request_interval'];
        graph_timeout_again =  mission['graph_timeout_again'];
        graph_version = mission['graph_version'];
        site = mission['site'];
        fields = mission['fields'];
    }
    catch(e){
        err_flag=1;
        err_msg=e;
    }
    finally{
        if(err_flag==1){
            res.send("[Error] "+err_msg);
            track_tool.sendResponse(res,'process_err',);
        }
        else{
            var i;
            var ids='';
            for(i=0;i<post_id.length;i++){
                trackids.push(post_id[i]);
            }
            track_tool.sendResponse(res,'ok',200);
        }
    }
});
module.exports = crawler;

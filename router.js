'use strict'
var track_tool = require('./tool/track_tool.js');
var express = require('express');
var router = express.Router();
router.post('/mission',function(req,res){
    var err_flag=0;
    var err_msg='';
    if(apply_status==0){
        track_tool.sendResponse(res,403,'Service has not prepared yet, please retry afetr 1 minutes.');
    }
    else{
        try{
            if(mode['status']=='test1'){
                mission_token = req.body['mission_token'];
                post_id =  req.body['post_id'];
                var mission = req.body['mission'];
                graph_request_interval = mission['graph_request_interval'];
                graph_timeout_again =  mission['graph_timeout_again'];
                graph_version = mission['graph_version'];
                site = mission['site'];
                fields = mission['fields'];
            }
        }
        catch(e){
            err_flag=1;
            err_msg=e;
        }
        finally{
            if(err_flag==1){
                res.send("[Error] "+err_msg);
            }
            else{
                var i;
                var ids='';
                for(i=0;i<post_id.length;i++){
                    trackids.push(post_id[i]);
                }
                res.send('id:'+JSON.stringify(trackids,null,3));
            }
        }
    }


});
module.exports = router;

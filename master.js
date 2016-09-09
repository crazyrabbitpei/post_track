'use strict'
var fs = require('fs');
var LineByLineReader = require('line-by-line');
var express = require('express');
var md5 = require('md5');
var dateFormat = require('dateformat');
var master = express.Router();

var master_setting = JSON.parse(fs.readFileSync('./service/master_setting.json'));
var mission = JSON.parse(fs.readFileSync('./service/mission.json'));

/*
* crawle申請
* 條件:invite key
* 回傳:mission_token(api存取權限), mission(交代要抓哪些post，以及詳細方法)
* */
master.post('/',function(req,res){
    var invite_token = req.body['invite_token'];
    if(invite_token!=master_setting['invite_token']){
        sendResponse(res,'token_err','','');
    }
    else{
        var mission_token = md5(req.id+new Date()+Math.floor(Math.random() * (master_setting.random['max'] - master_setting.random['min'] + 1)) + master_setting.random['min']);
        var result = {mission_token:mission_token};
        mission['track_posts']=[];
        pushIds((ids)=>{
            var i=0;
            for(i=0;i<ids.length;i++){
                mission['track_posts'].push(ids[i]);
            }
            result['mission'] = mission;
            sendResponse(res,'ok',200,result);
        });
    }
});
/*
 * crawler回報任務已完成
 * 條件:missin_token
 * */
master.post('/mission_status',function(req,res){
    var mission_token = req.body['mission_token'];
    var mission_status = req.body['mission_status'];
    if(invite_token!=master_setting['invite_token']){
        sendResponse(res,'token_err','','');
    }
    else{
        var result='testing';
        sendResponse(res,'ok',200,result);
    }
});

module.exports = master;

function pushIds(fin){
    var ids = [];
    var lr = new LineByLineReader('./service/track_id.list');
    lr.on('error', function (err) {
        console.log('[pushIds] err:'+err);
        writeLog('err',err);
    });
    lr.on('line', function (line) {
        ids.push(line);
    });

    lr.on('end', function () {
        fin(ids);
    });
}
function writeLog(type,msg){
    var now = new Date();
    var date = dateFormat(now,'yyyymmdd');
    var filename=crawler_setting['log_dir']+'/';
    if(type=='err'){
        filename += date+crawler_setting['err_filename'];
    }
    else if(type=='process'){
        filename += date+crawler_setting['process_filename'];
    }
    else{
        filename += date+crawler_setting['other_filename'];
    }

    fs.appendFile(filename,'['+now+'] Type:'+type+' Message:'+msg,(err)=>{
        if(err){
            console.log('[writeLog] Error:'+err);
        }
    });
}
function writeRec(type,track_id,msg){
    var now = new Date();
    var date = dateFormat(now,'yyyymmdd');
    var filename=crawler_setting['rec_dir']+'/';
    if(type=='gais'){
        filename += track_id+'.'+date+crawler_setting['gaisrec_filename'];
    }
    else{
        filename += track_id+'.'+date+crawler_setting['jsonrec_filename'];
    }
    fs.writeFile(filename,msg,(err)=>{
        if(err){
            console.log('[writeLog] Error:'+err);
        }
    });
}
function sendResponse(res,type,status_code,msg){
    var result = new Object();
    if(type=='token_err'){
        result['data']='';
        result['err']=master_setting['err_msg']['token_err'];
        res.status(403).send(result);
    }
    else if(type=='process_err'){
        result['data']='';
        result['err']=master_setting['err_msg']['process_err'];
        res.status(503).send(result);
    }
    else if(status_code>=200&&status_code<300){
        result['data']=msg;
        result['err']='';
        res.status(status_code).send(result);
    }
    else{
        result['data']='';
        result['err']=msg;
        res.status(status_code).send(result);
        
    }
}

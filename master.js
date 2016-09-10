'use strict'
var request = require('request');
var fs = require('fs');
var LineByLineReader = require('line-by-line');
var express = require('express');
var md5 = require('md5');
var dateFormat = require('dateformat');
var HashMap = require('hashmap');
var master = express.Router();

var master_setting = JSON.parse(fs.readFileSync('./service/master_setting.json'));
var mission = JSON.parse(fs.readFileSync('./service/mission.json'));

var crawler_tokens = new HashMap();
var graph_tokens = [];
var track_ids = [];
/*
* crawle申請
* 條件:invite key
* 回傳:mission_token(api存取權限), mission(交代要抓哪些post，以及詳細方法)
* */
loadGraphToken();
loadIds();

master.post('/',function(req,res){
    var invite_token = req.body['invite_token'];
    if(invite_token!=master_setting['invite_token']){
        sendResponse(res,'token_err','','');
    }
    else{
        /* 申請物件：
        *  1.權限token
        *  2.基本mission，包含graph_request_interval, graph_timeout_again, site, graph_version, fields, limit
        *  3.額外的mission，在基本設定檔裡(missoin.json)不會出現，為動態配置，包含graph_token, track_posts(欲追蹤的post ids)
        *
        * */
        var result = new Object();
        var min=-1;
        var token;
        
        var mission_token = md5(req.id+new Date()+Math.floor(Math.random() * (master_setting.random['max'] - master_setting.random['min'] + 1)) + master_setting.random['min']);
        while(crawler_tokens.has(mission_token)){
            mission_token = md5(req.id+new Date()+Math.floor(Math.random() * (master_setting.random['max'] - master_setting.random['min'] + 1)) + master_setting.random['min']);
        }
        manageCrawler(mission_token,req,'init');

        result['mission_token'] = mission_token;
        mission['graph_token']=getGraphToken();
        mission['track_posts'] = [];
        var id = track_ids.shift();
        var i=0;
        while(typeof id!=='undefined'){
            mission['track_posts'].push(id);
            i++;
            if(i<master_setting['ids_num']){
                id = track_ids.shift();
            }
            else{
                break;
            }
        }
        /*若有馬上指派任務，則將crawler_tokens的狀態改為1，代表目前正在搜集資料*/
        if(i!=0){
            manageCrawler(mission_token,req,'ing');
        }
        result['mission'] = mission;
        sendResponse(res,'ok',200,result);
    }
});
/*
 * POST:
 * crawler回報任務已完成
 * 條件:missin_token
 * GET:
 * 查詢crawler狀態
 * */
master.route('/mission_status')
.post(function(req,res){
    var mission_token = req.body['mission_token'];
    var mission_status = req.body['mission_status'];
    if(!crawler_tokens.has(mission_token)){
        sendResponse(res,'token_err','','');
    }
    else{
        /*將狀態給為0，代表此crawler為閒置*/
        manageCrawler(mission_token,req,'done');
        var result='get token:'+mission_token+' get status:'+mission_status;
        sendResponse(res,'ok',200,result);
        var ip='nubot3.ddns.net';
        var port='3790';
        var crawler_name='trackingCrawler';
        var crawler_version='v1.0';
        var control_token='rabbit';
        sendMission(ip,port,crawler_name,crawler_version,control_token,mission_token,(flag,msg)=>{
            if(flag=='ok'){
                console.log('get res from crawler:\n'+msg['data']);
            }
            else{
                console.log('['+flag+'] '+msg);
            }
            
        });
    }
})
.get(function(req,res){
    var crawlers=[];
    var info=new Object();
    crawler_tokens.forEach(function(value,key){
        info['token']=key;
        info['info']=value;
        crawlers.push(info);   
    });
    sendResponse(res,'ok',200,crawlers);
})

module.exports = master;

function manageCrawler(token,crawler,type){
    var info = new Object()
    if(type=='init'){
        info['cnt']=0;
        info['ip']=crawler['ip'];
        info['port']=crawler['port'];
        info['active']=new Date();
    }
    else if(type=='ing'){
        if(!crawler_tokens.has(token)){
            writeLog('err','manageCrawler, '+token+' not exists');
        }
        else{
            var temp = crawler_tokens.get(token);
            temp['cnt']++;
            info['cnt']=temp['cnt'];
            info['ip']=crawler['ip'];
            info['port']=crawler['port'];
        }
    }
    else if(type=='done'){
        if(!crawler_tokens.has(token)){
            writeLog('err','manageCrawler, '+token+' not exists');
        }
        else{
            var temp = crawler_tokens.get(token);
            temp['cnt']--;
            info['cnt']=temp['cnt'];
            info['ip']=crawler['ip'];
            info['port']=crawler['port'];
            info['active']=new Date();
        }
    }
    crawler_tokens.set(token,info);
}
function sendMission(ip,port,crawler_name,crawler_version,control_token,crawler_token,fin){
    var crawler = new Object();
    crawler['ip']=ip;
    crawler['port']=port
    mission['track_posts'] = [];
    var id = track_ids.shift();
    var i=0;
    while(typeof id!=='undefined'){
        mission['track_posts'].push(id);
        i++;
        if(i<master_setting['ids_num']){
            id = track_ids.shift();
        }
        else{
            break;
        }
    }
    if(i==0){
        fin('err','Do not have any track job.');
        return;
    }
    request({
        method:'POST',
        json:true,
        headers:{
            "content-type":"application/json"
        },
        body:{
            control_token:control_token,
            mission:mission
        },
        url:'http://'+ip+':'+port+'/'+crawler_name+'/'+crawler_version+'/mission',
        timeout:master_setting['request_timeout']*1000
    },(err,res,body)=>{
        if(!err&&res.statusCode==200){
            var err_msg='';
            var err_flag=0
            try{
                var content = body;
            }
            catch(e){
                err_flag=1;
                err_msg=e;
            }
            finally{
                if(err_flag==1){
                    writeLog('err','sendMission, '+err_msg);
                    fin('err',err_msg);
                }
                else{
                    manageCrawler(crawler_token,crawler,'ing');
                    fin('ok',content);   
                }
            }
        }
        else{
            if(err){
                if(err.code.indexOf('TIME')!=-1||err.code.indexOf('ENOT')!=-1||err.code.indexOf('ECONN')!=-1||err.code.indexOf('REACH')!=-1){
                    setTimeout(function(){
                        sendMission(ip,port,crawler_name,crawler_version,token,fin)
                    },master_setting['crawler_timeout_again']*1000);
                }
                else{
                    writeLog('err','sendMission, '+err);
                    fin('err','sendMission, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        sendMission(ip,port,crawler_name,crawler_version,token,fin);
                    },master_setting['crawler_timeout_again']*1000);
                }
                else{
                    writeLog('err','sendMission, '+res['statusCode']+', '+body['err']);
                    fin('err','sendMission, '+res['statusCode']+', '+body['err']);
                }
            }
       }
    });
}

function getGraphToken(){
    var min=-1;
    var token;
    graph_tokens.map(function(x){
        if(min==-1){
            min = x.cnt;
            token = x.token;
        }
        else{
            if(min>x.cnt){
                min = x.cnt;
                token = x.token;
            }
        }
    });
    var getTokenContent = graph_tokens.find(function(x){
        return x.token===token;
    });
    getTokenContent.cnt++;
    return token;
}

function loadGraphToken(){
    var lr = new LineByLineReader(master_setting['list_dir']+'/'+master_setting['graph_token_list']);
    lr.on('error', function (err) {
        console.log('[loadGraphToken] err:'+err);
        writeLog('err',err);
    });
    lr.on('line', function (line) {
        var parts = line.split(',');
        if(parts.length!=2){
            console.log('[loadGraphToken] err:File:graph_token.list format error');
            writeLog('err','File:'+master_setting['graph_token_list']+' format error');
            lr.end();
        }
        else{
            graph_tokens.push({token:parts[0],cnt:parts[1]});
        }
    });

    lr.on('end', function () {
        console.log('[loadGraphToken] Loading done!');
        console.log('graph_tokens:'+JSON.stringify(graph_tokens));
    });
}

function loadIds(){
    var lr = new LineByLineReader(master_setting['list_dir']+'/'+master_setting['track_id_list']);
    lr.on('error', function (err) {
        console.log('[pushIds] err:'+err);
        writeLog('err',err);
    });
    lr.on('line', function (line) {
        track_ids.push(line);
    });

    lr.on('end', function () {
        console.log('[loadIds] Loading done!');
        console.log('track_ids:'+JSON.stringify(track_ids));
    });
}
function writeLog(type,msg){
    var now = new Date();
    var date = dateFormat(now,'yyyymmdd');
    var filename=master_setting['log_dir']+'/';
    if(type=='err'){
        filename += date+master_setting['err_filename'];
    }
    else if(type=='process'){
        filename += date+master_setting['process_filename'];
    }
    else{
        filename += date+master_setting['other_filename'];
    }

    fs.appendFile(filename,'['+now+'] Type:'+type+' Message:'+msg+'\n',(err)=>{
        if(err){
            console.log('[writeLog] Error:'+err);
        }
    });
}
function writeRec(type,track_id,msg){
    var now = new Date();
    var date = dateFormat(now,'yyyymmdd');
    var filename=master_setting['rec_dir']+'/';
    if(type=='gais'){
        filename += track_id+'.'+date+master_setting['gaisrec_filename'];
    }
    else{
        filename += track_id+'.'+date+master_setting['jsonrec_filename'];
    }
    fs.writeFile(filename,msg,(err)=>{
        if(err){
            console.log('[writeLog] Error:'+err);
        }
    });
}
function sendResponse(res,type,status_code,msg){
    var result = new Object();
    result['status']=type;
    if(type=='token_err'){
        result['data']='';
        result['err']=master_setting['err_msg']['token_err'];
        res.status(403).send(result);
    }
    else if(type=='process_err'){
        result['data']='';
        result['err']=master_setting['err_msg']['process_err']+', '+msg;
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

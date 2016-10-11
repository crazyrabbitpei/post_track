'use strict'

/*
 * TODO: 
 *  1. track data center
 *  2. 接收data crawler token的API，會由crawler master那邊發出，再由track master儲存起來，token用於辨識是否為合法註冊的data crawler，才能使用/post_id功能
 *  3. data crawler要與這邊api做測試
* */
var master_tool = require('./tool/master_tool.js');
var request = require('request');
var fs = require('fs');
var LineByLineReader = require('line-by-line');
var express = require('express');
var md5 = require('md5');
var dateFormat = require('dateformat');
var HashMap = require('hashmap');
var master = express.Router();

var graph_tokens = new Map();
var track_ids = [];
var track = new master_tool.Track();
/*
 * Config
 * 讀取
 *  -master基本設定檔
 *  -Graph api token list
 *  -demo用追蹤post id，之後要以追蹤週期為單位存放post id，並讀入相對pool
 */
var master_setting = JSON.parse(fs.readFileSync('./service/master_setting.json'));
var mission = JSON.parse(fs.readFileSync('./service/mission.json'));
loadGraphToken();
loadIds();
/*給予demo用的通行証*/
//crawler_info.set(master_setting['demo_token'],new Object());

/*所有的request都必須被檢查其access token*/
master.use(function(req,res,next){
    var access_token = req.body['access_token'];
    if(!access_token){
        access_token = req.query['access_token'];
    }
    if((!track.getCrawler(access_token)&&req.path!='/apply')||(access_token!=master_setting['invite_token'])&&req.path=='/apply'){
        console.log('err token ['+access_token+']');
        sendResponse(res,'token_err','','');
        return;
    }
    next();
});
/*
 * POST
 * crawler向master申請token，並拿取任務內容
 *  -需要邀請碼
 *  -回傳任務和access_token
 */
master.post('/apply',function(req,res){
    /* 申請物件：
     *  1.權限token
     *  2.基本mission，包含graph_request_interval, graph_timeout_again, site, graph_version, fields, limit
     *  3.額外的mission，在基本設定檔裡(missoin.json)不會出現，為動態配置，包含graph_token, track_posts(欲追蹤的post ids)
     *
     * */
    var result = new Object();
    if(!(result['graph_token'] = getGraphToken())){
        sendResponse(res,'fail',200,'Do not have any usebale graph token, please try again.');
        return;
    }


    var min=-1;
    var token;

    var access_token = md5(req.ip+new Date()+Math.floor(Math.random() * (master_setting.random['max'] - master_setting.random['min'] + 1)) + master_setting.random['min']);
    while(track.getCrawler(access_token)){
        access_token = md5(req.ip+new Date()+Math.floor(Math.random() * (master_setting.random['max'] - master_setting.random['min'] + 1)) + master_setting.random['min']);
    }
    result['access_token'] = access_token;
    
    req.port = req.body.port;
    track.insertCrawler(access_token,{ip:req.ip,port:req.port});

    sendResponse(res,'ok',200,result);
    track.sendApply2DataCenter(access_token,{ip:req.ip,port:req.port},{center_ip:master_setting['center_ip'],center_port:master_setting['center_port'],center_name:master_setting['center_name'],center_version:master_setting['center_version'],control_token:master_setting['control_token']});   
    //console.log('Master:\n'+JSON.stringify(track.listCrawlers(),null,3));

});
/*
 * POST
 * crawler回報任務已完成
 *  -需要access_token
 *  -回傳是否有接收到crawler的訊息、錯誤訊息(token無效)、若有可以發出的任務 並且該crawler也有空(crawler_info map的cnt資訊記錄著目前有幾個任務還在該crawler的待辦任務裡)，則給予新任務
 * GET
 * 查詢crawler狀態
 *  -需要crawler之token
 *  -回傳該crawler目前有幾個任務正在進行、已完成幾個任務、運行時間、所在ip、名字
 * */
var test_flag=0;
master.route('/mission_report')
.post(function(req,res){
    var access_token = req.body['access_token'];
    var mission_status = req.body['mission_status'];
    var success_ids=req.body['data']['success'];
    var fai_ids=req.body['data']['fail'];
    console.log('Master receive:\n'+JSON.stringify(req.body,null,3));


    var result;

    if(track.missionStatus(access_token,mission_status)){
        result='get token:'+access_token+' get status:'+mission_status;
        sendResponse(res,'ok',200,result);
    }
    else{
        result='Update crawler ['+access_token+'] status fail!';
        sendResponse(res,'fail',200,result);
    }
    if(test_flag==0){
        //randomId({time:10,rec_num:5});
        /*
        setTimeout(function(){
            track.startSchedules('demo2');
            track.stopSchedules('demo1');
        },10*1000);
        */
        /*
        setTimeout(function(){
            track.deleteSchedule('demo1');
        },15*1000);
        */
        test_flag=1;
    }

    return;

    /*
    var ip='nubot3.ddns.net';
    var port='3790';
    var crawler_name='trackingCrawler';
    var crawler_version='v1.0';
    var control_token='rabbit';
    sendMission(ip,port,crawler_name,crawler_version,control_token,access_token,(flag,msg)=>{
        if(flag=='ok'){
            console.log('Master get res from crawler:\n'+msg['data']);
        }
        else{
            console.log('Master ['+flag+'] '+msg);
        }
    });
    */
})
.get(function(req,res){
    /*
    var crawlers=[];
    var info=new Object();
    crawler_info.forEach(function(value,key){
        info['token']=key;
        info['info']=value;
        crawlers.push(info);   
    });
    */
    sendResponse(res,'ok',200,track.listCrawlers());
})
/* POST
 * 接收來自data crawler傳來的post id
 *  -需要access_token、欲insert之id以及多久後要追蹤此post
 *  -回傳成功與否、錯誤訊息(id已存在)
 * PUT
 * 更新post id所在pool
 *  -需要access_token、欲更新之id及對象pool
 *  -回傳成功與否、錯誤訊息(id不存在)
 * DELETE
 *  -需要access_token、欲刪除之id
 *  -回傳成功與否、錯誤訊息(id不存在)
 * GET
 *  -需要欲查詢之id
 *  -回傳查詢結果(是否存在、所在pool)
 */

master.route('/post_id')
.post(function(req,res){
    var track_pages = req.body['data']['track_pages'];
    var result=new Object();
    var fail_id=[];
    track_pages.map(function(post){
        /*該post已存在於tracking list*/
        if(!track.insertIdsToPool(post.id,{created_time:post.created_time,pool_name:post.pool_name})){
            fail_id.push(post.id);
            //sendResponse(res,'ok',200,'Post id ['+post.id+'] exist.');
        }
    });
    result['fail']=fail_id;
    //result='Upload fail:['+fail_id+']';
    sendResponse(res,'ok',200,result);
})
.put(function(req,res){

})
.delete(function(req,res){
    
})
.get(function(req,res){
    var date = req.query.pool_name;
    var before = req.query.before;
    var after = req.query.after;
    var days = req.query.days;
    var result;
    if(pool_name){
        result = listPoolByName(pool_name);
    }
    else if(before||after){
        before=track.parsePoolName(before);
        after=track.parsePoolName(after);
        result = listPoolByInterval({before:before,after:after,days:days});
    }
    else{
        result = listPoolAll();       
    }

    if(!result){
        result='Can\' find any track job in pool '+pool_name; 
        sendResponse(res,'fail',200,result);
    }
    else{
        sendResponse(res,'ok',200,result);
    }

});

master.route('/status')
.get(function(req,res){
    var result={};
    result['crawlers']=track.listCrawlers();
    result['pools']=listPoolAll();
    result['schedules']=track.listSchedules();
    sendResponse(res,'ok',200,result);
});

module.exports = master;
function listPoolAll(){
    return track.listPools();
}
function listPoolByName(name){
    var pool_name;
    if(!(pool_name=track.parsePoolName(name))){
        return false;
    }
    else{
        return track.getPool(pool_name);
    }
}
/*TODO:testing*/
function listPoolByInterval({before,after,days=0}){
    var result={};
    var cnt_date = new Date(after).getDate();
    var next_date;
    if(after){
        next_date=addDays(new Date(after),1);
        result['after']=getIntervalPool(next_date,addDays(new Date(after),days));
    }
    if(before){
        if(days>0){
            days=0-days;
        }
        next_date=addDays(new Date(before),-1);
        result['before']=getIntervalPool(addDays(new Date(before),days),next_date);
    }

    return result;
}
function addDays(date,days){
    return new Date(date.getTime()+days*24*60*60*1000);
}
function getIntervalPool(from_date,to_date){
    var result=[];
    while(from_date.getTime()<to_date.getTime()){
        result.push(listPoolByName(from_date));   
        from_date=addDays(from_date,1);
    }
    return result;
}

function getGraphToken(){
    var token;
    for(let [key,value] of graph_tokens.entries()){
        if(value<master_setting.graph_token_limit){
            token=key;
            graph_tokens.set(key,value+1);
            break;
        }
    }
    if(!token){
        return false;
    }
    else{
        return token;
    }
}

function loadGraphToken(){
    var lr = new LineByLineReader(master_setting['list_dir']+'/'+master_setting['graph_token_list']);
    lr.on('error', function (err) {
        console.log('Master [loadGraphToken] err:'+err);
        writeLog('err',err);
    });
    lr.on('line', function (line) {
        var parts = line.split(',');
        if(parts.length!=2){
            console.log('Master [loadGraphToken] err:File:graph_token.list format error');
            writeLog('err','File:'+master_setting['graph_token_list']+' format error');
            lr.end();
        }
        else{
            if(!parts[1]||isNaN(parseInt(parts[1]))){
                parts[1]=0;
            }
            else{
                parts[1]=parseInt(parts[1]);
            }
            graph_tokens.set(parts[0],parts[1]);
            //graph_tokens.push({token:parts[0],cnt:parts[1]});
        }
    });

    lr.on('end', function () {

        for(let [key,value] of graph_tokens.entries()){
            console.log('Graph token:'+key+' cnt:'+value);
        }
        console.log('Master [loadGraphToken] Loading done!');
    });
}

function loadIds(){
    var lr = new LineByLineReader(master_setting['list_dir']+'/'+master_setting['track_id_list']);
    lr.on('error', function (err) {
        console.log('Master [pushIds] err:'+err);
        writeLog('err',err);
    });
    lr.on('line', function (line) {
        //track_ids.push(line);
        /*TODO:構想追蹤id list存放格式，之後要依欄位讀入memory，1.將post id作為key  2.是否被追蹤過 true/false ，被追蹤過的話則可以忽略不讀進memory 3.是否再次追蹤 true/false 4.發佈時間，沒有就留空 5.欲放入的pool name，沒有就留空*/
        var parts = line.split(',');
        if(!track.insertIdsToPool(parts[0],{pool_name:parts[4],created_time:parts[3]})){

        }
    });
    lr.on('end', function () {
        console.log('Init Master track_ids:\n'+JSON.stringify(track.listPools(),null,3));
        console.log('Master [loadIds] Loading done!');
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
            console.log('Master [writeLog] Error:'+err);
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
            console.log('Master [writeLog] Error:'+err);
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
function randomId({time=3,rec_num=5}){
    setTimeout(function(){
        produceIds('test1',{rec_num:rec_num});
    },time*1000);
}
function produceIds(option='test1',{rec_num=5,id_max=1000000,id_min=200000,date_max=0,date_min=-14,track_min=-30,track_max=30}){
    var now,plus,after;
    var track_after;
    var post=new Object();
    for(var i=0;i<rec_num;i++){
        now = new Date();
        plus = (Math.floor(Math.random()*(date_max-date_min+1)+date_min));
        after = now.getDate()+plus;
        track_after = Math.floor(Math.random()*(track_max-track_min+1)+track_min);
        post['id']=Math.floor(Math.random()*(id_max-id_min+1)+id_min);
        post['created_time']=new Date(now.setDate(after));
        after = new Date(now.setDate(now.getDate()+track_after));
        if(Math.floor(Math.random()*(10-1+1)+1)>5){
            //post['pool_name']=after.getMonth()+1+'/'+after.getDate();
            //post['pool_name']='test1';
        }
        else{
            //post['created_time']='2016/09/28 12:00:12';
        }
        if(option.indexOf('test')!=-1){
            post['pool_name']=option;
        }
        else{
            post['created_time']='2016/09/28 16:00:12';
        }
        if(!track.insertIdsToPool(post['id'],{pool_name:post['pool_name'],created_time:post['created_time']})){
            //if(!test.insertIdsToPool(post['id'],{pool_name:post['pool_name'],created_time:post['created_time']})){
            console.log('Insert id ['+post['id']+'] ['+post['created_time']+']to pool ['+post['pool_name']+'] fail!');
        }
        else{
            console.log('Insert id ['+post['id']+'] ['+post['created_time']+']to pool ['+post['pool_name']+'] success!');
        }
    }
}

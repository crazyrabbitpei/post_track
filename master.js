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


var data_crawler = new Map();
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
const service_store_info = master_setting['service_store_info'];
var mission = JSON.parse(fs.readFileSync('./service/mission.json'));
loadGraphToken();
//loadIds();

/*給予demo用的通行証*/
//crawler_info.set(master_setting['demo_token'],new Object());

/*
process.on('SIGTERM',()=>{
    console.log("[Server stop] ["+new Date()+"]");
    track.storeInfo2File('end');
});
*/

process.on('SIGINT',()=>{
    console.log("[Server stop] ["+new Date()+"]");
    clearInterval(store_service_file);
    clearInterval(store_graph_file);
    storeGraphToken2File('graph_tokens',()=>{
        console.log('Store info done : '+service_store_info['info_type']['graph_tokens']);
        track.storeInfo2File('end');
    });
});

var store_service_file = setInterval(()=>{
    track.storeInfo2File('');
},30*1000);

var store_graph_file = setInterval(()=>{
    storeGraphToken2File('graph_tokens',()=>{
        console.log('Store info done : '+service_store_info['info_type']['graph_tokens']);
    });
},300*1000);

function storeGraphToken2File(info_type,fin){
    var writeStream = fs.createWriteStream(service_store_info['dir']+'/'+service_store_info['info_type'][info_type]);
    for(let [key,value] of graph_tokens.entries()){
        writeStream.write(key+','+value+'\n');
    }
    writeStream.end();
    writeStream.on('error',(err)=>{
        console.log(err);
    });
    writeStream.on('finish',()=>{
        fin();
    });
}

/*所有的request都必須被檢查其access token*/
master.use(function(req,res,next){
    /*master專用API*/
    if(((req.path=='/dataCrawler/new')||(req.path=='/dataCrawler/delete')||(req.path=='/dataCrawler/clearall'))){
        if(req.query['control_token']!=master_setting['control_token']){
            console.log('Path:'+req.path+' [Matser API] err token ['+req.params['control_token']+']');
            sendResponse(res,'token_err','','');
        }
        else{
            next();
        }
    }
    /*data crawler上傳post id API*/
    else if(req.path=='/post_id'){
        var access_token = req.body['access_token'];
        if(!access_token){
            access_token = req.query['access_token'];
        }
        if(!data_crawler.get(access_token)&&master_setting['control_token']!=access_token){
            console.log('Path:'+req.path+' [Data crawler API] err token :['+access_token+']');
            sendResponse(res,'token_err','','');
        }
        else{
            next();
        }
    }
    /*其他track master以及查詢用API*/
    else{
        var access_token = req.body['access_token'];
        if(!access_token){
            access_token = req.query['access_token'];
        }
        if((!track.getCrawler(access_token)&&req.path!='/apply')||(access_token!=master_setting['invite_token'])&&req.path=='/apply'){
            console.log('Path:'+req.path+' [Track crawler and normal API] err token ['+access_token+']');
            sendResponse(res,'token_err','','');
        }
        else{
            next();
        }
    }
});
/*master之間用的API*/
master.get('/dataCrawler/:action(new|delete)',function(req,res){
    var action = req.params.action;
    var tokens = req.query.tokens;
    var token = tokens.split(',');
    var result=[];
    var msg;
    for(let i=0;i<token.length;i++){
        msg={};
        if(action=='new'&&data_crawler.has(token[i])){
            msg['action']='new';
            msg['token']=token[i];
            msg['status']='false';
            msg['msg']='Token ['+token[i]+'] has exists!';
            result.push(msg);
            continue;
        }

        else if(action=='delete'&&!data_crawler.has(token[i])){
            msg['action']='delete';
            msg['token']=token[i];
            msg['status']='false';
            msg['msg']='Token ['+token[i]+'] not exists!';
            result.push(msg);
            continue;
        }

        if(action=='new'){
            data_crawler.set(token[i],new Date());
            msg['action']='new';
            msg['token']=token[i];
            msg['status']='ok';
            msg['msg']='Success add data crawler token ['+token[i]+']!';
            result.push(msg);
        }
        else if(action=='delete'){
            data_crawler.delete(token[i]);
            msg['action']='delete';
            msg['token']=token[i];
            msg['status']='ok';
            msg['msg']='Success delete data crawler token ['+token[i]+']!';
            result.push(msg);
        }
    }
    sendResponse(res,'ok',200,result);
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
    result['check'] = 'Hello '+req.body['crawler_name']+' version:'+req.body['crawler_version']+' control_token:'+req.body['control_token'];

    req.port = req.body.port;
    track.insertCrawler(access_token,{ip:req.ip,port:req.port,crawler_name:req.body['crawler_name'],crawler_version:req.body['crawler_version'],control_token:req.body['control_token']});

    sendResponse(res,'ok',200,result);
    track.sendApply2DataCenter(access_token,{ip:req.ip,port:req.port},{center_ip:master_setting['my_center_ip'],center_port:master_setting['my_center_port'],center_name:master_setting['my_center_name'],center_version:master_setting['my_center_version'],control_token:master_setting['control_token']});   
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
    var fail_ids=req.body['data']['fail'];

    //console.log('Master receive:\n'+JSON.stringify(req.body,null,3));
    //將已完成的post從post_idInfo中移除，將失敗的post也移除，並放入失敗pool裡待抓取
    if(success_ids.length!=0){
        track.postTrackFinish('success',success_ids);
    }
    if(fail_ids.length!=0){
        track.postTrackFinish('fail',fail_ids);
    }
    var result='';
    if(mission_status=='offline'){
        result='Receive last mission report, good night '+req.body['crawler_name']+'. ';
    }
    if(track.missionStatus(access_token,mission_status)){
        result+='Success update crawler status.';
        sendResponse(res,'ok',200,result);
    }
    else{
        result='Update crawler status fail, who are you?';
        sendResponse(res,'fail',200,result);
    }
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

master.route('/service_status')
.get(function(req,res){
    var result={};
    result['crawlers']=track.listCrawlers();
    result['pools']=listPoolAll();
    result['schedules']=track.listSchedules();
    sendResponse(res,'ok',200,result);
});

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
/*TODO:目前雖然會接受post id，但是不讀進memory，純粹搜集 append到檔案裡存放，等到有獨立機器，或是有夠多cralwers可以消化時，在放進memory裡追蹤*/
master.route('/post_id')
.post(function(req,res){
    var track_pages = req.body['data']['track_pages'];
    var result=new Object();
    var fail_id=[];
    var success_id=[];
    track_pages.map(function(post){
        if(!track.insertIdToPool(post.id,{created_time:post.created_time,pool_name:post.pool_name})){
            fail_id.push(post.id);
        }
        else{
            success_id.push(post.id);
        }
    });
    result['success']=success_id;
    result['fail']=fail_id;
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
    if(date){
        result = listPoolByName(date);
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
        result='Can\' find any track job in pool '+date; 
        sendResponse(res,'fail',200,result);
    }
    else{
        sendResponse(res,'ok',200,result);
    }

});

//TODO:
//  1. Start
//  2. Stop

master.route('/manage_schedule/:button(on|off)')
.get(function(req,res){
    var schedule_name = req.query.schedule_name;//122344,123123,1231....
    var button = req.params.button;
    var flag;
    if(schedule_name){
        schedule_name = schedule_name.split(',');
    }
    else{
        sendResponse(res,'fail',200,'Need schedule name!');
        return;
    }

    if(button=='on'){
        console.log('Start:'+schedule_name);
        track.startSchedules(schedule_name);        
    }   
    else{
        console.log('Stop:'+schedule_name);
        track.stopSchedules(schedule_name);        
    }
    sendResponse(res,'ok',200,'Schedule:'+schedule_name+' => '+button);
})


//TODO:
//  1.Control schedule

master.route('/schedule')
.get(function(req,res){
    var schedule_name = req.query.schedule_name;//122344,123123,1231....
    var result={};
    if(schedule_name){
        var schedules = schedule_name.split(',');
        result['schedules']=track.listSchedules(schedules);
    }
    else{
        result['schedules']=track.listSchedules();
    }

    sendResponse(res,'ok',200,result);
})
.post(function(req,res){

})
.delete(function(req,res){

})
.put(function(req,res){
    
})
//TODO:
//  1.Control crawler
master.route('/crawler')
.get(function(req,res){
    
})
.post(function(req,res){

})
.delete(function(req,res){

})
.put(function(req,res){
    
})

exports.master = master;


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
    var min;
    for(let [key,value] of graph_tokens.entries()){
        //TODO:目前先不設限制，但之後要記錄
        if(!min){
            min = value;
            token = key;
            continue
        }
        if(value<min){
            min = value;
            token = key;
            graph_tokens.set(key,value+1);
            if(value==0){
                break;
            }
        }
        /*
        if(value<master_setting.graph_token_limit){
            token=key;
            graph_tokens.set(key,value+1);
            break;
        }
        */
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
    let line_cnt=0;
    lr.on('error', function (err) {
        console.log('Master [pushIds] err:'+err);
        writeLog('err',err);
    });
    lr.on('line', function (line) {
        line_cnt++;
        /*TODO:構想追蹤id list存放格式，之後要依欄位讀入memory，1.將post id作為key  2.是否被追蹤過 true/false ，被追蹤過的話則可以忽略不讀進memory 3.是否再次追蹤 true/false 4.發佈時間，沒有就留空 5.欲放入的pool name，沒有就留空*/
        var parts = line.split(',');
        if(!track.insertIdToPool(parts[0],{pool_name:parts[4],created_time:parts[3]})){
            console.log(`Loading error : ${line_cnt}`);
        }
    });
    lr.on('end', function () {
        console.log('Init Master track_ids:');
        //console.dir(track.listPools(),{colors:true});
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
        if(!track.insertIdToPool(post['id'],{pool_name:post['pool_name'],created_time:post['created_time']})){
            //if(!test.insertIdToPool(post['id'],{pool_name:post['pool_name'],created_time:post['created_time']})){
            console.log('Insert id ['+post['id']+'] ['+post['created_time']+']to pool ['+post['pool_name']+'] fail!');
        }
        else{
            console.log('Insert id ['+post['id']+'] ['+post['created_time']+']to pool ['+post['pool_name']+'] success!');
        }
    }
}

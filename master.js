'use strict'

/*
 * TODO: 
 *  1.查詢crawler狀態
 *  2.查詢追蹤pool
 *  3.管理追蹤清單
 *  4.管理設定檔
 *  5.設定檔：是否抓原文？
 *
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

var crawler_info = new HashMap();
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
//var mission = JSON.parse(fs.readFileSync('./service/mission.json'));
loadGraphToken();
loadIds();
/*給予demo用的通行証*/
//crawler_info.set(master_setting['demo_token'],new Object());

/*所有的request都必須被檢查其access token*/
master.use(function(req,res,next){
    var access_token = req.body['access_token'];
    if((!track.getCrawler(access_token)&&req.path!='/apply')||(access_token!=master_setting['invite_token'])&&req.path=='/apply'){
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
    if(!(mission['graph_token'] = getGraphToken())){
        sendResponse(res,'fail',200,'Do not have any usebale graph token, please try again.');
        return;
    }

    var result = new Object();
    var min=-1;
    var token;

    var access_token = md5(req.ip+new Date()+Math.floor(Math.random() * (master_setting.random['max'] - master_setting.random['min'] + 1)) + master_setting.random['min']);
    while(track.getCrawler(access_token)){
        access_token = md5(req.ip+new Date()+Math.floor(Math.random() * (master_setting.random['max'] - master_setting.random['min'] + 1)) + master_setting.random['min']);
    }
    result['access_token'] = access_token;
    
    req.port = req.body.port;
    track.insertCrawler(access_token,{ip:req.ip,port:req.port});


    result['mission'] = mission;
    sendResponse(res,'ok',200,result);
    console.log('Master:\n'+JSON.stringify(track.listCrawlers(),null,3));
    /*
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
    */
    /*若有馬上指派任務，則將crawler_info的狀態改為ing，代表目前正在搜集資料，若沒任務可指派 則維持init*/
    /*
    if(i!=0){
        manageCrawler(access_token,req,'ing');
    }

    */

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
master.route('/mission_status')
.post(function(req,res){
    var access_token = req.body['access_token'];
    var mission_status = req.body['mission_status'];
    var result;
    if(track.missionStatus(access_token,mission_status)){
        result='get token:'+access_token+' get status:'+mission_status;
        sendResponse(res,'ok',200,result);
    }
    else{
        result='Update crawler ['+access_token+'] status fail!';
        sendResponse(res,'fail',200,result);
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

var trackList = new HashMap();
var trackingPool = new Object();
master.route('/manageTracking')
.post(function(req,res){
    var track_pages = req.body['data']['track_pages'];
    var result=new Object();
    var fail_id=[];
    track_pages.map(function(post){
        /*該post已存在於tracking list*/
        if(!track.insertIdsToPool(post.id)){
            fail_id.push(post.id);
            //sendResponse(res,'ok',200,'Post id ['+post.id+'] exist.');
        }
    });
    result='Upload fail:'+fail_id;
    sendResponse(res,'ok',200,result);
})
.put(function(req,res){

})
.delete(function(req,res){
    
})
.get(function(req,res){
    var date = req.query.date;
    var before = req.query.before;
    var after = req.query.after;
    if(date){
        var result = listPoolByDate(date);
        if(!result){
            result='Can\' find any track job in pool '+date; 
            sendResponse(res,'fail',200,result);
        }
        else{
            sendResponse(res,'ok',200,result);
        }
    }
    else if(before||after){
        before=track.parsePoolName(before);
        after=track.parsePoolName(after);
        listPoolByInterval({before:before,after:after});
    }
    else{
        listPoolAll();       
    }

    return;
    /*沒有指定任何區間，則預設都不顯示*/
    if((typeof before!=='undefined'&&new Date(before).isValid)||(typeof after!=='undefined'&&new Date(after).isValid)){
        result.push(listTrackByInterval(before,after));
    }
    else{
        console.log(new Date(before)+', '+new Date(after));
        console.log(new Date(before).isValid+' ,'+new Date(after).isValid);
    }


});

module.exports = master;
function listPoolAll(){
    return track.listPools();
}
function listPoolByDate(date){
    var pool_name;
    if(!(pool_name=track.parsePoolName(date))){
        return false;
    }
    else{
        return track.getPool(pool_name);
    }
}
/*TODO*/
function listPoolByInterval({before,after}){
    /*如果未指定要顯示哪個日期前的資料，則預設*/
    /*
    if(typeof before=='undefined'){
        before = new Date(after).setDate(new Date(after).getDate()+31);
        console.log('before:'+before);
    }
    if(typeof after=='undefined'){
        after= new Date(after).setDate(new Date(after).getDate()+31);
        console.log('after:'+after);
    }
    */
    var result=[];
    var cnt_date = new Date(after).getDate();
    var next_date;
    if(after){
        next_date=new Date(after);
    }
    if(before){
        next_date=new Date(before);
    }

    for(){
        next_date = next_date.setDate(next_date.getDate()+1);
    }
    return {id:'0000'};
}
function searchPostById(id){
    
}
/*將從crawler拿到的資訊包起來，並算出此貼文應該在什麼時候被追蹤(pool)*/
function trackJob(id,date,pool){
    var now;
    if(typeof date==='undefined'){
        now = new Date();
    }
    else{
        now = new Date(date);
    }
    date = dateFormat(date,'yyyy/mm/dd HH:MM:ss');

    if(typeof pool==='undefined'||new Date(pool).isValid){
        var after = now.getDate()+master_setting['track_interval'];
        now.setDate(after);
        var mm = now.getMonth()+1;
        var dd = now.getDate();
        pool = mm+'/'+dd;
    }
    else{
        var is_expire;
        pool=new Date(pool);
        /*檢查crawler是否給了一個已經過期的追蹤區間*/
        if(pool.getTime()<new Date().getTime()){
            is_expire=true;
        }
        else{
            var mm = pool.getMonth()+1;
            var dd = pool.getDate();
            pool = mm+'/'+dd;
        }
    }

    this.id=id;
    this.date=date;
    /*如果欲追蹤的日期是過去的日期，則丟進past裡等待*/
    if(is_expire){
        this.pool='past';
    }
    else{
        this.pool=pool;
    }

}
/*crawler資訊都存在crawler_info map裡，key為token value為物件，包著所有crawler相關資訊*/
function manageCrawler(token,crawler,type){
    var info = new Object()
    if(type=='init'){
        info['cnt']=Math.floor(Math.random()*(5-1+1)+1);
        //info['cnt']=0;
        info['ip']=crawler['ip'];
        info['port']=crawler['port'];
        info['active']=new Date();
    }
    else if(type=='ing'){
        if(!track.getCrawler(token)){
            writeLog('err','manageCrawler, '+token+' not exists');
        }
        else{
            var temp = crawler_info.get(token);
            temp['cnt']++;
            info['cnt']=temp['cnt'];
            info['ip']=crawler['ip'];
            info['port']=crawler['port'];
        }
    }
    else if(type=='done'){
        if(!track.getCrawler(token)){
            writeLog('err','manageCrawler, '+token+' not exists');
        }
        else{
            var temp = crawler_info.get(token);
            temp['cnt']--;
            info['cnt']=temp['cnt'];
            info['ip']=crawler['ip'];
            info['port']=crawler['port'];
            info['active']=new Date();
        }
    }
    crawler_info.set(token,info);
}
function getCrawler(){
    var keys = crawler_info.keys();
    var i,min,index,result;
    min = crawler_info.get(keys[1]).cnt;//因為第一項為demo用token，所以index 從1開始
    index=1;
    for(i=2;i<keys.length;i++){
        console.log(keys[i]+':'+JSON.stringify(crawler_info.get(keys[i]))+':'+crawler_info.get(keys[i]).cnt);
        if(crawler_info.get(keys[i]).cnt<min){
            min=crawler_info.get(keys[i]).cnt;
            index=i;
        }
    }
    return crawler_info.get(keys[index]);
}
function sendMission(ip,port,crawler_name,crawler_version,control_token,crawler_token,fin){
    var crawler = new Object();
    crawler['ip']=ip;
    crawler['port']=port
    mission['track_posts'] = [];
    var id = track_ids.shift();
    var i=0;
    while(typeof id!=='undefined'){
        console.log('Push out:'+id);
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
            if(!parta[1]||isNaN(parseInt(parts[1]))){
                parts[1]=0;
            }
            else{
                part[1]=parseInt(parts[1]);
            }
            graph_tokens.set(parts[0],parts[1]);
            //graph_tokens.push({token:parts[0],cnt:parts[1]});
        }
    });

    lr.on('end', function () {
        console.log('Master [loadGraphToken] Loading done!');
        console.log('Master graph_tokens:'+JSON.stringify(graph_tokens));
    });
}

function loadIds(){
    var lr = new LineByLineReader(master_setting['list_dir']+'/'+master_setting['track_id_list']);
    lr.on('error', function (err) {
        console.log('Master [pushIds] err:'+err);
        writeLog('err',err);
    });
    lr.on('line', function (line) {
        track_ids.push(line);
    });

    lr.on('end', function () {
        console.log('Master [loadIds] Loading done!');
        console.log('Master track_ids:'+JSON.stringify(track_ids));
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

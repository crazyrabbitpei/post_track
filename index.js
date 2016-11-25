    'use strict'
/*
 * TODO:
 *  1.如果沒有同時要求reactoins, comments...有多個下一頁的欄位，要讓程式還是能執行
 *  2.將資料回傳到data center
 * */


var track_tool = require('./tool/track_tool.js');
var crawler = require('./crawler.js');

var master = require('./master.js').master;
var master_tool = require('./tool/master_tool.js');

var CronJob = require('cron').CronJob;
var EventEmitter = require('events');
var querystring = require('querystring');
var fs = require('graceful-fs');
var express = require('express');
var bodyParser = require('body-parser');

const Console = require('console').Console;
const output = fs.createWriteStream('./logs/execute.log');
const errorOutput = fs.createWriteStream('./logs/execute_err.log');
const logger = new Console(output, errorOutput);

var app = express();
var http = require('http');
var server = http.createServer(app);


var crawler_setting = JSON.parse(fs.readFileSync('./service/crawler_setting.json'));
var crawler_ip = crawler_setting['crawler_ip'];
var crawler_port = crawler_setting['crawler_port'];
var crawler_name = crawler_setting['crawler_name'];
var crawler_version = crawler_setting['crawler_version'];
var control_token = crawler_setting['control_token']

var write2Local = crawler_setting['write2Local'];

var master_ip = crawler_setting['master_ip'];
var master_port = crawler_setting['master_port'];
var master_name = crawler_setting['master_name'];
var master_version = crawler_setting['master_version'];
var master_timeout_again = crawler_setting['master_timeout_again'];
var invite_token = crawler_setting['invite_token'];
var request_timeout = crawler_setting['request_timeout'];

const _version=crawler_setting['mission']['status'];

var mission={};//存放Master給予的追蹤設定檔
mission['token']={};
mission['info']={};
var trackids=[];//存放待追蹤的post id
var success_ids=[];
var fail_ids=[];

var start_track,end_track;
var current_post_id='';//目前正在抓取的id
/*設定上傳條件*/
var upload_schedule,rec_size=0,rec_num=0,upload_process=false;
var UPLOAD_INTERVAL,WAIT_TIME,REC_NUM,REC_SIZE,UPLOAD_TIME;

var force_upload_flag=false;
var all_fetch=3;//有兩個資訊有下一頁問題：shareposts, reactions, comments，要等到這兩個都抓完後程式才算完成
var graph_request=0;//計算總共發了多少Graph api;
var processing=0;//如果目前有post還在進行追蹤就是1，否則為0

var processing_flag=false;//會先完成手邊任務(現有的post追蹤完畢、資料都上傳完，才能結束程式) 可停止:true
var uploading_flag=false;//當正在上傳到data center時，是無法強行中止程式
var uploading_flag_my=false;//當正在上傳到my data center時，是無法強行中止程式

var single_result;//存放單一筆結果
var final_result={};//存放所有結果
final_result['data']=[];

class MyEmitter extends EventEmitter {};
const myEmitter = new MyEmitter();

process.on('SIGINT',()=>{
    waitingStop('[SIGINT] Press Ctrl+C');
});
function waitingStop(msg){
    let stop = setInterval(()=>{
        if(!uploading_flag&&!uploading_flag_my&&!processing_flag){
            /*
            if(success_ids.length!=0||fail_ids.length!=0){//尚未回報目前狀態給track master的話，必須將現有資訊回傳才能結束程式
                missionReport('offline');
                let wait_for_report = setInterval(()=>{
                    if(!uploading_flag&&!uploading_flag_my&&!processing_flag){
                        clearInterval(wait_for_report)
                        console.log("[Crawler stop] ["+new Date()+"] Reason:"+msg);
                        process.exit(0);
                    }
                },100);
            }
            else{
                console.log("[Crawler stop] ["+new Date()+"] Reason:"+msg);
                process.exit(0);
            }
            */
            missionReport('offline');
            let wait_for_report = setInterval(()=>{
                if(!uploading_flag&&!uploading_flag_my&&!processing_flag){
                    clearInterval(wait_for_report)
                    console.log("[Crawler stop] ["+new Date()+"] Reason:"+msg);
                    process.exit(0);
                }
            },100);
            clearInterval(stop);
        }
    },500);
}

/*控制何時將搜集資料upload到master*/

myEmitter.on('nextcomment', (link) => {
    //console.log('nextcomment=>'+link);
    graph_request++;
    logger.time('fetchNextPage-nextcomment');
    track_tool.fetchNextPage(mission,link,function(flag,msg){
        logger.timeEnd('fetchNextPage-nextcomment');
        if(flag=='err'){
            console.log('[fetchNextPage-nextcomment] err:'+JSON.stringify(msg));
            myEmitter.emit('one_post_done',{track_status:'nexterr',data:link});
        }
        else{
            var i;
            var next_result = new track_tool.parseComment(mission['info']['field2'],msg);
            for(i=0;i<next_result.comments.length;i++){
                single_result.comments.push(next_result.comments[i]);
            }
            //single_result.next_comments = next_result.next_comments;
            if(next_result.next_comments!=null){
                myEmitter.emit('nextcomment',next_result.next_comments);
            }
            else{
                all_fetch--;
                if(all_fetch==0){
                    myEmitter.emit('one_post_done',{track_status:'ok',data:''});
                }
            }
        }
    });
});
myEmitter.on('nextsharedpost', (link) => {
    graph_request++;
    logger.time('fetchNextPage-nextsharedpost');
    track_tool.fetchNextPage(mission,link,function(flag,msg){
        logger.timeEnd('fetchNextPage-nextsharedpost');
        if(flag=='err'){
            console.log('[fetchNextPage-nextsharedpost] err:'+JSON.stringify(msg));
            myEmitter.emit('one_post_done',{track_status:'nexterr',data:link});
        }
        else{
            var next_result = new track_tool.parseSharedpost(mission['info']['field1'],msg);
            var i;
            for(i=0;i<next_result.sharedposts.length;i++){
                single_result.sharedposts.push(next_result.sharedposts[i]);
            }
            //single_result.next_sharedposts = next_result.next_sharedposts;
            if(next_result.next_sharedposts!=null){
                myEmitter.emit('nextcomment',next_result.next_sharedposts);
            }
            else{
                all_fetch--;
                if(all_fetch==0){
                    myEmitter.emit('one_post_done',{track_status:'ok',data:''});
                }
            }
        }
    });
});
myEmitter.on('nextreaction', (link) => {
    graph_request++;
    logger.time('fetchNextPage-nextreaction');
    track_tool.fetchNextPage(mission,link,function(flag,msg){
        logger.timeEnd('fetchNextPage-nextreaction');
        if(flag=='err'){
            console.log('[fetchNextPage-nextreaction] err:'+JSON.stringify(msg));
            myEmitter.emit('one_post_done',{track_status:'nexterr',data:link});
        }
        else{
            var next_result = new track_tool.parseReaction(mission['info']['field1'],msg);
            var i;
            var reac_type = Object.keys(next_result.reactions);
            for(i=0;i<reac_type.length;i++){
                if(typeof single_result.reactions[reac_type[i]]==='undefined'){
                    single_result.reactions[reac_type[i]]=0;
                }
                single_result.reactions[reac_type[i]]+=next_result.reactions[reac_type[i]];
            }
            if(next_result.next_reactions!=null){
                myEmitter.emit('nextreaction',next_result.next_reactions);
            }
            else{
                all_fetch--;
                if(all_fetch==0){
                    myEmitter.emit('one_post_done',{track_status:'ok',data:''});
                }
            }
        }
    });
});

/*
* flag:
*   1.ok:完整追蹤完畢
*   2.nexterr:追蹤不完整
*   3.err:完全追蹤失敗
* */


myEmitter.on('one_post_done',({track_status,data})=>{
    if(track_status=='ok'){
        /*計算出share, reactions, comment總數*/
        if(cntTrackLog()){
            /*當有新的一筆資料產生時，放入資料水桶中，等待被upload到master*/
            resultBucket();
            /*將追蹤資訊存到local端*/
            writeTrackLog();
        }
        else{
            fs.appendFile('./check_one_post_done','id:'+current_post_id+' not have single_result\n',()=>{});
        }
        /*記錄追蹤成功的id*/
        success_ids.push(current_post_id);
        /*開始搜集下一個*/
        nextTrack();
    }
    else if(track_status=='err'){
        console.log('Can\'t track post id ['+current_post_id+']!');
        fail_ids.push(current_post_id);
        nextTrack();
    }
    else{
        console.log('Stop track at link:'+data);
        track_tool.writeLog('err','track next, stop track at link:'+data);
    }
});

function reset(){
    all_fetch=3;
    graph_request=0;
    single_result=null;
    processing=0;//該貼文追蹤完畢，可以繼續接收下個任務

}
function nextTrack(){
    var temp = trackids.shift();
    if(temp){
        processing_flag=false;
        /*Reset基本記錄*/
        reset();//成功和失敗track id不會在reset裡清空
        current_post_id = temp;
        start(current_post_id);
    }
    else{
        if(_version=='test'){
            console.log('All post id have been tracked.');
            processing_flag=false;
        }
        else if(_version=='test1'||_version=='test2'||_version=='test3'){
            console.log('All post id have been tracked, waiting for next mission!');
            /*現有追蹤id已搜集完畢，但是資料水桶裡還有尚未被上傳的資料，可能原因：尚未湊滿上傳條件(rec_num, rec_size...)，需等待下一批任務來湊滿條件，但也可能沒有下一批任務，故需要設定一等待區間，若超過這個區間沒有湊滿上傳條件，則忽略條件 直接上傳和清空資料水桶裡的資料*/
            if(final_result['data'].length!=0){
                if(!force_upload_flag){
                    force_upload_flag=true;//若在時間內沒有接收到下一批任務(force_upload_flag=false)，則代表條件可能暫時不會滿足，所以就強制上傳現有資料
                    setTimeout(()=>{
                        if(force_upload_flag){
                            flush();
                        }
                    },WAIT_TIME*1000);
                }
            }
            /*向track master通知任務已完成*/
            var mission_status='done';
            missionReport(mission_status);
        }
        else{
            console.log('Can\'t handle other mission version ['+_version+']');
            //console.dir(mission,{colors:true});
        }
    }



}
function missionReport(mission_status){
    processing_flag=true;
    var data={};
    data['success']=success_ids;
    data['fail']=fail_ids;
    logger.time('missionReport');
    track_tool.missionReport({crawler_name,data,master_ip,master_port,master_name,master_version,access_token:mission['token']['access_token'],mission_status},(flag,msg)=>{
        logger.timeEnd('missionReport');
        processing_flag=false;//回報完後就可終止程式
        if(flag=='ok'&&msg&&msg['data']&&msg['status']=='ok'){
            console.log('[missionReport] :'+JSON.stringify(msg,null,2));
            //console.dir(msg,{colors:true});
            /*Reset基本記錄*/
            reset();
            /*因為要重新拿取下一批track id，所以可以把已回報的成功 失敗清單清空*/
            success_ids=[];
            fail_ids=[];
        }
        else{
            console.log('[missionReport] err:'+JSON.stringify(msg));
            //console.dir(msg,{colors:true});
        }

    });
}
function resultBucket(){
    if(single_result==null){
        console.log('[resultBucket] single_result is null');
        waitingStop('[resultBucket] single_result is null');
        return;
    }
    //console.log(JSON.stringify(single_result));
    rec_num++;
    rec_size+=Buffer.byteLength(JSON.stringify(single_result));
    
    fs.appendFile('./check_one_post_done','id:'+current_post_id+' total size:'+rec_size+' single size:'+Buffer.byteLength(JSON.stringify(single_result))+'\n',()=>{});
    /*將當前資料合併到final_result*/
    final_result['data'].push(single_result);
    //final_result[current_post_id]=single_result;
    
    track_tool.writeLog('process','Data bucket info, current fecth id:'+current_post_id+' total size:'+rec_size+' toal num:'+rec_num+' single size:'+Buffer.byteLength(JSON.stringify(single_result)));
    /*將資料存到data server*/
    if(UPLOAD_INTERVAL=='real_time'){
        track_tool.writeLog('process','Upload by ['+UPLOAD_INTERVAL+']! total size:'+rec_size+' toal num:'+rec_num);
        flush();
    }
    /*TODO:無法成功設定*/
    else if(UPLOAD_INTERVAL=='rec_size'){
        if(rec_size>=REC_SIZE){
            track_tool.writeLog('process','Upload by ['+UPLOAD_INTERVAL+']! total size:'+rec_size+' toal num:'+rec_num);
            flush();
        }
    }
    else if(UPLOAD_INTERVAL=='rec_num'){
        if(rec_num>=REC_NUM){
            track_tool.writeLog('process','Upload by ['+UPLOAD_INTERVAL+']! total size:'+rec_size+' toal num:'+rec_num);
            flush();
        }
    }
    /*TODO:testing*/
    else if(UPLOAD_INTERVAL=='time'){
        if(upload_process){
            track_tool.writeLog('process','Upload by ['+UPLOAD_INTERVAL+']! total size:'+rec_size+' toal num:'+rec_num);
            flush();
            upload_process=false;
        }
    }
}


function cntTrackLog(){
    var i,cnt=0;
    var err_flag=0;

    logger.timeEnd('track_post');
    console.log('==ok==ok==['+current_post_id+']==ok==ok==')
    logger.log('==ok==ok==['+current_post_id+']==ok==ok==');
    if(single_result){
        var reac_type = Object.keys(single_result.reactions);
        for(i=0;i<reac_type.length;i++){
            cnt+=single_result.reactions[reac_type[i]];
        }
        single_result.reactions_cnt = cnt;
        single_result.comments_cnt = single_result.comments.length;
        single_result.shared_posts_cnt = single_result.sharedposts.length;
        return true;
    }
    else{
        return false;
    }
}
function writeTrackLog(){
    end_track = new Date();
    var duration = end_track.getTime()-start_track.getTime();
    /*記錄追蹤資訊*/
    duration /=1000;
    var track_log={};
    track_log['post_id']=current_post_id;
    track_log['track_time']=duration;
    track_log['comments_cnt']=single_result.comments_cnt;
    track_log['reactions_cnt']=single_result.reactions_cnt;
    track_log['sharedposts_cnt']=single_result.sharedposts_cnt;
    track_tool.writeLog('process','Track info, '+JSON.stringify(track_log));
}
function flush(){
    
    if(final_result['data'].length==0||typeof final_result==='undefined'){
        console.log('[flush] final_result length:'+final_result['data'].length+' final_result:'+final_result);
        return;
    }
    if(final_result['data'][0]=='@'){
        fs.appendFile('./err.check','[flush] '+data+'\n',(err)=>{
            waitingStop('[flush] final_result[\'data\'] format error');
        })
        return;
    }

    let temp = Object.assign({}, final_result);
    final_result={};//存放所有結果
    final_result['data']=[];
    if(write2Local){
        console.log('Flush data to local!');
        track_tool.writeRec(mission['info']['datatype'],temp);
    }
    uploading_flag_my=true;
    logger.time('my_uploadTrackPostData');
    track_tool.my_uploadTrackPostData(mission['info']['master'],mission['token']['access_token'],{data:temp,datatype:mission['info']['datatype']},{center_ip:mission['info']['my_center_ip'],center_port:mission['info']['my_center_port'],center_name:mission['info']['my_center_name'],center_version:mission['info']['my_center_version']},(flag,msg)=>{
        logger.timeEnd('my_uploadTrackPostData');
        uploading_flag_my=false;
        if(flag=='ok'){
            console.log('[my_uploadTrackPostData] '+msg);
        }
        else if(flag=='err'){
            console.log('[my_uploadTrackPostData] '+msg);
        }
        else if(flag=='off'){
            console.log('[my_uploadTrackPostData] '+msg);
        }
        

    });
    uploading_flag=true;
    logger.time('uploadTrackPostData');
    track_tool.uploadTrackPostData(mission['info']['master'],{data:temp,datatype:mission['info']['datatype']},{center_ip:mission['info']['center_ip'],center_port:mission['info']['center_port'],center_url:mission['info']['center_url']},(flag,msg)=>{
        uploading_flag=false;
        logger.timeEnd('uploadTrackPostData');
        if(flag=='ok'){
            console.log('[uploadTrackPostData] '+msg);
        }
        else if(flag=='err'){
            console.log('[uploadTrackPostData] '+msg);
        }
        else if(flag=='off'){
            console.log('[uploadTrackPostData] '+msg);
        }
    });

    rec_num=0;
    rec_size=0;
}
/**
 * --階段--
 * 測試模式(test)，為單機執行 不需要開啟api，先測試是否可以正確搜集到資料 並將資料合併成gaisRec，基本參數設定在loca端的crawler_setting.json file裡
 *  - log, error formater
 *  - request to fb
 *      v nexp page
 *      r
 *      - error handler
 *      - retry handler
 *  - single_result formater
 *  - track_posts array manager
 *  - next page manager
 * 正式模式(formal)，需要監聽master傳送的資料，包含可搜集的track_posts和mission內容，並能將資料正確回傳至master，且告知當前任務已完成
 *  - express mission api
 *  - request to track master
 *  - service status api:查詢當前搜集器的狀況
 *      - 運行時間
 *      - 正在追蹤or待機中
 *      - 已追蹤數量and正在追蹤的進度，ex: 3/10
 * 
**/
if(_version=='test'){
    var track_posts;
    invite_token='test';
    track_posts = crawler_setting['mission']['track_posts'];

    trackids = track_posts;
    var temp = trackids.shift();
    if(!temp){
        current_post_id = temp;
        start(current_post_id);
    }
}
else{
    app.use(bodyParser.json());
    app.use(bodyParser.urlencoded({ extended: true  }));

    //程式啟動，向tracking master報告
    server.listen(crawler_port,crawler_ip,function(){
            console.log("[Server "+crawler_name+" start] ["+new Date()+"] http work at "+crawler_ip+":"+crawler_port);
            if(_version=='test1'){
                /*client同時扮演master，沒有這行的話，就是一個完整的post track crawler*/
                //app.use('/'+master_name+'/'+master_version,master);
                /* TODO
                *  ok 1. 要將搜集到的訊息上傳至資料中心存放
                *  2. 當程式停止時，主動向master發送停止訊息
                *  3. 將從master那得來的設定檔和任務儲存到temp_pool裡，若有重新認證的情況時再將新的覆蓋回去
                */
                logger.time('applyCrawler');
                track_tool.applyCrawler({control_token,crawler_name,crawler_version,crawler_port,master_ip,master_port,master_name,master_version,invite_token},(flag,msg)=>{
                    logger.timeEnd('applyCrawler');
                    if(flag=='ok'&&msg&&msg['data']&&msg['status']=='ok'){
                        console.log('[applyCrawler] success:'+msg['data']['check']);
                        app.use('/'+crawler_name+'/'+crawler_version,crawler);
                        mission['token']['graph_token']=msg['data']['graph_token'];
                        mission['token']['access_token']=msg['data']['access_token'];

                        track_tool.reImportData('mydata',mission['info']['master'],mission['token']['access_token'],{datatype:mission['info']['datatype']},{center_ip:mission['info']['my_center_ip'],center_port:mission['info']['my_center_port'],center_name:mission['info']['my_center_name'],center_version:mission['info']['my_center_version']},(flag,msg)=>{
                            if(flag=='ok'){
                                console.log('[reImportData-mydata] ok:'+msg);
                                track_tool.writeLog('process','[reImportData-mydata] ok:'+msg);
                            }
                            else if(flag=='err'){
                                console.log('[reImportData-mydata] '+msg);
                                waitingStop('[reImportData-mydata] err:'+msg);
                            }
                            else if(flag=='off'){
                                console.log('[reImportData-mydata] '+msg);
                                track_tool.writeLog('process','[reImportData-mydata] '+msg);
                            }

                        });
                        track_tool.reImportData('data',mission['info']['master'],{datatype:mission['info']['datatype']},{center_ip:mission['info']['center_ip'],center_port:mission['info']['center_port'],center_url:mission['info']['center_url']},(flag,msg)=>{
                            if(flag=='ok'){
                                console.log('[reImportData-data] ok:'+msg);
                                track_tool.writeLog('process','[reImportData-data] ok:'+msg);
                            }
                            else if(flag=='err'){
                                console.log('[reImportData-data] '+msg);
                                waitingStop('[reImportData-data] err:'+msg);
                            }
                            else if(flag=='off'){
                                console.log('[reImportData-data] '+msg);
                                track_tool.writeLog('process','[reImportData-data] '+msg);
                            }
                        });

                        var mission_status='done';
                        track_tool.reImportMission('missreport',{crawler_name,master_ip,master_port,master_name,master_version,access_token:mission['token']['access_token'],mission_status},(flag,msg)=>{
                            if(flag=='ok'&&msg&&msg['data']&&msg['status']=='ok'){
                                console.log('[reImportMission] ok:'+JSON.stringify(msg,null,2));
                            }
                            else{
                                console.log('[reImportMission] err:'+JSON.stringify(msg));
                                waitingStop('[reImportMission] err:'+msg);
                            }
                        });
                    }
                    else{
                        console.log('[applyCrawler] err:'+JSON.stringify(msg));
                        waitingStop('[applyCrawler] err:'+JSON.stringify(msg));
                    }

                });
            }
            else if(_version=='test2'){
                mission['token']['access_token']=crawler_setting['mission']['access_token'];
                master_tool.connect2MyDataCenter((flag,msg)=>{
                    console.log('[connect2MyDataCenter] '+msg);
                    if(flag||flag=='off'){
                        master_tool.connect2DataCenter((flag,msg)=>{
                            console.log('[connect2DataCenter] '+msg);
                            if(flag||flag=='off'){
                                app.use('/'+master_name+'/'+master_version,master);
                            }
                            else{
                                console.log('Can\'t connect to Data Center!');
                                waitingStop('Can\'t connect to Data Center!');
                            }
                        });
                    }
                    else{
                        console.log('Can\'t connect to My Data Center!');
                        waitingStop('Can\'t connect to My Data Center!');
                    }
                });
            }
            /*此模式為了要測試master的manageTracking API，需要準備一個偽data crawler來抓取fb資料，並呼叫manageTracking API來將抓取到的post id, created_time塞給track master做儲存*/
            else if(_version=='test3'){
                mission['token']['access_token']=crawler_setting['mission']['access_token'];
                master_tool.connect2MyDataCenter((flag,msg)=>{
                    console.log('[connect2MyDataCenter] '+msg);
                    if(flag||flag=='off'){
                        master_tool.connect2DataCenter((flag,msg)=>{
                            console.log('[connect2DataCenter] '+msg);
                            if(flag||flag=='off'){

                                app.use('/'+master_name+'/'+master_version,master);
                                logger.time('applyCrawler');
                                track_tool.applyCrawler({control_token,crawler_name,crawler_version,crawler_port,master_ip,master_port,master_name,master_version,invite_token},(flag,msg)=>{
                                    logger.timeEnd('applyCrawler');
                                    if(flag=='ok'&&msg&&msg['data']&&msg['status']=='ok'){
                                        app.use('/'+crawler_name+'/'+crawler_version,crawler);
                                        mission['token']['graph_token']=msg['data']['graph_token'];
                                        mission['token']['access_token']=msg['data']['access_token'];
                                        console.log('[applyCrawler] success');
                                        //console.dir(msg,{colors:true});
                                        logger.time('listTrack');
                                        track_tool.listTrack({master_ip,master_port,master_name,master_version,access_token:mission['token']['access_token']},(flag,msg)=>{
                                            logger.timeEnd('listTrack');
                                            //console.log('listTrack:\n'+JSON.stringify(msg,null,3));
                                        });

                                        track_tool.reImportData('mydata',mission['info']['master'],mission['token']['access_token'],{datatype:mission['info']['datatype']},{center_ip:mission['info']['my_center_ip'],center_port:mission['info']['my_center_port'],center_name:mission['info']['my_center_name'],center_version:mission['info']['my_center_version']},(flag,msg)=>{
                                            if(flag=='ok'){
                                                console.log('[reImportData-mydata] ok:'+msg);
                                                track_tool.writeLog('process','[reImportData-mydata] ok:'+msg);
                                            }
                                            else if(flag=='err'){
                                                console.log('[reImportData-mydata] '+msg);
                                                waitingStop('[reImportData-mydata] err:'+msg);
                                            }
                                            else if(flag=='off'){
                                                console.log('[reImportData-mydata] '+msg);
                                                track_tool.writeLog('process','[reImportData-mydata] '+msg);
                                            }

                                        });
                                        track_tool.reImportData('data',mission['info']['master'],{datatype:mission['info']['datatype']},{center_ip:mission['info']['center_ip'],center_port:mission['info']['center_port'],center_url:mission['info']['center_url']},(flag,msg)=>{
                                            if(flag=='ok'){
                                                console.log('[reImportData-data] ok:'+msg);
                                                track_tool.writeLog('process','[reImportData-data] ok:'+msg);
                                            }
                                            else if(flag=='err'){
                                                console.log('[reImportData-data] '+msg);
                                                waitingStop('[reImportData-data] err:'+msg);
                                            }
                                            else if(flag=='off'){
                                                console.log('[reImportData-data] '+msg);
                                                track_tool.writeLog('process','[reImportData-data] '+msg);
                                            }
                                        });
                                        var mission_status='done';
                                        track_tool.reImportMission('missreport',{crawler_name,master_ip,master_port,master_name,master_version,access_token:mission['token']['access_token'],mission_status},(flag,msg)=>{
                                            if(flag=='ok'&&msg&&msg['data']&&msg['status']=='ok'){
                                                console.log('[reImportMission] ok:'+JSON.stringify(msg,null,2));
                                            }
                                            else{
                                                console.log('[reImportMission] err:'+JSON.stringify(msg));
                                                waitingStop('[reImportMission] err:'+msg);
                                            }
                                        });

                                    }
                                    else{
                                        console.log('[applyCrawler] err:'+JSON.stringify(msg));
                                        waitingStop('[applyCrawler] err:'+JSON.stringify(msg));
                                    }
                                });
                            }
                            else{
                                console.log('Can\'t connect to Data Center!');
                                waitingStop('Can\'t connect to Data Center!');
                            }
                        });
                    }
                    else{
                        console.log('Can\'t connect to My Data Center!');
                        waitingStop('Can\'t connect to My Data Center!');
                    }
                });

            }
    })
}

function start(track_id){
    if(processing==1){//還有任務沒完成，不會進下個任務
        console.log('Push back:'+track_id);
        trackids.push(track_id);//將任務再塞回去
        return;
    }
    start_track = new Date();

    processing=1;//開關打開，在完成當前任務前都不會接受任何新任務，亦即，同時只會追蹤一個貼文
    processing_flag=true;//資料開始抓，在搜集完成前都無法停止程式

    logger.time('track_post');
    logger.log('=>==>==>=['+track_id+']=>==>==>')
    console.log('=>==>==>=['+track_id+']=>==>==>');
    current_post_id = track_id;
    graph_request++;
    /*先抓貼文的基本資訊，ex:有幾個人按讚、有多少人分項...*/
    logger.time('trackPost-post-basic');
    track_tool.trackPost('field1',mission,track_id,(flag,msg)=>{
        logger.timeEnd('trackPost-post-basic');
        if(flag=='err'){
            logger.log('[trackPost err] '+msg);
            myEmitter.emit('one_post_done',{track_status:'err',data:''});
        }
        else{
            logger.time('trackPost-post-comments');
            /*接著抓回文的資訊，ex:貼文內容、有多少喜歡這篇回文...*/
            track_tool.trackPost('field2',mission,track_id,(flag,comments)=>{
                logger.timeEnd('trackPost-post-comments');
                if(flag=='err'){
                    console.log('[trackPost err] '+msg);
                    myEmitter.emit('one_post_done',{track_status:'err',data:''});
                }
                else{
                    if(comments['data']!=''){
                        msg['comments']=comments;
                    }
                    single_result = new track_tool.initContent([mission['info']['field1'],mission['info']['field2']],msg);

                    /*搜集下一頁的comments*/
                    if(single_result.next_comments!=null){
                        myEmitter.emit('nextcomment',single_result.next_comments);
                        single_result.next_comments=null;
                    }
                    else{
                        all_fetch--;
                        if(all_fetch==0){
                            myEmitter.emit('one_post_done',{track_status:'ok',data:''});
                        }
                    }
                    /*搜集下一頁的sharedposts*/
                    if(single_result.next_sharedposts!=null){
                        myEmitter.emit('nextsharedpost',single_result.next_sharedposts);                   
                        single_result.next_sharedposts=null;
                    }
                    else{
                        all_fetch--;
                        if(all_fetch==0){
                            myEmitter.emit('one_post_done',{track_status:'ok',data:''});
                        }
                    }
                    /*搜集下一頁reactions*/
                    var reac_type = Object.keys(single_result.reactions);

                    //console.log('link:'+JSON.stringify(single_result.next_reactions));
                    if(single_result.next_reactions!=null){
                        myEmitter.emit('nextreaction',single_result.next_reactions); 
                        single_result.next_reactions=null;
                    }
                    else{
                        all_fetch--;
                        if(all_fetch==0){
                            myEmitter.emit('one_post_done',{track_status:'ok',data:''});
                        }
                    }
                }

            });
        }
    });
}
function addTrackId(ids){
    force_upload_flag=0;//有新的任務，所以如果在上一個任務中 資料水桶裡有尚未被上傳的資料 在master指定上傳時間到時 也不會強制上傳，而是會等到條件滿足才會上傳
    trackids.push(ids);
    /*
    console.log('[harmony] new ids:'+ids);
    console.log('Total post id:'+trackids);
    */
}

function updateMission(assign_mission){
    mission['info']=assign_mission;
    track_tool.updateFieldMap(assign_mission['fields_mapping']);//欄位名稱對照表

    //console.log('[harmony] mission:');
    //console.dir(mission,{colors:true});
    if(UPLOAD_INTERVAL&&UPLOAD_INTERVAL=='time'&&mission['info']['UPLOAD_INTERVAL']['type']!='time'){//先前上傳條件是一照'時間'，若新的設定有改，則先停掉舊有的上傳行程
        upload_process=false;
        upload_schedule.stop();
    }

    UPLOAD_INTERVAL = mission['info']['UPLOAD_INTERVAL']['type'];//設定上傳資料的區間，1.即時 2.定量 3.定時
    if(UPLOAD_INTERVAL=='rec_num'){
        REC_NUM=mission['info']['UPLOAD_INTERVAL']['option'];
    }
    if(UPLOAD_INTERVAL=='rec_size'){
        REC_SIZE=mission['info']['UPLOAD_INTERVAL']['option'];
    }
    if(UPLOAD_INTERVAL=='time'){//如果想依照時間上傳
        if(UPLOAD_TIME!=mission['info']['UPLOAD_INTERVAL']['option']){//檢查與原本的上傳時間是否一樣
            UPLOAD_TIME=mission['info']['UPLOAD_INTERVAL']['option'];//不一樣的話則更新，並重新設定上傳行程
            /*每隔一段時間就會將upload_process flag拉起，所以當一個post追蹤完時，就會檢查該flag是否為true，若為true，代表上傳時間到，進行一次上傳，再將flag=flase*/
            upload_schedule = new CronJob({
                cronTime:UPLOAD_TIME,
                onTick:function(){
                    console.log('UPLOAD_INTERVAL:'+UPLOAD_INTERVAL+' UPLOAD_TIME:'+UPLOAD_TIME);
                    if(final_result['data'].length==0){
                        console.log('\tData bucket is empty! Waiting for ['+WAIT_TIME+'] secs to check again the bucket...');
                        /*沒有可以傳出的資料，則代表兩主原因，1.資料消化速率太快/搜集速度太慢 2.沒有接收到任何任務*/
                        upload_schedule.stop();

                        /*等待一段時間，若時間內有等到新資料則將上傳行程再次啟動 並立即上傳現有資料，若無，則將UPLOAD_TIME清空和將upload_process設為true，代表若有一個追蹤工作完成時 會立即flush並開啟行程，以及代表至少要等到拿到下一任務時才會啟動上傳行程*/
                        setTimeout(()=>{
                            console.log('Checking the bucket...');
                            if(final_result['data'].length!=0){
                                console.log('\tFind some data in bucket! Flush them immediately. And restrat upload_schedule.')
                                flush();
                                upload_schedule.start();
                            }
                            else{
                                console.log('\tData bucket still empty, the next data-upload schedule will restart while receiving next missison, ane will flush data immediately when a track job is finished also restart the data-upload schedule...')
                                upload_process=true;
                                UPLOAD_TIME='restart';//清空該參數後，當接到下一批任務時就會重設上傳行程，故在接到新任務前crawler都是停滯上傳狀態
                            }
                        },WAIT_TIME*1000);
                    }
                    else{
                        console.log('Start upload processing...');
                        flush();

                    }
                },
                start:true,
                timeZone:'Asia/Taipei'
            });
            /*
            upload_counting = setInterval(()=>{
                upload_process=true;
            },UPLOAD_TIME*1000);
            */
        }
    }
    WAIT_TIME =  mission['info']['UPLOAD_INTERVAL']['wait_time'];

}
exports.start=start;
exports.addTrackId=addTrackId;
exports.updateMission=updateMission;
exports.waitingStop=waitingStop;

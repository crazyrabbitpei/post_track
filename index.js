'use strict'
/*
 * TODO:
 *  1.如果沒有同時要求reactoins, comments...有多個下一頁的欄位，要讓程式還是能執行
 *  2.將資料回傳到data center
 * */
var track_tool = require('./tool/track_tool.js');
var master_tool = require('./tool/master_tool.js');
var crawler = require('./crawler.js');
var master = require('./master.js');

var EventEmitter = require('events');
var querystring = require('querystring');
var fs = require('graceful-fs');
var express = require('express');
var bodyParser = require('body-parser');
var app = express();
var http = require('http');
var server = http.createServer(app);


var crawler_setting = JSON.parse(fs.readFileSync('./service/crawler_setting.json'));
var crawler_ip = crawler_setting['crawler_ip'];
var crawler_port = crawler_setting['crawler_port'];
var crawler_name = crawler_setting['crawler_name'];
var crawler_version = crawler_setting['crawler_version'];


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
var uploadInterval,rec_size=0,rec_num=0,upload_time=0;
var WAIT_TIME,REC_NUM,REC_SIZE,UPLOAD_TIME;

var force_upload_flag=0;
var all_fetch=3;//有兩個資訊有下一頁問題：shareposts, reactions, comments，要等到這兩個都抓完後程式才算完成
var graph_request=0;//計算總共發了多少Graph api;
var processing=0;//如果目前有post還在進行追蹤就是1，否則為0
var single_result;//存放單一筆結果
var final_result={};//存放所有結果
final_result['data']=[];

class MyEmitter extends EventEmitter {};
const myEmitter = new MyEmitter();

/*控制何時將搜集資料upload到master*/

myEmitter.on('nextcomment', (link) => {
    //console.log('nextcomment=>'+link);
    graph_request++;
    track_tool.fetchNextPage(mission,link,function(flag,msg){
        if(flag=='err'){
            console.log('[fetchNextPage err] '+msg);
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
    track_tool.fetchNextPage(mission,link,function(flag,msg){
        if(flag=='err'){
            console.log('[fetchNextPage err] '+msg);
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
    track_tool.fetchNextPage(mission,link,function(flag,msg){
        if(flag=='err'){
            console.log('[fetchNextPage err] '+msg);
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
        /*Reset基本記錄*/
        reset();
        current_post_id = temp;
        start(current_post_id);
    }
    else{
        if(_version=='test'){
            console.log('All post id have been tracked.');
        }
        else if(_version=='test1'||_version=='test2'){
            console.log('All post id have been tracked, waiting for next mission!');
            /*現有追蹤id已搜集完畢，但是資料水桶裡還有尚未被上傳的資料，可能原因：尚未湊滿上傳條件(rec_num, rec_size...)，需等待下一批任務來湊滿條件，但也可能沒有下一批任務，故需要設定一等待區間，若超過這個區間沒有湊滿上傳條件，則忽略條件 直接上傳和清空資料水桶裡的資料*/
            if(final_result['data'].length!=0){
                force_upload_flag=1;//若在時間內沒有接收到下一批任務(force_upload_flag=0)，則代表條件可能暫時不會滿足，所以就強制上傳現有資料
                setTimeout(()=>{
                    if(force_upload_flag==1){
                        flush();
                    }
                },WAIT_TIME*1000);
            }
            /*向track master通知任務已完成*/
            var mission_status='done';
            var data={};
            data['success']=success_ids;
            data['fail']=fail_ids;
            track_tool.missionReport({data,master_ip,master_port,master_name,master_version,access_token:mission['token']['access_token'],mission_status},(flag,msg)=>{
                if(flag=='ok'&&msg&&msg['data']&&msg['status']=='ok'){
                    console.log('missionReport success:'+JSON.stringify(msg,null,2));
                    /*Reset基本記錄*/
                    reset();
                    success_ids=[];
                    fail_ids=[];
                }
                else{
                    console.log('['+flag+'] '+JSON.stringify(msg,null,3));
                }

            });
        }
        else{
            console.log('Other mission['+_version+']:\n'+JSON.stringify(mission,null,3));
        }
    }



}

function resultBucket(){
    rec_num++;
    rec_size+=Buffer.byteLength(single_result);
    /*將當前資料合併到final_result*/
    final_result['data'].push(single_result);
    //final_result[current_post_id]=single_result;
    
    /*將資料存到data server*/
    if(uploadInterval=='real_time'){
        flush();
    }
    else if(uploadInterval=='rec_size'){
        if(rec_size>REC_SIZE){
            flush();
        }
    }
    else if(uploadInterval=='rec_num'){
        if(rec_num>=REC_NUM){
            flush();
        }
    }
    else if(uploadInterval=='time'){

    }
}
function cntTrackLog(){
    var i,cnt=0;
    var err_flag=0;
    console.log('==ok==ok==['+current_post_id+']==ok==ok==')
    if(single_result){
        var reac_type = Object.keys(single_result.reactions);
        for(i=0;i<reac_type.length;i++){
            cnt+=single_result.reactions[reac_type[i]];
        }
        single_result.reactions_cnt = cnt;
        single_result.comments_cnt = single_result.comments.length;
        single_result.sharedposts_cnt = single_result.sharedposts.length;
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
    if(!final_result){
        return;
    }
    if(write2Local){
        track_tool.writeRec(mission['info']['datatype'],final_result);
    }
    track_tool.uploadTrackPostData(mission['token']['access_token'],{data:final_result,datatype:mission['info']['datatype']},(flag,msg)=>{
        if(flag=='ok'){
            console.log(msg);
        }
        else if(flag=='err'){
            console.log(msg);
        }
        final_result={};//存放所有結果
        final_result['data']=[];
        rec_num=0;
        rec_size=0;
    });
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
                track_tool.applyCrawler({crawler_port,master_ip,master_port,master_name,master_version,invite_token},(flag,msg)=>{
                    if(flag=='ok'&&msg&&msg['data']&&msg['status']=='ok'){
                        app.use('/'+crawler_name+'/'+crawler_version,crawler);
                        mission['token']['graph_token']=msg['data']['graph_token'];
                        mission['token']['access_token']=msg['data']['access_token'];
                    }
                    else{
                        console.log('['+flag+'] '+JSON.stringify(msg,null,2));
                        process.exit();
                    }

                });
            }
            /*此模式為了要測試master的manageTracking API，需要準備一個偽data crawler來抓取fb資料，並呼叫manageTracking API來將抓取到的post id, created_time塞給track master做儲存*/
            else if(_version=='test2'){
                mission['token']['access_token']=crawler_setting['mission']['access_token'];
                master_tool.connect2DataCenter((flag,msg)=>{
                    console.log('connect2DataCenter, '+msg);
                    if(flag){
                        app.use('/'+master_name+'/'+master_version,master);
                        /*準備測試資料*/
                        var data ={
                            track_pages:[]
                        }
                        var i,rec_num=5;
                        var id_max=100000;
                        var id_min=200000;
                        var now,plus,after;;
                        var date_max=0;
                        var date_min=-14;
                        var track_after;
                        var track_max=30;
                        var track_min=-30;
                        var post=new Object();
                        for(i=0;i<rec_num;i++){
                            now = new Date();
                            plus = (Math.floor(Math.random()*(date_max-date_min+1)+date_min));
                            after = now.getDate()+plus;
                            track_after = Math.floor(Math.random()*(track_max-track_min+1)+track_min);
                            post['id']=Math.floor(Math.random()*(id_max-id_min+1)+id_min);
                            post['created_time']=new Date(now.setDate(after));
                            post['pool_name']='test1';
                            //post['track_time']=new Date(now.setDate(now.getDate()+track_after));
                            data['track_pages'].push(post);
                            post=new Object();
                        }
                        // console.log('Client:'+JSON.stringify(data));

                        track_tool.uploadTrackPostId({master_ip,master_port,master_name,master_version,access_token:mission['token']['access_token'],data},(flag,msg)=>{
                            if(flag=='ok'&&msg&&msg['data']&&msg['status']=='ok'){
                                console.log(JSON.stringify(msg,null,3));
                                /*apply成功後會得到 1.access_token 2.graph token*/
                                track_tool.applyCrawler({crawler_port,master_ip,master_port,master_name,master_version,invite_token},(flag,msg)=>{
                                    if(flag=='ok'&&msg&&msg['data']&&msg['status']=='ok'){
                                        app.use('/'+crawler_name+'/'+crawler_version,crawler);
                                        mission['token']['graph_token']=msg['data']['graph_token'];
                                        mission['token']['access_token']=msg['data']['access_token'];
                                        console.log('Apply success:'+JSON.stringify(msg,null,3));
                                        track_tool.listTrack({master_ip,master_port,master_name,master_version,access_token:mission['token']['access_token']},(flag,msg)=>{
                                            //console.log('listTrack:\n'+JSON.stringify(msg,null,3));
                                        });
                                    }
                                    else{
                                        console.log('['+flag+'] '+msg);
                                    }

                                });
                            }
                            else{
                                console.log('['+flag+'] '+msg);
                            }
                        });
                    }
                    else{
                        console.log('Can\'t connect to Data Center!');
                        process.exit();
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
    console.log('=>==>==>=['+track_id+']=>==>==>=')
    current_post_id = track_id;
    graph_request++;
    /*先抓貼文的基本資訊，ex:有幾個人按讚、有多少人分項...*/
    track_tool.trackPost('field1',mission,track_id,(flag,msg)=>{
        if(flag=='err'){
            console.log('[trackPost err] '+msg);
            myEmitter.emit('one_post_done',{track_status:'err',data:''});
        }
        else{
            /*接著抓回文的資訊，ex:貼文內容、有多少喜歡這篇回文...*/
            track_tool.trackPost('field2',mission,track_id,(flag,comments)=>{
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
    console.log('[harmony] new ids:'+ids);
    console.log('Total post id:'+trackids);
}

function updateMission(assign_mission){
    mission['info']=assign_mission;
    console.log('[harmony] mission'+JSON.stringify(mission));
    uploadInterval = mission['info']['uploadInterval']['type'];//設定上傳資料的區間，1.即時 2.定量 3.定時
    if(uploadInterval=='rec_num'){
        REC_NUM=mission['info']['uploadInterval']['option'];
    }
    if(uploadInterval=='rec_size'){
        REC_SIZE=mission['info']['uploadInterval']['option'];
    }
    if(uploadInterval=='time'){
        UPLOAD_TIME=mission['info']['uploadInterval']['option'];
    }
    WAIT_TIME =  mission['info']['uploadInterval']['wait_time'];
}
exports.start=start;
exports.addTrackId=addTrackId;
exports.updateMission=updateMission;

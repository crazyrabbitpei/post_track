'use strict'
/*
 * TODO:
 *  1.如果沒有同時要求reactoins, comments...有多個下一頁的欄位，要讓程式還是能執行
 *  2.將資料回傳到data center
 * */
var track_tool = require('./tool/track_tool.js');
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
var serverip = crawler_setting['serverip'];
var serverport = crawler_setting['serverport'];
var crawler_name = crawler_setting['crawler_name'];
var crawler_version = crawler_setting['crawler_version'];

var master_ip = crawler_setting['master_ip'];
var master_port = crawler_setting['master_port'];
var master_name = crawler_setting['master_name'];
var master_version = crawler_setting['master_version'];
var master_timeout_again = crawler_setting['master_timeout_again'];
var invite_token = crawler_setting['invite_token'];
var request_timeout = crawler_setting['request_timeout'];

var mission = crawler_setting['mission'];

var access_token = '';
var trackids=[];
var current_post_id='';//目前正在抓取的id

var all_fetch=3;//有兩個資訊有下一頁問題：reactions, comments，要等到這兩個都抓完後程式才算完成
var graph_request=0;//計算總共發了多少Graph api;
var processing=0;//如果目前有任務還在進行就是1，否則為0
var final_result;//存放所有結果

class MyEmitter extends EventEmitter {}
const myEmitter = new MyEmitter();
myEmitter.on('nextcomment', (link) => {
    //console.log('nextcomment=>'+link);
    graph_request++;
    track_tool.fetchNextPage(request_timeout,mission,link,function(flag,msg){
        if(flag=='err'){
            console.log('[fetchNextPage err] '+msg);
        }
        else{
            var i;
            var next_result = new track_tool.parseComment(mission['fields'],msg);
            for(i=0;i<next_result.comments.length;i++){
                final_result.comments.push(next_result.comments[i]);
            }
            //final_result.next_comments = next_result.next_comments;
            if(next_result.next_comments!=null){
                myEmitter.emit('nextcomment',next_result.next_comments);
            }
            else{
                all_fetch--;
                if(all_fetch==0){
                    myEmitter.emit('one_post_done');
                }
            }
        }
    });
});
myEmitter.on('nextsharedpost', (link) => {
    graph_request++;
    track_tool.fetchNextPage(request_timeout,mission,link,function(flag,msg){
        if(flag=='err'){
            console.log('[fetchNextPage err] '+msg);
        }
        else{
            var next_result = new track_tool.parseSharedpost(mission['fields'],msg);
            var i;
            for(i=0;i<next_result.sharedposts.length;i++){
                final_result.sharedposts.push(next_result.sharedposts[i]);
            }
            //final_result.next_sharedposts = next_result.next_sharedposts;
            if(next_result.next_sharedposts!=null){
                myEmitter.emit('nextcomment',next_result.next_sharedposts);
            }
            else{
                all_fetch--;
                if(all_fetch==0){
                    myEmitter.emit('one_post_done');
                }
            }
        }
    });
});
myEmitter.on('nextreaction', (link) => {
    graph_request++;
    track_tool.fetchNextPage(request_timeout,mission,link,function(flag,msg){
        if(flag=='err'){
            console.log('[fetchNextPage err] '+msg);
        }
        else{
            var next_result = new track_tool.parseReaction(mission['fields'],msg);
            var i;
            var reac_type = Object.keys(next_result.reactions);
            for(i=0;i<reac_type.length;i++){
                if(typeof final_result.reactions[reac_type[i]]==='undefined'){
                    final_result.reactions[reac_type[i]]=0;
                }
                final_result.reactions[reac_type[i]]+=next_result.reactions[reac_type[i]];
            }
            if(next_result.next_reactions!=null){
                myEmitter.emit('nextreaction',next_result.next_reactions);
            }
            else{
                all_fetch--;
                if(all_fetch==0){
                    myEmitter.emit('one_post_done');
                }
            }
        }
    });
});
myEmitter.on('one_post_done', () => {
    var i,cnt=0;
    var reac_type = Object.keys(final_result.reactions);
    console.log('==ok==ok==['+current_post_id+']==ok==ok==')
    //console.log('--All reactions--');
    for(i=0;i<reac_type.length;i++){
        //console.log('['+reac_type[i]+'] '+final_result.reactions[reac_type[i]]);
        cnt+=final_result.reactions[reac_type[i]];
    }
    final_result.reactions_cnt = cnt;
    //console.log('final_result.reactions_cnt:'+final_result.reactions_cnt);

    //console.log('--All comments--');
    final_result.comments_cnt = final_result.comments.length;
    //console.log('final_result.comments_cnt:'+final_result.comments_cnt);

    //console.log('--All sharedposts--');
    final_result.sharedposts_cnt = final_result.sharedposts.length;
    //console.log('final_result.sharedposts_cnt:'+final_result.sharedposts_cnt);
    /*
       for(i=0;i<final_result.comments.length;i++){
       console.log('['+i+'] '+final_result.comments[i].message);
       }
    */

   //console.log('--Total graph request ['+graph_request+'] --');
   track_tool.writeRec('gais',current_post_id,final_result);
   /*Reset基本記錄*/
   all_fetch=3;
   graph_request=0;
   final_result=null;
   processing=0;//該貼文追蹤完畢，可以繼續接收下個任務
   /*開始搜集下一個*/
   var temp = trackids.shift();
   if(mission['status']=='test'){
       if(typeof temp!=='undefined'){
           current_post_id = temp['id'];
           start(current_post_id);
       }
       else{
           console.log('All post id have been tracked.');
       }
   }
   else if(mission['status']=='test1'){
       if(typeof temp!=='undefined'){
           current_post_id = temp;
           start(current_post_id);
       }
       else{
           console.log('All post id have been tracked.');
           /*向track master通知任務已完成*/
           var mission_status='done';
           track_tool.missionReport(master_ip,master_port,master_name,master_version,access_token,mission_status,request_timeout,master_timeout_again,(flag,msg)=>{
               if(flag=='ok'&&sg&&mag['data']&&msg['status']=='ok'){
               }
               else{
                   console.log('['+flag+'] '+msg);
               }
           });
       }    
   }
   else{
       console.log('Other mission:\n'+JSON.stringify(mission,null,3));
   }
});
/**
 * --階段--
 * 測試模式(test)，為單機執行 不需要開啟api，先測試是否可以正確搜集到資料 並將資料合併成gaisRec，基本參數設定在loca端的crawler_setting.json file裡
 *  - log, error formater
 *  - request to fb
 *      v nexp page
 *      r
 *      - error handler
 *      - retry handler
 *  - final_result formater
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
if(mission['status']=='test'){
    var track_posts;
    invite_token='test';
    track_posts = mission['track_posts'];

    trackids = track_posts;
    var temp = trackids.shift();
    if(typeof temp!=='undefined'){
        current_post_id = temp['id'];
        start(current_post_id);
    }
}
else{
    app.use(bodyParser.json());
    app.use(bodyParser.urlencoded({ extended: true  }));

    //程式啟動，向tracking master報告
    server.listen(serverport,serverip,function(){
            console.log("[Server start] ["+new Date()+"] http work at "+serverip+":"+serverport);
            if(mission['status']=='test1'){
                /*client同時扮演master，沒有這行的話，就是一個完整的post track crawler*/
                app.use('/'+master_name+'/'+master_version,master);
                /* TODO
                *  1. 要將搜集到的訊息上傳至資料中心存放
                *  2. 當程式停止時，主動向master發送停止訊息
                *  3. 將從master那得來的設定檔和任務儲存到temp_pool裡，若有重新認證的情況時再將新的覆蓋回去
                */
                track_tool.applyCrawler(serverport,master_ip,master_port,master_name,master_version,invite_token,request_timeout,master_timeout_again,(flag,msg)=>{
                    if(flag=='ok'&&sg&&mag['data']&&msg['status']=='ok'){
                        if(msg&&mag['data']&&)
                        app.use('/'+crawler_name+'/'+crawler_version,crawler);
                        access_token = msg['data']['access_token'];   
                        trackids = msg['data']['mission']['track_posts'];
                        mission = msg['data']['mission'];
                        mission['status']='test1';

                        var temp = trackids.shift();
                        console.log('id:'+temp);

                        if(typeof temp!=='undefined'){
                            current_post_id = temp;
                            start(current_post_id);
                        }
                        else{
                            console.log('Waiting for mission...');
                        }
                    }
                    else{
                        console.log('Response:'+flag+' Matser:\n'+JSON.stringify(msg,null,3));
                    }

                });

            }
            /*此模式為了要測試master的manageTracking API，需要準備一個偽data crawler來抓取fb資料，並呼叫manageTracking API來將抓取到的post id, created_time塞給track master做儲存*/
            else if(mission['status']=='test2'){
                access_token=mission['access_token'];
                app.use('/'+master_name+'/'+master_version,master);
                app.enable('trust proxy');

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
                    post['track_time']=new Date(now.setDate(now.getDate()+track_after));
                    data['track_pages'].push(post);
                    post=new Object();
                }
                console.log('Client:'+JSON.stringify(data));

                track_tool.uploadTrackPost(master_ip,master_port,master_name,master_version,access_token,data,request_timeout,master_timeout_again,(flag,msg)=>{
                    if(flag=='ok'&&sg&&mag['data']&&msg['status']=='ok'){
                        console.log(JSON.stringify(msg,null,3));
                        /*測試*/
                        track_tool.applyCrawler(serverport,master_ip,master_port,master_name,master_version,invite_token,request_timeout,master_timeout_again,(flag,msg)=>{
                            if(flag=='ok'&&sg&&mag['data']&&msg['status']=='ok'){
                                app.use('/'+crawler_name+'/'+crawler_version,crawler);
                                access_token = msg['data']['access_token'];   
                                trackids = msg['data']['mission']['track_posts'];
                                mission = msg['data']['mission'];
                                mission['status']='test1';

                                var temp = trackids.shift();
                                console.log('id:'+temp);
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

    })
}

function start(track_id){
    if(processing==1){//還有任務沒完成，不會進下個任務
        console.log('Push back:'+track_id);
        trackids.push(track_id);//將任務再塞回去
        return;
    }

    processing=1;//開關打開，在完成當前任務前都不會接受任何新任務，亦即，同時只會追蹤一個貼文
    console.log('=>==>==>=['+track_id+']=>==>==>=')
    current_post_id = track_id;
    graph_request++;
    track_tool.trackPost(request_timeout,mission,track_id,(flag,msg)=>{
        if(flag=='err'){
            console.log('[trackPost err] '+msg);
        }
        else{
            final_result = new track_tool.initContent(mission['fields'],msg);

            /*搜集下一頁的comments*/
            if(final_result.next_comments!=null){
                myEmitter.emit('nextcomment',final_result.next_comments);
                final_result.next_comments=null;
            }
            else{
                all_fetch--;
                if(all_fetch==0){
                    myEmitter.emit('one_post_done');
                }
            }
            /*搜集下一頁的sharedposts*/
            if(final_result.next_sharedposts!=null){
                myEmitter.emit('nextsharedpost',final_result.next_sharedposts);                   
                final_result.next_sharedposts=null;
            }
            else{
                all_fetch--;
                if(all_fetch==0){
                    myEmitter.emit('one_post_done');
                }
            }
            /*搜集下一頁reactions*/
            var i;
            var reac_type = Object.keys(final_result.reactions);

            //console.log('link:'+JSON.stringify(final_result.next_reactions));
            if(final_result.next_reactions!=null){
                myEmitter.emit('nextreaction',final_result.next_reactions); 
                final_result.next_reactions=null;
            }
            else{
                all_fetch--;
                if(all_fetch==0){
                    myEmitter.emit('one_post_done');
                }
            }
        }
    });
}
function harmony(job,ids){
    console.log('[harmony] new ids:'+ids);
    trackids.push(ids);
    console.log('Total misison:'+trackids);
    mission = job;
    mission['status']='test1';
}
exports.start=start;
exports.harmony=harmony;

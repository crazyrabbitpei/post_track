'use strict'
var track_tool = require('./tool/track_tool.js');
var router = require('./router.js');
var EventEmitter = require('events');
var fs = require('graceful-fs');
var express = require('express');
var bodyParser = require('body-parser');
var app = express();
var http = require('http');
var server = http.createServer(app);


var setting = JSON.parse(fs.readFileSync('./service/setting.json'));
var serverip = setting['serverip'];
var serverport = setting['serverport'];
var crawler_name = setting['crawler_name'];
var crawler_version = setting['crawler_version'];

var master_ip = setting['master_ip'];
var master_port = setting['master_port'];
var master_name = setting['master_name'];
var master_version = setting['master_version'];
var master_timeout_again = setting['master_timeout_again'];
var invite_token = setting['invite_token'];
var request_timeout = setting['request_timeout'];

var mode = setting['mode'];
var track_posts;

var all_fetch=3;//有兩個資訊有下一頁問題：reactions, comments，要等到這兩個都抓完後程式才算完成
var graph_request=0;//計算總共發了多少Graph api;
var trackids=[];
var current_post_id='';//目前正在抓取的id
var final_result;//存放所有結果

class MyEmitter extends EventEmitter {}
const myEmitter = new MyEmitter();
myEmitter.on('nextcomment', (link) => {
    graph_request++;
    track_tool.fetchNextPage(request_timeout,mode,link,function(flag,msg){
        if(flag=='err'){
            console.log('[fetchNextPage err] '+msg);
        }
        else{
            track_tool.parseComment(msg);
            if(final_result.next_comments!=null){
                myEmitter.emit('nextcomment',final_result.next_comments);
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
    track_tool.fetchNextPage(request_timeout,mode,link,function(flag,msg){
        if(flag=='err'){
            console.log('[fetchNextPage err] '+msg);
        }
        else{
            track_tool.parseSharedpost(msg);
            if(final_result.next_sharedposts!=null){
                myEmitter.emit('nextcomment',final_result.next_sharedposts);
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
    track_tool.fetchNextPage(request_timeout,mode,link,function(flag,msg){
        if(flag=='err'){
            console.log('[fetchNextPage err] '+msg);
        }
        else{
            track_tool.parseReaction(msg);
            var i;
            var reac_type = Object.keys(final_result.reactions);
            //console.log('link:'+JSON.stringify(final_result.next_reactions));
            if(final_result.next_reactions!=null){
                myEmitter.emit('nextreaction',final_result.next_reactions);
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
    console.log('======['+current_post_id+']======')
    console.log('--All reactions--');
    for(i=0;i<reac_type.length;i++){
        console.log('['+reac_type[i]+'] '+final_result.reactions[reac_type[i]]);
        cnt+=final_result.reactions[reac_type[i]];
    }
    final_result.reactions_cnt = cnt;
    console.log('final_result.reactions_cnt:'+final_result.reactions_cnt);

    console.log('--All comments--');
    final_result.comments_cnt = final_result.comments.length;
    console.log('final_result.comments_cnt:'+final_result.comments_cnt);

    console.log('--All sharedposts--');
    final_result.sharedposts_cnt = final_result.sharedposts.length;
    console.log('final_result.sharedposts_cnt:'+final_result.sharedposts_cnt);
    /*
       for(i=0;i<final_result.comments.length;i++){
       console.log('['+i+'] '+final_result.comments[i].message);
       }
    */

   console.log('--Total graph request ['+graph_request+'] --');
   if(mode['status']=='test'){
       track_tool.writeRec('json',current_post_id,JSON.stringify(final_result,null,3));
   }
   /*Reset基本記錄*/
   all_fetch=2;
   graph_request=0;
   final_result=null;
   /*開始搜集下一個*/
   var temp = trackids.shift();
   if(typeof temp!=='undefined'){
       current_post_id = temp['id'];
       start(current_post_id);
   }
   else{
        console.log('All post id have been tracked.');
        /*向track master要下一批post id*/
        if(mode['status']!='test'){
            
        }
   }


});
/**
 * --階段--
 * 測試模式(test)，為單機執行 不需要開啟api，先測試是否可以正確搜集到資料 並將資料合併成gaisRec，基本參數設定在loca端的setting.json file裡
 *  - log, error formater
 *  - request to fb
 *      - next page
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
if(mode['status']=='test'){
    invite_token='test';
    track_posts = mode['track_posts'];

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
    app.use('/'+crawler_name+'/'+crawler_version,router);
    //程式啟動，向tracking master報告
    server.listen(serverport,serverip,function(){
            console.log("[Server start] ["+new Date()+"] http work at "+serverip+":"+serverport);
            //TODO
            /*啟動後，向master報告並等待master指派任務，在master認證成功前 mission api都會鎖住*/
            applyCrawler(master_ip,master_port,master_name,master_version,invite_token,request_timeout,master_timeout_again,(err,mission)=>{
                track_posts = mission['track_posts'];
                trackids = track_posts;
                var temp = trackids.shift();
                if(typeof temp!=='undefined'){
                    current_post_id = temp['id'];
                    start(current_post_id);
                }
            });
    })
}

function start(track_id){
    graph_request++;
    track_tool.trackPost(request_timeout,mode,track_id,(flag,msg)=>{
        if(flag=='err'){
            console.log('[trackPost err] '+msg);
        }
        else{
            final_result = track_tool.initContent(msg);

            /*搜集下一頁的comments*/
            if(final_result.next_comments!=null){
                myEmitter.emit('nextcomment',final_result.next_comments);                   
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
            }
            else{
                all_fetch--;
                if(all_fetch==0){
                    myEmitter.emit('one_post_done');
                }
            }
            /*
             * 下一頁存放位置: 
             *   comments next link : this.next_comments
             *   reactions next link : this.next_reactions
             *   comments' likes next link : this.comments[i]['likes_next']
             *
             * 數值還會在變動:(因為下一頁)
             *   reactions['LIKE'] reaction['HAHA']...
             *   comments[i]['likes']
             * */
        }
    });
}

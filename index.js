'use strict'
var track_tool = require('./tool/track_tool.js');
var router = require('./router.js');
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
var mission_token = setting['mission_token'];
var mode = setting['mode'];

var mission_token;
var track_posts;
var graph_request_interval;
var graph_timeout_again;
var site;
var graph_version;
var graph_token;
var fields;

var apply_status;
var trackids=[];
/**
 * --階段--
 * 測試模式(test)，為單機執行 不需要開啟api，先測試是否可以正確搜集到資料 並將資料合併成gaisRec，基本參數設定在loca端的setting.json file裡
 *  - log, error formater
 *  - request to fb
 *      - next page
 *      - error handler
 *      - retry handler
 *  - result formater
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
    mission_token='test';
    track_posts = mode['track_posts'];
    graph_request_interval = mode['graph_request_interval'];
    graph_timeout_again = mode['graph_timeout_again'];
    site = mode['site'];
    graph_version = mode['graph_version'];
    graph_token = mode['graph_token'];
    fields = mode['fields'];

    var post_id = track_posts.shift()['id'];
    track_tool.trackPost(setting['request_timeout'],mode,post_id,(flag,msg)=>{
        if(flag=='err'){
            console.log('[err] '+msg);
        }
        else{
            //console.log(msg);
            var result = track_tool.parseContent(msg);
            console.log(JSON.stringify(result,null,3));
        }
    });
}
else{
    apply_status=0;
    app.use(bodyParser.json());
    app.use(bodyParser.urlencoded({ extended: true  }));
    app.use('/'+crawler_name+'/'+crawler_version,router);
    //程式啟動，向tracking master報告
    server.listen(serverport,serverip,function(){
        console.log("[Server start] ["+new Date()+"] http work at "+serverip+":"+serverport);
        //TODO
        /*啟動後，向master報告並等待master指派任務，在master認證成功前 mission api都會鎖住*/
        /*
           applyCrawler(serverport,serverip,mission_token,(err,msg){
           });
           */
    })
}


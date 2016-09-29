'use strict'
var fs = require('graceful-fs');
var track_tool = require('./tool/track_tool.js');
var master_tool = require('./tool/master_tool.js');
var crawler_setting = JSON.parse(fs.readFileSync('./service/crawler_setting.json'));

var master_ip = crawler_setting['master_ip'];
var master_port = crawler_setting['master_port'];
var master_name = crawler_setting['master_name'];
var master_version = crawler_setting['master_version'];
var master_timeout_again = crawler_setting['master_timeout_again'];
var invite_token = crawler_setting['invite_token'];
var request_timeout = crawler_setting['request_timeout'];

var mission = crawler_setting['mission'];

var access_token = mission['access_token'];

var test = new master_tool.Track();

var data ={
    track_pages:[]
}
var i,rec_num=5;
var id_max=1000000;
var id_min=200000;
var now,plus,after;
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
    after = new Date(now.setDate(now.getDate()+track_after));
    if(Math.floor(Math.random()*(10-1+1)+1)>9){
        post['pool_name']=after.getMonth()+1+'/'+after.getDate();
    }

    if(!test.insertIdsToPool(post['id'],{pool_name:post['pool_name'],created_time:post['created_time']})){
    //if(!test.insertIdsToPool(post['id'],{pool_name:post['pool_name'],created_time:post['created_time']})){
        console.log('Insert id ['+post['id']+'] ['+post['created_time']+']to pool ['+post['pool_name']+'] fail!');
    }
    else{
        console.log('Insert id ['+post['id']+'] ['+post['created_time']+']to pool ['+post['pool_name']+'] success!');
    }
}
/*
console.log('Update pool:'+test.updatePool('test1',{new_pool_name:'',description:''}));
console.log('Delete pool:'+test.deletePool('past'));
console.log('New pool:'+test.newPool('pei',{description:'new pei'}));
console.log('Get:'+JSON.stringify(test.listPools(),null,2));
*/

rec_num=5;
var token_max=400000000;
var token_min=50000000;
var token,tokens=[];
var crawlers={};
for(i=0;i<rec_num;i++){
    token=Math.floor(Math.random()*(token_max-token_min+1)+token_min);
    crawlers['ip']=getInterval(100,300)+'.'+getInterval(100,300)+'.'+getInterval(100,300)+'.'+getInterval(100,300);
    crawlers['port']=getInterval(1000,8000);
    if(!test.insertCrawler(token,{ip:crawlers['ip'],port:crawlers['port']})){
        console.log('Insert token ['+token+'] ip ['+crawlers['ip']+'] ['+crawlers['port']+'] fail!');
    }
    else{
        tokens.push(token);
        console.log('Insert token ['+token+'] ip ['+crawlers['ip']+'] ['+crawlers['port']+'] success!');
    }
}

for(i=0;i<tokens.length;i++){
    if(getInterval(0,1)==0){
        console.log('Delete crawler ['+tokens[i]+']:'+JSON.stringify(test.deleteCrawler(tokens[i]),null,2));
    }
    else if(getInterval(0,1)==0){
        console.log('Update crawler:'+JSON.stringify(test.updateCrawler(tokens[i],{status:'ing',ip:'1.2.3.4',active_time:new Date()}),null,2));
    }
    
}

console.log('Get:'+JSON.stringify(test.listCrawlers(),null,2));
console.log('findMissionCrawler:'+JSON.stringify(test.findMissionCrawler(),null,2));







function getInterval(a,b){
    return Math.floor(Math.random()*(b-a+1)+a);
}



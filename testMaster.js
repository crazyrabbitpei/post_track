'use strict'
var fs = require('graceful-fs');
var track_tool = require('./tool/track_tool.js');
var master_tool = require('./tool/master_tool.js');

var test = new master_tool.Track();
var tokens=[];


var schedule1='demo1';
var schedule2='demo2';
var schedule3='demo3';
var schedule4='demo4';
var schedule5='demo5';

//produceIds('test2',{rec_num:8});
//produceCrawlers({rec_num:2});
//produceIds('10/03',{rec_num:8});

//console.log('List Crawlers:'+JSON.stringify(test.listCrawlers(),null,2));
//console.log('List Pools:'+JSON.stringify(test.listPools(),null,2));
//console.log('List Schedules:'+JSON.stringify(test.listSchedules(),null,2));
/*
randomId({time:5,rec_num:4});
randomCrawler({time:7,rec_num:3});
*/
/*

randomId({time:20,rec_num:8});

randomSchedule({time:4});
*/
/*
console.log('Get:'+JSON.stringify(test.listCrawlers(),null,2));
var result = test.findMissionCrawler();
console.log('findMissionCrawler:'+JSON.stringify(result,null,2));
result['info']['port']='hello!';
result['token']='token #1';
console.log('Get:'+JSON.stringify(test.listCrawlers(),null,2));
console.log('Search:'+JSON.stringify(test.getSchedule(schedule1),null,2));
console.log(JSON.stringify(test.getPool('past1')));
*/

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

        if(!insertIdsToPool(post['id'],{pool_name:post['pool_name'],created_time:post['created_time']})){
            //if(!test.insertIdsToPool(post['id'],{pool_name:post['pool_name'],created_time:post['created_time']})){
            console.log('Insert id ['+post['id']+'] ['+post['created_time']+']to pool ['+post['pool_name']+'] fail!');
        }
        else{
            console.log('Insert id ['+post['id']+'] ['+post['created_time']+']to pool ['+post['pool_name']+'] success!');
        }
    }
}
function produceCrawlers({rec_num=10,token_max=80000000,token_min=50000000}){
    var token,tokens=[];
    var crawlers={};
    for(let i=0;i<rec_num;i++){
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
}
function randomPool({time=3,rec_num=5}){
    setTimeout(function(){
        console.log('Update pool:'+test.updatePool('test1',{new_pool_name:'',description:''}));
        console.log('Delete pool:'+test.deletePool('past'));
    },time*1000)
    
    setTimeout(function(){
        console.log('New pool:'+test.newPool('pei',{description:'new pei'}));
    },(time+2)*1000)

}
function randomId({time=3,rec_num=5}){
    setTimeout(function(){
        produceIds('test1',{rec_num:rec_num});
    },time*1000);
}
exports.randomId=randomId;
function randomCrawler({time=3,rec_num=5}){
    for(let i=0;i<tokens.length;i++){
        if(getInterval(0,1)==0){
            console.log('Delete crawler ['+tokens[i]+']:'+JSON.stringify(test.deleteCrawler(tokens[i]),null,2));
        }
        else if(getInterval(0,1)==0){
            console.log('Update crawler:'+JSON.stringify(test.updateCrawler(tokens[i],{status:'ing',ip:'1.2.3.4',active_time:new Date()}),null,2));
        }
    }
    setTimeout(function(){
        produceCrawlers({rec_num:rec_num});
    },time*1000);

}
function randomSchedule({time=3,rec_num=5}){
    setTimeout(function(){
        //console.log('Delete:'+JSON.stringify(test.deleteSchedule(schedule1),null,2));
        console.log('New:'+JSON.stringify(test.newSchedule(schedule4,{schedule_status:'on',description:'Another schedule'}),null,2));
        console.log('After:'+JSON.stringify(test.listSchedules(),null,2));
    },time*1000);
    
    setTimeout(function(){
        //console.log('Update:'+test.updateSchedule(schedule1,{description:'Update '+schedule1,track_time:'*/5 * * * * *',new_schedule_name:schedule5}));
        //console.log('After update:'+JSON.stringify(test.listSchedules(),null,2));
        console.log('Start:'+test.startSchedules(schedule1));
    },(time+3)*1000);
    /*   
    setTimeout(function(){
        console.log('Stop:'+test.stopSchedules());
        console.log('After:'+JSON.stringify(test.listSchedules(),null,2));
    },(time+6)*1000);
    */
}

function getInterval(a,b){
    return Math.floor(Math.random()*(b-a+1)+a);
}



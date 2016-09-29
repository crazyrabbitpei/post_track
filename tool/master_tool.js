'use strict'
var fs = require('fs');
var CronJob = require('cron').CronJob;
var dateFormat = require('dateformat');

var master_setting = JSON.parse(fs.readFileSync('./service/master_setting.json'));


class Track{
    constructor(){
        this.trackPools={};
        for(let i=0,pools=master_setting.trackpools,keys=Object.keys(pools);i<keys.length;i++){
            let key=keys[i];
            //console.log('key['+i+'] '+key+' value:'+pools[keys[i]]['name']);
            this.trackPools[pools[keys[i]]['name']]={};
            this.trackPools[pools[keys[i]]['name']]['description']=pools[keys[i]]['description'];
            this.trackPools[pools[keys[i]]['name']]['data']=[];
        }

        this.crawlersInfo=new Map();
        this.crawlerSatus=new Map();
        for(let i=0,crawlers=master_setting.demo;i<crawlers.length;i++){
            let info={};
            info['ip']=crawlers[i]['ip'];
            info['port']=crawlers[i]['port'];
            info['created_time']=crawlers[i]['created_time'];
            info['active_time']=crawlers[i]['active_time'];
            this.crawlersInfo.set(crawlers[i]['token'],info);
            this.crawlerSatus.set(crawlers[i]['token'],crawlers[i]['status']);
        }

    }

    listPools(...pool_names){
        var results=[];
        var i=0;
        if(pool_names.length==0){
            while(i<master_setting.list_size&&i<Object.keys(this.trackPools).length){//TODO:「下一頁」功能，目前最多只會顯示master setting所設定的數目
                results.push({pool_name:Object.keys(this.trackPools)[i],info:this.trackPools[Object.keys(this.trackPools)[i]]});
                i++;
            }
        }
        else{
            while(i<pool_names.length){
                this.trackPools[pool_names[i]]?results.push({pool_name:pool_names[i],info:this.trackPools[pool_names[i]]}):null;  
                i++;
            }
        }
        return results;
    }
    listCrawlers(...crawler_tokens){
        var results=[];
        var i=0;
        if(crawler_tokens.length==0){
            /*
            while(i<master_setting.list_size&&i<this.crawlersInfo.size){//TODO:「下一頁」功能，目前最多只會顯示master setting所設定的數目
                results.push({token:this.crawlersInfo.,info:this.trackPools[Object.keys(this.trackPools)[i]]});
                i++;
            }
            */
            for (var [key, value] of this.crawlersInfo.entries()) {
                let status = this.crawlerSatus.get(key);
                results.push({token:key,info:value,status:status});
                i++;
                if(i>master_setting.list_size){break;}
            }
        }
        else{
            var key,value;
            while(i<crawler_tokens.length){
                key = crawler_tokens[i];
                if(this.crawlersInfo.has(key)){
                    let status = this.crawlerSatus.get(key);
                    value = this.crawlersInfo.get(key);
                    results.push({token:key,info:value,status:status});
                }
                i++;
                if(i>master_setting.list_size){break;}
            }
        }
        return results;
    }
    /*
    * 回傳fasle時機:
    *   1.沒有insert id 
    *   2.指定的pool_name不存在
    * 流程:
    *   1.沒有id則回傳錯誤 
    *   2.有指定insert的pool_name則檢查它是否存在，不存在回傳錯誤 
    *   3.檢查是否有發文時間，有=>檢查日期格式，格式正確則強制轉換成指定日期格式，格式錯誤則給予預設日期(當天日期)
    *                       沒有=>直接給予預設日期(當天日期)
    *   4.將id insert到特定pool，優先順序:pool_name>created_time，pool_name不會檢查日期格式，可任意取名
    *       *若算出的追蹤日期大於今天日期，則列到'past' pool
    *
    * */
    insertIdsToPool(post_id,...args){
    //insertIdsToPool(post_id,{pool_name,created_time}){

        if(!post_id){
            return false;
        }
        
        var name,pool_name,created_time;
        if(args[0]){
            pool_name=args[0]['pool_name'],created_time=args[0]['created_time'];
        }

        if(pool_name){
            if(!this.checkPoolName(pool_name)){
                return false
            }
            else{
                name=pool_name;
            }
        }

        if(created_time){
            if(checkDateFormat(created_time)){
                created_time=deafultDate(created_time);
                if(isPast(created_time)){
                    if(!name){
                        name=master_setting.trackpools['past']['name'];
                    }
                }
                else{
                    created_time=dateFormat(created_time,'yyyy/mm/dd HH:MM:ss');
                }

            }
            else{
                created_time=deafultDate();
            }
        }
        else{
            created_time=deafultDate();
        }
        
        if(!name){
            name = this.parsePoolName(created_time);
        }
        if(!this.checkPoolName(name)){
            this.newPool(name);
        }
        this.trackPools[name]['data'].push({post_id:post_id,created_time:created_time,pool_name:name});
        return true;
    }
    deletePostIds(){
        return false;
    }

    newPool(pool_name,...args){
        if(this.checkPoolName(pool_name)){
            return false;
        }
        var description='test_'+pool_name;
        if(args[0]){
            if(args[0]['description']){
                description=args[0]['description'];
            }
        }

        this.trackPools[pool_name]={};
        this.trackPools[pool_name]['description']=description;
        this.trackPools[pool_name]['data']=[];
        return true;
    }
    deletePool(pool_name){
        if(this.trackPools[pool_name]){
            delete this.trackPools[pool_name];
            return true;
        }   
        else{
            return false;
        }
    }
    //updatePool(pool_name,{new_pool_name=pool_name,description='Update at '+new Date()+this.trackPools[pool_name]['description']}){
    updatePool(pool_name,...args){
        if(!args[0]){
            return false;
        }
        if(!this.trackPools[pool_name]){
            return false;
        }
        if(!args[0]['new_pool_name']&&!args[0]['description']){
            return false;
        }

        var name=pool_name,new_pool_name,description;
        if(args[0]['description']){
            description=args[0]['description'];
            this.trackPools[pool_name]['description']=description;
        }

        if(args[0]['new_pool_name']){//若有要改新名字
            name=new_pool_name=args[0]['new_pool_name'];
            if(this.trackPools[new_pool_name]){//新名字不可與舊有的名字重複
                return false;
            }
            else{
                this.trackPools[new_pool_name]=Object.assign({},this.trackPools[pool_name]);
                delete this.trackPools[pool_name];
            }
        }

             
        return true;
    }

    checkPoolName(pool_name){
        if(this.trackPools[pool_name]===undefined){
            return false;
        }
        else{
            return true;
        }
    }
    /*依照給予的發文日期來賦予追蹤pool_name*/
    parsePoolName(created_time){
        created_time=new Date(created_time);
        var after = created_time.getDate()+master_setting['track_interval'];
        created_time.setDate(after);
        var mm = created_time.getMonth()+1;
        var dd = created_time.getDate();
        return  mm+'/'+dd;
    }

    insertCrawler(token,...args){
        if(!token){
            return false;
        }
        else if(this.crawlersInfo.has(token)){
            return false;
        }
        if(!args[0]){
            return false;
        }
        else if(!args[0]['ip']||!args[0]['port']){
            return false;
        }
        let info={};
        info['ip']=args[0]['ip'];
        info['port']=args[0]['port'];
        info['created_time']=dateFormat(new Date(),'yyyy/mm/dd HH:MM:ss');
        info['active_time']=dateFormat(new Date(),'yyyy/mm/dd HH:MM:ss');
        this.crawlersInfo.set(token,info);
        this.crawlerSatus.set(token,'init');
        return true;
    }

    deleteCrawler(token){
        if(!this.crawlersInfo.has(token)||!token){
            return false;
        }
        this.crawlersInfo.delete(token);
        this.crawlerSatus.delete(token);
        return true;
    }
    

    updateCrawler(token,...args){
        if(!token){
            return false;
        }
        else if(!this.crawlersInfo.has(token)){
            return false;
        }
        if(!args[0]){
            return false;
        }
        
        var info=this.crawlersInfo.get(token);
        if(args[0]['ip']){
            info['ip']=args[0]['ip'];
        }
        if(args[0]['port']){
            info['port']=args[0]['port'];
        }
        if(args[0]['active_time']){
            info['active_time']=dateFormat(new Date(args[0]['active_time']),'yyyy/mm/dd HH:MM:ss');
        }
        if(args[0]['status']){
            this.crawlerSatus.set(token,args[0]['status']);
        }

        return true;
    }
        
    getCrawler(token){
        if(!token){
            return false;
        }
        else if(!this.crawlersInfo.has(token)){
            return false;
        }
        return this.crawlersInfo.get(token);
        
    }

    findMissionCrawler(){
        var token;
        for (var [key,value] of this.crawlerSatus.entries()) {
            if(value=='init'||value=='done'){
                token=key;
                break;
            }

        }
        if(token===undefined){
            return false;
        }
        var result={};
        result['token']=token;
        result['info']=this.crawlersInfo.get(token);
        this.crawlerSatus.set(token,'ing');
        return result;
    }
}

exports.Track=Track;

function checkDateFormat(created_time){
    var time = new Date(created_time).getTime();
    return isNaN(time)?false:true;
}   
function isPast(created_time){
    created_time=new Date(created_time);
    var track_date = created_time.getDate()+master_setting['track_interval'];
    created_time.setDate(track_date);
    var time = new Date(created_time).getTime();
    return time<new Date().getTime()?true:false;
}   
function deafultDate(created_time){
    return created_time?dateFormat(created_time,'yyyy/mm/dd HH:MM:ss'):dateFormat(new Date(),'yyyy/mm/dd HH:MM:ss');   
}


/*
function trackJob(schedule,mode){
    this.schedule=schedule||"* * * * * *";
    this.mode=mode||{type:'normal',date:''};
    this.button = new CronJob({
        cronTime: this.schedule,
        onTick: function() {
            console.log('[setTrackJob] processing...');
        },
        start: false,
        timeZone: 'Asia/Taipei'
    })
}
trackJob.prototype.startTrack = function(){
    console.log('[trackJob] button on');
    this.button.start();
}
trackJob.prototype.stopTrack = function(){
    console.log('[trackJob] button off');
    this.button.stop();
}
trackJob.prototype.setTrack = function(schedule,mode){
    this.button.stop();
    this.schedule=schedule||"* * * * * *";
    this.mode=mode||{type:'normal',date:''};
    this.button = new CronJob({
        cronTime: this.schedule,
        onTick: function() {
            console.log('[setTrackJob] processing...');
        },
        start: false,
        timeZone: 'Asia/Taipei'
    });
    this.button.start();
}

exports.trackJob=trackJob;
*/

'use strict'
var fs = require('fs');
var CronJob = require('cron').CronJob;
var dateFormat = require('dateformat');
var request = require('request');

var master_setting = JSON.parse(fs.readFileSync('./service/master_setting.json'));

class Track{
    constructor(){
        this.mission = JSON.parse(fs.readFileSync('./service/mission.json'));
        this.post_idInfo =  new Map();//(token,發出去的時間)

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
        for(let i=0,crawlers=master_setting.crawlers;i<crawlers.length;i++){
            let info={};
            info['ip']=crawlers[i]['ip'];
            info['port']=crawlers[i]['port'];
            info['created_time']=crawlers[i]['created_time'];
            info['active_time']=crawlers[i]['active_time'];
            this.crawlersInfo.set(crawlers[i]['token'],info);
            this.missionStatus(crawlers[i]['token'],crawlers[i]['status']);
            //this.crawlerSatus.set(crawlers[i]['token'],crawlers[i]['status']);
        }
        
        this.schedulesInfo=new Map();
        this.schedulesJob=new Map();
        for(let i=0,schedules=master_setting.schedules;i<schedules.length;i++){
            if(!this.newSchedule(schedules[i]['name'],{description:schedules[i]['description'],track_time:schedules[i]['track_time'],track_pool_name:schedules[i]['track_pool_name'],schedule_status:schedules[i]['status']})){
                return false;
            }
            /*
            if(schedules[i]['status']=='on'){
                this.startSchedules(schedules[i]['name']);
            }
            */
            /*
            let info={};
            info['description']=schedules[i]['description'];
            info['track_time']=schedules[i]['track_time'];
            info['track_pool_name']=schedules[i]['track_pool_name'];
            info['status']='off';
            this.schedulesInfo.set(schedules[i]['name'],info);
            */
            //this.newSchedule({name:schedules[i]['name'],description:schedules[i]['description'],track_time:schedules[i]['track_time'],track_pool_name:schedules[i]['track_time']})
        }
        return true;
    }


    listSchedules(...schedules){
        var results=[];
        var status;
        var i=0;
        if(schedules.length==0){
            for (var [key, value] of this.schedulesInfo.entries()) {
                results.push({name:key,info:value});
                i++;
                if(i>master_setting.list_size){break;}
            }
        }
        else{
            var key,value;
            while(i<schedules.length){
                key = schedules[i];
                if(this.schedulesInfo.has(key)){
                    value = this.schedulesInfo.get(key);
                    results.push({name:key,info:value});
                }
                i++;
                if(i>master_setting.list_size){break;}
            }
        }
        return results;
    }

    listPools(...pool_names){
        var results=[];
        var i=0;
        if(pool_names.length==0){
            while(i<master_setting.list_size&&i<Object.keys(this.trackPools).length){//TODO:「下一頁」功能，目前最多只會顯示master setting所設定的數目
                results.push({name:Object.keys(this.trackPools)[i],info:this.trackPools[Object.keys(this.trackPools)[i]]});
                i++;
            }
        }
        else{
            while(i<pool_names.length){
                this.trackPools[pool_names[i]]?results.push({name:pool_names[i],info:this.trackPools[pool_names[i]]}):null;  
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
                results.push({name:key,info:value,status:status});
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
                    results.push({name:key,info:value,status:status});
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
    //insertIdsToPool(post_id,...args){
    insertIdsToPool(post_id,{pool_name,created_time}={}){

        if(!post_id){
            return false;
        }
        /*TODO*/
        /*不允許post id重複，即使是在不同pool*/
        if(this.hasPostId()){
            return false;
        }
        else{
            this.post_idInfo.set(post_id,0);
        }
        console.log('Set:'+post_id+' '+this.post_idInfo.get(post_id)+' pool:'+pool_name);

        var name;
        /*
        if(args[0]){
            pool_name=args[0]['pool_name'],created_time=args[0]['created_time'];
        }
        */

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
        /*如果沒有指定的pool name，則用日期來解析出pool_name，例：2016 09/11 09:11:22 => poolname: 09/11*/
        if(!name){
            if(!(name = this.parsePoolName(created_time))){//如果created_time格是不是日期，則回傳原本的created_time值，例：I'm not a date=> poolname:I'm not a date
                return false;
            }
        }
        if(!this.checkPoolName(name)){
            this.newPool(name);
        }
        this.trackPools[name]['data'].push({post_id:post_id,created_time:created_time,pool_name:name});

        return true;
    }
    shiftTrackId(pool_name,track_num=master_setting.ids_num){
        if(!pool_name||!this.trackPools[pool_name]){
            return false;
        }
        else{
            var result=[];
            var i;
            //for(let i=0,id = this.trackPools[pool_name]['data'].shift();i<track_num&&id;i++){
            for(i=0;i<track_num;i++){
                let id = this.trackPools[pool_name]['data'].shift();
                if(!id||!this.post_idInfo.has(id['post_id'])){
                    break;
                }
                result.push(id);
            }
        }
        if(i==0){
            return false;
        }
        return result;
    }
    /*TODO*/
    missionStatus(token,mission_status){
        if(!token||!mission_status||!this.crawlersInfo.has(token)){
            return false;
        }
        this.crawlerSatus.set(token,mission_status);
        return true;
    }
    /*TODO:testing*/
    hasPostId(id){
        if(!id||!this.post_idInfo.has(id)){
            return false;
        }
        return true;
    }
    /*TODO*/
    getPostIds(post_id){
        return false;
    }
    /*TODO*/
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
    getPool(pool_name){
        var result={};
        if(!pool_name){
            return false;
        }
        else if(!this.trackPools[pool_name]){
            return false;
        }

        result={name:pool_name,info:this.trackPools[pool_name]};
        return result;
    }
    deletePool(pool_name){
        if(!pool_name){
            return false;
        }
        else if(!this.trackPools[pool_name]){
            return false;
        }
        else if(this.trackPools[pool_name]){
            delete this.trackPools[pool_name];
        }   
        return true;
    }
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
        if(isNaN(new Date(created_time).getTime())){
            return created_time;
            //return false;
        }
        created_time=new Date(created_time);
        var after = created_time.getDate()+master_setting['track_interval'];
        created_time.setDate(after);
        var mm = created_time.getMonth()+1;
        var dd = created_time.getDate();
        return  mm+'/'+dd;
    }

    //insertCrawler(token,...args){
    insertCrawler(token,{ip,port}={}){
        if(!token){
            return false;
        }
        else if(this.crawlersInfo.has(token)){
            return false;
        }
        /*
        if(!args[0]){
            return false;
        }
        else if(!args[0]['ip']||!args[0]['port']){
            return false;
        }
        */
        if(!ip||!port){
           return false;
        }
        let info={};
        info['ip']=ip;
        info['port']=port;
        info['created_time']=dateFormat(new Date(),'yyyy/mm/dd HH:MM:ss');
        info['active_time']=dateFormat(new Date(),'yyyy/mm/dd HH:MM:ss');
        this.crawlersInfo.set(token,info);
        //this.crawlerSatus.set(token,'init');
        this.missionStatus(token,'init');
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
    /*TODO:testing*/
    getCrawler(token){
        var result={};
        if(!token){
            return false;
        }
        else if(!this.crawlersInfo.has(token)){
            return false;
        }
        result={name:token,info:this.crawlersInfo.get(token),status:this.crawlerSatus.get(token)};
        return result; 
        //return this.crawlersInfo.get(token);
        
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
        /*回傳值會被引用端改變*/
        result['info']=this.crawlersInfo.get(token);


        return result;
    }
    /*先創建行程資訊，實際的cronjob先不用創建*/
    newSchedule(schedule_name,{description='Schedule_'+schedule_name,track_time=master_setting.default_track_time,track_pool_name=master_setting.default_track_pool_name,schedule_status='off',track_num=master_setting.ids_num}={}){
        if(!schedule_name){
            return false;
        }   
        if(this.schedulesInfo.has(schedule_name)){
            return false;
        }

        let info={};
        info['description']=description;
        info['track_time']=track_time;
        info['track_pool_name']=track_pool_name;
        info['track_num']=track_num;
        info['status']='off';

        this.schedulesInfo.set(schedule_name,info);
        if(schedule_status=='on'){
            this.startSchedules(schedule_name);
        }

        return true;
    }
    startSchedules(...schedules){
        if(schedules.length==0){
            for(var [key,value] of this.schedulesInfo.entries()){
                if(!this.createSingleSchedule(key)){
                    continue;
                }
            }
        }
        else{
            for(var i=0;i<schedules.length;i++){
                if(!this.createSingleSchedule(schedules[i])){
                    continue;
                }
            }
        }
        return true;
    }
    createSingleSchedule(schedule_name){
        if(!this.schedulesInfo.has(schedule_name)){return false;}//此行程名稱不存在清單裡
        let info = this.schedulesInfo.get(schedule_name);
        if(info['status']=='on'){return false;}//此行程已開啓，不能再開第二次
        var _self=this;
        let name = schedule_name;
        let schedule = new CronJob({
            cronTime:info['track_time'],
            onTick:function(){

                let track_pool;
                if(info['track_pool_name']=='default'){
                    track_pool = (new Date().getMonth()+1)+'/'+new Date().getDate();
                }
                else{
                    track_pool = info['track_pool_name'];
                }
                console.log('Start ['+name+'], time:'+info['track_time']+' pool:'+track_pool);
                var crawler,ids;
                /*行程設定時間一到，先檢查有無可指派任務的crawler，再檢查有無id可發出。*/
                while((crawler = _self.findMissionCrawler())&&(ids = _self.shiftTrackId(track_pool,info['track_num']))){
                    //_self.crawlerSatus.set(crawler['token'],'ing');
                    /*若任務指派成功，則記錄該crawler被指派任務的時間，只要任務完成，就會改回'DONE'，否則就會一直是時間，可用來觀察crawler是否停擺*/
                    _self.missionStatus(crawler['token'],new Date());
                    /*TODO:tetsing*/
                    _self.recordPostSendTime(ids);


                    console.log('Crawler:'+JSON.stringify(crawler,null,3)+'\nids:'+JSON.stringify(ids,null,3));
                    /*TODO:crawler_name,crawler_version,control_token之後也是由client給予，資訊會一起包在info裡，目前先用server端預設*/
                    _self.sendMission(crawler['token'],{ip:crawler['info']['ip'],port:crawler['info']['port'],crawler_name:master_setting.crawler_name,crawler_version:master_setting.crawler_version,control_token:master_setting.control_token,track_ids:ids});
                }
                /*
                if(_self.hasTrackId(track_pool)&&(crawler = _self.findMissionCrawler())){
                    let ids = _self.shiftTrackId(track_pool,info['track_num']);
                    console.log('Crawler:'+JSON.stringify(crawler,null,3)+'\nids:'+JSON.stringify(ids,null,3));
                }
                else{
                    delaySchdule({pool_name:track_pool,time:info['track_time'],track_num:info['track_num']});
                }
                */
                /* TODO:如果目前該pool沒有任何id可以追蹤，或是沒有可以用的crawler，則將追蹤行程延後*/
                /*分配id給crawler*/


            },
            start:true,
            timeZone:'Asia/Taipei'
        });
        info['status']='on';
        this.schedulesJob.set(schedule_name,schedule);
        return true;
        
    }
    /*TODO*/
    recordPostSendTime([...ids]){
        for(let i=0;i<ids.length;i++){
            this.post_idInfo.set(ids[i]['post_id'],new Date());//記錄發出時間
            console.log('Send:'+ids[i]['post_id']+' '+this.post_idInfo.get(ids[i]['post_id']));
        }
    }
    /*停止時，會連同整個行程(schedulesJob)一起刪除，但是資訊(schedulesInfo)還會在*/
    stopSchedules(...schedules){
        if(schedules.length==0){
            for(var [key,value] of this.schedulesInfo.entries()){
                this.shutdownSingleSchedule(key);
            }
        }
        else{
            for(var i=0;i<schedules.length;i++){
                this.shutdownSingleSchedule(schedules[i]);
            }
        }
        return true;
    }
    shutdownSingleSchedule(schedule_name){
        if(!this.schedulesJob.has(schedule_name)){return false;}
        this.schedulesJob.get(schedule_name).stop();
        this.schedulesJob.delete(schedule_name);

        let info = this.schedulesInfo.get(schedule_name);
        info['status']='off';
        return true;
    }
    deleteSchedule(schedule_name){
        if(!schedule_name){
            return false;
        }
        else if(!this.schedulesInfo.has(schedule_name)){
            return false;
        }
        
        if(this.schedulesInfo.get(schedule_name)['status']=='on'){
            this.stopSchedules(schedule_name);    
        }

        this.schedulesInfo.delete(schedule_name);
        return true;
    }
    /*TODO:testing*/
    getSchedule(schedule_name){
        var result={};
        if(!schedule_name){
            return false;
        }
        else if(!this.schedulesInfo.has(schedule_name)){
            return false;
        }
        result={name:result,info:this.schedulesInfo.get(schedule_name)};
        return result; 
        //return this.schedulesInfo.get(schedule_name);
    }

    updateSchedule(schedule_name,{new_schedule_name,track_time,track_pool_name,description}={}){
        if(!schedule_name||!this.schedulesInfo.has(schedule_name)||schedule_name==new_schedule_name){
            return false;
        }
        if(!new_schedule_name&&!track_time&&!track_pool_name&&!description){
            return false;
        }

        var info = this.schedulesInfo.get(schedule_name);
        var schedule_status = info['status'];
        var old_name,restart=0;
        if(new_schedule_name&&new_schedule_name!=schedule_name){
            if(this.schedulesInfo.has(new_schedule_name)){
                return false;
            }

            this.schedulesInfo.set(new_schedule_name,info);
            old_name = schedule_name;
            schedule_name=new_schedule_name;
            restart=1;
        }
        if(track_time){
            info['track_time']=track_time;
            restart=1;
        }
        if(track_pool_name){
            info['track_pool_name']=track_pool_name;
            restart=1;
        }
        if(description){
            info['description']=description;
        }

        //this.schedulesInfo.set(schedule_name,info);
        

        if(restart==1){
            if(old_name){
                this.stopSchedules(old_name);
                this.schedulesInfo.delete(old_name);
            }
            else{
                this.stopSchedules(schedule_name);
            }

            if(schedule_status=='on'){
                this.startSchedules(schedule_name);
            }
        }
        return true;
    }
    /*TODO*/
    sendMission(crawler_token,{ip,port,crawler_name,crawler_version,control_token,track_ids}){
        var _self=this;
        this.mission['track_posts']=[];
        this.mission['track_posts'] = track_ids.map(function(x){
            return x.post_id;
        });
        //console.log('Master send:'+JSON.stringify(this.mission,null,3));
        //return;
        request({
            method:'POST',
            json:true,
            headers:{
                "content-type":"application/json"
            },
            body:{
                control_token:control_token,
                mission:this.mission
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
                        return false;
                    }
                    else{

                        return true;
                    }
                }
            }
            else{
                if(err){
                    if(err.code.indexOf('TIME')!=-1||err.code.indexOf('ENOT')!=-1||err.code.indexOf('ECONN')!=-1||err.code.indexOf('REACH')!=-1){
                        setTimeout(function(){
                            _self.sendMission(crawler_token,{ip,port,crawler_name,crawler_version,control_token,track_ids});
                        },master_setting['crawler_timeout_again']*1000);
                    }
                    else{
                        writeLog('err','sendMission, '+err);
                        return false;
                    }
                }
                else{
                    if(res.statusCode>=500&&res.statusCode<600){
                        setTimeout(function(){
                            _self.sendMission(crawler_token,{ip,port,crawler_name,crawler_version,control_token,track_ids});
                        },master_setting['crawler_timeout_again']*1000);
                    }
                    else{
                        writeLog('err','sendMission, '+res['statusCode']+', '+body['err']);
                        return false;
                    }
                }
            }
        });
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

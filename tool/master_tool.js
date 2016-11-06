'use strict'
//TODO:
//  1. retry limit，控制timeout後 重新連線的次數
//  2. server開始前，將crawler資訊和data center同步
var fs = require('fs');
var CronJob = require('cron').CronJob;
var dateFormat = require('dateformat');
var request = require('request');
var LineByLineReader = require('line-by-line');

var master_setting = JSON.parse(fs.readFileSync('./service/master_setting.json'));

const center_ip = master_setting['center_ip'];
const center_port = master_setting['center_port'];
const center_url = master_setting['center_url'];

const my_center_ip = master_setting['my_center_ip'];
const my_center_port = master_setting['my_center_port'];
const my_center_name = master_setting['my_center_name'];
const my_center_version = master_setting['my_center_version'];
const service_store_info = master_setting['service_store_info'];


class Track{
    constructor(){
        this.mission = JSON.parse(fs.readFileSync('./service/mission.json'));
        this.sendoutList =  new Map();//(token,發出去的時間)
        this.trackPools={};//記錄pool name, pool用途描述, pool內擁有的track post info({post_id,created_time,pool_name})
        this.crawlersInfo=new Map();//(token, info) -> info({ip, port, created_time, active_time}), active_time預設為crawler最後一次回報的時間, 但是crawlerSatus有記錄crawler的狀態(init, done, ${time}), time為crawler接收到任務的時間, 假設一直都是時間, 則代表自上次發出任務以來, crawler都沒有回報, 該crawler可能已經停擺, 需要做過期處理
        this.crawlerSatus=new Map();//(token, status), status(init/done/${time}), 交由missionStatus來管理, 記錄crawler的狀態
        this.schedulesInfo=new Map();//(schedule name, info), info({description, track_time, track_pool_name, track_num, status}), track_num:一次追蹤既幾個post id, status:該行程的開關狀態(on/off)
        this.cnt_store_file=0;
        this.total_files_num=Object.keys(master_setting['service_store_info']['info_type']).length;
        this.schedulesJob=new Map();//(schedule name, schedule本體), 可借由schedulesJob(name).stop/start
        
        this.detect_expire = this.startDetectExpire();

        this.readInfoFromFile('');
        return true;

        
        
        /* 關於schedulesInfo, schedulesJob:
         * 創造新的行程時, 會先創建schedulesInfo, schedulesJob則會在開啟該行程時創建, 若行程關閉, 則
         * 會呼叫shutdownSingleSchedule來移除schedulesJob中的資訊, schedulesInfo則保留
         * 等到下次再次開啟時才會呼叫createSingleSchedule去讀取schedulesInfo的資訊來創建schedulesJob
         * */
        
        /*以下為舊程式，所有設定檔和環境由master setting理所指定*/

        for(let i=0,pools=master_setting.trackpools,keys=Object.keys(pools);i<keys.length;i++){
            let key=keys[i];
            //console.log('key['+i+'] '+key+' value:'+pools[keys[i]]['name']);
            this.trackPools[pools[keys[i]]['name']]={};
            this.trackPools[pools[keys[i]]['name']]['description']=pools[keys[i]]['description'];
            this.trackPools[pools[keys[i]]['name']]['data']=[];
        }

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
        

        for(let i=0,schedules=master_setting.schedules;i<schedules.length;i++){
            console.log(JSON.stringify({description:schedules[i]['description'],track_time:schedules[i]['track_time'],track_pool_name:schedules[i]['track_pool_name'],schedule_status:schedules[i]['status']},null,3));
            if(!this.newSchedule(schedules[i]['name'],{description:schedules[i]['description'],track_time:schedules[i]['track_time'],track_pool_name:schedules[i]['track_pool_name'],schedule_status:schedules[i]['status']})){
                return false;
            }
            else{
                console.log('Success new schedule:['+schedules[i]['name']+']');
                console.log('Content:'+JSON.stringify(this.schedulesInfo.get(schedules[i]['name']),null,2));
            }
        }
        return true;
    }
    startDetectExpire(){
        var _self = this;
       //偵測很久都沒收到追蹤回應的post id，將其從sendoutList刪除，並移到expire pool
        return new CronJob({
            cronTime:master_setting['expire_detect'],
            onTick:function(){
                for(let [key,value] of _self.sendoutList.entries()){
                    if(value!=0&&isExpire(value)){
                        console.log('Expire:'+key+' Move to expire pool!');
                        _self.sendoutList.delete(key);
                        _self.insertIdToPool(key,{pool_name:'expire'});
                    }
                }    
            },
            start:true,
            timeZone:'Asia/Taipei'
        });
    } 

    storeInfo2File(option){
        this.write2File('map','sendoutList',this.sendoutList,false,option);
        this.write2File('map','crawlersInfo',this.crawlersInfo,true,option);
        this.write2File('map','crawlerSatus',this.crawlerSatus,false,option);
        this.write2File('map','schedulesInfo',this.schedulesInfo,true,option);
        this.write2File('object','trackPools',this.trackPools,true,option);
    }

    //TODO:testing
    write2File(flag,info_type,info,parseFlag,option){
        console.log(`Store ${info_type} to file :`+service_store_info['info_type'][info_type]);
        writeLog('process',`Store ${info_type} to file :`+service_store_info['info_type'][info_type]);
        var writeStream = fs.createWriteStream(service_store_info['dir']+'/'+service_store_info['info_type'][info_type]);

        if(flag=='map'){
            for(let [key,value] of info.entries()){
                if(parseFlag){
                    value = JSON.stringify(value);
                }
                writeStream.write(key+','+value+'\n');
            }
        }
        else{
            writeStream.write(JSON.stringify(info,null,2)); 
        }
        writeStream.end();
        writeStream.on('error',(err)=>{
            console.log(err);
        })
        writeStream.on('finish',()=>{
            this.cnt_store_file++;
            console.log('Store info done : '+service_store_info['info_type'][info_type]);
            if(this.cnt_store_file==this.total_files_num){
                if(option=='end'){
                    console.log('Shutdown server after 5 secs...');
                    setTimeout(()=>{
                        process.exit(0);
                    },5*1000);
                }
                else{
                    this.cnt_store_file=0;
                    console.log('All service has been saved!');
                }
            }
        });

    }

    //TODO:testing
    readInfoFromFile(option){
        this.read2memory('map','sendoutList',this.sendoutList,false,option);
        this.read2memory('map','crawlersInfo',this.crawlersInfo,true,option);
        this.read2memory('map','crawlerSatus',this.crawlerSatus,false,option);
        this.read2memory('map','schedulesInfo',this.schedulesInfo,true,option);
        this.read2memory('trackPools','trackPools',this.trackPools,true,option);
    }
    /*TODO:讀取trackPools的方式比較奇怪，要改，無法借由改動info來改變this.trackPools，所以這邊直接改this.trackPools*/
    read2memory(flag,info_type,info,parseFlag,option){
        writeLog('process',`Read ${info_type} to memory :`+service_store_info['info_type'][info_type]);
        if(flag=='trackPools'){
            fs.readFile(service_store_info['dir']+'/'+service_store_info['info_type'][info_type],(err,data)=>{
                var err_flag=0,err_msg='';
                if(err){
                    console.log('Can\'t read : '+service_store_info['dir']+'/'+service_store_info['info_type'][info_type]+' err:'+err);
                    process.exit(0);
                }
                else{
                    try{
                        this.trackPools = JSON.parse(data);
                    }
                    catch(e){
                        err_flag=1;
                        err_msg=e;
                    }
                    finally{
                        if(err_flag){
                            console.log('Can\'t parse : '+service_store_info['dir']+'/'+service_store_info['info_type'][info_type]+' err:'+err_msg);
                            process.exit(0);
                        }
                        else{
                            console.log('Loading success : '+service_store_info['dir']+'/'+service_store_info['info_type'][info_type]);
                            writeLog('process','Loading success : '+service_store_info['dir']+'/'+service_store_info['info_type'][info_type]);
                            /*
                            fs.appendFile('./pool_test',JSON.stringify(this.trackPools,null,2)+'\n',(err)=>{
                                if(err){
                                    console.log(err);
                                }
                                else{
                                    console.log('Write post pool! Length:'+Object.keys(this.trackPools).length);
                                    //this.listPools();
                                }
                            });
                            */

                            //console.log(JSON.stringify(info,null,2));
                        }
                    }

                }
            })
        }
        else if(flag=='map'){
            var lr = new LineByLineReader(service_store_info['dir']+'/'+service_store_info['info_type'][info_type]);
            let line_cnt=0;
            let err_flag=0,err_msg='';
            lr.on('error', function (err) {
                console.log('Can\'t parse : '+service_store_info['dir']+'/'+service_store_info['info_type'][info_type]+' err:'+err_msg);
                process.exit(0);
            });
            lr.on('line', function (line) {
                line_cnt++;

                if(!parseFlag){
                    var parts = line.split(',');
                    console.log(parts[0]+','+parts[1]);
                    info.set(parts[0],parts[1]);
                }
                else{
                    var parts = line.split(',{');
                    parts[1]='{'+parts[1];
                    console.log(parts[0]+','+parts[1]);
                    try{
                        var  obj = JSON.parse(parts[1]);
                    }
                    catch(e){
                        err_flag=1;
                        err_msg=e;
                    }
                    finally{
                        if(err_flag){
                            console.log('Can\'t parse : '+service_store_info['dir']+'/'+service_store_info['info_type'][info_type]+' err:'+err_msg);
                            process.exit(0);
                        }
                        else{
                            info.set(parts[0],obj);
                        }
                    }
                }

            });
            lr.on('end', function () {
                console.log('Loading success : '+service_store_info['dir']+'/'+service_store_info['info_type'][info_type]);
                writeLog('process','Loading success : '+service_store_info['dir']+'/'+service_store_info['info_type'][info_type]);
            });
        }
    }

    listSchedules([...schedules]=[]){
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
                console.log('['+i+']listSchedules:'+key);
                if(this.schedulesInfo.has(key)){
                    console.log('Has it!');
                    value = this.schedulesInfo.get(key);
                    results.push({name:key,info:value});
                }
                i++;
                if(i>master_setting.list_size){break;}
            }
        }
        return results;
    }

    listPools([...pool_names]=[]){
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
    listCrawlers([...crawler_tokens]=[]){
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
    * 回傳false時機:
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
    insertIdToPool(post_id,{pool_name,created_time}={}){
        if(!post_id){
            return false;
        }
        /*
        if(this.hasPostId()){
            return false;
        }
        else{//新的post id，儲存
            if(pool_name&&pool_name!='expire'){
                this.sendoutList.set(post_id,0);
            }
        }
        */
        //console.log('Set:'+post_id+' '+this.sendoutList.get(post_id)+' pool:'+pool_name);

        var name;
        /*
        if(args[0]){
            pool_name=args[0]['pool_name'],created_time=args[0]['created_time'];
        }
        */
        //有指定塞入的pool_name
        if(pool_name){
            //若該pool不存在，則拒絕塞入
            if(!this.checkPoolName(pool_name)){
                return false
            }
            else{
                name=pool_name;
            }
        }

        if(created_time){
            //檢查日期格式是否正確
            if(checkDateFormat(created_time)){
                created_time=deafultDate(created_time);//將日期格式統一
                if(isPast(created_time)){//檢查貼文是否已超過可追蹤的日期
                    if(!name){//pool_name指定高於貼文日期
                        name=master_setting.trackpools['past']['name'];
                    }
                }
                /*
                else{//追蹤範圍日期
                    created_time=dateFormat(created_time,'yyyy/mm/dd HH:MM:ss');
                }
                */

            }
            else{//若是錯誤的日期，則將貼文日期設為今日
                created_time=deafultDate();
            }
        }
        else{
            created_time=deafultDate();
        }
        /*如果沒有指定的pool name，則用日期來解析出pool_name，例：2016 09/11 09:11:22，指定3天後追蹤 => poolname: 09/14*/
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
    //一律從trackPools[pool_name]裡拿post id，sendoutList只是用來記錄此id的發出狀況，若成功抓取，則應該從sendoutList中移除，並定時觀察sendoutList裡是否有超過一定時間沒搜集完成的id(有發出時間在上面，並且已超過指定搜集時間)
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
                if(!id){//沒有可以追蹤的項目
                    break;
                }
                //檢查拿到的id是否目前正在被抓取
                if(!this.sendoutList.has(id['post_id'])){//不存在正在抓取清單
                    result.push(id);
                }
                else{
                    i--;
                }
            }
        }

        if(result.length==0){
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
        if(!id||!this.sendoutList.has(id)){
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
            this.startSchedules([schedule_name]);
        }
        return true;
    }
    startSchedules([...schedules]=[]){

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
        writeLog('process','Creating single schedule:'+schedule_name);
        console.log('1. Creating single schedule:'+schedule_name);
        let info = this.schedulesInfo.get(schedule_name);
        if(info['status']=='on'){return false;}//此行程已開啓，不能再開第二次
        writeLog('process','Turn on shedule:'+schedule_name);
        console.log('2. Turn on shedule:'+schedule_name);
        var _self=this;
        let name = schedule_name;
        console.log('Start ['+name+'], time:'+info['track_time']);
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
                console.log('Schedule:'+name+' Detecting pool '+track_pool+'...');
                var crawler=true,ids=true;
                /*行程設定時間一到，先檢查有無可指派任務的crawler，再檢查有無id可發出。*/
                while((crawler = _self.findMissionCrawler())&&(ids = _self.shiftTrackId(track_pool,info['track_num']))){
                    //_self.crawlerSatus.set(crawler['token'],'ing');
                    /*若任務指派成功，則記錄該crawler被指派任務的時間，只要任務完成，就會改回'DONE'，否則就會一直是時間，可用來觀察crawler是否停擺*/
                    _self.missionStatus(crawler['token'],new Date());
                    /*TODO:testing*/
                    _self.recordPostSendTime(ids);

                    console.log('Crawler:'+JSON.stringify(crawler,null,3)+'\nids:'+JSON.stringify(ids,null,3));
                    /*TODO:crawler_name,crawler_version,control_token之後也是由client給予，資訊會一起包在info裡，目前先用server端預設*/
                    _self.sendMission(crawler['token'],{ip:crawler['info']['ip'],port:crawler['info']['port'],crawler_name:master_setting.crawler_name,crawler_version:master_setting.crawler_version,control_token:master_setting.control_token,track_ids:ids});
                }
                
                //如果該pool已經空了，並且不為expire, past, fail pool(代表為一般追蹤pool，時間到就會去搜集，假設空了代表已清空，ex:預計9/11追蹤的pool已經沒有可追蹤的id，則可以停止追蹤該pool的schedule)，expire那些pool可能隨時會新增，為常駐schedule，不用停止
                if(!ids){
                    if(track_pool!='expire'&&track_pool!='past'&&track_pool!='fail'){
                        console.log('Stop schedule:'+name);
                        writeLog('process','Stop schedule:'+name);
                        _self.stopSchedules([name]);    
                    }
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
    recordPostSendTime([...ids]=[]){
        for(let i=0;i<ids.length;i++){
            this.sendoutList.set(ids[i]['post_id'],new Date());//記錄發出時間
            console.log('Send:'+ids[i]['post_id']+' '+this.sendoutList.get(ids[i]['post_id']));
        }
    }
    postTrackFinish(stat,[...ids]=[]){
        for(let i=0;i<ids.length;i++){
            this.sendoutList.delete(ids[i]);
            if(stat=='fail'){
                this.insertIdToPool(ids[i],{pool_name:'fail'});
            }
        }
        if(ids.length!=0){
            if(stat=='success'){
                console.log('Finish:'+ids.join(','));
            }
            else if(stat=='fail'){
                console.log('Fail:'+ids.join(',')+'  Move to fail pool!');
            }
        }

    }
    /*停止時，會連同整個行程(schedulesJob)一起刪除，但是資訊(schedulesInfo)還會在*/
    stopSchedules([...schedules]=[]){
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
        writeLog('process','Shutdown schedule:'+schedule_name);
        if(!this.schedulesJob.has(schedule_name)){
            writeLog('process','Schedule:'+schedule_name+' not exist!');
            console.log('Schedule:'+schedule_name+' not exist!');
            return false;
        }
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
            this.stopSchedules([schedule_name]);    
        }
        
        writeLog('process','Delete schedule:'+schedule_name);
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
                this.stopSchedules([old_name]);
                this.schedulesInfo.delete(old_name);
            }
            else{
                this.stopSchedules([schedule_name]);
            }

            if(schedule_status=='on'){
                this.startSchedules([schedule_name]);
            }
        }
        return true;
    }
    sendApply2DataCenter(crawler_token,crawler_info,{center_ip,center_port,center_name,center_version,control_token}){
        
        writeLog('process','Connecting to MyDataCenter...'+'http://'+center_ip+':'+center_port+'/'+center_name+'/'+center_version+'/apply/{crawler_token}');
        //console.log('sendApply2DataCenter:'+JSON.stringify(crawler_info)+'\n'+'http://'+center_ip+':'+center_port+'/'+center_name+'/'+center_version+'/apply/'+crawler_token);
        var _self=this;
        request({
            method:'POST',
            json:true,
            headers:{
                "content-type":"application/json"
            },
            body:{
                access_token:control_token,
                data:crawler_info
            },
            url:'http://'+center_ip+':'+center_port+'/'+center_name+'/'+center_version+'/apply/'+crawler_token,
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
                        writeLog('err','sendApply2DataCenter, '+err_msg);
                        return false;
                    }
                    else{
                        writeLog('process','Success connect to MyDataCenter!');
                        return true;
                    }
                }
            }
            else{
                if(err){
                    console.log('sendApply2DataCenter, '+err.code);
                    if(err.code.indexOf('TIME')!=-1||err.code.indexOf('ENOT')!=-1||err.code.indexOf('ECONN')!=-1||err.code.indexOf('REACH')!=-1){
                        setTimeout(function(){
                            _self.sendApply2DataCenter(crawler_token,crawler_info,{center_ip,center_port,center_name,center_version,control_token});
                        },master_setting['crawler_timeout_again']*1000);
                    }
                    else{
                        writeLog('err','sendApply2DataCenter, '+err);
                        return false;
                    }
                }
                else{
                    console.log('sendApply2DataCenter, '+res.statusCode);
                    if(res.statusCode>=500&&res.statusCode<600){
                        setTimeout(function(){
                            _self.sendApply2DataCenter(crawler_token,crawler_info,{center_ip,center_port,center_name,center_version,control_token});
                        },master_setting['crawler_timeout_again']*1000);
                    }
                    else{
                        writeLog('err','sendApply2DataCenter, '+res['statusCode']+', '+body['err']);
                        return false;
                    }
                }
            }
        });
    }
    /*TODO*/
    sendMission(crawler_token,{ip,port,crawler_name,crawler_version,control_token,track_ids}){
        var _self=this;
        this.mission['track_posts']=[];
        this.mission['track_posts'] = track_ids.map(function(x){
            return x.post_id;
        });
        //console.log('Master send:'+JSON.stringify(this.mission,null,3)+'URL:http://'+ip+':'+port+'/'+crawler_name+'/'+crawler_version+'/mission');
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

class DataCenter{
    constructor(){
        this.crawlersInfo=new Map();
    }
    insertCrawler(token,{ip,port}={}){
        if(!token){
            return false;
        }
        else if(this.crawlersInfo.has(token)){
            return false;
        }
        if(!ip||!port){
           return false;
        }
        let info={};
        info['ip']=ip;
        info['port']=port;
        info['active_time']=dateFormat(new Date(),'yyyy/mm/dd HH:MM:ss');
        this.crawlersInfo.set(token,info);
        return true;
    }

    deleteCrawler(token){
        if(!this.crawlersInfo.has(token)||!token){
            return false;
        }
        this.crawlersInfo.delete(token);
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
        result={name:token,info:this.crawlersInfo.get(token)};
        return result; 
        
    }
    listCrawlers([...crawler_tokens]=[]){
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
                results.push({name:key,info:value});
                i++;
                if(i>master_setting.list_size){break;}
            }
        }
        else{
            var key,value;
            while(i<crawler_tokens.length){
                key = crawler_tokens[i];
                if(this.crawlersInfo.has(key)){
                    value = this.crawlersInfo.get(key);
                    results.push({name:key,info:value});
                }
                i++;
                if(i>master_setting.list_size){break;}
            }
        }
        return results;
    }

}
function connect2DataCenter(fin){
    if(master_setting['master']['data']=='off'){
        fin('off','Need not connect to my data center.');
        return;
    }
    writeLog('process','Connecting to DataCenter...'+'http://'+center_ip+':'+center_port+'/');
    var _self=this;
    request({
        method:'GET',
        url:'http://'+center_ip+':'+center_port+'/',
        timeout:master_setting['request_timeout']*1000
    },(err,res,body)=>{
        if(!err&&res.statusCode==200){
            var err_msg='';
            var err_flag=0
            try{
                var content = JSON.parse(body);
            }
            catch(e){
                err_flag=1;
                err_msg=e;
            }
            finally{
                if(err_flag==1){
                    writeLog('err','connect2DataCenter, '+err_msg);
                    fin(false,err_msg);
                }
                else{
                    if(content['error']&&content['error']!=''){
                        fin(false,body);
                    }
                    else{
                        writeLog('process','Success connect to DataCenter...'+'http://'+center_ip+':'+center_port+'/'+' Response:'+body);
                        fin(true,body);


                    }
                }
            }
        }
        else{
            if(err){
                console.log('connect2DataCenter, '+err.code);
                writeLog('err','connect2DataCenter, '+err);
                fin(false,err);
            }
            else{
                console.log('connect2DataCenter, '+res.statusCode);
                writeLog('err','connect2DataCenter, '+res['statusCode']+', '+body);
                fin(false,res['statusCode']+', '+body);
            }
        }
    });
}
function connect2MyDataCenter(fin){
    if(master_setting['master']['my_data']=='off'){
        fin('off','Need not connect to my data center.');
        return;
    }
    var _self=this;
    writeLog('process','Connecting to MyDataCenter...'+'http://'+center_ip+':'+center_port+'/'+my_center_name+'/'+my_center_version+'/testConnect?access_token='+master_setting['control_token']);
    request({
        method:'GET',
        url:'http://'+my_center_ip+':'+my_center_port+'/'+my_center_name+'/'+my_center_version+'/testConnect?access_token='+master_setting['control_token'],
        timeout:master_setting['request_timeout']*1000
    },(err,res,body)=>{
        if(!err&&res.statusCode==200){
            var err_msg='';
            var err_flag=0
            try{
                var content = JSON.parse(body);
            }
            catch(e){
                err_flag=1;
                err_msg=e;
            }
            finally{
                if(err_flag==1){
                    writeLog('err','connect2MyDataCenter, '+err_msg);
                    fin(false,err_msg);
                }
                else{
                    writeLog('process','Success connect to MyDataCenter...'+'http://'+center_ip+':'+center_port+'/'+my_center_name+'/'+my_center_version+'/testConnect?access_token='+master_setting['control_token']+' Response:'+body);
                    fin(true,body);
                }
            }
        }
        else{
            if(err){
                console.log('connect2MyDataCenter, '+err.code);
                writeLog('err','connect2MyDataCenter, '+err);
                fin(false,err);
            }
            else{
                console.log('connect2MyDataCenter, '+res.statusCode);
                writeLog('err','connect2MyDataCenter, '+res['statusCode']+', '+body);
                fin(false,res['statusCode']+', '+body);
            }
        }
    });
}

exports.DataCenter=DataCenter;
exports.Track=Track;
exports.connect2DataCenter=connect2DataCenter;
exports.connect2MyDataCenter=connect2MyDataCenter;
function checkDateFormat(created_time){
    var time = new Date(created_time).getTime();
    return isNaN(time)?false:true;
}   
function isPast(created_time){
    created_time=new Date(created_time);
    //計算出該貼文預計要被追蹤的日期
    var track_date = created_time.getDate()+master_setting['track_interval'];
    created_time.setDate(track_date);
    var time = new Date(created_time).getTime();
    //如果追蹤日期是在今日之前，則代表未來不可能有機會追蹤，故需要放進past pool裡
    return time<new Date().getTime()?true:false;
}
function isExpire(time){
    var send_time=new Date(time).getTime();
    var now = new Date().getTime();
    var expire_time = master_setting['expire_time']*1000;

    if(now-send_time>expire_time){
        return true;   
    }
    else{
        return false;
    }
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

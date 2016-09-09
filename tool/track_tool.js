'use strict'
var request = require('request');
var fs = require('fs');
var dateFormat = require('dateformat');
var HashMap = require('hashmap');
var reactions;
var crawler_setting = JSON.parse(fs.readFileSync('./service/crawler_setting.json'));

function applyCrawler(ip,port,master_name,master_version,token,timeout,retryt,fin){
    request({
        method:'POST',
        json:true,
        headers:{
            "content-type":"application/json"
        },
        body:{
            invite_token:token
        },
        url:'http://'+ip+':'+port+'/'+master_name+'/'+master_version,
        timeout:timeout*1000
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
                    fin('err',err_msg);
                }
                else{
                    fin('ok',content);   
                }
            }

        }
        else{
            if(err){
                if(err.code.indexOf('TIME')!=-1||err.code.indexOf('ENOT')!=-1||err.code.indexOf('ECONN')!=-1||err.code.indexOf('REACH')!=-1){
                    setTimeout(function(){
                        applyCrawler(ip,port,master_name,master_version,token,timeout,retryt,fin)
                    },retryt*1000);
                }
                else{
                    writeLog('err','applyCrawler, '+err);
                    fin('err','applyCrawler, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        applyCrawler(ip,port,master_name,master_version,token,timeout,retryt,fin)
                    },retryt*1000);
                }
                else{
                    writeLog('err','applyCrawler, '+res['statusCode']+', '+res['body']);
                    fin('err','applyCrawler, '+res['statusCode']+', '+res['body']);
                }
            }
       }
    });
}
function missionReport(ip,port,master_name,master_version,token,mission_status,timeout,retryt,fin){
    request({
        method:'POST',
        json:true,
        headers:{
            "content-type":"application/json"
        },
        body:{
            mission_token:token,
            mission_status:mission_status
        },
        url:'http://'+ip+':'+port+'/'+master_name+'/'+master_version+'/mission_status',
        timeout:timeout*1000
    },(err,res,body)=>{
        if(!err&&res.statusCode==200){
            var err_msg='';
            var err_flag=0
            try{
                var content = body;
                console.log('=>mission report:\n'+body);
            }
            catch(e){
                err_flag=1;
                err_msg=e;
            }
            finally{
                if(err_flag==1){
                    fin('err',err_msg);
                }
                else{
                    fin('ok',content);   
                }
            }

        }
        else{
            if(err){
                if(err.code.indexOf('TIME')!=-1||err.code.indexOf('ENOT')!=-1||err.code.indexOf('ECONN')!=-1||err.code.indexOf('REACH')!=-1){
                    setTimeout(function(){
                        missionReport(ip,port,master_name,master_version,token,timeout,retryt,fin)
                    },retryt*1000);
                }
                else{
                    writeLog('err','missionReport, '+err);
                    fin('err','missionReport, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        missionReport(ip,port,master_name,master_version,token,timeout,retryt,fin)
                    },retryt*1000);
                }
                else{
                    writeLog('err','missionReport, '+res['statusCode']+', '+res['body']);
                    fin('err','missionReport, '+res['statusCode']+', '+res['body']);
                }
            }
       }
    });
}
function trackPost(timeout,mission,post_id,fin){
    var site = mission['site']+mission['graph_version']+'/'+post_id+'?fields='+mission['fields']+'&access_token='+mission['graph_token']+'&limit='+mission['limit'];
    console.log('\nRequest:'+site);
    request({
        url:site,
        timeout:timeout*1000
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
                    fin('err',err_msg);
                }
                else{
                    fin('ok',content);   
                }
            }

        }
        else{
            if(err){
                if(err.code.indexOf('TIME')!=-1||err.code.indexOf('ENOT')!=-1||err.code.indexOf('ECONN')!=-1||err.code.indexOf('REACH')!=-1){
                    setTimeout(function(){
                        trackPost(site,timeout);
                    },mission['graph_timeout_again']*1000);
                }
                else{
                    writeLog('err','trackPost, '+err);
                    fin('err','trackPost, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        trackPost(site,timeout);
                    },mission['graph_timeout_again']*1000);
                }
                else{
                    writeLog('err','trackPost, '+res['statusCode']+', '+res['body']);
                    fin('err','trackPost, '+res['statusCode']+', '+res['body']);
                }
            }
       }
    });
}
function initContent(content){
    this.groupid = content['from']['id'];
    this.groupname = content['from']['name'];
    this.postid = content['id'];
    this.created_time = dateFormat(content['created_time'],'yyyy/mm/dd HH:MM:ss');
    this.permalink_url = content['permalink_url'];
    this.link = content['link'];
    this.reactions = {};
    this.comments = [];
    this.sharedposts = [];
    /*若此篇文章無人分享，則不會有shares欄位*/
    if(typeof content['shares']!=='undefined'&&content['shares']!=''){
        this.shares = content['shares']['count'];
    }
    else{
        this.shares = 0;
    }
    /*逐步確認reactions, comments, sharedposts的下一頁狀況，若無該欄位或是該欄位的paging子欄位的沒有next則代表無下一頁*/
    if(typeof content['reactions']==='undefined'||content['reactions']==''){
        this.next_reactions = null;
    }
    else{
        if(typeof content['reactions']['paging']['next']==='undefined'){
            this.next_reactions = null;
        }
        else{
            this.next_reactions = content['reactions']['paging']['next'];
        }
    }

    if(typeof content['comments']==='undefined'||content['comments']==''){
            this.next_comments = null;
    }
    else{
        if(content['comments']['paging']['next']==='undefined'){
            this.next_comments = null;
        }
        else{
            this.next_comments = content['comments']['paging']['next'];
        }

    }

    if(typeof content['sharedposts']==='undefined'||content['sharedposts']==''){
            this.next_sharedposts = null;
    }
    else{
        if(content['sharedposts']['paging']['next']==='undefined'){
            this.next_sharedposts = null;
        }
        else{
            this.next_sharedposts = content['sharedposts']['paging']['next'];
        }

    }


    /*解析出reactions中的各個使用者反應和數量*/
    if(typeof content['reactions']!=='undefined'&&content['reactions']!=''){
        var type,cnt,i;
        for(i=0;i<content['reactions']['data'].length;i++){
            type = content['reactions']['data'][i]['type'];
            if(typeof this.reactions[type]==='undefined'){
                this.reactions[type]=1;
            }
            else{
                this.reactions[type]++;
            }
        }
    }
    /*將當前頁面的comments詳細資料塞進array*/
    if(typeof content['comments']!=='undefined'&&content['comments']!=''){
        var data,likes_next='',comments_likes;
        for(i=0;i<content['comments']['data'].length;i++){
            data = content['comments']['data'];
            if(typeof data[i]['like_cnt']==='undefined'){
                comments_likes=0;
            }
            else{
                comments_likes=data[i]['like_cnt'];
            }
            this.comments.push({id:data[i].id,created_time:dateFormat(data[i].created_time,'yyyy/mm/dd HH:MM:ss'),likes:comments_likes,message:data[i].message});

        }
    }
    /*將當前頁面的sharedposts詳細資料塞進array*/
    if(typeof content['sharedposts']!=='undefined'&&content['sharedposts']!=''){
        var data,likes_next='',sharedposts_likes;
        //TODO:將來要將所有的sharedposts的likes page都拜訪完，目前先記錄下一頁的連結和第一頁的likes數
        for(i=0;i<content['sharedposts']['data'].length;i++){
            data = content['sharedposts']['data'];
            if(typeof data[i]['like_count']==='undefined'){
                sharedposts_likes=0;
            }
            else{
                sharedposts_likes=data[i]['like_count'];
            }
            this.sharedposts.push({id:data[i].id,created_time:dateFormat(data[i].created_time,'yyyy/mm/dd HH:MM:ss'),likes:sharedposts_likes,message:data[i].message});

        }
    }
    return this;
}
function parseComment(content){
    this.comments=[];
    if(typeof content['data']==='undefined'||content['data']==''){
        this.next_comments = null;
        return this;
    }
    else{
        if(content['paging']['next']==='undefined'){
            this.next_comments = null;
        }
        else{
            this.next_comments = content['paging']['next'];
        }

        var data,likes_next='',comments_likes,i;
        //TODO:需要將所有的comments和comment的likes page都拜訪完
        for(i=0;i<content['data'].length;i++){
            data = content['data'];
            if(typeof data[i]['message']==='undefined'){
                continue;
            }
            if(typeof data[i]['like_count']==='undefined'){
                comments_likes=0;
            }
            else{
                comments_likes=data[i]['like_count'];
            }
            this.comments.push({id:data[i].id,created_time:dateFormat(data[i].created_time,'yyyy/mm/dd HH:MM:ss'),likes:comments_likes,message:data[i].message});
        }
        return this;
    }
}
function parseSharedpost(content){
    this.sharedposts=[];
    if(typeof content['data']==='undefined'||content['data']==''){
        this.next_sharedposts = null;
        return this;
    }
    else{
        if(content['paging']['next']==='undefined'){
            this.next_sharedposts = null;
        }
        else{
            this.next_sharedposts = content['paging']['next'];
        }
        var data,likes_next='',sharedposts_likes,i;
        for(i=0;i<content['data'].length;i++){
            data = content['data'];
            if(typeof data[i]['message']==='undefined'){
                continue;
            }

            if(typeof data[i]['like_count']==='undefined'){
                sharedposts_likes=0;
            }
            else{
                sharedposts_likes=data[i]['like_count'];
            }
            this.sharedposts.push({id:data[i].id,created_time:dateFormat(data[i].created_time,'yyyy/mm/dd HH:MM:ss'),likes:sharedposts_likes,message:data[i].message});
        }
        return this;
    }
}
function parseReaction(content){
    this.reactions={};
    if(typeof content['data']==='undefined'||content['data']==''){
        this.next_reactions = null;
        return this;
    }
    else{
        if(typeof content['paging']['next']==='undefined'){
            this.next_reactions = null;
        }
        else{
            this.next_reactions = content['paging']['next'];
        }

        var type,cnt,i;
        for(i=0;i<content['data'].length;i++){
            type = content['data'][i]['type'];
            if(typeof this.reactions[type]==='undefined'){
                this.reactions[type]=1;
            }
            else{
                this.reactions[type]++;
            }

        }
        return this;
    }
}
function fetchNextPage(timeout,mission,site,fin){
    request({
        url:site,
        timeout:timeout*1000
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
                    fin('err',err_msg);
                }
                else{
                    fin('ok',content);   
                }
            }

        }
        else{
            if(err){
                if(err.code.indexOf('TIME')!=-1||err.code.indexOf('ENOT')!=-1||err.code.indexOf('ECONN')!=-1||err.code.indexOf('REACH')!=-1){
                    setTimeout(function(){
                        fetchNextPage(site,timeout);
                    },mission['graph_timeout_again']*1000);
                }
                else{
                    writeLog('err','fetchNextPage, '+err);
                    fin('err','fetchNextPage, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        fetchNextPage(site,timeout);
                    },mission['graph_timeout_again']*1000);
                }
                else{
                    writeLog('err','fetchNextPage, '+res['statusCode']+', '+res['body']);
                    fin('err','fetchNextPage, '+res['statusCode']+', '+res['body']);
                }
            }
       }
    });

}
function mission(){

}

function sendResponse(res,type,status_code,msg){
    if(type=='token_err'){
        res.status(403).send(crawler_setting['err_msg']['token_err']);
    }
    else if(type=='process_err'){
        res.status(503).send(crawler_setting['err_msg']['process_err']);
    }
    else{
        res.status(status_code).send(msg);
    }

}
function writeLog(type,msg){
    var now = new Date();
    var date = dateFormat(now,'yyyymmdd');
    var filename=crawler_setting['log_dir']+'/';
    if(type=='err'){
        filename += date+crawler_setting['err_filename'];
    }
    else if(type=='process'){
        filename += date+crawler_setting['process_filename'];
    }
    else{
        filename += date+crawler_setting['other_filename'];
    }

    fs.appendFile(filename,'['+now+'] Type:'+type+' Message:'+msg+'\n',(err)=>{
        if(err){
            console.log('[writeLog] Error:'+err);
        }
    });
}

function writeRec(type,track_id,msg){
    var now = new Date();
    var date = dateFormat(now,'yyyymmdd');
    var filename=crawler_setting['rec_dir']+'/';
    if(type=='gais'){
        filename += track_id+'.'+date+crawler_setting['gaisrec_filename'];
    }
    else{
        filename += track_id+'.'+date+crawler_setting['jsonrec_filename'];
    }
    fs.writeFile(filename,msg,(err)=>{
        if(err){
            console.log('[writeLog] Error:'+err);
        }
    });
}
exports.sendResponse=sendResponse;
exports.writeLog=writeLog;
exports.writeRec=writeRec;
exports.applyCrawler=applyCrawler;
exports.missionReport=missionReport;
exports.trackPost=trackPost;
exports.mission=mission;
exports.initContent=initContent;
exports.fetchNextPage=fetchNextPage;
exports.parseComment=parseComment;
exports.parseSharedpost=parseSharedpost;
exports.parseReaction=parseReaction;

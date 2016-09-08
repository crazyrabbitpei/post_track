'use strict'
var request = require('request');
var fs = require('fs');
var dateFormat = require('dateformat');
var HashMap = require('hashmap');
var reactions;
var setting = JSON.parse(fs.readFileSync('./service/setting.json'));

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
    this.shares = content['shares']['count'];
    this.reactions = {};
    this.comments = [];
    this.sharedposts = [];

    //TODO:需要測試沒有下一頁狀況的數值為和
    if(typeof content['reactions']==='undefined'){
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

    if(typeof content['comments']==='undefined'){
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

    if(typeof content['sharedposts']==='undefined'){
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

    var type,cnt,i;
    //TODO:需要一次將所有reactions page拜訪完
    for(i=0;i<content['reactions']['data'].length;i++){
        type = content['reactions']['data'][i]['type'];
        if(typeof this.reactions[type]==='undefined'){
            this.reactions[type]=1;
        }
        else{
           this.reactions[type]++;
        }
    }
    
    var data,likes_next='',comments_likes;
    if(typeof content['comments']!=='undefined'){
        //TODO:將來要將所有的comment的likes page都拜訪完，目前先記錄下一頁的連結和第一頁的likes數
        for(i=0;i<content['comments']['data'].length;i++){
            data = content['comments']['data'];
            if(typeof data[i]['likes']==='undefined'){
                likes_next=null;
                comments_likes=0;
            }
            else{
                comments_likes=data[i]['likes']['data'].length;
                if(typeof data[i]['likes']['paging']['next']==='undefined'){
                    likes_next=null;
                }
                else{
                    likes_next=data[i]['likes']['paging']['next'];
                }
            }
            this.comments.push({id:data[i].id,created_time:dateFormat(data[i].created_time,'yyyy/mm/dd HH:MM:ss'),likes:comments_likes,likes_next:likes_next,message:data[i].message});

        }
    }


    var data,likes_next='',sharedposts_likes;
    //TODO:將來要將所有的sharedposts的likes page都拜訪完，目前先記錄下一頁的連結和第一頁的likes數
    for(i=0;i<content['sharedposts']['data'].length;i++){
        data = content['sharedposts']['data'];
        if(typeof data[i]['likes']==='undefined'){
            likes_next=null;
            sharedposts_likes=0;
        }
        else{
            sharedposts_likes=data[i]['likes']['data'].length;
            if(typeof data[i]['likes']['paging']['next']==='undefined'){
                likes_next=null;
            }
            else{
                likes_next=data[i]['likes']['paging']['next'];
            }
        }
        this.sharedposts.push({id:data[i].id,created_time:dateFormat(data[i].created_time,'yyyy/mm/dd HH:MM:ss'),likes:sharedposts_likes,likes_next:likes_next,message:data[i].message});

    }
    return this;
}
function parseComment(content){
    if(typeof content['data']==='undefined'){
            this.next_comments = null;
    }
    else{
        if(content['paging']['next']==='undefined'){
            this.next_comments = null;
        }
        else{
            this.next_comments = content['paging']['next'];
        }

    }
    var data,likes_next='',comments_likes,i;
    //TODO:需要將所有的comments和comment的likes page都拜訪完
    for(i=0;i<content['data'].length;i++){
        data = content['data'];
        if(typeof data[i]['message']==='undefined'){
            continue;
        }

        if(typeof data[i]['likes']==='undefined'){
                likes_next=null;
                comments_likes=0;
        }
        else{
            comments_likes=data[i]['likes']['data'].length;
            if(typeof data[i]['likes']['paging']['next']==='undefined'){
                likes_next=null;
            }
            else{
                likes_next=data[i]['likes']['paging']['next'];
            }
        }
        this.comments.push({id:data[i].id,created_time:dateFormat(data[i].created_time,'yyyy/mm/dd HH:MM:ss'),likes:comments_likes,likes_next:likes_next,message:data[i].message});
    }
    return this;
}
function parseSharedpost(content){
    if(typeof content['data']==='undefined'){
            this.next_sharedposts = null;
    }
    else{
        if(content['paging']['next']==='undefined'){
            this.next_sharedposts = null;
        }
        else{
            this.next_sharedposts = content['paging']['next'];
        }

    }
    var data,likes_next='',sharedposts_likes,i;
    for(i=0;i<content['data'].length;i++){
        data = content['data'];
        if(typeof data[i]['message']==='undefined'){
            continue;
        }

        if(typeof data[i]['likes']==='undefined'){
                likes_next=null;
                sharedposts_likes=0;
        }
        else{
            sharedposts_likes=data[i]['likes']['data'].length;
            if(typeof data[i]['likes']['paging']['next']==='undefined'){
                likes_next=null;
            }
            else{
                likes_next=data[i]['likes']['paging']['next'];
            }
        }
        this.sharedposts.push({id:data[i].id,created_time:dateFormat(data[i].created_time,'yyyy/mm/dd HH:MM:ss'),likes:sharedposts_likes,likes_next:likes_next,message:data[i].message});
    }
    return this;
}
function parseReaction(content){
    //TODO:需要測試沒有下一頁狀況的數值為和
    if(typeof content['data']==='undefined'){
        this.next_reactions = null;
    }
    else{
        if(typeof content['paging']['next']==='undefined'){
            this.next_reactions = null;
        }
        else{
            this.next_reactions = content['paging']['next'];
        }
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
        res.status(403).send(setting['err_msg']['token_err']);
    }
    else if(type=='process_err'){
        res.status(503).send(setting['err_msg']['process_err']);
    }
    else{
        res.status(status_code).send(msg);
    }

}
function writeLog(type,msg){
    var now = new Date();
    var date = dateFormat(now,'yyyymmdd');
    var filename=setting['log_dir']+'/';
    if(type=='err'){
        filename += date+setting['err_filename'];
    }
    else if(type=='process'){
        filename += date+setting['process_filename'];
    }
    else{
        filename += date+setting['other_filename'];
    }

    fs.appendFile(filename,'['+now+'] Type:'+type+' Message:'+msg,(err)=>{
        if(err){
            console.log('[writeLog] Error:'+err);
        }
    });
}

function writeRec(type,track_id,msg){
    var now = new Date();
    var date = dateFormat(now,'yyyymmdd');
    var filename=setting['rec_dir']+'/';
    if(type=='gais'){
        filename += track_id+'.'+date+setting['gaisrec_filename'];
    }
    else{
        filename += track_id+'.'+date+setting['jsonrec_filename'];
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
exports.trackPost=trackPost;
exports.mission=mission;
exports.initContent=initContent;
exports.fetchNextPage=fetchNextPage;
exports.parseComment=parseComment;
exports.parseSharedpost=parseSharedpost;
exports.parseReaction=parseReaction;

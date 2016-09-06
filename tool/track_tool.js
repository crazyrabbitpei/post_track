'use strict'
var request = require('request');
var fs = require('fs');
var dateFormat = require('dateformat');
var HashMap = require('hashmap');
var reactions;
var setting = JSON.parse(fs.readFileSync('./service/setting.json'));

function applyCrawler(ip,port,master_name,master_version,token,timeout,fin){
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
        timeout:timeout
    },(err,res,body)=>{

    });
}
function trackPost(timeout,mission,post_id,fin){
    var site = mission['site']+mission['graph_version']+'/'+post_id+'?fields='+mission['fields']+'&access_token='+mission['graph_token'];
    console.log('Request:'+site);
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
                    },retryt*1000);
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
                    },retryt*1000);
                }
                else{
                    writeLog('err','trackPost, '+res['statusCode']+', '+res['body']);
                    fin('err','trackPost, '+res['statusCode']+', '+res['body']);
                }
            }
        }
    });
}
function parseContent(content){
    this.id = content['id'];
    this.created_time = content['created_time'];
    this.permalink_url = content['permalink_url'];
    this.link = content['link'];
    this.shares = content['shares'];
    this.reactions = {};
    this.comments = [];

    //TODO:需要測試沒有下一頁狀況的數值為和
    this.next_reactions = content['reactions']['paging']['next'];
    this.next_comments = content['comments']['paging']['next'];

    var type,cnt,i;
    var cnt_reaction = new HashMap();

    //TODO:需要一次將所有reactions page拜訪完
    for(i=0;i<content['reactions']['data'].length;i++){
        type = content['reactions']['data'][i]['type'];
        if(!cnt_reaction.has(type)){
            cnt_reaction.set(type,0);
            this.reactions[type]=0;
        }
        else{
           cnt = cnt_reaction.get(type);
           cnt++;
           cnt_reaction.set(type,cnt);
           this.reactions[type]++;
        }
    }
    
    var data;
    //TODO:需要將所有的comments和comment的likes page都拜訪完
    for(i=0;i<content['comments']['data'].length;i++){
        data = content['comments']['data'];
        this.comments.push({id:data[i].id,created_time:data[i].created_time,likes:0,likes_next:data[i]['paging']['next'],message:data[i].message});
    }
    return this;
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

exports.sendResponse=sendResponse;
exports.trackPost=trackPost;
exports.mission=mission;
exports.parseContent=parseContent;

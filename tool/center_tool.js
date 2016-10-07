'use strict'
var request = require('request');
var fs = require('fs');
var dateFormat = require('dateformat');
var HashMap = require('hashmap');
var querystring = require('querystring');
var reactions;

function transGais(data){
    var i,j,k;
    var current_post_id;
    var gaisrec='@Gais_REC\n';
    var sub_gaisrec='';
    var keys = Object.keys(data);
    var sub_keys;
    var new_name,new_sub_name;
    for(i=0;i<keys.length;i++){
        /*字首大寫轉換，以及將欄位名稱轉成指定名字*/
        new_name = mappingGaisFileds(keys[i],keys[i]);
        if(keys[i]=='id'){
            current_post_id = data[keys[i]];
        }
        /*會有多種reaction種類，每個種類會分成不同子類別，放在reactions欄位之下*/
        if(keys[i]=='reactions'){
            sub_gaisrec='';
            gaisrec+='@'+new_name+':\n';
            sub_keys = Object.keys(data[keys[i]]);
            for(j=0;j<sub_keys.length;j++){
                //console.log('['+j+'] ['+sub_keys[j]+']'+data[keys[i]][sub_keys[j]]);
                if(typeof data[keys[i]][sub_keys[j]]==='undefined'||data[keys[i]][sub_keys[j]]==null){
                    data[keys[i]][sub_keys[j]]='';
                }
                /*將欄位名稱轉換成指定名稱*/
                sub_gaisrec+='\t'+sub_keys[j]+':'+data[keys[i]][sub_keys[j]]+'\n';

            }
            gaisrec+=sub_gaisrec;
        }
        /*因為comments, sharedposts有陣列的議題(多個回應、分享)，需要將每個回應、分享轉換成子欄位*/
        else if(keys[i]=='comments'||keys[i]=='sharedposts'){
            gaisrec+='@'+new_name+':\n';
            for(j=0;j<data[keys[i]].length;j++){
                sub_gaisrec='\t'+mappingGaisFileds(keys[i],'')+'_'+j;
                sub_keys = Object.keys(data[keys[i]][j]);
                for(k=0;k<sub_keys.length;k++){
                    //console.log('['+j+'] ['+sub_keys[k]+'] '+data[keys[i]][j][sub_keys[k]]);
                    if(typeof data[keys[i]][j][sub_keys[k]]==='undefined'||data[keys[i]][j][sub_keys[k]]==null){
                        data[keys[i]][j][sub_keys[k]]='';
                    }
                    else if(sub_keys[k]=='message'){
                        data[keys[i]][j][sub_keys[k]]=data[keys[i]][j][sub_keys[k]].replace(/\n/g,' ');
                    }
                    new_sub_name = mappingGaisFileds(sub_keys[k],keys[i]);
                    sub_gaisrec+=' '+new_sub_name+':'+data[keys[i]][j][sub_keys[k]];
                }
                gaisrec+=sub_gaisrec+'\n';
            }
        }
        else if(keys[i]=='message'){
            data[keys[i]] = data[keys[i]].replace(/\n/g,' ');
            gaisrec+='@'+new_name+':\n'
            gaisrec+=data[keys[i]]+'\n';
        }

        /*reactoins, comments, sharedposts以外的欄位*/
        else{
            if(typeof data[keys[i]]==='undefined'||data[keys[i]]==null){
                data[keys[i]]='';
            }
            gaisrec+='@'+new_name+':'+data[keys[i]]+'\n';

        }
    }
    return gaisrec;
    //writeRec('gais',current_post_id,gaisrec);
}
function mappingGaisFileds(field,type){
    if(field=='created_time'){
        field = 'Doctime';
    }
    else if(field=='permalink_url'){
        field = 'Url';   
    }
    else if(field=='link'){
        field = 'Related_link';
    }
    else if(field=='id'){
        if(type=='comments'){
            field='Comment_id';
        }
        else if(type=='sharedposts'){
            field='Sharepost_id';
        }
        else{
            field='Post_id';
        }
    }
    else if(field=='from'){
        field='From_id';
    }
    else if(field=='attachments_src'){
        field = 'ImageLink';
    }
    else if(field=='message'){
        field='Body';
    }
    else{
        field = wordToUpper(1,field);
    }
    return field;
}

function sendResponse(res,type,status_code,msg){
    var result = new Object();
    if(type=='token_err'){
        result['data']='';
        result['err']=dataCenter_setting['err_msg']['token_err'];
        res.status(403).send(result);
    }
    else if(type=='process_err'){
        result['data']='';
        result['err']=dataCenter_setting['err_msg']['process_err']+'. Reason:'+msg;
        res.status(503).send(result);
    }
    else if(status_code>=200&&status_code<300){
        result['data']=msg;
        result['err']='';
        res.status(status_code).send(result);
    }
    else{
        result['data']='';
        result['err']=msg;
        res.status(status_code).send(result);
    }
}
function writeLog(type,msg){
    var now = new Date();
    var date = dateFormat(now,'yyyymmdd');
    var filename=dataCenter_setting['log_dir']['root']+'/';
    if(type=='err'){
        filename += date+'.err';
    }
    else if(type=='process'){
        filename += date+'.process';
    }
    else{
        filename += date+'.other';
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
    if(type!='json'&&type!='gais'){
        type='other';
    }
    var filename=dataCenter_setting['data_dir']['root']+'/';

    /*要將欄位名稱改過，以及字首轉大寫*/
    if(type=='gais'){
        msg = transGais(msg);
    }
    /*照原本格式儲存資料*/
    else{
        msg = JSON.stringify(msg,null,3);
    }
    filename += type+'/';
    filename += track_id+'_'+date+'.'+type;

    fs.appendFile(filename,msg,(err)=>{
        if(err){
            console.log('[writeLog] Error:'+err);
        }
    });
}
function wordToUpper(index,str){
    var cnt=1;
    var map = Array.prototype.map;
    return map.call(str,function(x){
        if(cnt==index){
            x = x.toUpperCase();
        }
        cnt++;
        return x;
    }).join('');
}
function initDir(dir){
    var root = dir['root'];
    var type = dir['type'];
    if(!root){
        console.log('Must have root dir!');
        return false;
    }
    fs.access(root,fs.constants.F_OK,(err)=>{
        if(err){
            addDir(root,(flag)=>{
                if(flag){
                    addSubDir({parent:root,childs:type});
                }
            });
        }
        else if(type){
            addSubDir({parent:root,childs:type});
        }
    });
}
function addDir(dir,next){
    if(!dir){
        return false;
    }
    fs.mkdir(dir,'0744',(err)=>{
        if(err){
            next(false)
        }
        else{
            console.log('Init dir:'+dir);
            next(true);
        }
    });
}
function addSubDir({parent,childs}){
    if(!parent||!childs){
    }
    else{
        for(let i=0;i<childs.length;i++){
            let sub_name = childs[i]['name'];
            fs.access(parent+'/'+sub_name,fs.constants.F_OK,(err)=>{
                if(err){
                    addDir(parent+'/'+sub_name,()=>{})
                }
            });
        }
    }
}
exports.initDir=initDir;
exports.sendResponse=sendResponse;
exports.writeLog=writeLog;
exports.writeRec=writeRec;
exports.transGais=transGais;

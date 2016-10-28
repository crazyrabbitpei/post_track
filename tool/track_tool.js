'use strict'
var request = require('request');
var fs = require('fs');
var dateFormat = require('dateformat');
var HashMap = require('hashmap');
var querystring = require('querystring');
var crawler_setting = JSON.parse(fs.readFileSync('./service/crawler_setting.json'));
const center_ip = crawler_setting['center_ip'];
const center_port = crawler_setting['center_port'];
const center_name = crawler_setting['center_name'];
const center_version = crawler_setting['center_version'];

const request_timeout = crawler_setting['request_timeout'];
const master_timeout_again = crawler_setting['master_timeout_again'];
const graph_timeout_again = crawler_setting['graph_timeout_again'];
const retry_limit = crawler_setting['retry_limit'];
var cnt_retry=0;
var fieldMap={};

function updateFieldMap(data){
    fieldMap = data;
}


function applyCrawler({crawler_port,master_ip,master_port,master_name,master_version,invite_token},fin){
    if(cnt_retry>retry_limit){
        writeLog('err','applyCrawler, retry over limit:'+cnt_retry);
        fin('err','applyCrawler, retry over limit:'+cnt_retry);
        cnt_retry=0;
        return;
    }

    console.log('Apply:'+'http://'+master_ip+':'+master_port+'/'+master_name+'/'+master_version+'/apply');
    request({
        method:'POST',
        json:true,
        headers:{
            "content-type":"application/json"
        },
        body:{
            access_token:invite_token,
            port:crawler_port
        },
        url:'http://'+master_ip+':'+master_port+'/'+master_name+'/'+master_version+'/apply',
        timeout:request_timeout*1000
    },(err,res,body)=>{
        if(!err&&(res.statusCode>=200&&res.statusCode<300)){
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
                console.log('applyCrawler, '+err.code);
                if(err.code.indexOf('TIME')!=-1||err.code.indexOf('ENOT')!=-1||err.code.indexOf('ECONN')!=-1||err.code.indexOf('REACH')!=-1){
                    setTimeout(function(){
                        cnt_retry++;
                        applyCrawler({crawler_port,master_ip,master_port,master_name,master_version,invite_token},fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','applyCrawler, '+err);
                    fin('err','applyCrawler, '+err);
                }
            }
            else{
                console.log('applyCrawler, '+res['statusCode']+', '+body['err']);
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        cnt_retry++;
                        applyCrawler({crawler_port,master_ip,master_port,master_name,master_version,invite_token},fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','applyCrawler, '+res['statusCode']+', '+body['err']);
                    fin('err','applyCrawler, '+res['statusCode']+', '+body['err']);
                }
            }
       }
    });
}
function uploadTrackPostData(access_token,{data,datatype},fin){
    if(datatype=='gais'){
        data = transGais(data);
    }
    else if(datatype=='json'){
        data = JSON.stringify(data);
    }
    console.log('Upload data:http://'+center_ip+':'+center_port+'/'+center_name+'/'+center_version+'/data/'+datatype+'access_token='+access_token);
    request({
        method:'POST',
        body:data,
        url:'http://'+center_ip+':'+center_port+'/'+center_name+'/'+center_version+'/data/'+datatype+'?access_token='+access_token,
        timeout:request_timeout*1000
    },(err,res,body)=>{
        if(!err&&(res.statusCode>=200&&res.statusCode<300)){
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
                        uploadTrackPostData(access_token,data,fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','uploadTrackPostData, '+err);
                    fin('err','uploadTrackPostData, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        uploadTrackPostData(access_token,data,fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','uploadTrackPostData, '+res['statusCode']+', '+body['err']);
                    fin('err','uploadTrackPostData, '+res['statusCode']+', '+body['err']);
                }
            }
       }
    });
}
//function uploadTrackPostId(ip,port,master_name,master_version,token,data,timeout,retryt,fin){
//data cralwer用
function uploadTrackPostId({master_ip,master_port,master_name,master_version,access_token,data},fin){
    console.log('http://'+master_ip+':'+master_port+'/'+master_name+'/'+master_version+'/post_id');
    request({
        method:'POST',
        json:true,
        headers:{
            "content-type":"application/json"
        },
        body:{
            access_token:access_token,
            data:data
        },
        url:'http://'+master_ip+':'+master_port+'/'+master_name+'/'+master_version+'/post_id',
        timeout:request_timeout*1000
    },(err,res,body)=>{
        if(!err&&(res.statusCode>=200&&res.statusCode<300)){
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
                        uploadTrackPostId({master_ip,master_port,master_name,master_version,access_token,data},fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','uploadTrackPostId, '+err);
                    fin('err','uploadTrackPostId, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        uploadTrackPostId({master_ip,master_port,master_name,master_version,access_token,data},fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','uploadTrackPostId, '+res['statusCode']+', '+body['err']);
                    fin('err','uploadTrackPostId, '+res['statusCode']+', '+body['err']);
                }
            }
       }
    });
}
//function listTrack(ip,port,master_name,master_version,token,date,timeout,retryt,fin){
function listTrack({master_ip,master_port,master_name,master_version,access_token},fin){
    console.log('http://'+master_ip+':'+master_port+'/'+master_name+'/'+master_version+'/service_status')
    request({
        method:'GET',
        json:true,
        body:{
            access_token:access_token
        },
        url:'http://'+master_ip+':'+master_port+'/'+master_name+'/'+master_version+'/service_status',
        timeout:request_timeout*1000
    },(err,res,body)=>{
        if(!err&&(res.statusCode>=200&&res.statusCode<300)){
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
                        listTrack(ip,port,master_name,master_version,token,date,timeout,retryt,fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','listTrack, '+err);
                    fin('err','listTrack, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        listTrack(ip,port,master_name,master_version,token,date,timeout,retryt,fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','listTrack, '+res['statusCode']+', '+body['err']);
                    fin('err','listTrack, '+res['statusCode']+', '+body['err']);
                }
            }
       }
    });
}
function missionReport({data,master_ip,master_port,master_name,master_version,access_token,mission_status},fin){
    request({
        method:'POST',
        json:true,
        headers:{
            "content-type":"application/json"
        },
        body:{
            access_token:access_token,
            mission_status:mission_status,
            data:data
        },
        url:'http://'+master_ip+':'+master_port+'/'+master_name+'/'+master_version+'/mission_report',
        timeout:request_timeout*1000
    },(err,res,body)=>{
        if(!err&&(res.statusCode>=200&&res.statusCode<300)){
            var err_msg='';
            var err_flag=0
            try{
                var content = body;
                console.log('Waiting for mission...');
                //console.log('=>mission report:\n'+JSON.stringify(body));

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
                        missionReport({data,master_ip,master_port,master_name,master_version,access_token,mission_status},fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','missionReport, '+err);
                    fin('err','missionReport, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        missionReport({data,master_ip,master_port,master_name,master_version,access_token,mission_status},fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','missionReport, '+res['statusCode']+', '+body['err']);
                    fin('err','missionReport, '+res['statusCode']+', '+body['err']);
                }
            }
       }
    });
}
function trackPost(graph_option,mission,post_id,fin){
    if(!graph_option){
        fin('err','trackPost, need graph_option paramater');
        return;
    }

    if(cnt_retry>retry_limit){
        writeLog('err','trackPost, retry over limit:'+cnt_retry);
        fin('err','trackPost, retry over limit:'+cnt_retry);
        cnt_retry=0;
        return;
    }
    console.log('Mission:\n'+JSON.stringify(mission,null,2));
    var site = mission['info']['site']+mission['info']['graph_version']+'/'+post_id;
    if(graph_option=='field1'){
        site+='?fields='+mission['info']['field1']+'&access_token='+mission['token']['graph_token']+'&limit='+mission['info']['limit'];
    }
    else if(graph_option=='field2'){
        site+='/comments?fields='+mission['info']['field2']+'&access_token='+mission['token']['graph_token']+'&limit='+mission['info']['limit'];
    }
    else{
        fin('err','trackPost, graph_option must be [field1/field2]');
        return;
    }
    console.log('\nRequest:'+site);
    //return;
    request({
        url:site,
        timeout:request_timeout*1000
    },(err,res,body)=>{
        if(!err&&(res.statusCode>=200&&res.statusCode<300)){
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
                    console.log('Retry [trackPost]:'+err.code);
                    setTimeout(function(){
                        cnt_retry++;
                        trackPost(graph_option,mission,post_id,fin);
                    },graph_timeout_again*1000);
                }
                else{
                    writeLog('err','trackPost, '+err+', site:'+site);
                    fin('err','trackPost, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    console.log('Retry [trackPost]:'+res.statusCode);
                    setTimeout(function(){
                        cnt_retry++;
                        trackPost(graph_option,mission,post_id,fin)
                    },graph_timeout_again*1000);
                }
                else{
                    writeLog('err','trackPost, '+res['statusCode']+', '+res['body']+', site:'+site);
                    fin('err','trackPost, '+res['statusCode']+', '+res['body']);
                }
            }
       }
    });
}
function transFieldName(option,field,type){
    if(option=='lowercase'){    
        return field.toLowerCase();
    }
    else{
        if(field=='id'){
            if(fieldMap[field]&&fieldMap[field][type]){
                return fieldMap[field][type];
            }
            else if(field==type){
                return fieldMap[field]['post'];
            }
        }
        else{
            if(fieldMap[field]){
                return fieldMap[field];
            }
            else{
                return field;
            }
        }
    }
}
/*欄位名稱在json格式時就已經轉換指定格式，故這邊只要按照原本的名稱轉成gais rec即可*/
function transGais(data){
    var i,j,k;
    var gaisrec='';
    data = data['data'];
    for(let l=0;l<data.length;l++){
        gaisrec+='@Gais_REC\n';
        var current_post_id;
        var sub_gaisrec='';
        var new_name,new_sub_name;
        var keys = Object.keys(data[l]);
        var sub_keys;
        for(i=0;i<keys.length;i++){
            if(keys[i]=='next_reactions'||keys[i]=='next_comments'||keys[i]=='next_sharedposts'){
                continue;
            }
            //new_name = mappingGaisFileds(keys[i],keys[i]);
            new_name = keys[i];
            if(keys[i]=='post_id'){
                current_post_id = data[l][keys[i]];
            }
            /*會有多種reaction種類，每個種類會分成不同子類別，放在reactions欄位之下*/
            if(keys[i]=='reactions'){
                sub_gaisrec='';
                gaisrec+='@'+new_name+':\n';
                sub_keys = Object.keys(data[l][keys[i]]);
                for(j=0;j<sub_keys.length;j++){
                    //console.log('['+j+'] ['+sub_keys[j]+']'+data[l][keys[i]][sub_keys[j]]);
                    if(typeof data[l][keys[i]][sub_keys[j]]==='undefined'||data[l][keys[i]][sub_keys[j]]==null){
                        data[l][keys[i]][sub_keys[j]]='';
                    }
                    /*將欄位名稱轉換成指定名稱*/
                    sub_keys[j] = sub_keys[j].toLowerCase();
                    sub_gaisrec+='\t'+sub_keys[j]+':'+data[l][keys[i]][sub_keys[j]]+' ';

                }
                gaisrec+=sub_gaisrec+'\n';
            }
            /*因為comments, sharedposts有陣列的議題(多個回應、分享)，需要將每個回應、分享轉換成子欄位*/
            else if(keys[i]=='comments'||keys[i]=='shared_posts'){
                gaisrec+='@'+new_name+':\n';
                for(j=0;j<data[l][keys[i]].length;j++){
                    sub_gaisrec='\t'+keys[i]+'_'+j;
                    sub_keys = Object.keys(data[l][keys[i]][j]);
                    for(k=0;k<sub_keys.length;k++){
                        //console.log('['+j+'] ['+sub_keys[k]+'] '+data[l][keys[i]][j][sub_keys[k]]);
                        if(typeof data[l][keys[i]][j][sub_keys[k]]==='undefined'||data[l][keys[i]][j][sub_keys[k]]==null){
                            data[l][keys[i]][j][sub_keys[k]]='';
                        }
                        else if(sub_keys[k]=='body'){
                            data[l][keys[i]][j][sub_keys[k]]=data[l][keys[i]][j][sub_keys[k]].replace(/\n/g,' ');
                            data[l][keys[i]][j][sub_keys[k]]=data[l][keys[i]][j][sub_keys[k]].replace(/\r/g,' ');
                        }
                        //new_sub_name = mappingGaisFileds(sub_keys[k],keys[i]);
                        new_sub_name = sub_keys[k];
                        sub_gaisrec+=' '+new_sub_name+':'+data[l][keys[i]][j][sub_keys[k]];
                    }
                    gaisrec+=sub_gaisrec+'\n';
                }
            }
            else if(keys[i]=='body'){
                data[l][keys[i]] = data[l][keys[i]].replace(/\n/g,' ');
                data[l][keys[i]] = data[l][keys[i]].replace(/\r/g,' ');
                gaisrec+='@'+new_name+':\n'
                gaisrec+=data[l][keys[i]]+'\n';
            }

            /*reactoins, comments, sharedposts以外的欄位*/
            else{
                if(typeof data[l][keys[i]]==='undefined'||data[l][keys[i]]==null){
                    data[keys[i]]='';
                }
                gaisrec+='@'+new_name+':'+data[l][keys[i]]+'\n';

            }
        }
    }


    return gaisrec;
}
/*
function mappingGaisFileds(field,type){
    if(field=='created_time'){
        field = 'post_time';
    }
    else if(field=='permalink_url'){
        field = 'url';   
    }
    else if(field=='link'){
        field = 'related_link';
    }
    else if(field=='id'){
        if(type=='comments'){
            field='comment_id';
        }
        else if(type=='sharedposts'){
            field='shared_posts_id';
        }
        else{
            field='post_id';
        }
    }
    else if(field=='from'){
            field='from_id';
    }

    else if(field=='attachments_url'){
        field = 'image_links';
    }
    else if(field=='message'){
        field='body';
    }
    else{//之前欄位格式為一個字大寫，現在一律小寫
        //field = wordToUpper(1,field);
        //field = wordToLower(1,field);
    }
    return field;
}
*/
//TODO:
//  1.有兩類的回傳格式，一個是有comments的 可以得到回應本身的資訊， 一個是沒有comments的 可以得到貼文本身的資訊 ，要分別處理
//  2.換成class可能比較好寫
//  3.要發兩次request分別得到兩種回傳格式，/comments? 稍微與之前不同
    //feed? API為attachments, 有media{image{height, src, width}}, target, description, title, type, url欄位
    //comments? API為attachment, 且只有type和url欄位，不需要有縮圖欄位，只需要知道回應的種類為貼圖或是連結...
function initContent([...fields],content){
    var i,j,k;
    /*--------------------------------------------*/
    /*將資訊依照欄位解析出後放進對應的物件和陣列裡*/
    /*--------------------------------------------*/

    /*先針對一般欄位作處理：comments, sharedposts, reactions以外的欄位都可用此段程序處理，若沒有該欄位的資訊則給''，避免出現undefined和null*/
    var keys = Object.keys(content);
    var sub_keys;
    var type;
    var temp;
    var data;

    this.reactions = {};
    this.comments = [];
    this.sharedposts = [];
    for(i=0;i<keys.length;i++){
        //console.log('['+i+'] '+keys[i]);   
        if(keys[i]!='comments'&&keys[i]!='sharedposts'&&keys[i]!='reactions'&&keys[i]!='attachment'){
            /*該欄位有其他子欄位，ex:from:{id:'',name:''}，在fields裡可以用fields欄位中是否有{}來判定該欄位是否有子欄位，ex: from{id,name}*/
            if(fields[0].indexOf(keys[i]+'{')!=-1){
                if(typeof content[keys[i]]!=='undefined'){
                    sub_keys = Object.keys(content[keys[i]]);
                    for(j=0;j<sub_keys.length;j++){
                        //console.log('['+i+'-'+j+'] '+sub_keys[j]+' data:'+content[keys[i]][sub_keys[j]]);
                        if(typeof content[keys[i]][sub_keys[j]]==='undefined'||content[keys[i]][sub_keys[j]]==null){
                            content[keys[i]][sub_keys[j]]='';
                        }
                        let map_field = transFieldName('',keys[i]+'_'+sub_keys[j],'');
                        this[map_field] = content[keys[i]][sub_keys[j]]
                        //this[keys[i]+'_'+sub_keys[j]] = content[keys[i]][sub_keys[j]];
                    }
                }

            }
            else{
                if(typeof content[keys[i]]==='undefined'||content[keys[i]]==null){
                    content[keys[i]]='';
                }
                else if(keys[i]=='created_time'){
                    content[keys[i]] = dateFormat(content[keys[i]],'yyyy/mm/dd HH:MM:ss');
                }
                let map_field = transFieldName('',keys[i],keys[i]);
                this[map_field] = content[keys[i]];
                //this[keys[i]] = content[keys[i]];
            }
        }
        //TODO:此處的attachment為貼文本身的附件，要將這段移到data crawler上，蒐集atatchhment資訊 
        /*attachments的回傳結果比較特別，會將結果包在'data' array=> data:[{field1:'',...}]*/
        else if(keys[i]=='attachment'){
            if(typeof content[keys[i]]['data']!=='undefined'&&content[keys[i]]['data']!=null&&content[keys[i]]['data']!=''){
                sub_keys = Object.keys(content[keys[i]]['data'][0]['media']['image']);
                for(j=0;j<sub_keys.length;j++){
                    //console.log('['+i+'-'+j+'] '+sub_keys[j]+' data:'+content[keys[i]]['data'][0]['media']['image'][sub_keys[j]]);
                    if(typeof content[keys[i]]['data'][0]['media']['image'][sub_keys[j]]==='undefined'||content[keys[i]]['data'][0]['media']['image'][sub_keys[j]]==null){
                        content[keys[i]]['data'][0]['media']['image'][sub_keys[j]]='';
                    }
                    let map_field = transFieldName('',keys[i]+'_'+sub_keys[j],keys[i]);
                    this[map_field] = content[keys[i]]['data'][0]['media']['image'][sub_keys[j]];
                }
            }
        }
        else{
            /*解析出reactions中的各個使用者反應和數量，因為要將每個reaction的數量加總，所以會跟一般欄位分開*/
            if(keys[i]=='reactions'){
                if(typeof content['reactions']!=='undefined'&&content['reactions']!=''&&content['reactions']!=null){
                    for(j=0;j<content['reactions']['data'].length;j++){
                        type = content['reactions']['data'][j]['type'];
                        let map_field = transFieldName('lowercase',type,keys[i]);
                        if(typeof this.reactions[map_field]==='undefined'||this.reactions[map_field]==null){
                            this.reactions[map_field]=1;
                        }
                        else{
                            this.reactions[map_field]++;
                        }
                    }
                }
            }
            else if(keys[i]=='comments'){
                /*將有array的欄位資訊依序記錄：commemts, sharepedposts*/
                /*將當前頁面的comments詳細資料塞進array*/
                if(typeof content['comments']!=='undefined'&&content['comments']!=''&&content['comments']!=null){
                    for(j=0;j<content['comments']['data'].length;j++){
                        temp = new Object();
                        data = content['comments']['data'];
                        sub_keys = Object.keys(content['comments']['data'][j]);
                        /*檢查有無該欄位，如果沒有則還是會保留這個欄位，並且給予''值*/
                        for(k=0;k<sub_keys.length;k++){
                            let map_field = transFieldName('',sub_keys[k],'comment');
                            if(typeof data[j][sub_keys[k]]==='undefined'||data[j][sub_keys[k]]==null){
                                data[j][sub_keys[k]]='';
                            }
                            if(sub_keys[k]=='created_time'){
                                data[j][sub_keys[k]] = dateFormat(data[j][sub_keys[k]],'yyyy/mm/dd HH:MM:ss');
                                temp[map_field] = data[j][sub_keys[k]];
                            }
                            else if(sub_keys[k]=='from'||sub_keys[k]=='attachment'){//拿取回文附件的type, url兩個欄位(已寫在mission設定檔裡)
                                let sub_sub_keys = Object.keys(data[j][sub_keys[k]]);
                                for(let l=0;l<sub_sub_keys.length;l++){
                                    let map_field = transFieldName('',sub_keys[k]+'_'+sub_sub_keys[l],'comment');
                                    temp[map_field] = data[j][sub_keys[k]][sub_sub_keys[l]];
                                    //temp[sub_keys[k]+'_'+sub_sub_keys[l]] = data[j][sub_keys[k]][sub_sub_keys[l]];
                                }
                            }
                            else{
                                temp[map_field] = data[j][sub_keys[k]];
                            }
                            /*
                            if(typeof data[j][sub_keys[k]]==='undefined'||data[j][sub_keys[k]]==null){
                                data[j][sub_keys[k]]='';
                            }
                            if(sub_keys[k]=='created_time'){
                                data[j][sub_keys[k]] = dateFormat(data[j][sub_keys[k]],'yyyy/mm/dd HH:MM:ss');
                                temp[sub_keys[k]] = data[j][sub_keys[k]];
                            }
                            else if(sub_keys[k]=='from'||sub_keys[k]=='attachment'){//拿取回文附件的type, url兩個欄位(已寫在mission設定檔裡)
                                let sub_sub_keys = Object.keys(data[j][sub_keys[k]]);
                                for(let l=0;l<sub_sub_keys.length;l++){
                                    temp[sub_keys[k]+'_'+sub_sub_keys[l]] = data[j][sub_keys[k]][sub_sub_keys[l]];
                                }
                            }
                            else{
                                temp[sub_keys[k]] = data[j][sub_keys[k]];
                            }
                            */
                        }
                        this.comments.push(temp);
                        //this.comments.push({id:data[i].id,created_time:dateFormat(data[i].created_time,'yyyy/mm/dd HH:MM:ss'),like_count:data[i].like_count,message:data[i].message});
                    }
                }
            }
            else if(keys[i]=='sharedposts'){
                /*將當前頁面的sharedposts詳細資料塞進array*/
                if(typeof content['sharedposts']!=='undefined'&&content['sharedposts']!=''){
                    for(j=0;j<content['sharedposts']['data'].length;j++){
                        temp = new Object();
                        data = content['sharedposts']['data'];
                        sub_keys = Object.keys(content['sharedposts']['data'][j]);
                        /*檢查有無該欄位，如果沒有則給''*/
                        for(k=0;k<sub_keys.length;k++){
                            let map_field = transFieldName('',sub_keys[k],'sharedpost');
                            
                            if(typeof data[j][sub_keys[k]]==='undefined'||data[j][sub_keys[k]]==null){
                                data[j][sub_keys[k]]='';
                            }
                            if(sub_keys[k]=='created_time'){
                                data[j][sub_keys[k]] = dateFormat(data[j][sub_keys[k]],'yyyy/mm/dd HH:MM:ss');
                                temp[map_field] = data[j][sub_keys[k]];
                            }
                            else if(sub_keys[k]=='from'){
                                let sub_sub_keys = Object.keys(data[j][sub_keys[k]]);
                                for(let l=0;l<sub_sub_keys.length;l++){
                                    let map_field = transFieldName('',sub_keys[k]+'_'+sub_sub_keys[l],'sharedpost');
                                    temp[map_field] = data[j][sub_keys[k]][sub_sub_keys[l]];
                                    //temp[sub_keys[k]+'_'+sub_sub_keys[l]] = data[j][sub_keys[k]][sub_sub_keys[l]];
                                }
                            }
                            else{
                                temp[map_field] = data[j][sub_keys[k]];
                            }
                            /*
                            if(typeof data[j][sub_keys[k]]==='undefined'||data[j][sub_keys[k]]==null){
                                data[j][sub_keys[k]]='';
                            }
                            if(sub_keys[k]=='created_time'){
                                data[j][sub_keys[k]] = dateFormat(data[j][sub_keys[k]],'yyyy/mm/dd HH:MM:ss');
                                temp[sub_keys[k]] = data[j][sub_keys[k]];
                            }
                            else if(sub_keys[k]=='from'){
                                let sub_sub_keys = Object.keys(data[j][sub_keys[k]]);
                                for(let l=0;l<sub_sub_keys.length;l++){
                                    temp[sub_keys[k]+'_'+sub_sub_keys[l]] = data[j][sub_keys[k]][sub_sub_keys[l]];
                                }
                            }
                            else{
                                temp[sub_keys[k]] = data[j][sub_keys[k]];
                            }
                            */
                        }
                        this.sharedposts.push(temp);
                        //this.sharedposts.push({id:data[i].id,created_time:dateFormat(data[i].created_time,'yyyy/mm/dd HH:MM:ss'),likes:data[i].like_count,message:data[i].message});
                    }
                }

            }
        }
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /*逐步確認reactions, comments, sharedposts的下一頁狀況，若無該欄位或是該欄位的paging子欄位的沒有next則代表無下一頁*/
    /*----------------------------------------------------------------------------------------------------------------*/
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
    return this;
}
function parseComment(fields,content){
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

        var i,j,k;
        var data,temp;
        var keys,sub_keys;
        for(i=0;i<content['data'].length;i++){
            data = content['data'];
            temp = new Object();
            keys = Object.keys(content['data'][i]);
            /*檢查有無該欄位，如果沒有則給''*/
            for(j=0;j<keys.length;j++){
                let map_field = transFieldName('',keys[j],'comment');
                //comments裡若有圖片或其他附件，目前無法使用attachment{url,...}的方式抓到，故如果回應是一張圖的話，message為空
                if(typeof data[i][keys[j]]==='undefined'||data[i][keys[j]]==null){
                    data[i][keys[j]]='';
                }
                if(keys[j]=='created_time'){
                    data[i][keys[j]] = dateFormat(data[i][keys[j]],'yyyy/mm/dd HH:MM:ss');
                    temp[map_field] = data[i][keys[j]];
                }
                else if(keys[j]=='from'||keys[j]=='attachment'){//只拿取回文附件的type, url兩個欄位(已寫在mission設定檔裡)
                    let sub_sub_keys = Object.keys(data[i][keys[j]]);
                    for(let l=0;l<sub_sub_keys.length;l++){
                        let map_field = transFieldName('',keys[j]+'_'+sub_sub_keys[l],'comment');
                        temp[map_field] = data[i][keys[j]][sub_sub_keys[l]];
                    }
                }
                else{
                    temp[map_field] = data[i][keys[j]];
                }
                /*
                if(typeof data[i][keys[j]]==='undefined'||data[i][keys[j]]==null){
                    data[i][keys[j]]='';
                }
                if(keys[j]=='created_time'){
                    data[i][keys[j]] = dateFormat(data[i][keys[j]],'yyyy/mm/dd HH:MM:ss');
                    temp[keys[j]] = data[i][keys[j]];
                }
                else if(keys[j]=='from'||keys[j]=='attachment'){//只拿取回文附件的type, url兩個欄位(已寫在mission設定檔裡)
                    let sub_sub_keys = Object.keys(data[i][keys[j]]);
                    for(let l=0;l<sub_sub_keys.length;l++){
                        temp[keys[j]+'_'+sub_sub_keys[l]] = data[i][keys[j]][sub_sub_keys[l]];
                    }
                }
                else{
                    temp[keys[j]] = data[i][keys[j]];
                }
                */

            }
            this.comments.push(temp);
            //this.comments.push({id:data[i].id,created_time:dateFormat(data[i].created_time,'yyyy/mm/dd HH:MM:ss'),likes:data[i].like_count,message:data[i].message});
        }
        return this;
    }
}
function parseSharedpost(fields,content){
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
        var i,j;
        var data,temp;
        var keys;
        for(i=0;i<content['data'].length;i++){
            data = content['data'];
            temp = new Object();
            keys = Object.keys(content['data'][i]);
            /*檢查有無該欄位，如果沒有則給''*/
            for(j=0;j<keys.length;j++){
                let map_field = transFieldName('',keys[j],'sharedpost');
                if(typeof data[i][keys[j]]==='undefined'||data[i][keys[j]]==null){
                    data[i][keys[j]]='';
                }
                if(keys[j]=='created_time'){
                    data[i][keys[j]] = dateFormat(data[i][keys[j]],'yyyy/mm/dd HH:MM:ss');
                    temp[map_field] = data[i][keys[j]];
                }
                else if(keys[j]=='from'){
                    let sub_sub_keys = Object.keys(data[i][keys[j]]);
                    for(let l=0;l<sub_sub_keys.length;l++){
                        let map_field = transFieldName('',keys[j]+'_'+sub_sub_keys[l],'sharedpost');
                        temp[map_field] = data[i][keys[j]][sub_sub_keys[l]];
                    }
                }
                else{
                    temp[map_field] = data[i][keys[j]];
                }
                /*
                if(keys[j]=='created_time'){
                    data[i][keys[j]] = dateFormat(data[i][keys[j]],'yyyy/mm/dd HH:MM:ss');
                    temp[keys[j]] = data[i][keys[j]];
                }
                else if(keys[j]=='from'){
                    let sub_sub_keys = Object.keys(data[i][keys[j]]);
                    for(let l=0;l<sub_sub_keys.length;l++){
                        temp[keys[j]+'_'+sub_sub_keys[l]] = data[i][keys[j]][sub_sub_keys[l]];
                    }
                }
                else{
                    temp[keys[j]] = data[i][keys[j]];
                }
                */
            }
            this.sharedposts.push(temp);
            //this.sharedposts.push({id:data[i].id,created_time:dateFormat(data[i].created_time,'yyyy/mm/dd HH:MM:ss'),likes:data[i].like_count,message:data[i].message});
        }
        return this;
    }
}
function parseReaction(fields,content){
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
            let map_field = transFieldName('lowercase',type,'');
            if(typeof this.reactions[map_field]==='undefined'||this.reactions[map_field]==null){
                this.reactions[map_field]=1;
            }
            else{
                this.reactions[map_field]++;
            }

        }
        return this;
    }
}
function fetchNextPage(mission,site,fin){
    request({
        url:site,
        timeout:request_timeout*1000
    },(err,res,body)=>{
        if(!err&&(res.statusCode>=200&&res.statusCode<300)){
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
                console.log('err:'+err);
                if(err.code.indexOf('TIME')!=-1||err.code.indexOf('ENOT')!=-1||err.code.indexOf('ECONN')!=-1||err.code.indexOf('REACH')!=-1){
                    setTimeout(function(){
                        fetchNextPage(mission,site,fin);
                    },graph_timeout_again*1000);
                }
                else{
                    writeLog('err','fetchNextPage, '+err);
                    fin('err','fetchNextPage, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        fetchNextPage(mission,site,fin);
                    },graph_timeout_again*1000);
                }
                else{
                    writeLog('err','fetchNextPage, '+res['statusCode']+', '+res['body']);
                    fin('err','fetchNextPage, '+res['statusCode']+', '+res['body']);
                }
            }
        }
    });

}
function sendResponse(res,type,status_code,msg){
    var result = new Object();
    if(type=='token_err'){
        result['data']='';
        result['err']=crawler_setting['err_msg']['token_err'];
        res.status(403).send(result);
    }
    else if(type=='process_err'){
        result['data']='';
        result['err']=crawler_setting['err_msg']['process_err']+'. Reason:'+msg;
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

function writeRec(type,msg){
    var now = new Date();
    var date = dateFormat(now,'yyyymmdd');
    var filename=crawler_setting['rec_dir']+'/';
    /*要將欄位名稱改過，以及字首轉大寫*/
    if(type=='gais'){
        filename += date+crawler_setting['gaisrec_filename'];
        msg = transGais(msg);
    }
    /*照原本格式儲存資料*/
    else{
        filename += date+crawler_setting['jsonrec_filename'];
        msg = JSON.stringify(msg,null,3);
        msg+='\n';
    }
    fs.appendFile(filename,msg,(err)=>{
        if(err){
            console.log('[writeLog] Error:'+err);
        }
    });
}
function temp(){
    /*解析出reactions中的各個使用者反應和數量，因為要將每個reaction的數量加總，所以會跟一般欄位分開*/
    if(typeof content['reactions']!=='undefined'&&content['reactions']!=''&&content['reactions']!=null){
        var type,cnt,i;
        for(i=0;i<content['reactions']['data'].length;i++){
            type = content['reactions']['data'][i]['type'];
            if(typeof this.reactions[type]==='undefined'||this.reactions[type]==null){
                this.reactions[type]=1;
            }
            else{
                this.reactions[type]++;
            }
        }
    }

    /*將有array的欄位資訊依序記錄：commemts, sharepedposts*/
    /*將當前頁面的comments詳細資料塞進array*/
    if(typeof content['comments']!=='undefined'&&content['comments']!=''&&content['comments']!=null){
        var data;
        for(i=0;i<content['comments']['data'].length;i++){
            temp = new Object();
            data = content['comments']['data'];
            keys = Object.keys(content['comments']['data'][i]);
            /*檢查有無該欄位，如果沒有則還是會保留這個欄位，並且給予''值*/
            for(j=0;j<keys.length;j++){
                if(typeof data[i][keys[j]]==='undefined'||data[i][keys[j]]==null){
                    data[i][keys[j]]='';
                }
                if(keys[j]=='created_time'){
                    data[i][keys[j]] = dateFormat(data[i][keys[j]],'yyyy/mm/dd HH:MM:ss');
                    temp[keys[j]] = data[i][keys[j]];
                }
                else{
                    temp[keys[j]] = data[i][keys[j]];
                }
            }
            this.comments.push(temp);
        }
    }
    /*將當前頁面的sharedposts詳細資料塞進array*/
    if(typeof content['sharedposts']!=='undefined'&&content['sharedposts']!=''){
        for(i=0;i<content['sharedposts']['data'].length;i++){
            temp = new Object();
            data = content['sharedposts']['data'];
            keys = Object.keys(content['sharedposts']['data'][i]);
            /*檢查有無該欄位，如果沒有則給''*/
            for(j=0;j<keys.length;j++){
                if(typeof data[i][keys[j]]==='undefined'||data[i][keys[j]]==null){
                    data[i][keys[j]]='';
                }
                if(keys[j]=='created_time'){
                    data[i][keys[j]] = dateFormat(data[i][keys[j]],'yyyy/mm/dd HH:MM:ss');
                    temp[keys[j]] = data[i][keys[j]];
                }
                else{
                    temp[keys[j]] = data[i][keys[j]];
                }
            }
            this.sharedposts.push(temp);
        }
    }
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
function wordToLower(index,str){
    var cnt=1;
    var map = Array.prototype.map;
    return map.call(str,function(x){
        if(cnt==index){
            x = x.toLowerCase();
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
exports.applyCrawler=applyCrawler;
exports.missionReport=missionReport;
exports.trackPost=trackPost;
exports.uploadTrackPostId=uploadTrackPostId;
exports.uploadTrackPostData=uploadTrackPostData;
exports.listTrack=listTrack;
exports.initContent=initContent;
exports.fetchNextPage=fetchNextPage;
exports.parseComment=parseComment;
exports.parseSharedpost=parseSharedpost;
exports.parseReaction=parseReaction;
exports.transGais=transGais;
exports.updateFieldMap=updateFieldMap;

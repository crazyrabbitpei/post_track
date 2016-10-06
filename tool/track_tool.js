'use strict'
var request = require('request');
var fs = require('fs');
var dateFormat = require('dateformat');
var HashMap = require('hashmap');
var querystring = require('querystring');
var reactions;
var crawler_setting = JSON.parse(fs.readFileSync('./service/crawler_setting.json'));

//function applyCrawler(crawler_port,ip,port,master_name,master_version,token,timeout,retryt,fin){
function applyCrawler({crawler_port,master_ip,master_port,master_name,master_version,invite_token,request_timeout,master_timeout_again},fin){
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
                if(err.code.indexOf('TIME')!=-1||err.code.indexOf('ENOT')!=-1||err.code.indexOf('ECONN')!=-1||err.code.indexOf('REACH')!=-1){
                    setTimeout(function(){
                        applyCrawler({crawler_port,master_ip,master_port,master_name,master_version,invite_token,request_timeout,master_timeout_again},fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','applyCrawler, '+err);
                    fin('err','applyCrawler, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        applyCrawler({crawler_port,master_ip,master_port,master_name,master_version,invite_token,request_timeout,master_timeout_again},fin);
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
//function uploadTrackPost(ip,port,master_name,master_version,token,data,timeout,retryt,fin){
function uploadTrackPost({master_ip,master_port,master_name,master_version,access_token,data,request_timeout,master_timeout_again},fin){
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
                        uploadTrackPost({master_ip,master_port,master_name,master_version,access_token,data,request_timeout,master_timeout_again},fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','uploadTrackPost, '+err);
                    fin('err','uploadTrackPost, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        uploadTrackPost({master_ip,master_port,master_name,master_version,access_token,data,request_timeout,master_timeout_again},fin);
                    },master_timeout_again*1000);
                }
                else{
                    writeLog('err','uploadTrackPost, '+res['statusCode']+', '+body['err']);
                    fin('err','uploadTrackPost, '+res['statusCode']+', '+body['err']);
                }
            }
       }
    });
}
//function listTrack(ip,port,master_name,master_version,token,date,timeout,retryt,fin){
function listTrack({master_ip,master_port,master_name,master_version,access_token,request_timeout,master_timeout_again},fin){
    console.log('http://'+master_ip+':'+master_port+'/'+master_name+'/'+master_version+'/status')
    request({
        method:'GET',
        json:true,
        body:{
            access_token:access_token
        },
        url:'http://'+master_ip+':'+master_port+'/'+master_name+'/'+master_version+'/status',
        timeout:master_timeout_again*1000
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
function missionReport({data,master_ip,master_port,master_name,master_version,access_token,mission_status,request_timeout,master_timeout_again},fin){
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
                        missionReport(ip,port,master_name,master_version,token,timeout,retryt,fin)
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
                        missionReport(ip,port,master_name,master_version,token,timeout,retryt,fin)
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
function trackPost(timeout,mission,post_id,fin){
    console.log('Mission:\n'+JSON.stringify(mission,null,2));
    var site = mission['info']['site']+mission['info']['graph_version']+'/'+post_id+'?fields='+mission['info']['fields']+'&access_token='+mission['token']['graph_token']+'&limit='+mission['info']['limit'];
    //console.log('\nRequest:'+site);
    //return;
    request({
        url:site,
        timeout:timeout*1000
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
                        trackPost(site,timeout);
                    },mission['info']['graph_timeout_again']*1000);
                }
                else{
                    writeLog('err','trackPost, '+err);
                    fin('err','trackPost, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    console.log('Retry [trackPost]:'+res.statusCode);
                    setTimeout(function(){
                        trackPost(site,timeout);
                    },mission['info']['graph_timeout_again']*1000);
                }
                else{
                    writeLog('err','trackPost, '+res['statusCode']+', '+res['body']);
                    fin('err','trackPost, '+res['statusCode']+', '+res['body']);
                }
            }
       }
    });
}
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

function initContent(fields,content){
    var i,j,k;

    /*若此篇文章無人分享，則不會有shares欄位，shares參數=0*/
    /*
    if(typeof content['shares']!=='undefined'&&content['shares']!=''){
        this.shares = content['shares']['count'];
    }
    else{
        this.shares = 0;
    }
    */


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
        if(keys[i]!='comments'&&keys[i]!='sharedposts'&&keys[i]!='reactions'&&keys[i]!='attachments'){
            /*該欄位有其他子欄位，ex:from:{id:'',name:''}，在fields裡可以用fields欄位中是否有{}來判定該欄位是否有子欄位，ex: from{id,name}*/
            if(fields.indexOf(keys[i]+'{')!=-1){
                if(typeof content[keys[i]]!=='undefined'){
                    sub_keys = Object.keys(content[keys[i]]);
                    for(j=0;j<sub_keys.length;j++){
                        //console.log('['+i+'-'+j+'] '+sub_keys[j]+' data:'+content[keys[i]][sub_keys[j]]);
                        if(typeof content[keys[i]][sub_keys[j]]==='undefined'||content[keys[i]][sub_keys[j]]==null){
                            content[keys[i]][sub_keys[j]]='';
                        }
                        this[keys[i]+'_'+sub_keys[j]] = content[keys[i]][sub_keys[j]];
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
                this[keys[i]] = content[keys[i]];
            }
        }
        /*attachments的回傳結果比較特別，會將結果包在'data' array=> data:[{field1:'',...}]*/
        else if(keys[i]=='attachments'){
            if(typeof content[keys[i]]['data']!=='undefined'&&content[keys[i]]['data']!=null&&content[keys[i]]['data']!=''){
                sub_keys = Object.keys(content[keys[i]]['data'][0]['media']['image']);
                for(j=0;j<sub_keys.length;j++){
                    //console.log('['+i+'-'+j+'] '+sub_keys[j]+' data:'+content[keys[i]]['data'][0]['media']['image'][sub_keys[j]]);
                    if(typeof content[keys[i]]['data'][0]['media']['image'][sub_keys[j]]==='undefined'||content[keys[i]]['data'][0]['media']['image'][sub_keys[j]]==null){
                        content[keys[i]]['data'][0]['media']['image'][sub_keys[j]]='';
                    }
                    this[keys[i]+'_'+sub_keys[j]] = content[keys[i]]['data'][0]['media']['image'][sub_keys[j]];
                }
            }
        }
        else{
            /*解析出reactions中的各個使用者反應和數量，因為要將每個reaction的數量加總，所以會跟一般欄位分開*/
            if(keys[i]=='reactions'){
                if(typeof content['reactions']!=='undefined'&&content['reactions']!=''&&content['reactions']!=null){
                    for(j=0;j<content['reactions']['data'].length;j++){
                        type = content['reactions']['data'][j]['type'];
                        if(typeof this.reactions[type]==='undefined'||this.reactions[type]==null){
                            this.reactions[type]=1;
                        }
                        else{
                            this.reactions[type]++;
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
                            if(typeof data[j][sub_keys[k]]==='undefined'||data[j][sub_keys[k]]==null){
                                data[j][sub_keys[k]]='';
                            }
                            if(sub_keys[k]=='created_time'){
                                data[j][sub_keys[k]] = dateFormat(data[j][sub_keys[k]],'yyyy/mm/dd HH:MM:ss');
                                temp[sub_keys[k]] = data[j][sub_keys[k]];
                            }
                            else if(sub_keys[k]=='from'){
                                temp[sub_keys[k]] = data[j][sub_keys[k]]['id'];
                            }
                            else{
                                temp[sub_keys[k]] = data[j][sub_keys[k]];
                            }
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
                            if(typeof data[j][sub_keys[k]]==='undefined'||data[j][sub_keys[k]]==null){
                                data[j][sub_keys[k]]='';
                            }
                            if(sub_keys[k]=='created_time'){
                                data[j][sub_keys[k]] = dateFormat(data[j][sub_keys[k]],'yyyy/mm/dd HH:MM:ss');
                                temp[sub_keys[k]] = data[j][sub_keys[k]];
                            }
                            else if(sub_keys[k]=='from'){
                                temp[sub_keys[k]] = data[j][sub_keys[k]]['id'];
                            }
                            else{
                                temp[sub_keys[k]] = data[j][sub_keys[k]];
                            }
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
                //comments裡若有圖片或其他附件，目前無法使用attachment{url,...}的方式抓到，故如果回應是一張圖的話，message為空
                /*
                if(fields.indexOf(keys[j]+'{')!=-1){
                    if(typeof content['data'][keys[j]]!=='undefined'){
                        sub_keys = Object.keys(content['data'][keys[j]]);
                        for(k=0;k<sub_keys.length;k++){
                            temp[keys[j]+'_'+sub_keys[k]] = data[i][keys[j]][sub_keys[k]];
                        }
                    }

                }
                */
                //else{
                    if(typeof data[i][keys[j]]==='undefined'||data[i][keys[j]]==null){
                        data[i][keys[j]]='';
                    }
                    if(keys[j]=='created_time'){
                        data[i][keys[j]] = dateFormat(data[i][keys[j]],'yyyy/mm/dd HH:MM:ss');
                        temp[keys[j]] = data[i][keys[j]];
                    }
                    else if(keys[j]=='from'){
                        temp[keys[j]] = data[i][keys[j]]['id'];
                    }
                    else{
                        temp[keys[j]] = data[i][keys[j]];
                    }
                //}

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
                if(typeof data[i][keys[j]]==='undefined'||data[i][keys[j]]==null){
                    data[i][keys[j]]='';
                }
                if(keys[j]=='created_time'){
                    data[i][keys[j]] = dateFormat(data[i][keys[j]],'yyyy/mm/dd HH:MM:ss');
                    temp[keys[j]] = data[i][keys[j]];
                }
                else if(keys[j]=='from'){
                    temp[keys[j]] = data[i][keys[j]]['id'];
                }
                else{
                    temp[keys[j]] = data[i][keys[j]];
                }
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
            if(typeof this.reactions[type]==='undefined'||this.reactions[type]==null){
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
                        fetchNextPage(timeout,mission,site,fin);
                    },mission['info']['graph_timeout_again']*1000);
                }
                else{
                    writeLog('err','fetchNextPage, '+err);
                    fin('err','fetchNextPage, '+err);
                }
            }
            else{
                if(res.statusCode>=500&&res.statusCode<600){
                    setTimeout(function(){
                        fetchNextPage(timeout,mission,site,fin);
                    },mission['info']['graph_timeout_again']*1000);
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

function writeRec(type,track_id,msg){
    var now = new Date();
    var date = dateFormat(now,'yyyymmdd');
    var filename=crawler_setting['rec_dir']+'/';
    /*要將欄位名稱改過，以及字首轉大寫*/
    if(type=='gais'){
        filename += track_id+'.'+date+crawler_setting['gaisrec_filename'];
        msg = transGais(msg);
    }
    /*照原本格式儲存資料*/
    else{
        filename += track_id+'.'+date+crawler_setting['jsonrec_filename'];
        msg = JSON.stringify(msg,null,3);
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
exports.sendResponse=sendResponse;
exports.writeLog=writeLog;
exports.writeRec=writeRec;
exports.applyCrawler=applyCrawler;
exports.missionReport=missionReport;
exports.trackPost=trackPost;
exports.uploadTrackPost=uploadTrackPost;
exports.listTrack=listTrack;
exports.initContent=initContent;
exports.fetchNextPage=fetchNextPage;
exports.parseComment=parseComment;
exports.parseSharedpost=parseSharedpost;
exports.parseReaction=parseReaction;
exports.transGais=transGais;

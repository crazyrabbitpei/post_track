'use strict'
var express = require('express');
var bodyParser = require('body-parser');
var app = express();
var http = require('http');
var server = http.createServer(app);
var fs = require('graceful-fs');
var dateFormat = require('dateformat');

const master_tool = require('./tool/master_tool.js');
const center_tool = require('./tool/center_tool.js');
const dataCenter_setting = JSON.parse(fs.readFileSync('./service/dataCenter_setting.json'));
const center_port = dataCenter_setting['center_port'];
const center_ip = dataCenter_setting['center_ip'];
const center_name = dataCenter_setting['center_name'];
const center_version = dataCenter_setting['center_version'];
const center_token = dataCenter_setting['center_token'];
const data_dir = dataCenter_setting['data_dir']['root'];
const json_dir = dataCenter_setting['data_dir']['type']['json'];
const gais_dir = dataCenter_setting['data_dir']['type']['gais'];
const log_dir = dataCenter_setting['log_dir']['root'];

center_tool.initDir(dataCenter_setting['data_dir']);
center_tool.initDir(dataCenter_setting['log_dir']);

var track = new master_tool.DataCenter();

var center = express.Router();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true   }));

server.listen(center_port,center_ip,function(){
                console.log("[Server start] ["+new Date()+"] http work at "+center_ip+":"+center_port);
                app.use('/'+center_name+'/'+center_version,center);
});

center.use(function(req,res,next){
    var access_token = req.body['access_token'];
    if(!access_token){
        access_token = req.query['access_token'];
    }
    if(!track.getCrawler(access_token)&&access_token!=dataCenter_setting['control_token']){
        console.log('err token ['+access_token+']');
        sendResponse(res,'token_err','','');
        return;
    }
    console.log('Pass token:'+access_token);
    next();
});

center.get('/testConnect',function(req,res){
    var result = 'Success connect to Data Center!';
    sendResponse(res,'ok',200,result);
});
/*接收從tarck master那傳來的tarck crawler token*/
center.post('/apply/:access_token',function(req,res){
    var access_token = req.params.access_token;
    var crawler_info = req.body['data'];
    var ip = crawler_info['ip'];
    var port  = crawler_info['port'];

    var result = new Object();

    track.insertCrawler(access_token,{ip:ip,port:port});

    sendResponse(res,'ok',200,result);
    console.log('Data center:');
    console.dir(track.listCrawlers(),{colors:true});

});

center.post('/data/:datatype(json|gais)',function(req,res){
    var datatype = req.params.datatype;
    var now=dateFormat(new Date(),'yyyymmdd');
    var size=0;
    var temp='',err_flag=0,err_msg='';
    var dir=data_dir+'/';
    dir+=datatype+'/'+now+'.'+datatype;
    /*
    if(datatype=='json'){
        dir+=datatype+'/'+now+'.'+datatype;
    }
    else if(datatype=='gais'){

    }
    */
    console.log('Start reading data...');
    req.on('data', function(data){
        temp+=data;
        size+=Buffer.byteLength(data);
    });
    req.on('end', function(data){
        try{
            if(datatype=='json'){
                var test = JSON.parse(temp);
            }
        }
        catch(e){
            err_flag=1;
            err_msg=e;
            console.log('Parse upload data error:'+e);
        }
        finally{
            if(!err_flag){
                fs.appendFile(dir,temp,function(err){
                    if(err){
                        console.log('err:'+err);
                        writeLog('err','From '+req.ip+', upload fail:'+err);
                        sendResponse(res,'false',200,'Upload fail:'+err);
                    }
                    else{
                        if(datatype=='json'){
                            fs.appendFile(dir,'\n','utf8',()=>{});
                        }
                        console.log('Uplaod success!');
                        writeLog('process','From '+req.ip+', upload success:'+size);
                        sendResponse(res,'ok',200,'Upload success:'+size);
                    }
                });
            }
            else{
                console.log('Upload fail.');
                writeLog('err','From '+req.ip+', upload fail:'+err_msg);
                sendResponse(res,'false',200,'Upload fail.');
            }
        }

    });
});
function sendResponse(res,type,status_code,msg){
    var result = new Object();
    result['status']=type;
    if(type=='token_err'){
        result['data']='';
        result['err']=dataCenter_setting['err_msg']['token_err'];
        res.status(403).send(result);
    }
    else if(type=='process_err'){
        result['data']='';
        result['err']=dataCenter_setting['err_msg']['process_err']+', '+msg;
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
    var filename=log_dir+'/';
    if(type=='err'){
        filename += 'err/'+date+'.'+type;
    }
    else if(type=='process'){
        filename += 'process/'+date+'.'+type;
    }
    else{
        filename += 'other/'+date+'.'+type;
    }

    fs.appendFile(filename,'['+now+'] Type:'+type+' Message:'+msg+'\n',(err)=>{
        if(err){
            console.log('Master [writeLog] Error:'+err);
        }
    });
}

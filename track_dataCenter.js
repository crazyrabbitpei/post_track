'use strict'
var express = require('express');
var bodyParser = require('body-parser');
var app = express();
var http = require('http');
var server = http.createServer(app);
var fs = require('graceful-fs');

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
    next();
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
    console.log('Data center:\n'+JSON.stringify(track.listCrawlers(),null,3));

});

center.post('/data/:datatype(json|gais)',function(req,res){
    var datatype = req.params.datatype;a
    var now=dateFormat(new Date(),'yyyymmdd');
    var dir;
    if(datatype=='json'){
        dir=data_dir+'/'+json_dir+'/'+now;
    }
    else if(datatype=='gais'){
        dir=data_dir+'/'+gais_dir+'/'+now;
    }

    req.on('data', function(data){
        size+=Buffer.byteLength(data);
        fs.appendFile(dir,data,'utf8',function(err){
            if(err){
                console.log('err:'+err);
                writeLog('err','from '+req.ip+', upload fail:'+err);
                sendResponse(res,200,action,'false','','upload fail:'+err);
            }
        });

    });
    req.on('end', function(data){
        /*recording ip and datasize*/
        writeLog('process','from '+req.ip+', upload success:'+size);
        sendResponse(res,200,action,'ok','','');
    });
});

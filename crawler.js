'use strict'
var start = require('./index.js');
var track_tool = require('./tool/track_tool.js');
var express = require('express');
var fs = require('fs');
var crawler = express.Router();

var crawler_setting = JSON.parse(fs.readFileSync('./service/crawler_setting.json'));

crawler.post('/mission',function(req,res){
    var control_token = req.body['control_token'];
    //console.log('Crawler receive control_token:'+control_token);
    //console.log('Crawler receive mission:\n'+JSON.stringify(req.body['mission']))

    var mission = req.body['mission'];
    if(control_token!=crawler_setting['control_token']){
        track_tool.sendResponse(res,'token_err','','');
    }
    else{
        /*
        console.log('Get from master:');
        console.dir(mission,{colors:true});
        */
        track_tool.sendResponse(res,'ok',200,'Roger!');
        var current_post_id;
        if((current_post_id = mission['track_posts'].shift())){
            console.log('Start track ['+current_post_id+']!');
            console.log('Get new track ids:'+mission['track_posts'].length);
            for(let i=0;i<mission['track_posts'].length;i++){
                start.addTrackId(mission['track_posts'][i]);
            }
            start.updateMission(mission);
            start.start(current_post_id);
            return;
        }
    }
});
module.exports = crawler;

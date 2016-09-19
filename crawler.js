'use strict'
var start = require('./index.js');
var track_tool = require('./tool/track_tool.js');
var express = require('express');
var fs = require('fs');
var crawler = express.Router();

var crawler_setting = JSON.parse(fs.readFileSync('./service/crawler_setting.json'));

crawler.post('/mission',function(req,res){
    var control_token = req.body['control_token'];
    var mission = req.body['mission'];
    if(control_token!=crawler_setting['control_token']){
        track_tool.sendResponse(res,'token_err','','');
    }
    else{
        console.log('get from master:'+JSON.stringify(mission));
        track_tool.sendResponse(res,'ok',200,'Roger!');
        var temp = mission['track_posts'].shift();

        if(typeof temp!=='undefined'){
            var current_post_id = temp;
            start.harmony(mission,mission['track_posts'],current_post_id);
            start.start(current_post_id);

        }

    }
});
module.exports = crawler;

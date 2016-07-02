/**
 * Created by anshulkapoor on 6/30/16.
 */
var express = require('express');
var app = express();

app.use(express.static('public'));

app.get('/', function (req, res) {
    res.send('Hello World');
})

app.get('/', function (req, res) {
    res.sendFile( __dirname + "public/" + "index.htm" );
})

var server = app.listen(3333, function () {

    var host = server.address().address
    var port = server.address().port

    console.log("Example app listening at http://%s:%s", host, port)
})

//=====================================================================
var _ = require('underscore');
var fs = require('fs');
var csv = require('fast-csv');
var stream = fs.createReadStream("source1.csv");
var stream2 = fs.createReadStream("source2.csv");

var febCampaigns = [];
var plantCampaigns = [];

var plant_typeX = 0;
var plant_typeY = 0;

var allCampaigns = []; // total unique audience asset campaign combinations
var videoCampaigns = []; //all video campaigns
var allCPV = [];

var csvStream2 = csv()
    .on("data", function(data) {
        if(data[1] === 'video') {
            videoCampaigns.push(data[0]);
        }
    });

stream2.pipe(csvStream2);

var csvStream = csv()
    .on("data", function(data) {
        if(data[4] !== 'actions') {
            var conv_typeX = 0, conv_typeY = 0, views_typeX = 0, views_typeY=0;
            var actions = JSON.parse(data[4]);
            for(item in actions){
                if(actions[item].hasOwnProperty('x')) {
                    if(actions[item].action === 'conversions')
                        conv_typeX += actions[item].x;
                    if(actions[item].action === 'views')
                        views_typeX += actions[item].x
                }
                if(actions[item].hasOwnProperty('y')) {
                    if(actions[item].action === 'conversions')
                        conv_typeY += actions[item].y;
                    if(actions[item].action === 'views')
                        views_typeY += actions[item].y
                }
            }
            //1. How many unique campaigns ran in February?
            if (data[1].slice(5,7) === '04') {
                febCampaigns.push(data[0]);
            }
            //2. What is the total number of conversions on plants?
            if(data[0].includes('plant')) {
                plantCampaigns.push(data[4]);
                plant_typeX += conv_typeX;
                plant_typeY += conv_typeY;
            }
            //3. What audience, asset combination had the least expensive conversions?
            var campaign = data[0];
            var audience_asset = campaign.substring(campaign.indexOf('_')+1);
            var xy_conversion = conv_typeX+conv_typeY;
            var spentMoney = parseInt(data[2]);
            if(xy_conversion !== 0){
                allCampaigns.push({
                    "name":  audience_asset,
                    "ratio": xy_conversion/spentMoney //Get the ratio of total conversions per money spent.
                });
            }
            //4. What was the total cost per video view?
            var totalViews = views_typeX+views_typeY;
            var CPV = spentMoney/totalViews;
            if(totalViews !== 0) {
                allCPV.push({
                   "name":  data[0],
                    "CPV": CPV
                });
            }
        }

    })
    .on('error', function(error) {
        console.log(error);
    })
    .on("end", function(){
        var uniqFebCampaigns = _.uniq(febCampaigns);
        console.log("1. Unique campaigns for February: " + uniqFebCampaigns.length); //125

        var totalPlantConversions = plant_typeX+plant_typeY;
        console.log("2. Total conversions (of type X & Y only) on plants: " + totalPlantConversions); //100741

        var currRatio = allCampaigns[0].ratio;
        var leastExpensiveCombo = allCampaigns[0].name;
        for(item in allCampaigns) {
            if(allCampaigns[item].ratio > currRatio) {
                currRatio = allCampaigns[item].ratio;
                leastExpensiveCombo = allCampaigns[item].name;
            }

        }
        console.log("3. Combination of audience and asset with the least expensive conversion: " + leastExpensiveCombo); //cow_desert

        videoCampaigns =  _.uniq(videoCampaigns);
        var totalValidCPV = 0;
        for(item in allCPV) {
            if(_.indexOf(videoCampaigns, allCPV[item].name) > -1) {
                totalValidCPV += allCPV[item].CPV;
            }
        }

        console.log("4. Total cost per video view: $" + Math.round(totalValidCPV * 100) / 100);
    });

stream.pipe(csvStream);
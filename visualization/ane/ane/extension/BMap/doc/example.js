//排序函数
function compare(property){
    return function(a,b){
        var value1 = a[property];
        var value2 = b[property];
        return value2 - value1;
    }
}
//获取时间
function gettime(y, m , d, h, mm ,s,addmm){

    var htemp = parseInt((h*60+mm+addmm)/60);
    var mmtemp = parseInt((h*60+mm+addmm)%60);
    if(htemp>24){
        htemp = htemp-24;
        d = d+1;
    }
    var strh = htemp.toString();
    var strmm = mmtemp.toString();
    if(htemp<10){
        strh = "0" + htemp.toString();
    }

    if(mmtemp<10){
        strmm = "0" + mmtemp.toString();
    }
    var strs = s.toString();
    if(s<10){
        strs = "0" + s.toString();
    }



    var predictime = y + '-' + m + '-' + d + " " + strh + ":" + strmm + ":" + strs;
    return predictime;
}
//被调用两次，根据in 和 out 的区别，产生不同的内容
//需要传入的数据：index(1:out;2:in), traceadata 总的数组

function getdetail(index ,tracedatainout){
    var tracetypetype = "unknown";
    if(index === 1 ){
        tracetypetype = "out";
    }
    else if(index === 2){
        tracetypetype = "in";
    }
    //alert(tracetypetype);
    //console.log(tracedatain);
    //alert(tracedatain.length);//20个订单
    var tracedata = tracedatainout;


    //只需要5个
    var detailcount = 0;
    var detailall = {};
    for(var index1 in tracedata){
        if(detailcount===5){
            break;
        }

        //二级目录，存储一条信息
        var detailarrsec = [];
        //一次循环检测一个单号
        var secarr = tracedata[index1];
        var sortarr = [];


        secarr.sort(function(a,b){
            return Date.parse(a.SCAN_TIME) - Date.parse(b.SCAN_TIME);//时间正序
        });
        var newsecarr=[];
        newsecarr.push(secarr[0]);
        var stripindex = 0;
        for(var order in secarr){
            //alert(secarr[stripindex+1]["SCAN_TIME "]);

            if(secarr[stripindex+1]["SCAN_TIME"]!==secarr[stripindex]["SCAN_TIME"]){
                newsecarr.push(secarr[stripindex+1]);

            }
            stripindex++;
            if (stripindex>secarr.length-2){
                break;
            }

        }
        //排序去重完成
        console.log("newsecarr");
        console.log(newsecarr);

        //console.log(secarr);
        //alert(secarr.length);
        //安全性检查，如果不符合出发地目的地一样对应，剔除
        var counte = 0;
        var inflag = true;
        //console.log(newsecarr);
        if(inflag===true){
            detailcount++;

            //alert(detailcount);


            //获取了1条信息，把这条信息写入数组中
            var d = new Date();
            var curh = d.getHours();
            var curm = d.getMinutes();
            //alert(curh);
            //alert(curm);
            var currenttime = "2017-10-10 "+curh+":"+curm+":00";
            //alert(currenttime);
            //获取最新的时间
            var latestinfo = 0;
            for(var indexg in newsecarr){
                var tracetime = newsecarr[indexg]["SCAN_TIME"];
                if(tracetime<currenttime){
                    latestinfo = indexg;
                    //alert("合理");
                    //alert(tracetime+"<"+currenttime);
                }
                else{
                    //alert("不合理");
                    //alert(tracetime+">"+currenttime);
                }
            }
            var currposi = newsecarr[latestinfo]["SITE_ID"]+":"+newsecarr[latestinfo]["SITE_NAME"];
            var nextposi = newsecarr[latestinfo]["DEST_SITE_ID"]+":"+newsecarr[latestinfo]["DEST_SITE_NAME"];
            //现在进行时间预测
            var  latesttime = newsecarr[latestinfo]["SCAN_TIME"];
            var latesttemparr1 = latesttime.split(" ");
            var latesttemparr2 = latesttemparr1[1].split(":");
            var latesttemparr3 = latesttemparr1[0].split("-");
            var latesty =  parseInt(latesttemparr3[0]);
            var latestm =  parseInt(latesttemparr3[1]);
            var latestd =  parseInt(latesttemparr3[2]);
            var latesth = parseInt(latesttemparr2[0]);
            var latestmm = parseInt(latesttemparr2[1]);
            var latests = parseInt(latesttemparr2[2]);

            var calla = latesth*60+latestmm;

            var calcurr = parseInt(curh)*60+parseInt(curm);
            var transtime = parseInt(newsecarr[latestinfo]["PREDICT_TIME"]);
            var delayinfo = "未知";
            var calpredict = calla+transtime;
            if (calpredict>1440){
                calpredict = calpredict-1440;
            }
            if((calla+transtime)>calcurr)
            {

                delayinfo="未延误";
                // alert((calla+transtime)+ ">" + calcurr);
                // alert(delayinfo);
            }

            else{
                delayinfo="已经延误";
                // alert((calla+transtime)+ "<" + calcurr);
                // alert(delayinfo);
            }



            //gettime(y, m , d, h, mm ,s,addmm)
            //获取了预测时间
            var guesstime = gettime(latesty,latestm,latestd,latesth,latestmm,latests,transtime);
            //alert(guesstime);


            //解析完毕，将5条信息写入二级数组
            detailarrsec.push(currposi);
            detailarrsec.push(nextposi);
            detailarrsec.push(delayinfo);
            detailarrsec.push(guesstime);
            var stedetailcount = detailcount.toString();
            detailall[stedetailcount]= detailarrsec;

            // console.log("detailarrsec");
            // console.log(detailarrsec);

            var jtr =  document.createElement("tr");
            var jeattribute  = "mydetail"+stedetailcount + tracetypetype;

            var jtrindex= document.createElement("td");

            var jindex = document.createTextNode(detailcount.toString());
            jtrindex.appendChild(jindex);
            jtrindex.setAttribute("id", jeattribute);
            jtr.appendChild(jtrindex);
            for (var i = 0 ; i<4;i ++){
                var jtd = document.createElement("td");
                var dearreach = detailarrsec[i];
                //alert(detailarrsec[i]);
                var contd = document.createTextNode(dearreach);
                //console.log(contd);
                jtd.appendChild(contd);
                //console.log(jtd);
                jtr.appendChild(jtd);
                //console.log(jtr);
            }

            //tracetypetype = "out";
            var jtbody = document.getElementById("jouttb" + tracetypetype);
            jtbody.appendChild(jtr);



            //上面的以此循环将5条信息塞入detail div中
            //下面需要将每一个订单的信息都打印出来
            //newsecarr[latestinfo]表示没有超过未来时间的最后一组可用信息
            for(var deindex = 0;deindex<=latestinfo;deindex++){
                if(deindex===7){
                    break;
                }
                //以此循环遍历一个订单的一条物流信息
                var jearr =[];

                jearr.push(newsecarr[deindex]["TRACE_ID"]);
                jearr.push(newsecarr[deindex]["EWB_NO"]);
                jearr.push(newsecarr[deindex]["SITE_ID"]);
                jearr.push(newsecarr[deindex]["SITE_NAME"]);
                jearr.push(newsecarr[deindex]["SCAN_TIME"]);
                jearr.push(newsecarr[deindex]["DEST_SITE_ID"]);
                jearr.push(newsecarr[deindex]["DEST_SITE_NAME"]);
                jearr.push(newsecarr[deindex]["PREDICT_TIME"]);
                //alert(newsecarr[deindex]["PREDICT_TIME"]);
                var jetr =  document.createElement("tr");
                var jetrindex= document.createElement("td");
                var jeindexcon = document.createTextNode(deindex.toString());
                jetrindex.appendChild(jeindexcon);
                jetr.appendChild(jetrindex);
                for(var p = 0;p<8;p++){
                    var jetd = document.createElement("td");
                    var jeetd = jearr[p];
                    //alert(jeetd);
                    var contde = document.createTextNode(jeetd);
                    jetd.appendChild(contde);

                    jetr.appendChild(jetd);
                }


                //根据不同的div进行插入
                //alert(detailcount);

                var jetbid  = "deachtb" + detailcount.toString() + tracetypetype;
                jetbid = jetbid.trim();
                var jetbody = document.getElementById(jetbid);


                jetbody.appendChild(jetr);






            }








        }//一次if判断结束，表示一条订单的信息完成



    }//计算5份订单的循环跳出
    console.log("5份订单的信息");
    console.log(detailall);


    return 0;

}



(function () {

//**************************************
    var localdata;
    var serverdata;



    //存储名称-经纬度的键值对信息，可以直接被echarts调用
    var geoCoord = {};

    //存储 [{name:'北京'},{name:'南昌'}],集合，表示出发地目的地指向关系
    var start_end = [];


    //用来存储前十的中文名，可以直接被echarts调用
    var toptenname = [];



    var hsarr = [[],[],[],[],[],[],[],[],[],[]];
    var nsarr = [[],[],[],[],[],[],[],[],[],[]];


    var id2siteName = {};

    var traveCount = "";
    $.ajax({
        async: false,
        type: "get",        //type：(string)请求方式，POST或GET
        dataType: "json",    //dataType：(string)预期返回的数据类型。xml,html,json,text等
        url: "../../../data/complete.json",  //url：(string)发送请求的地址，可以是服务器页面也可以是WebService动作。
        success: function (msg) {
            //console.log(msg);
            var rowajax = msg["Row"];
            //console.log(rowajax);
            localdata = rowajax;
            for (var i = 0; i < rowajax.length; i++) {
              id2siteName[rowajax[i]["name"]] = rowajax[i]["number"]
            }
        }
    });

    $.ajax({
        async: false,
        type: "get",        //type：(string)请求方式，POST或GET
        dataType: "json",    //dataType：(string)预期返回的数据类型。xml,html,json,text等
        url: "/atcal/currentinfo",  //url：(string)发送请求的地址，可以是服务器页面也可以是WebService动作。
        // url : "../../../data/tinyserverdata.json",
        success: function (msg) {
            serverdata = msg;
        }
    });

    $.ajax({
        async: false,
        type: "get",        //type：(string)请求方式，POST或GET
        dataType: "json",    //dataType：(string)预期返回的数据类型。xml,html,json,text等
        url: "/atcal/inTraveCount",  //url：(string)发送请求的地址，可以是服务器页面也可以是WebService动作。
        // url : "../../../data/tinyserverdata.json",
        success: function (msg) {
            traveCount = msg;
        }
    });


    console.log("本地数据");
    console.log(localdata);
    console.log("服务器数据");
    console.log(serverdata);

    //分别对本地和服务器信息进行处理，抽取序号，以{id:i}的形式存储
    //localdic
    var localdic = {};//{id:i,.....}
    for (var locali in localdata ){
        localdic[localdata[locali]["number"]] = locali;
    }

    //serverdic以新的json格式进行存储
    //serverdic
    var serverdic = {};
    for (var serveri in serverdata ){
        var key ;
        key = serverdata[serveri]["startId"] +"-"+ serverdata[serveri]["endId"];
        serverdic[key] = serveri;



    }
    console.log(serverdic);
    // for(var azxc in serverdic){
    //     alert(azxc);
    // }


    //利用上面两个dic生成坐标系数据和出发地-目的地数据
    //用来存储服务器中出现过的地点的id，以set 的形式存储
    var serverid = []

    for(var se in serverdata){
        //alert(serverdata[se]["startId"]);
        serverid.push(serverdata[se]["startId"]);
        serverid.push(serverdata[se]["endId"]);
    }
    //去重的存储了所有的地点的id
    //console.log(serverid);
    //对set进行遍历，得到名称，存储到map中


    console.log("++++++++++++debug+++++++++++=serverId");
    console.log(serverid);
    for(var sdf in serverid){
        var key_set = localdic[serverid[sdf]];
        if(key_set!==undefined){
            var nameeach = localdata[key_set]["name"];
            var positioneach = localdata[key_set]["position"];
            geoCoord[nameeach] = positioneach;
        }
    }
    //成功生成geoCoord全部的经纬度集合
    console.log("经纬度集合");
    console.log(geoCoord);


    for(var rt in serverdata){
        //alert(serverdata[rt]["startId"]);
        var start_id = serverdata[rt]["startId"];
        var start_key = localdic[start_id];
        //出发地可能是重复的
        //var start_name = localdata[start_key]["name"];

        var end_id = serverdata[rt]["endId"];
        var end_key = localdic[end_id];
        //var end_name = localdata[end_key]["name"];

        try{
            var start_name = localdata[start_key]["name"];
            var end_name = localdata[end_key]["name"];
            var start_object = {};
            start_object.name = start_name;
            var end_object = {};
            end_object.name = end_name;
            var start_end_each = [];
            start_end_each.push(start_object);
            start_end_each.push(end_object);
            start_end.push(start_end_each);
        }
        catch(err){
            //console.log(err)

        }

    }
    //成功获取出发地-目的地的数组，可以直接调用
    console.log("浅色的出发地-目的地的数组");
    console.log(start_end);


    //将服务器的信息的startid和total注入到对象中,对total进行累加，以便排序

    var serversort = {};
    for(var i in serverdata){
        if(serversort[serverdata[i]["startId"]] !== undefined)
        {
            serversort[serverdata[i]["startId"]] = serversort[serverdata[i]["startId"]]+serverdata[i]["total"];
        }
        else{
            serversort[serverdata[i]["startId"]] = serverdata[i]["total"];
        }

    }
    //成功进行了累加{将相同出发地的所有发出去的订单进行相加}
    //console.log(serversort);
    //现在需要对临时对象进行排序，得到前十的id

    //****排序前的数据预处理
    //用来临时存储排序前的大数组
    var toptempall= [];
    //用来存储排序前大数组中的每一个小对象
    var toptempeach = {};
    for (var keyw in serversort){
        toptempeach = {};
        toptempeach["id"] = keyw;
        toptempeach["total"] = serversort[keyw];
        toptempall.push(toptempeach);
    }
    //console.log(toptempall);
    //下面为排序后的结果
    toptempall = toptempall.sort(compare('total'));
    //console.log(toptempall);
    //alert(toptempall[0]["id"]);


    //用来存储前十的id
    var toptenid = [];

    var toptencount = 0;
    for(var topindex = 0;topindex<30;topindex++)
    {
        var topid = toptempall[topindex]["id"];
        //topkey：在localdic数组中的位置（索引）
        var topkey = localdic[topid];
        if(topkey!==undefined){
            toptencount++;
            //alert("topkey"+topkey);
            var toptenideach = localdata[topkey]["number"];

            var topnameeach = localdata[topkey]["name"];
            //alert("name"+topnameeach);
            toptenname.push(topnameeach);
            toptenid.push(toptenideach);

        }
        if(toptencount === 10){
            break;
        }
    }
    console.log("top10的名称");
    console.log(toptenname);
    console.log(toptenid);




    for(var ttiindex in toptenid){
        //根据top10id获取相关信息

        //alert(ttiindex);
        var ttid = toptenid[ttiindex];
        //下面的for循环一轮结束表示top10中的一处出发地所有信息收集完毕
        for(var sindex in serverdata){
            //一次if判断表示一条物流信息
            if(serverdata[sindex]["startId"]===ttid){
                var startnameobj = {};
                var localkey = localdic[ttid];

                //alert(localdata[localkey]["name"]);
                var startname  = localdata[localkey]["name"];
                startnameobj.name = startname;

                var eobj  = {};

                var hstarteach = [];

                //alert(serverdata[sindex]["endId"]);
                var ei = serverdata[sindex]["endId"];
                var e_key = localdic[ei];

                try{
                    var e_name =localdata[e_key]["name"];
                    eobj.name = e_name;
                    eobj.value = serverdata[sindex]["total"];
                    //设置rgb颜色
		                var red = 0;
                    var green = 255;
                    var abnormalindex =  serverdata[sindex]["abnormal"];
                    var totalIndex = serverdata[sindex]["total"];

                    red = red+1*abnormalindex;
                    green = green-1*abnormalindex;
                    if(red>255){
                        red = 255;
                    }
                    if(green<0){
                        green = 0;
                    }
                    //3层对象
                    var normalobj = {};
                    var colorobj = {};
                    colorobj.color='rgb('+red+','+green+','+0+')';
                    normalobj.normal =  colorobj;
                    //eobj.itemStyle = normalobj;
                    var mlobj = {};
                    mlobj.name = e_name;
                    mlobj.value = serverdata[sindex]["total"];
                    mlobj.itemStyle = normalobj;

                    //console.log(eobj);
                    hstarteach.push(startnameobj);
                    hstarteach.push( mlobj);

                    hsarr[ttiindex].push(hstarteach);
                    nsarr[ttiindex].push(eobj);


                }
                catch(err) {

                }

            }
        }
    }
    //有出发地的data
    console.log(hsarr);
    //无出发地的data
    console.log(nsarr);





//*******************************************************************


    //路径配置
    require.config({
        paths: {
            echarts: '../../../doc/example/www/js'
        },
        packages: [
            {
                name: 'BMap',
                location: '../src',
                main: 'main'
            }
        ]
    });
    //使用
    require(
    [
        'echarts',
        'BMap',
        'echarts/chart/map'//按需加载某一个js
    ],
    function (echarts, BMapExtension) {
        $('#main').css({
            height:$('body').height(),
            width: $('#main').width()



        });
        //基于准备好的dom，初始化地图
        // 初始化地图
        var BMapExt = new BMapExtension($('#main')[0], BMap, echarts,{
            enableMapClick: false
        });
        var map = BMapExt.getMap();
        var container = BMapExt.getEchartsContainer();

        var startPoint = {
            x: 104.114129,
            y: 37.550339
        };

        var point = new BMap.Point(startPoint.x, startPoint.y);
        map.centerAndZoom(point, 5);
        map.enableScrollWheelZoom(true);
        // 地图自定义样式
        map.setMapStyle({
            styleJson: [
                  {
                       "featureType": "water",
                       "elementType": "all",
                       "stylers": {
                            "color": "#044161"
                       }
                  },
                  {
                       "featureType": "land",
                       "elementType": "all",
                       "stylers": {
                            "color": "#004981"
                       }
                  },
                  {
                       "featureType": "boundary",
                       "elementType": "geometry",
                       "stylers": {
                            "color": "#064f85"
                       }
                  },
                  {
                       "featureType": "railway",
                       "elementType": "all",
                       "stylers": {
                            "visibility": "off"
                       }
                  },
                  {
                       "featureType": "highway",
                       "elementType": "geometry",
                       "stylers": {
                            "color": "#004981"
                       }
                  },
                  {
                       "featureType": "highway",
                       "elementType": "geometry.fill",
                       "stylers": {
                            "color": "#005b96",
                            "lightness": 1
                       }
                  },
                  {
                       "featureType": "highway",
                       "elementType": "labels",
                       "stylers": {
                            "visibility": "off"
                       }
                  },
                  {
                       "featureType": "arterial",
                       "elementType": "geometry",
                       "stylers": {
                            "color": "#004981"
                       }
                  },
                  {
                       "featureType": "arterial",
                       "elementType": "geometry.fill",
                       "stylers": {
                            "color": "#00508b"
                       }
                  },
                  {
                       "featureType": "poi",
                       "elementType": "all",
                       "stylers": {
                            "visibility": "off"
                       }
                  },
                  {
                       "featureType": "green",
                       "elementType": "all",
                       "stylers": {
                            "color": "#056197",
                            "visibility": "off"
                       }
                  },
                  {
                       "featureType": "subway",
                       "elementType": "all",
                       "stylers": {
                            "visibility": "off"
                       }
                  },
                  {
                       "featureType": "manmade",
                       "elementType": "all",
                       "stylers": {
                            "visibility": "off"
                       }
                  },
                  {
                       "featureType": "local",
                       "elementType": "all",
                       "stylers": {
                            "visibility": "off"
                       }
                  },
                  {
                       "featureType": "arterial",
                       "elementType": "labels",
                       "stylers": {
                            "visibility": "off"
                       }
                  },
                  {
                       "featureType": "boundary",
                       "elementType": "geometry.fill",
                       "stylers": {
                            "color": "#029fd4"
                       }
                  },
                  {
                       "featureType": "building",
                       "elementType": "all",
                       "stylers": {
                            "color": "#1a5787"
                       }
                  },
                  {
                       "featureType": "label",
                       "elementType": "all",
                       "stylers": {
                            "visibility": "off"
                       }
                  }
            ]
        });

        option = {
            color: ['gold','aqua','lime','Beige','Black','DarkKhaki','LightCyan','MediumAquaMarine','MidNightBlue','Orchid'],
            title : {
                //主标题文本
                text: '物流信息',
                //副标题文本
                subtext:"在途订单总量:" + traveCount +" 链路总量：698000000",
                // 水平安放位置，默认为左侧，可选为：'center' | 'left' | 'right' | {number}（x坐标，单位px）
                x:'center',
                // 主标题文本样式
                textStyle : {
                    //text的颜色
                    color: '#fff'
                }
            },
            // 工具提示
            tooltip : {
                // 触发类型，默认数据触发，见下图，可选为：'item' | 'axis'
                trigger: 'item',
                formatter: function (v) {
                    return v[1].replace(':', ' > ');
                }
            },
            legend: {
                //一共有10个出发地，在左上角
                show : false,
                orient: 'vertical',
                x:'left',
                data:toptenname,
                selectedMode: 'multiple',
                selected:{

                    //'上海' : false,
                    //'广州' : false
                    //toptenname:false

                },
                textStyle : {
                    color: '#fff'
                }
            },
            // 工具箱，每个图表最多仅有一个工具箱
            toolbox: {
                show : true,
                orient : 'vertical',
                x: 'right',
                y: 'center',
                feature : {
                    mark : {show: true},
                    dataView : {show: true, readOnly: false},
                    restore : {show: true},
                    saveAsImage : {show: true}
                }
            },
            // 值域选择，每个图表最多仅有一个值域控件
            dataRange: {
                min : 0,
                max : 100,
                range: {
                    start: 10,
                    end: 90
                },
                x: 'right',
                calculable : true,
                color: ['#ff3333', 'orange', 'yellow','lime','aqua'],
                textStyle:{
                    color:'#fff'
                }
            },
            series : [
                {
                    // 系列名称
                    name:toptenname[0],
                    // 图表类型，必要参数！如为空或不支持类型，则该系列数据不被显示。
                    type:'map',
                    // 地图类型，支持world，china及全国34个省市自治区
                    mapType: 'none',
                    data:[],
                    //地址与它的坐标系
                    geoCoord: geoCoord,

                    markLine : {
                        smooth:true,
                        effect : {
                            show: true,
                            scaleSize: 1,
                            period: 30,
                            color: '#fff',
                            shadowBlur: 10
                        },
                        itemStyle : {
                            normal: {
                                borderWidth:1,
                                lineStyle: {
                                    type: 'solid',
                                    shadowBlur: 10
                                }
                            }
                        },
                        //北京指向各地，value为量
                        data : hsarr[0]
                    },
                    markPoint : {
                        symbol:'emptyCircle',
                        symbolSize : function (v){
                            return 10 + v/10
                        },
                        effect : {
                            show: true,
                            shadowBlur : 0
                        },
                        itemStyle:{
                            normal:{
                                label:{show:false}
                            }
                        },
                        data : nsarr[0]
                    }

                },
                {
                    name:toptenname[1],
                    type:'map',
                    mapType: 'none',
                    data:[],
                    markLine : {
                        smooth:true,
                        effect : {
                            show: true,
                            scaleSize: 1,
                            period: 30,
                            color: '#fff',
                            shadowBlur: 10
                        },
                        tooltip:{
                            showContent:true,
                            enterable:true,
                            formatter:function(){return false;}
                        },
                        itemStyle : {
                            normal: {
                                borderWidth:1,
                                lineStyle: {
                                    type: 'solid',
                                    shadowBlur: 10,
                                    //color: "black"
                                }
                            }
                        },
                        data : hsarr[1]
                    },
                    markPoint : {
                        symbol:'emptyCircle',
                        symbolSize : function (v){
                            return 10 + v/10
                        },
                        effect : {
                            show: true,
                            shadowBlur : 0
                        },
                        itemStyle:{
                            normal:{
                                label:{show:false}
                            }
                        },
                        data : nsarr[1]
                    }
                },
                {
                    name:toptenname[2],
                    type:'map',
                    mapType: 'none',
                    data:[],
                    markLine : {
                        smooth:true,
                        effect : {
                            show: true,
                            scaleSize: 1,
                            period: 30,
                            color: '#fff',
                            shadowBlur: 10
                        },
                        itemStyle : {
                            normal: {
                                borderWidth:1,
                                lineStyle: {
                                    type: 'solid',
                                    shadowBlur: 10
                                }
                            }
                        },
                        data : hsarr[2]
                    },
                    markPoint : {
                        symbol:'emptyCircle',
                        symbolSize : function (v){
                            return 10 + v/10
                        },
                        effect : {
                            show: true,
                            shadowBlur : 0
                        },
                        itemStyle:{
                            normal:{
                                label:{show:false}
                            }
                        },
                        data : nsarr[2]
                    }
                },
                {
                    name:toptenname[3],
                    type:'map',
                    mapType: 'none',
                    data:[],
                    markLine : {
                        smooth:true,
                        effect : {
                            show: true,
                            scaleSize: 1,
                            period: 30,
                            color: '#fff',
                            shadowBlur: 10
                        },
                        itemStyle : {
                            normal: {
                                borderWidth:1,
                                lineStyle: {
                                    type: 'solid',
                                    shadowBlur: 10
                                }
                            }
                        },
                        data : hsarr[3]
                    },
                    markPoint : {
                        symbol:'emptyCircle',
                        symbolSize : function (v){
                            return 10 + v/10
                        },
                        effect : {
                            show: true,
                            shadowBlur : 0
                        },
                        itemStyle:{
                            normal:{
                                label:{show:false}
                            }
                        },
                        data : nsarr[2]
                    }
                },
                {
                    name:toptenname[4],
                    type:'map',
                    mapType: 'none',
                    data:[],
                    markLine : {
                        smooth:true,
                        effect : {
                            show: true,
                            scaleSize: 1,
                            period: 30,
                            color: '#fff',
                            shadowBlur: 10
                        },
                        itemStyle : {
                            normal: {
                                borderWidth:1,
                                lineStyle: {
                                    type: 'solid',
                                    shadowBlur: 10
                                }
                            }
                        },
                        data : hsarr[4]
                    },
                    markPoint : {
                        symbol:'emptyCircle',
                        symbolSize : function (v){
                            return 10 + v/10
                        },
                        effect : {
                            show: true,
                            shadowBlur : 0
                        },
                        itemStyle:{
                            normal:{
                                label:{show:false}
                            }
                        },
                        data : nsarr[2]
                    }
                },
                {
                    name:toptenname[5],
                    type:'map',
                    mapType: 'none',
                    data:[],
                    markLine : {
                        smooth:true,
                        effect : {
                            show: true,
                            scaleSize: 1,
                            period: 30,
                            color: '#fff',
                            shadowBlur: 10
                        },
                        itemStyle : {
                            normal: {
                                borderWidth:1,
                                lineStyle: {
                                    type: 'solid',
                                    shadowBlur: 10
                                }
                            }
                        },
                        data : hsarr[5]
                    },
                    markPoint : {
                        symbol:'emptyCircle',
                        symbolSize : function (v){
                            return 10 + v/10
                        },
                        effect : {
                            show: true,
                            shadowBlur : 0
                        },
                        itemStyle:{
                            normal:{
                                label:{show:false}
                            }
                        },
                        data : nsarr[2]
                    }
                },
                {
                    name:toptenname[6],
                    type:'map',
                    mapType: 'none',
                    data:[],
                    markLine : {
                        smooth:true,
                        effect : {
                            show: true,
                            scaleSize: 1,
                            period: 30,
                            color: '#fff',
                            shadowBlur: 10
                        },
                        itemStyle : {
                            normal: {
                                borderWidth:1,
                                lineStyle: {
                                    type: 'solid',
                                    shadowBlur: 10
                                }
                            }
                        },
                        data : hsarr[6]
                    },
                    markPoint : {
                        symbol:'emptyCircle',
                        symbolSize : function (v){
                            return 10 + v/10
                        },
                        effect : {
                            show: true,
                            shadowBlur : 0
                        },
                        itemStyle:{
                            normal:{
                                label:{show:false}
                            }
                        },
                        data : nsarr[2]
                    }
                },
                {
                    name:toptenname[7],
                    type:'map',
                    mapType: 'none',
                    data:[],
                    markLine : {
                        smooth:true,
                        effect : {
                            show: true,
                            scaleSize: 1,
                            period: 30,
                            color: '#fff',
                            shadowBlur: 10
                        },
                        itemStyle : {
                            normal: {
                                borderWidth:1,
                                lineStyle: {
                                    type: 'solid',
                                    shadowBlur: 10
                                }
                            }
                        },
                        data : hsarr[7]
                    },
                    markPoint : {
                        symbol:'emptyCircle',
                        symbolSize : function (v){
                            return 10 + v/10
                        },
                        effect : {
                            show: true,
                            shadowBlur : 0
                        },
                        itemStyle:{
                            normal:{
                                label:{show:false}
                            }
                        },
                        data : nsarr[2]
                    }
                },
                {
                    name:toptenname[8],
                    type:'map',
                    mapType: 'none',
                    data:[],
                    markLine : {
                        smooth:true,
                        effect : {
                            show: true,
                            scaleSize: 1,
                            period: 30,
                            color: '#fff',
                            shadowBlur: 10
                        },
                        itemStyle : {
                            normal: {
                                borderWidth:1,
                                lineStyle: {
                                    type: 'solid',
                                    shadowBlur: 10
                                }
                            }
                        },
                        data : hsarr[8]
                    },
                    markPoint : {
                        symbol:'emptyCircle',
                        symbolSize : function (v){
                            return 10 + v/10
                        },
                        effect : {
                            show: true,
                            shadowBlur : 0
                        },
                        itemStyle:{
                            normal:{
                                label:{show:false}
                            }
                        },
                        data : nsarr[2]
                    }
                },
                {
                    name:toptenname[9],
                    type:'map',
                    mapType: 'none',
                    data:[],
                    markLine : {
                        smooth:true,
                        effect : {
                            show: true,
                            scaleSize: 1,
                            period: 30,
                            color: '#fff',
                            shadowBlur: 10
                        },
                        itemStyle : {
                            normal: {
                                borderWidth:1,
                                lineStyle: {
                                    type: 'solid',
                                    shadowBlur: 10
                                }
                            }
                        },
                        data : hsarr[9]
                    },
                    markPoint : {
                        symbol:'emptyCircle',
                        symbolSize : function (v){
                            return 10 + v/10
                        },
                        effect : {
                            show: true,
                            shadowBlur : 0
                        },
                        itemStyle:{
                            normal:{
                                label:{show:false}
                            }
                        },
                        data : nsarr[2]
                    }
                },
                {
                    name:'全国',
                    type:'map',
                    mapType: 'none',
                    data:[],
                    markLine : {
                        smooth:true,
                        symbol: ['none', 'circle'],
                        symbolSize : 1,
                        itemStyle : {
                            normal: {
                                color:'#fff',
                                borderWidth:1,
                                borderColor:'rgba(30,144,255,0.5)'
                            }
                        },
                        data : start_end
                    }
                }
            ]
        };

        var myChart = BMapExt.initECharts(container);
        window.onresize = myChart.onresize;
        BMapExt.setOption(option);


        var ecConfig = require('echarts/config');
        myChart.on(ecConfig.EVENT.CLICK, eConsole);
        function eConsole(param){

            //document.getElementById('detail').style.display='block';
            //document.getElementById('main').style.display='block';
            console.log(param.name);
            console.log(param.name.split(">")[0].trim());
            console.log();
            console.log("+++++++++++++++++++++++++++++++");
            //console.log(param);
            //判断点击的是线还是点
            var paramlist = [];
            paramlist = param.name.split(">");
            //alert (paramlist.length);
            //var detailinfo = document.createElement("div");
            //var node =  document.createTextNode(param.name);
            //detailinfo.appendChild(node);
            //这个变量用来定义点击的站点的名称
            var siteclickname ;
            //需要去除空格
            siteclickname = paramlist[0].trim('');
            //console.log(siteclickname);
            var idindex = toptenname.indexOf(siteclickname);
            //alert(idindex);
            var siteclickid = id2siteName[paramlist[0].trim()]
            //alert(siteclickid);
            //获取id，发送ajax请求，
            //1表示out
            //2表示in
            //10.10.0.91:8080/atcal/siteinfo?siteId=15265&size=20&tag=1

            var otherSiteId = id2siteName[paramlist[1].trim()]

            // 另外开一个 window 展示
            var windowHref = "/ane/ane/extension/BMap/doc/detail.html?siteId="+siteclickid+"&endId="+otherSiteId;

            // var otherHref = "/ane/ane/extension/BMap/doc/detail.html?siteId="+otherSiteId;
            window.open(windowHref,"_blank");
            window.open(otherHref, "_blank");
            return;

            var requin = "http://10.10.0.136/atcal/siteinfo?siteId="+siteclickid+"&size=5&tag=2";
            var requout= "http://10.10.0.136/atcal/siteinfo?siteId="+siteclickid+"&size=5&tag=1";

            //接入In的数据
            var tracedatain;
            $.ajax({
                async: false,
                type: "get",        //type：(string)请求方式，POST或GET
                dataType: "json",    //dataType：(string)预期返回的数据类型。xml,html,json,text等
                url: requin,  //url：(string)发送请求的地址，可以是服务器页面也可以是WebService动作。
                success: function (msgin) {
                    tracedatain = msgin;

                }
            });

            //接入out的数据
            var tracedataout;
            $.ajax({
                async: false,
                type: "get",        //type：(string)请求方式，POST或GET
                dataType: "json",    //dataType：(string)预期返回的数据类型。xml,html,json,text等
                url: requout,  //url：(string)发送请求的地址，可以是服务器页面也可以是WebService动作。
                success: function (msgout) {
                    tracedataout = msgout;

                }
            });

            var xin = getdetail(1,tracedataout);
            //alert(xin);
            var xout = getdetail(2,tracedatain);





        }


    }
);
})();

function add0(m){return m<10?'0'+m:m }
function formatDate(needTime)
{
     //needTime是整数，否则要parseInt转换
       var time = new Date(needTime);
       var y = time.getFullYear();
       var m = time.getMonth()+1;
       var d = time.getDate();
       var h = time.getHours();
      var mm = time.getMinutes();
      var s = time.getSeconds();
      return y+'-'+add0(m)+'-'+add0(d)+' '+add0(h)+':'+add0(mm)+':'+add0(s);
}

// 力导向图
function forceInit(nodesData,linksData){
  require.config({
      paths : {
          echarts : 'http://echarts.baidu.com/build/dist'
      }
  });
  require([ "echarts", "echarts/chart/force"], function(ec) {
      var myChart = ec.init(document.getElementById('main'), 'macarons');
      option = {
    title : {
        text: '物流流向图',
        subtext: '数据来自ane',
        x:'right',
        y:'bottom'
    },
    tooltip : {
        trigger: 'item',
        formatter: '{a} : {b}'
    },
    toolbox: {
        show : true,
        feature : {
            restore : {show: true},
            magicType: {show: true, type: ['force', 'chord']},
            saveAsImage : {show: true}
        }
    },
    legend: {
        x: 'left',
        data:[""]
    },
    series : [
        {
            type:'force',
            name : "物流关系",
            ribbonType: false,
            categories : [
                {
                    name:"物流关系"
                }
            ],
            itemStyle: {
                normal: {
                    label: {
                        show: true,
                        textStyle: {
                            color: '#333'
                        }
                    },
                    nodeStyle : {
                        brushType : 'both',
                        borderColor : 'rgba(255,215,0,0.4)',
                        borderWidth : 1
                    },
                    linkStyle: {
                        type: 'curve'
                    }
                },
                emphasis: {
                    label: {
                        show: false
                        // textStyle: null      // 默认使用全局文本样式，详见TEXTSTYLE
                    },
                    nodeStyle : {
                        //r: 30
                    },
                    linkStyle : {}
                }
            },
            useWorker: false,
            minRadius : 15,
            maxRadius : 25,
            gravity: 0.01,
            scaling: 2,
            roam: 'move',
            nodes:nodesData,
            links : linksData,
            force: {
                // gravity: 0  //引力
                edgeLength: 300, //默认距离
                repulsion: 100 //斥力
            },

        }
    ]
};

myChart.setOption(option)


  });
}

function displayTime() {
 var elt = document.getElementById("clock"); // 通过id= "clock"找到元素
 var now = new Date(); // 得到当前时间
 elt.innerHTML = now.toLocaleTimeString(); //让elt来显示它
 setTimeout(displayTime,1000); //在1秒后再次执行
}

function GetQueryString(name)
{
     var reg = new RegExp("(^|&)"+ name +"=([^&]*)(&|$)");
     var r = window.location.search.substr(1).match(reg);
     if(r!=null)return  unescape(r[2]); return null;
}
function GetCurrentTime(){
  var d = new Date();
  return d.getHours()+":"+d.getMinutes()+":"+d.getSeconds()
}

function handler(data, settingData){
  console.log("当前设定的时间 ："+settingData);
  handlerList = [];
  // currentHourMinSec = GetCurrentTime();
  // oldYearMonthDay = settingData;
  // settingData = oldYearMonthDay + " " + currentHourMinSec;
  settingTimestamp = new Date(settingData).getTime();
  for (var i = 0; i < data.length; i++) {
    traceList = data[i];
    minV = 9999999999;
    minTrace = traceList[0];
    for (var k = 0; k < traceList.length; k++) {
      traceObject = traceList[k];
      traceTimestamp = new Date(traceObject["SCAN_TIME"]).getTime()
      if (traceTimestamp< settingTimestamp && settingTimestamp-traceTimestamp<minV) {
        minV = settingTimestamp-traceTimestamp;
        minTrace = traceList[k];
      }
    }
    if (minV>0) {
      handlerList.push(minTrace);
    }
  }
  return handlerList;
}


function generateDetailHashMap(response, container){
  console.log("输出response data 长度 : " + response.length);
    for (var i = 0; i < response.length; i++) {
      traceList = response[i];
      ewb_no = traceList[0]["EWB_NO"];
      container[ewb_no+""] = traceList;
    }
    return
}


var vm = new Vue({
  el : "#app",
  data : {
    // 在途订单总数
    inTraveCount : [0],
    // 出站订单
    outCount : [0],
    // 进站订单
    inCount : [0],
    // abnormal
    abnormalCount : [0],

    // 每页数目
    pageSize : 10,
    // outCount
    outPageCount : [1000],
    // out current page
    outCurrentPage : 1,
    // inCount
    inPageCount : [1000],
    inCurrentPage : 1,

    // delayCount
    delayPageCount : [1000],
    delayCurrentPage : 1,

    siteId2Name : {},
    settingData : ["1909-01-01 00:00:00"],
    settingTimestamp : "",
    siteId : "",
    endId : "",
    // 出站列表
    outList : [],
    outDetailList : {},
    showDetailList : [],

    // 进站
    inList : [],
    inDetailList : {},
    inShowDetailList : [],

    // delay
    delayList : [],
    delayDetailList : {},
    delayShowDetailList : [],

    // 全局加载标记位
    allLoadingIf : [true,],
    inLoadingIf : [true,],
    outLoadingIf : [true,],
    delayLoadingIf : [true,],

    // 对应表生成完成之后，生成 force 图
    finishId : [0,],
  },
  mounted: function() {
  //应该注意的是，使用mounted 并不能保证钩子函数中的 this.$el 在 document 中。为此还应该引入             Vue.nextTick/vm.$nextTick
    // displayTime();
  },
  watch : {
    settingData : {
      handler : function(val, oldVal){
          console.log("+++++++++++++++++++++++++++++++改变值 : "+val);
          this.convert2Timestamp(val);
          // 暂时设置
          // this.settingData[0] ="2017-10-12";
          this.outLoad(this.siteId, this.outCurrentPage);
          this.inLoad(this.siteId, this.inCurrentPage);
          this.delayLoad(this.siteId, this.delayCurrentPage);
      },
      deep : true
    },
    finishId :{
      handler :  function(val, oldVal){
        if (val[0]===1) {
          console.log("**********************:");
          console.log(this.siteId2Name);
          this.force(this.siteId2Name);
        }
      },
      deep : true
    },
  },

  created : function(){
    this.generSiteId2Name();
    this.siteId = GetQueryString("siteId");
    this.endId = GetQueryString("endId");
    this.targetInfo(this.siteId);
  },

  methods : {
    // 生成 force 图
    force : function(mapper){
      var linkUrl = "/atcal/linkSites?siteId="+this.siteId;

      siteId2NameTmp = this.siteId2NameTmp;
      siteId = this.siteId;
      axios.get(linkUrl).then(function(response){
        console.log("=================================-");
        console.log(mapper);
        var data = response.data;
        if (data.length==2) {
          console.log("=================================-");
          var outLink = data[0];
          var inLink = data[1];

          nodesData = [];
          linksData = [];
          // 主节点
          var mainObj = {id:0,category:0,name:'0',label:mapper[siteId],value:10};
          nodesData.push(mainObj);
          var index = 1;
          for (var i = 0; i < outLink.length; i++) {
            if (!mapper.hasOwnProperty(outLink[i])) {
              continue;
            }
            var obj = {id:index,category:1,name:''+i,label:mapper[outLink[i]],value : 5};
            nodesData.push(obj);
            var linkObj = {source : 0,target : i+"",weight:1}
            linksData.push(linkObj);
            index ++;
          }

          console.log("=================================-");

          for (var i = 0; i < inLink.length; i++) {
            if (!mapper.hasOwnProperty(inLink[i])) {
              continue;
            }
            var obj = {id:index,category:1,name:''+index,label:mapper[inLink[i]],value : 5};
            nodesData.push(obj);
            var linkObj = {source : index+"",target : 0+"",weight:1}
            linksData.push(linkObj);
            index ++;
          }
          console.log("=================================-");
          forceInit(nodesData, linksData);
        }
      }).catch(function(er){
          console.log(er);
      });
    },
    // 站点间切换
    change : function(){
      var tem = this.siteId;
      this.siteId = this.endId;
      this.endId = tem;
      this.outCurrentPage = 1;
      this.inCurrentPage = 1;
      this.delayCurrentPage = 1;
      this.targetInfo(this.siteId);
      this.outLoad(this.siteId, this.outCurrentPage);
      this.inLoad(this.siteId, this.inCurrentPage);
      this.delayLoad(this.siteId, this.delayCurrentPage);
      this.force(this.siteId2Name);
    },
    targetInfo : function(siteId){
      targetInfoUrl = "/atcal/targetinfo?siteId="+siteId;
      inTraveCounTmp = this.inTraveCount;
      // 出站订单
      outCountTmp = this.outCount;
      // 进站订单
      inCountTmp = this.inCount;
      // abnormal
      abnormalCountTmp = this.abnormalCount;
      // 设定的时间
      settingDataTmp = this.settingData

      outPageCountTmp = this.outPageCount
      inPageCountTmp = this.inPageCount
      delayPageCountTmp = this.delayPageCount

      pageSizeTemp = this.pageSize

      allLoadingIfTmp = this.allLoadingIf;
      axios.get(targetInfoUrl).then(function(response){

        inTraveCounTmp.splice(0, 1, response.data["traveCount"])

        outCountTmp.splice(0,1, response.data["outCount"])
        inCountTmp.splice(0, 1, response.data["inCount"])
        abnormalCountTmp.splice(0, 1, response.data["delayCount"])

        settingDataTmp.splice(0, 1, response.data["settingData"])
        console.log("+++++++++++++++++++++++++++++++=");

        if (outCountTmp < pageSizeTemp) {
          outPageCountTmp.splice(0, 1, 1)
        }else {
          outPageCountTmp.splice(0, 1, Math.floor(outCountTmp / pageSizeTemp))
        }
        if (inCountTmp < pageSizeTemp) {
          inPageCountTmp.splice(0, 1, 1);
        }else{
          inPageCountTmp.splice(0, 1, Math.floor(inCountTmp / pageSizeTemp))
        }
        if (abnormalCountTmp < pageSizeTemp) {
          delayPageCountTmp.splice(0, 1, 1)
        }else{
          delayPageCountTmp.splice(0, 1, Math.floor(abnormalCountTmp / pageSizeTemp))
        }

        console.log(delayPageCountTmp)

        allLoadingIfTmp.splice(0,1,false);
        console.log("++++++++++++++++++++++++++++++=");

      }).catch(function(error){

      });
    },

    delayLoad : function(siteId, page){
      this.delayList = []
      this.delayDetailList = {}
      this.delayShowDetailList = []
      outWebServiceUrl = "/atcal/siteinfo?siteId="+siteId+"&size="+this.pageSize+"&tag=3&page="+page;
      // out出站记录
      outUrl = "../../../data/out_1.json";
      delayListTemp = this.delayList
      delayDetailListTemp = this.delayDetailList
      settingData = this.settingData[0]
      // alert(settingData)

      delayLoadingIfTmp = this.delayLoadingIf;
      delayLoadingIfTmp.splice(0, 1, true);

      axios.get(outWebServiceUrl).then(function(response){
          // 填充现有的列表页
          delayListTemp.splice(0, 1, handler(response.data, settingData));
          // 对应生成ewb_no的detail 列表
          generateDetailHashMap(response.data, delayDetailListTemp);

          delayLoadingIfTmp.splice(0, 1, false);
      }).catch(function(error){

      });
    },

    outLoad : function(siteId, page){
      this.outList = []
      this.outDetailList = {}
      this.showDetailList = []
      outWebServiceUrl = "/atcal/siteinfo?siteId="+siteId+"&size="+this.pageSize+"&tag=1&page="+page;
      // out出站记录
      outUrl = "../../../data/out_1.json";
      outListTemp = this.outList
      outDetailListTemp = this.outDetailList
      settingData = this.settingData[0]
      // alert(settingData)

      outLoadingIfTmp = this.outLoadingIf;
      outLoadingIfTmp.splice(0, 1, true);

      axios.get(outWebServiceUrl).then(function(response){
        console.log("--------------------------------------------------");
          // 填充现有的列表页
          outListTemp.splice(0, 1, handler(response.data, settingData));
          // 对应生成ewb_no的detail 列表
          generateDetailHashMap(response.data, outDetailListTemp);
          outLoadingIfTmp.splice(0, 1, false);
          console.log("--------------------------------------------------");

      }).catch(function(error){

      });
    },



    inLoad : function(siteId, page){
      this.inList = []
      this.inDetailList = {}
      this.inShowDetailList = []
      inWebServiceUrl = "/atcal/siteinfo?siteId="+siteId+"&size="+this.pageSize+"&tag=2&page="+page;
      // out出站记录
      outUrl = "../../../data/out_1.json";
      inListTemp = this.inList
      inDetailListTemp = this.inDetailList
      settingData = this.settingData[0]

      inLoadingIfTmp = this.inLoadingIf;
      inLoadingIfTmp.splice(0,1,true)

      axios.get(inWebServiceUrl).then(function(response){
          // 填充现有的列表页
          inListTemp.splice(0, 1, handler(response.data, settingData));
          // 对应生成ewb_no的detail 列表
          generateDetailHashMap(response.data, inDetailListTemp);
          inLoadingIfTmp.splice(0,1,false)

      }).catch(function(error){

      });
    },

    generSiteId2Name : function(){
      mapperUrl = "../../../data/complete.json";
      siteId2NameTmp = this.siteId2Name
      finishId = this.finishId
      axios.get(mapperUrl).then(function(response){
        arr = response.data["Row"];
        console.log(arr[0]);
        for (var i = 0; i < arr.length; i++) {
          siteId2NameTmp[arr[i]["number"]] = arr[i]["name"]
        }
        finishId.splice(0, 1, 1);
      }).catch(function(error){

      });
    },


    convert2Timestamp : function(val){
      // currentHourMinSec = GetCurrentTime();
      // completeTime = val + " " + currentHourMinSec;
      console.log("++++++++++++++++++++++设定当前时间戳");
      this.settingTimestamp = new Date(val).getTime();
    },

    showDetail : function(EWB_NO, tag){
      var compare = function (obj1, obj2) {
          var val1 = obj1["SCAN_TIME"];
          var val2 = obj2["SCAN_TIME"];
          if (val1 < val2) {
              return -1;
          } else if (val1 > val2) {
              return 1;
          } else {
              return 0;
          }
      }

      if (tag==1) {
        // TODO: 过滤掉重复数据

        // this.showDetailList = this.outDetailList[EWB_NO+""].sort(compare);
        this.showDetailList = this.filterRepertTraceData(EWB_NO,1).sort(compare);
      }
      if (tag==2) {
        this.inShowDetailList = this.filterRepertTraceData(EWB_NO,2).sort(compare);
      }
      if (tag==3) {
        this.delayShowDetailList = this.filterRepertTraceData(EWB_NO,3).sort(compare);
      }
    },

    filterRepertTraceData : function(EWB_NO, tag){
      filterDataList = [];
      setContainer = new Set();
      // 代表 out的数据过滤
      if (tag==1) {
        for (var i = 0; i < this.outDetailList[EWB_NO].length; i++) {
          obj = this.outDetailList[EWB_NO][i];
          if (!setContainer.has(obj["SCAN_TIME"])) {
              setContainer.add(obj["SCAN_TIME"]);
              filterDataList.push(this.outDetailList[EWB_NO][i]);
          }
        }
      }
      if (tag==2) {
        for (var i = 0; i < this.inDetailList[EWB_NO].length; i++) {
          obj = this.inDetailList[EWB_NO][i];
          if (!setContainer.has(obj["SCAN_TIME"])) {
              setContainer.add(obj["SCAN_TIME"]);
              filterDataList.push(this.inDetailList[EWB_NO][i]);
          }
        }
      }
      if (tag==3) {
        for (var i = 0; i < this.delayDetailList[EWB_NO].length; i++) {
          obj = this.delayDetailList[EWB_NO][i];
          if (!setContainer.has(obj["SCAN_TIME"])) {
              setContainer.add(obj["SCAN_TIME"]);
              filterDataList.push(this.delayDetailList[EWB_NO][i]);
          }
        }
      }
      return filterDataList;
    },

    pageClick : function(page, tag){
      if (tag==1) {
        this.outCurrentPage = page;
        this.outLoad(this.siteId, page);
      }
      if (tag==2) {
        this.inCurrentPage = page;
        this.inLoad(this.siteId, page);
      }
      if (tag==3) {
        this.delayCurrentPage = page;
        this.delayLoad(this.siteId, page);
      }

    },

    // 时间戳转换为日期
    add0 : function(){return m<10?'0'+m:m },
    formatDate : function(needTime)
    {
         //needTime是整数，否则要parseInt转换
           var time = new Date(needTime);
           var y = time.getFullYear();
           var m = time.getMonth()+1;
           var d = time.getDate();
           var h = time.getHours();
          var mm = time.getMinutes();
          var s = time.getSeconds();
          return y+'-'+add0(m)+'-'+add0(d)+' '+add0(h)+':'+add0(mm)+':'+add0(s);
    },
  }
})

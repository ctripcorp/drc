(window["webpackJsonp"]=window["webpackJsonp"]||[]).push([["chunk-2d22238a"],{ce4f:function(e,t,a){"use strict";a.r(t);var r=function(){var e=this,t=e.$createElement,a=e._self._c||t;return a("base-component",[a("Breadcrumb",{style:{margin:"15px 0 15px 185px",position:"fixed"}},[a("BreadcrumbItem",{attrs:{to:"/home"}},[e._v("首页")])],1),a("Content",{staticClass:"content",style:{padding:"10px",background:"#fff",margin:"50px 0 1px 185px",zIndex:"1"}},[a("Tabs",{model:{value:e.tabValue,callback:function(t){e.tabValue=t},expression:"tabValue"}},[a("TabPane",{attrs:{label:"冲突行",name:"rowsLog"}},[e.refresh?a("conflict-rows-log",{attrs:{gtid:e.gtid,searchMode:e.searchMode},on:{tabValueChanged:e.updateTabValue,gtidChanged:e.updateGtid}}):e._e()],1),a("TabPane",{attrs:{label:"冲突事务",name:"trxLog"}},[e.refresh?e._e():a("conflict-trx-log",{attrs:{gtid:e.gtid},on:{tabValueChanged:e.updateTabValue,gtidChanged:e.updateGtid,searchModeChanged:e.updateSearchMode}})],1)],1)],1)],1)},n=[],l=a("61b1"),i=function(){var e=this,t=e.$createElement,a=e._self._c||t;return a("div",{staticStyle:{padding:"1px 1px"}},[a("Row",{attrs:{gutter:10,align:"middle"}},[a("Col",{attrs:{span:"22"}},[a("Card",{attrs:{padding:5},scopedSlots:e._u([{key:"title",fn:function(){return[e._v("查询条件")]},proxy:!0}])},[a("Row",{attrs:{gutter:10}},[a("Col",{attrs:{span:"8"}},[a("Input",{attrs:{prefix:"ios-search",placeholder:"事务id"},on:{"on-enter":e.getTrxData},model:{value:e.queryParam.gtid,callback:function(t){e.$set(e.queryParam,"gtid",t)},expression:"queryParam.gtid"}})],1),a("Col",{attrs:{span:"4"}},[a("Input",{attrs:{prefix:"ios-search",placeholder:"源MHA"},on:{"on-enter":e.getTrxData},model:{value:e.queryParam.srcMhaName,callback:function(t){e.$set(e.queryParam,"srcMhaName",t)},expression:"queryParam.srcMhaName"}})],1),a("Col",{attrs:{span:"4"}},[a("Input",{attrs:{prefix:"ios-search",placeholder:"目标MHA"},on:{"on-enter":e.getTrxData},model:{value:e.queryParam.dstMhaName,callback:function(t){e.$set(e.queryParam,"dstMhaName",t)},expression:"queryParam.dstMhaName"}})],1),a("Col",{attrs:{span:"3"}},[a("DatePicker",{attrs:{type:"date",editable:e.editable,format:"yyyy-MM-dd",placeholder:"起始日期"},model:{value:e.queryParam.beginHandleTime,callback:function(t){e.$set(e.queryParam,"beginHandleTime",t)},expression:"queryParam.beginHandleTime"}})],1),a("Col",{attrs:{span:"3"}},[a("DatePicker",{attrs:{type:"date",editable:e.editable,placeholder:"结束日期"},model:{value:e.queryParam.endHandleTime,callback:function(t){e.$set(e.queryParam,"endHandleTime",t)},expression:"queryParam.endHandleTime"}})],1),a("Col",{attrs:{span:"2"}},[a("Select",{attrs:{filterable:"",clearable:"",placeholder:"执行结果"},on:{"on-change":e.getTrxData},model:{value:e.queryParam.trxResult,callback:function(t){e.$set(e.queryParam,"trxResult",t)},expression:"queryParam.trxResult"}},e._l(e.resultOpts,(function(t){return a("Option",{key:t.val,attrs:{value:t.val}},[e._v(e._s(t.name))])})),1)],1)],1)],1)],1),a("Col",{attrs:{span:"1"}},[a("Button",{attrs:{type:"primary",icon:"ios-search",loading:e.dataLoading},on:{click:e.getTrxData}},[e._v("查询")]),a("Button",{staticStyle:{"margin-top":"20px"},attrs:{icon:"md-refresh"},on:{click:e.resetParam}},[e._v("重置 ")])],1)],1),a("br"),a("Table",{attrs:{stripe:"",border:"",columns:e.columns,data:e.tableData},scopedSlots:e._u([{key:"action",fn:function(t){var r=t.row,n=t.index;return[a("Button",{staticStyle:{"margin-right":"5px"},attrs:{type:"primary",size:"small"},on:{click:function(t){return e.getLogDetail(r,n)}}},[e._v(" 详情 ")]),a("Button",{staticStyle:{"margin-right":"5px"},attrs:{type:"success",size:"small"},on:{click:function(t){return e.queryRowsLog(r,n)}}},[e._v(" 冲突行 ")])]}}])}),a("div",{staticStyle:{"text-align":"center",margin:"16px 0"}},[a("Page",{attrs:{transfer:!0,total:e.total,current:e.current,"page-size-opts":[10,20,50,100],"page-size":10,"show-total":"","show-sizer":"","show-elevator":""},on:{"update:current":function(t){e.current=t},"on-change":e.getTrxData,"on-page-size-change":e.handleChangeSize}})],1)],1)},s=[],o=(a("d3b7"),a("53ca")),u={name:"conflictTrxLog",props:{gtid:String},data:function(){return{editable:!1,clearable:!1,dataLoading:!1,queryParam:{srcMhaName:null,dstMhaName:null,gtid:this.gtid,beginHandleTime:null,endHandleTime:null,trxResult:null},tableData:[],columns:[{title:"事务ID",key:"gtid",tooltip:!0},{title:"源MHA",key:"srcMhaName",width:200},{title:"目标MHA",key:"dstMhaName",width:200},{title:"事务影响行数",key:"trxRowsNum",width:150,sortable:!0},{title:"冲突行数",key:"cflRowsNum",width:150,sortable:!0},{title:"事务提交时间",key:"handleTime",width:200,sortable:!0},{title:"执行结果",key:"trxResult",width:150,align:"center",render:function(e,t){var a=t.row,r=0===a.trxResult?"blue":"volcano",n=0===a.trxResult?"commit":"rollBack";return e("Tag",{props:{color:r}},n)}},{title:"操作",slot:"action",width:150,align:"center"}],total:0,current:1,size:10,pageSizeOpts:[10,20,50,100],resultOpts:[{name:"commit",val:0},{name:"rollBack",val:1}]}},methods:{getTrxData:function(){var e=this,t=this.queryParam.beginHandleTime,a=this.queryParam.endHandleTime,r=null===t||isNaN(t)?null:new Date(t).getTime(),n=null===a||isNaN(a)?null:new Date(a).getTime();console.log("beginTime: "+t),console.log("endTime: "+a);var l={gtId:this.queryParam.gtid,srcMhaName:this.queryParam.srcMhaName,dstMhaName:this.queryParam.dstMhaName,trxResult:this.queryParam.trxResult,pageReq:{pageSize:this.size,pageIndex:this.current}};isNaN(r)||(l.beginHandleTime=r),isNaN(n)||null===n||(l.endHandleTime=n+864e5-1);var i=this.flattenObj(l);this.dataLoading=!0,this.axios.get("/api/drc/v2/log/conflict/trx",{params:i}).then((function(t){var a=t.data,r=a.pageReq;1===a.status?e.$Message.error("查询失败"):0===a.data.length||0===r.totalCount?(e.total=0,e.current=1,e.tableData=a.data,e.$Message.warning("查询结果为空")):(e.total=r.totalCount,e.current=r.pageIndex,e.tableData=a.data,e.$Message.success("查询成功"))})).finally((function(){e.dataLoading=!1}))},flattenObj:function(e){var t={};for(var a in e)if("object"!==Object(o["a"])(e[a])||Array.isArray(e[a]))t[a]=e[a];else{var r=this.flattenObj(e[a]);for(var n in r)t[a+"."+n]=r[n]}return t},resetParam:function(){this.queryParam={srcMhaName:null,dstMhaName:null,gtid:null,beginHandleTime:null,endHandleTime:null,trxResult:null}},queryRowsLog:function(e,t){this.$emit("tabValueChanged","rowsLog"),this.$emit("gtidChanged",e.gtid),this.$emit("searchModeChanged","true")},getLogDetail:function(e,t){this.$router.push({path:"/conflictLogDetail",query:{conflictTrxLogId:e.conflictTrxLogId}})},handleChangeSize:function(e){var t=this;this.size=e,this.$nextTick((function(){t.getTrxData()}))}},created:function(){this.getTrxData()}},c=u,d=a("2877"),g=Object(d["a"])(c,i,s,!1,null,"975b9c86",null),m=g.exports,h={components:{conflictRowsLog:l["default"],conflictTrxLog:m},name:"conflictLog",data:function(){return{tabValue:"rowsLog",gtid:null,refresh:!0,searchMode:!1}},watch:{tabValue:function(e,t){this.refresh="trxLog"===t}},methods:{jump:function(e){this.tabValue=e},updateTabValue:function(e){this.tabValue=e},updateGtid:function(e){this.gtid=e},updateSearchMode:function(e){this.searchMode=e}}},p=h,f=Object(d["a"])(p,r,n,!1,null,"954cca46",null);t["default"]=f.exports}}]);
//# sourceMappingURL=chunk-2d22238a.4444e97f.js.map
(window["webpackJsonp"]=window["webpackJsonp"]||[]).push([["chunk-77a8808e","chunk-39ef8bd8","chunk-6304b0b8"],{"057f":function(t,e,r){var n=r("fc6a"),o=r("241c").f,i={}.toString,a="object"==typeof window&&window&&Object.getOwnPropertyNames?Object.getOwnPropertyNames(window):[],s=function(t){try{return o(t)}catch(e){return a.slice()}};t.exports.f=function(t){return a&&"[object Window]"==i.call(t)?s(t):o(n(t))}},"25f0":function(t,e,r){"use strict";var n=r("6eeb"),o=r("825a"),i=r("d039"),a=r("ad6d"),s="toString",c=RegExp.prototype,l=c[s],u=i((function(){return"/a/b"!=l.call({source:"a",flags:"b"})})),f=l.name!=s;(u||f)&&n(RegExp.prototype,s,(function(){var t=o(this),e=String(t.source),r=t.flags,n=String(void 0===r&&t instanceof RegExp&&!("flags"in c)?a.call(t):r);return"/"+e+"/"+n}),{unsafe:!0})},2909:function(t,e,r){"use strict";function n(t){if(Array.isArray(t)){for(var e=0,r=new Array(t.length);e<t.length;e++)r[e]=t[e];return r}}r.d(e,"a",(function(){return a}));r("a4d3"),r("e01a"),r("d28b"),r("a630"),r("e260"),r("d3b7"),r("25f0"),r("3ca3"),r("ddb0");function o(t){if(Symbol.iterator in Object(t)||"[object Arguments]"===Object.prototype.toString.call(t))return Array.from(t)}function i(){throw new TypeError("Invalid attempt to spread non-iterable instance")}function a(t){return n(t)||o(t)||i()}},"3ca3":function(t,e,r){"use strict";var n=r("6547").charAt,o=r("69f3"),i=r("7dd0"),a="String Iterator",s=o.set,c=o.getterFor(a);i(String,"String",(function(t){s(this,{type:a,string:String(t),index:0})}),(function(){var t,e=c(this),r=e.string,o=e.index;return o>=r.length?{value:void 0,done:!0}:(t=n(r,o),e.index+=t.length,{value:t,done:!1})}))},"4df4":function(t,e,r){"use strict";var n=r("0366"),o=r("7b0b"),i=r("9bdd"),a=r("e95a"),s=r("50c4"),c=r("8418"),l=r("35a1");t.exports=function(t){var e,r,u,f,d,g,p=o(t),v="function"==typeof this?this:Array,b=arguments.length,h=b>1?arguments[1]:void 0,m=void 0!==h,y=l(p),S=0;if(m&&(h=n(h,b>2?arguments[2]:void 0,2)),void 0==y||v==Array&&a(y))for(e=s(p.length),r=new v(e);e>S;S++)g=m?h(p[S],S):p[S],c(r,S,g);else for(f=y.call(p),d=f.next,r=new v;!(u=d.call(f)).done;S++)g=m?i(f,h,[u.value,S],!0):u.value,c(r,S,g);return r.length=S,r}},6547:function(t,e,r){var n=r("a691"),o=r("1d80"),i=function(t){return function(e,r){var i,a,s=String(o(e)),c=n(r),l=s.length;return c<0||c>=l?t?"":void 0:(i=s.charCodeAt(c),i<55296||i>56319||c+1===l||(a=s.charCodeAt(c+1))<56320||a>57343?t?s.charAt(c):i:t?s.slice(c,c+2):a-56320+(i-55296<<10)+65536)}};t.exports={codeAt:i(!1),charAt:i(!0)}},7358:function(t,e,r){"use strict";r("d2da")},"746f":function(t,e,r){var n=r("428f"),o=r("5135"),i=r("e538"),a=r("9bf2").f;t.exports=function(t){var e=n.Symbol||(n.Symbol={});o(e,t)||a(e,t,{value:i.f(t)})}},8418:function(t,e,r){"use strict";var n=r("c04e"),o=r("9bf2"),i=r("5c6c");t.exports=function(t,e,r){var a=n(e);a in t?o.f(t,a,i(0,r)):t[a]=r}},a4d3:function(t,e,r){"use strict";var n=r("23e7"),o=r("da84"),i=r("d066"),a=r("c430"),s=r("83ab"),c=r("4930"),l=r("fdbf"),u=r("d039"),f=r("5135"),d=r("e8b5"),g=r("861d"),p=r("825a"),v=r("7b0b"),b=r("fc6a"),h=r("c04e"),m=r("5c6c"),y=r("7c73"),S=r("df75"),x=r("241c"),L=r("057f"),w=r("7418"),T=r("06cf"),R=r("9bf2"),D=r("d1e7"),O=r("9112"),_=r("6eeb"),C=r("5692"),A=r("f772"),z=r("d012"),k=r("90e3"),I=r("b622"),E=r("e538"),j=r("746f"),P=r("d44e"),M=r("69f3"),q=r("b727").forEach,N=A("hidden"),B="Symbol",V="prototype",F=I("toPrimitive"),$=M.set,G=M.getterFor(B),Q=Object[V],H=o.Symbol,J=i("JSON","stringify"),W=T.f,K=R.f,U=L.f,X=D.f,Y=C("symbols"),Z=C("op-symbols"),tt=C("string-to-symbol-registry"),et=C("symbol-to-string-registry"),rt=C("wks"),nt=o.QObject,ot=!nt||!nt[V]||!nt[V].findChild,it=s&&u((function(){return 7!=y(K({},"a",{get:function(){return K(this,"a",{value:7}).a}})).a}))?function(t,e,r){var n=W(Q,e);n&&delete Q[e],K(t,e,r),n&&t!==Q&&K(Q,e,n)}:K,at=function(t,e){var r=Y[t]=y(H[V]);return $(r,{type:B,tag:t,description:e}),s||(r.description=e),r},st=l?function(t){return"symbol"==typeof t}:function(t){return Object(t)instanceof H},ct=function(t,e,r){t===Q&&ct(Z,e,r),p(t);var n=h(e,!0);return p(r),f(Y,n)?(r.enumerable?(f(t,N)&&t[N][n]&&(t[N][n]=!1),r=y(r,{enumerable:m(0,!1)})):(f(t,N)||K(t,N,m(1,{})),t[N][n]=!0),it(t,n,r)):K(t,n,r)},lt=function(t,e){p(t);var r=b(e),n=S(r).concat(pt(r));return q(n,(function(e){s&&!ft.call(r,e)||ct(t,e,r[e])})),t},ut=function(t,e){return void 0===e?y(t):lt(y(t),e)},ft=function(t){var e=h(t,!0),r=X.call(this,e);return!(this===Q&&f(Y,e)&&!f(Z,e))&&(!(r||!f(this,e)||!f(Y,e)||f(this,N)&&this[N][e])||r)},dt=function(t,e){var r=b(t),n=h(e,!0);if(r!==Q||!f(Y,n)||f(Z,n)){var o=W(r,n);return!o||!f(Y,n)||f(r,N)&&r[N][n]||(o.enumerable=!0),o}},gt=function(t){var e=U(b(t)),r=[];return q(e,(function(t){f(Y,t)||f(z,t)||r.push(t)})),r},pt=function(t){var e=t===Q,r=U(e?Z:b(t)),n=[];return q(r,(function(t){!f(Y,t)||e&&!f(Q,t)||n.push(Y[t])})),n};if(c||(H=function(){if(this instanceof H)throw TypeError("Symbol is not a constructor");var t=arguments.length&&void 0!==arguments[0]?String(arguments[0]):void 0,e=k(t),r=function(t){this===Q&&r.call(Z,t),f(this,N)&&f(this[N],e)&&(this[N][e]=!1),it(this,e,m(1,t))};return s&&ot&&it(Q,e,{configurable:!0,set:r}),at(e,t)},_(H[V],"toString",(function(){return G(this).tag})),_(H,"withoutSetter",(function(t){return at(k(t),t)})),D.f=ft,R.f=ct,T.f=dt,x.f=L.f=gt,w.f=pt,E.f=function(t){return at(I(t),t)},s&&(K(H[V],"description",{configurable:!0,get:function(){return G(this).description}}),a||_(Q,"propertyIsEnumerable",ft,{unsafe:!0}))),n({global:!0,wrap:!0,forced:!c,sham:!c},{Symbol:H}),q(S(rt),(function(t){j(t)})),n({target:B,stat:!0,forced:!c},{for:function(t){var e=String(t);if(f(tt,e))return tt[e];var r=H(e);return tt[e]=r,et[r]=e,r},keyFor:function(t){if(!st(t))throw TypeError(t+" is not a symbol");if(f(et,t))return et[t]},useSetter:function(){ot=!0},useSimple:function(){ot=!1}}),n({target:"Object",stat:!0,forced:!c,sham:!s},{create:ut,defineProperty:ct,defineProperties:lt,getOwnPropertyDescriptor:dt}),n({target:"Object",stat:!0,forced:!c},{getOwnPropertyNames:gt,getOwnPropertySymbols:pt}),n({target:"Object",stat:!0,forced:u((function(){w.f(1)}))},{getOwnPropertySymbols:function(t){return w.f(v(t))}}),J){var vt=!c||u((function(){var t=H();return"[null]"!=J([t])||"{}"!=J({a:t})||"{}"!=J(Object(t))}));n({target:"JSON",stat:!0,forced:vt},{stringify:function(t,e,r){var n,o=[t],i=1;while(arguments.length>i)o.push(arguments[i++]);if(n=e,(g(e)||void 0!==t)&&!st(t))return d(e)||(e=function(t,e){if("function"==typeof n&&(e=n.call(this,t,e)),!st(e))return e}),o[1]=e,J.apply(null,o)}})}H[V][F]||O(H[V],F,H[V].valueOf),P(H,B),z[N]=!0},a630:function(t,e,r){var n=r("23e7"),o=r("4df4"),i=r("1c7e"),a=!i((function(t){Array.from(t)}));n({target:"Array",stat:!0,forced:a},{from:o})},ad6d:function(t,e,r){"use strict";var n=r("825a");t.exports=function(){var t=n(this),e="";return t.global&&(e+="g"),t.ignoreCase&&(e+="i"),t.multiline&&(e+="m"),t.dotAll&&(e+="s"),t.unicode&&(e+="u"),t.sticky&&(e+="y"),e}},d28b:function(t,e,r){var n=r("746f");n("iterator")},d2da:function(t,e,r){},d7e3:function(t,e,r){"use strict";r.r(e);var n=function(){var t=this,e=t.$createElement,r=t._self._c||e;return r("div",[r("codemirror",{attrs:{options:t.options},model:{value:t.rowData,callback:function(e){t.rowData=e},expression:"rowData"}})],1)},o=[],i=r("8f94"),a=(r("0f7c"),r("ffda"),{name:"conflictRowsLogDetail",props:{rowData:String},components:{codemirror:i["codemirror"]},data:function(){return{options:{value:"",mode:"text/x-mysql",theme:"ambiance",lineWrapping:!0,height:100,readOnly:!0,lineNumbers:!0}}}}),s=a,c=(r("7358"),r("2877")),l=Object(c["a"])(s,n,o,!1,null,null,null);e["default"]=l.exports},ddb0:function(t,e,r){var n=r("da84"),o=r("fdbc"),i=r("e260"),a=r("9112"),s=r("b622"),c=s("iterator"),l=s("toStringTag"),u=i.values;for(var f in o){var d=n[f],g=d&&d.prototype;if(g){if(g[c]!==u)try{a(g,c,u)}catch(v){g[c]=u}if(g[l]||a(g,l,f),o[f])for(var p in i)if(g[p]!==i[p])try{a(g,p,i[p])}catch(v){g[p]=i[p]}}}},e01a:function(t,e,r){"use strict";var n=r("23e7"),o=r("83ab"),i=r("da84"),a=r("5135"),s=r("861d"),c=r("9bf2").f,l=r("e893"),u=i.Symbol;if(o&&"function"==typeof u&&(!("description"in u.prototype)||void 0!==u().description)){var f={},d=function(){var t=arguments.length<1||void 0===arguments[0]?void 0:String(arguments[0]),e=this instanceof d?new u(t):void 0===t?u():u(t);return""===t&&(f[e]=!0),e};l(d,u);var g=d.prototype=u.prototype;g.constructor=d;var p=g.toString,v="Symbol(test)"==String(u("test")),b=/^Symbol\((.*)\)[^)]+$/;c(g,"description",{configurable:!0,get:function(){var t=s(this)?this.valueOf():this,e=p.call(t);if(a(f,t))return"";var r=v?e.slice(7,-1):e.replace(b,"$1");return""===r?void 0:r}}),n({global:!0,forced:!0},{Symbol:d})}},e538:function(t,e,r){var n=r("b622");e.f=n},ece4:function(t,e,r){"use strict";r.r(e);var n=function(){var t=this,e=t.$createElement,r=t._self._c||e;return r("base-component",{attrs:{isFather:t.isFather,subMenuName:["1"],fatherMenu:t.fatherMenu}},[r("Breadcrumb",{style:{margin:"15px 0 15px 185px",position:"fixed"}},[r("BreadcrumbItem",{attrs:{to:"/home"}},[t._v("首页")]),r("BreadcrumbItem",{attrs:{to:"/conflictLog"}},[t._v("冲突事务")]),r("BreadcrumbItem",[t._v("冲突详情")])],1),r("Content",{staticClass:"content",style:{padding:"10px",background:"#fff",margin:"50px 0 1px 185px",zIndex:"1"}},[r("div",{style:{padding:"1px 1px",height:"100%"}},[r("Card",[r("p",{attrs:{slot:"title"},slot:"title"},[t._v(" 自动冲突处理结果 ")]),r("div",{staticClass:"ivu-list-item-meta-title"},[t._v("事务提交结果： "),r("Button",{attrs:{loading:t.logTableLoading,size:"small",type:0==t.trxLog.trxResult?"success":"error"}},[t._v(t._s(t.trxLog.trxResultStr))])],1),r("div",{staticClass:"ivu-list-item-meta-title"},[t._v("所有机房当前冲突事务记录： "),r("Tooltip",{attrs:{content:"数据一致性比对忽略字段过滤的列"}},[r("Button",{attrs:{loading:t.recordLoading,size:"small",type:1==t.trxLog.recordEqual?"success":"error"}},[t._v(t._s(t.trxLog.diffStr))])],1)],1),r("Divider"),r("div",{staticClass:"ivu-list-item-meta-title"},[t._v("源机房("+t._s(t.trxLog.srcDc)+")")]),t._l(t.srcRecords,(function(e,n){return r("Card",{key:n},[r("div",{staticClass:"ivu-list-item-meta-title"},[t._v("表名："+t._s(e.tableName)+" "),r("Tooltip",{attrs:{content:1==e.doubleSync?"双向同步":"单向同步"}},[r("Button",{attrs:{size:"small",type:1==e.doubleSync?"success":"primary"}},[t._v(t._s(1==e.doubleSync?"双向同步":"单向同步"))])],1)],1),r("Table",{attrs:{size:"small",stripe:"",columns:e.columns,data:e.records,border:""}})],1)})),r("Divider"),r("div",{staticClass:"ivu-list-item-meta-title"},[t._v("目标机房("+t._s(t.trxLog.dstDc)+")")]),t._l(t.dstRecords,(function(e,n){return r("Card",{key:n},[r("div",{staticClass:"ivu-list-item-meta-title"},[t._v("表名："+t._s(e.tableName)+" "),r("Tooltip",{attrs:{content:1==e.doubleSync?"双向同步":"单向同步"}},[r("Button",{attrs:{size:"small",type:1==e.doubleSync?"success":"primary"}},[t._v(t._s(1==e.doubleSync?"双向同步":"单向同步"))])],1)],1),r("Table",{attrs:{size:"small",stripe:"",columns:e.columns,data:e.records,border:""}})],1)}))],2),r("Divider"),r("Card",[r("p",{attrs:{slot:"title"},slot:"title"},[t._v(" 自动冲突处理流程 ")]),r("Table",{attrs:{stripe:"",loading:t.logTableLoading,columns:t.trxLog.columns,data:t.dataWithPage}}),r("div",[r("Page",{attrs:{transfer:!0,total:t.trxLog.tableData.length,current:t.trxLog.current,"page-size-opts":t.trxLog.pageSizeOpts,"page-size":this.trxLog.size,"show-total":"","show-sizer":"","show-elevator":""},on:{"update:current":function(e){return t.$set(t.trxLog,"current",e)},"on-page-size-change":t.handleChangeSize}})],1)],1)],1)])],1)},o=[],i=(r("fb6a"),r("d3b7"),r("2909")),a=r("d7e3"),s={name:"conflictLogDetail",props:{rowData:String},data:function(){return{conflictTrxLogId:0,recordLoading:!1,logTableLoading:!1,srcRecords:[],dstRecords:[],srcTable:{data:[],columns:[]},dstTable:{data:[],columns:[]},trxLog:{srcDc:"",dstDc:"",trxResult:null,trxResultStr:"",hasDiff:null,recordEqual:null,diffStr:"",tableData:[],columns:[{type:"expand",width:40,render:function(t,e){var r=e.row;return t(a["default"],{props:{rowData:"/*原始SQL*/\n"+r.rawSql+"\n/*原始SQL处理结果: "+r.rawSqlResult+"*/\n\n/*冲突时行记录*/\n"+r.dstRowRecord+"\n\n/*冲突处理SQL*/\n"+r.handleSql+"\n/*冲突处理SQL处理结果: "+r.handleSqlResult+"*/"}})}},{title:"执行顺序",key:"rowId",width:100},{title:"原始sql",key:"rawSql",tooltip:!0},{title:"处理结果",key:"rowResult",width:120,align:"center",render:function(t,e){var r=e.row,n=0===r.rowResult?"blue":"volcano",o=0===r.rowResult?"commit":"rollback";return t("Tag",{props:{color:n}},o)}}],total:0,current:1,size:10,pageSizeOpts:[10,20,50,100]}}},methods:{handleSpan:function(t){t.row,t.column,t.rowIndex;var e=t.columnIndex;if(0===e)return[0,0]},handleChangeSize:function(t){this.trxLog.size=t},getTrxLogDetail:function(){var t=this;this.logTableLoading=!0,this.axios.get("/api/drc/v2/log/conflict/detail?conflictTrxLogId="+this.conflictTrxLogId).then((function(e){if(1===e.data.status)t.$Message.error("查询冲突详情失败!");else{var r=e.data.data;t.trxLog.srcDc=r.srcDc,t.trxLog.dstDc=r.dstDc,t.trxLog.trxResult=r.trxResult,t.trxLog.trxResultStr=0===r.trxResult?"commit":"rollback",t.trxLog.tableData=r.rowsLogDetailViews}})).finally((function(){t.logTableLoading=!1}))},getTrxRecords:function(){var t=this;this.recordLoading=!0,this.axios.get("/api/drc/v2/log/conflict/records?conflictTrxLogId="+this.conflictTrxLogId+"&columnSize=12").then((function(e){if(1===e.data.status)t.trxLog.diffStr="数据比对失败";else{var r=e.data.data;t.trxLog.recordEqual=r.recordIsEqual,t.trxLog.diffStr=r.recordIsEqual?"数据一致":"数据不一致",t.srcRecords=r.srcRecords,t.dstRecords=r.dstRecords}})).finally((function(){t.recordLoading=!1}))}},computed:{dataWithPage:function(){var t=this.trxLog.tableData,e=this.trxLog.current*this.trxLog.size-this.trxLog.size,r=e+this.trxLog.size;return Object(i["a"])(t).slice(e,r)}},created:function(){this.conflictTrxLogId=this.$route.query.conflictTrxLogId,this.getTrxLogDetail(),this.getTrxRecords()}},c=s,l=r("2877"),u=Object(l["a"])(c,n,o,!1,null,"f361e29e",null);e["default"]=u.exports},fb6a:function(t,e,r){"use strict";var n=r("23e7"),o=r("861d"),i=r("e8b5"),a=r("23cb"),s=r("50c4"),c=r("fc6a"),l=r("8418"),u=r("b622"),f=r("1dde"),d=r("ae40"),g=f("slice"),p=d("slice",{ACCESSORS:!0,0:0,1:2}),v=u("species"),b=[].slice,h=Math.max;n({target:"Array",proto:!0,forced:!g||!p},{slice:function(t,e){var r,n,u,f=c(this),d=s(f.length),g=a(t,d),p=a(void 0===e?d:e,d);if(i(f)&&(r=f.constructor,"function"!=typeof r||r!==Array&&!i(r.prototype)?o(r)&&(r=r[v],null===r&&(r=void 0)):r=void 0,r===Array||void 0===r))return b.call(f,g,p);for(n=new(void 0===r?Array:r)(h(p-g,0)),u=0;g<p;g++,u++)g in f&&l(n,u,f[g]);return n.length=u,n}})},fdbc:function(t,e){t.exports={CSSRuleList:0,CSSStyleDeclaration:0,CSSValueList:0,ClientRectList:0,DOMRectList:0,DOMStringList:0,DOMTokenList:1,DataTransferItemList:0,FileList:0,HTMLAllCollection:0,HTMLCollection:0,HTMLFormElement:0,HTMLSelectElement:0,MediaList:0,MimeTypeArray:0,NamedNodeMap:0,NodeList:1,PaintRequestList:0,Plugin:0,PluginArray:0,SVGLengthList:0,SVGNumberList:0,SVGPathSegList:0,SVGPointList:0,SVGStringList:0,SVGTransformList:0,SourceBufferList:0,StyleSheetList:0,TextTrackCueList:0,TextTrackList:0,TouchList:0}}}]);
//# sourceMappingURL=chunk-77a8808e.1bf8959f.js.map
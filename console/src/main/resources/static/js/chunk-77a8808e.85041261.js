(window["webpackJsonp"]=window["webpackJsonp"]||[]).push([["chunk-77a8808e","chunk-39ef8bd8","chunk-6304b0b8"],{"057f":function(t,r,e){var n=e("fc6a"),o=e("241c").f,i={}.toString,a="object"==typeof window&&window&&Object.getOwnPropertyNames?Object.getOwnPropertyNames(window):[],s=function(t){try{return o(t)}catch(r){return a.slice()}};t.exports.f=function(t){return a&&"[object Window]"==i.call(t)?s(t):o(n(t))}},"25f0":function(t,r,e){"use strict";var n=e("6eeb"),o=e("825a"),i=e("d039"),a=e("ad6d"),s="toString",c=RegExp.prototype,l=c[s],u=i((function(){return"/a/b"!=l.call({source:"a",flags:"b"})})),f=l.name!=s;(u||f)&&n(RegExp.prototype,s,(function(){var t=o(this),r=String(t.source),e=t.flags,n=String(void 0===e&&t instanceof RegExp&&!("flags"in c)?a.call(t):e);return"/"+r+"/"+n}),{unsafe:!0})},2909:function(t,r,e){"use strict";function n(t){if(Array.isArray(t)){for(var r=0,e=new Array(t.length);r<t.length;r++)e[r]=t[r];return e}}e.d(r,"a",(function(){return a}));e("a4d3"),e("e01a"),e("d28b"),e("a630"),e("e260"),e("d3b7"),e("25f0"),e("3ca3"),e("ddb0");function o(t){if(Symbol.iterator in Object(t)||"[object Arguments]"===Object.prototype.toString.call(t))return Array.from(t)}function i(){throw new TypeError("Invalid attempt to spread non-iterable instance")}function a(t){return n(t)||o(t)||i()}},"3ca3":function(t,r,e){"use strict";var n=e("6547").charAt,o=e("69f3"),i=e("7dd0"),a="String Iterator",s=o.set,c=o.getterFor(a);i(String,"String",(function(t){s(this,{type:a,string:String(t),index:0})}),(function(){var t,r=c(this),e=r.string,o=r.index;return o>=e.length?{value:void 0,done:!0}:(t=n(e,o),r.index+=t.length,{value:t,done:!1})}))},"4df4":function(t,r,e){"use strict";var n=e("0366"),o=e("7b0b"),i=e("9bdd"),a=e("e95a"),s=e("50c4"),c=e("8418"),l=e("35a1");t.exports=function(t){var r,e,u,f,d,g,p=o(t),v="function"==typeof this?this:Array,h=arguments.length,b=h>1?arguments[1]:void 0,m=void 0!==b,y=l(p),x=0;if(m&&(b=n(b,h>2?arguments[2]:void 0,2)),void 0==y||v==Array&&a(y))for(r=s(p.length),e=new v(r);r>x;x++)g=m?b(p[x],x):p[x],c(e,x,g);else for(f=y.call(p),d=f.next,e=new v;!(u=d.call(f)).done;x++)g=m?i(f,b,[u.value,x],!0):u.value,c(e,x,g);return e.length=x,e}},6547:function(t,r,e){var n=e("a691"),o=e("1d80"),i=function(t){return function(r,e){var i,a,s=String(o(r)),c=n(e),l=s.length;return c<0||c>=l?t?"":void 0:(i=s.charCodeAt(c),i<55296||i>56319||c+1===l||(a=s.charCodeAt(c+1))<56320||a>57343?t?s.charAt(c):i:t?s.slice(c,c+2):a-56320+(i-55296<<10)+65536)}};t.exports={codeAt:i(!1),charAt:i(!0)}},7358:function(t,r,e){"use strict";e("d2da")},"746f":function(t,r,e){var n=e("428f"),o=e("5135"),i=e("e538"),a=e("9bf2").f;t.exports=function(t){var r=n.Symbol||(n.Symbol={});o(r,t)||a(r,t,{value:i.f(t)})}},8418:function(t,r,e){"use strict";var n=e("c04e"),o=e("9bf2"),i=e("5c6c");t.exports=function(t,r,e){var a=n(r);a in t?o.f(t,a,i(0,e)):t[a]=e}},a4d3:function(t,r,e){"use strict";var n=e("23e7"),o=e("da84"),i=e("d066"),a=e("c430"),s=e("83ab"),c=e("4930"),l=e("fdbf"),u=e("d039"),f=e("5135"),d=e("e8b5"),g=e("861d"),p=e("825a"),v=e("7b0b"),h=e("fc6a"),b=e("c04e"),m=e("5c6c"),y=e("7c73"),x=e("df75"),S=e("241c"),L=e("057f"),w=e("7418"),T=e("06cf"),D=e("9bf2"),R=e("d1e7"),O=e("9112"),A=e("6eeb"),C=e("5692"),_=e("f772"),k=e("d012"),I=e("90e3"),z=e("b622"),E=e("e538"),j=e("746f"),P=e("d44e"),M=e("69f3"),q=e("b727").forEach,N=_("hidden"),V="Symbol",B="prototype",F=z("toPrimitive"),$=M.set,G=M.getterFor(V),Q=Object[B],H=o.Symbol,J=i("JSON","stringify"),W=T.f,K=D.f,U=L.f,X=R.f,Y=C("symbols"),Z=C("op-symbols"),tt=C("string-to-symbol-registry"),rt=C("symbol-to-string-registry"),et=C("wks"),nt=o.QObject,ot=!nt||!nt[B]||!nt[B].findChild,it=s&&u((function(){return 7!=y(K({},"a",{get:function(){return K(this,"a",{value:7}).a}})).a}))?function(t,r,e){var n=W(Q,r);n&&delete Q[r],K(t,r,e),n&&t!==Q&&K(Q,r,n)}:K,at=function(t,r){var e=Y[t]=y(H[B]);return $(e,{type:V,tag:t,description:r}),s||(e.description=r),e},st=l?function(t){return"symbol"==typeof t}:function(t){return Object(t)instanceof H},ct=function(t,r,e){t===Q&&ct(Z,r,e),p(t);var n=b(r,!0);return p(e),f(Y,n)?(e.enumerable?(f(t,N)&&t[N][n]&&(t[N][n]=!1),e=y(e,{enumerable:m(0,!1)})):(f(t,N)||K(t,N,m(1,{})),t[N][n]=!0),it(t,n,e)):K(t,n,e)},lt=function(t,r){p(t);var e=h(r),n=x(e).concat(pt(e));return q(n,(function(r){s&&!ft.call(e,r)||ct(t,r,e[r])})),t},ut=function(t,r){return void 0===r?y(t):lt(y(t),r)},ft=function(t){var r=b(t,!0),e=X.call(this,r);return!(this===Q&&f(Y,r)&&!f(Z,r))&&(!(e||!f(this,r)||!f(Y,r)||f(this,N)&&this[N][r])||e)},dt=function(t,r){var e=h(t),n=b(r,!0);if(e!==Q||!f(Y,n)||f(Z,n)){var o=W(e,n);return!o||!f(Y,n)||f(e,N)&&e[N][n]||(o.enumerable=!0),o}},gt=function(t){var r=U(h(t)),e=[];return q(r,(function(t){f(Y,t)||f(k,t)||e.push(t)})),e},pt=function(t){var r=t===Q,e=U(r?Z:h(t)),n=[];return q(e,(function(t){!f(Y,t)||r&&!f(Q,t)||n.push(Y[t])})),n};if(c||(H=function(){if(this instanceof H)throw TypeError("Symbol is not a constructor");var t=arguments.length&&void 0!==arguments[0]?String(arguments[0]):void 0,r=I(t),e=function(t){this===Q&&e.call(Z,t),f(this,N)&&f(this[N],r)&&(this[N][r]=!1),it(this,r,m(1,t))};return s&&ot&&it(Q,r,{configurable:!0,set:e}),at(r,t)},A(H[B],"toString",(function(){return G(this).tag})),A(H,"withoutSetter",(function(t){return at(I(t),t)})),R.f=ft,D.f=ct,T.f=dt,S.f=L.f=gt,w.f=pt,E.f=function(t){return at(z(t),t)},s&&(K(H[B],"description",{configurable:!0,get:function(){return G(this).description}}),a||A(Q,"propertyIsEnumerable",ft,{unsafe:!0}))),n({global:!0,wrap:!0,forced:!c,sham:!c},{Symbol:H}),q(x(et),(function(t){j(t)})),n({target:V,stat:!0,forced:!c},{for:function(t){var r=String(t);if(f(tt,r))return tt[r];var e=H(r);return tt[r]=e,rt[e]=r,e},keyFor:function(t){if(!st(t))throw TypeError(t+" is not a symbol");if(f(rt,t))return rt[t]},useSetter:function(){ot=!0},useSimple:function(){ot=!1}}),n({target:"Object",stat:!0,forced:!c,sham:!s},{create:ut,defineProperty:ct,defineProperties:lt,getOwnPropertyDescriptor:dt}),n({target:"Object",stat:!0,forced:!c},{getOwnPropertyNames:gt,getOwnPropertySymbols:pt}),n({target:"Object",stat:!0,forced:u((function(){w.f(1)}))},{getOwnPropertySymbols:function(t){return w.f(v(t))}}),J){var vt=!c||u((function(){var t=H();return"[null]"!=J([t])||"{}"!=J({a:t})||"{}"!=J(Object(t))}));n({target:"JSON",stat:!0,forced:vt},{stringify:function(t,r,e){var n,o=[t],i=1;while(arguments.length>i)o.push(arguments[i++]);if(n=r,(g(r)||void 0!==t)&&!st(t))return d(r)||(r=function(t,r){if("function"==typeof n&&(r=n.call(this,t,r)),!st(r))return r}),o[1]=r,J.apply(null,o)}})}H[B][F]||O(H[B],F,H[B].valueOf),P(H,V),k[N]=!0},a630:function(t,r,e){var n=e("23e7"),o=e("4df4"),i=e("1c7e"),a=!i((function(t){Array.from(t)}));n({target:"Array",stat:!0,forced:a},{from:o})},ad6d:function(t,r,e){"use strict";var n=e("825a");t.exports=function(){var t=n(this),r="";return t.global&&(r+="g"),t.ignoreCase&&(r+="i"),t.multiline&&(r+="m"),t.dotAll&&(r+="s"),t.unicode&&(r+="u"),t.sticky&&(r+="y"),r}},d28b:function(t,r,e){var n=e("746f");n("iterator")},d2da:function(t,r,e){},d7e3:function(t,r,e){"use strict";e.r(r);var n=function(){var t=this,r=t.$createElement,e=t._self._c||r;return e("div",[e("codemirror",{attrs:{options:t.options},model:{value:t.rowData,callback:function(r){t.rowData=r},expression:"rowData"}})],1)},o=[],i=e("8f94"),a=(e("0f7c"),e("ffda"),{name:"conflictRowsLogDetail",props:{rowData:String},components:{codemirror:i["codemirror"]},data:function(){return{options:{value:"",mode:"text/x-mysql",theme:"ambiance",lineWrapping:!0,height:100,readOnly:!0,lineNumbers:!0}}}}),s=a,c=(e("7358"),e("2877")),l=Object(c["a"])(s,n,o,!1,null,null,null);r["default"]=l.exports},ddb0:function(t,r,e){var n=e("da84"),o=e("fdbc"),i=e("e260"),a=e("9112"),s=e("b622"),c=s("iterator"),l=s("toStringTag"),u=i.values;for(var f in o){var d=n[f],g=d&&d.prototype;if(g){if(g[c]!==u)try{a(g,c,u)}catch(v){g[c]=u}if(g[l]||a(g,l,f),o[f])for(var p in i)if(g[p]!==i[p])try{a(g,p,i[p])}catch(v){g[p]=i[p]}}}},e01a:function(t,r,e){"use strict";var n=e("23e7"),o=e("83ab"),i=e("da84"),a=e("5135"),s=e("861d"),c=e("9bf2").f,l=e("e893"),u=i.Symbol;if(o&&"function"==typeof u&&(!("description"in u.prototype)||void 0!==u().description)){var f={},d=function(){var t=arguments.length<1||void 0===arguments[0]?void 0:String(arguments[0]),r=this instanceof d?new u(t):void 0===t?u():u(t);return""===t&&(f[r]=!0),r};l(d,u);var g=d.prototype=u.prototype;g.constructor=d;var p=g.toString,v="Symbol(test)"==String(u("test")),h=/^Symbol\((.*)\)[^)]+$/;c(g,"description",{configurable:!0,get:function(){var t=s(this)?this.valueOf():this,r=p.call(t);if(a(f,t))return"";var e=v?r.slice(7,-1):r.replace(h,"$1");return""===e?void 0:e}}),n({global:!0,forced:!0},{Symbol:d})}},e538:function(t,r,e){var n=e("b622");r.f=n},ece4:function(t,r,e){"use strict";e.r(r);var n=function(){var t=this,r=t.$createElement,e=t._self._c||r;return e("base-component",{attrs:{isFather:t.isFather,subMenuName:["1"],fatherMenu:t.fatherMenu}},[e("Breadcrumb",{style:{margin:"15px 0 15px 185px",position:"fixed"}},[e("BreadcrumbItem",{attrs:{to:"/home"}},[t._v("首页")]),e("BreadcrumbItem",{attrs:{to:"/conflictLog"}},[t._v("冲突事务")]),e("BreadcrumbItem",[t._v("冲突详情")])],1),e("Content",{staticClass:"content",style:{padding:"10px",background:"#fff",margin:"50px 0 1px 185px",zIndex:"1"}},[e("div",{style:{padding:"1px 1px",height:"100%"}},[e("Card",[e("p",{attrs:{slot:"title"},slot:"title"},[t._v(" 自动冲突处理结果 ")]),e("div",{staticClass:"ivu-list-item-meta-title"},[t._v("事务提交结果： "),e("Button",{attrs:{loading:t.logTableLoading,size:"small",type:0==t.trxLog.trxResult?"success":"error"}},[t._v(t._s(t.trxLog.trxResultStr))])],1),e("div",{staticClass:"ivu-list-item-meta-title"},[t._v("所有机房当前冲突事务记录： "),e("Tooltip",{attrs:{content:"数据一致性比对忽略字段过滤的列"}},[e("Button",{attrs:{loading:t.recordLoading,size:"small",type:1==t.trxLog.recordEqual?"success":"error"}},[t._v(t._s(t.trxLog.diffStr))])],1)],1),e("Divider"),e("div",{staticClass:"ivu-list-item-meta-title"},[t._v("源机房("+t._s(t.trxLog.srcDc)+")")]),t._l(t.srcRecords,(function(t,r){return e("Table",{key:r,attrs:{size:"small",stripe:"",columns:t.columns,data:t.records,border:""}})})),e("Divider"),e("div",{staticClass:"ivu-list-item-meta-title"},[t._v("目标机房("+t._s(t.trxLog.dstDc)+")")]),t._l(t.dstRecords,(function(t,r){return e("Table",{key:r,attrs:{size:"small",stripe:"",columns:t.columns,data:t.records,border:""}})})),e("Divider")],2),e("Divider"),e("Card",[e("p",{attrs:{slot:"title"},slot:"title"},[t._v(" 自动冲突处理流程 ")]),e("Table",{attrs:{stripe:"",loading:t.logTableLoading,columns:t.trxLog.columns,data:t.dataWithPage}}),e("div",[e("Page",{attrs:{transfer:!0,total:t.trxLog.tableData.length,current:t.trxLog.current,"page-size-opts":t.trxLog.pageSizeOpts,"page-size":this.trxLog.size,"show-total":"","show-sizer":"","show-elevator":""},on:{"update:current":function(r){return t.$set(t.trxLog,"current",r)},"on-page-size-change":t.handleChangeSize}})],1)],1)],1)])],1)},o=[],i=(e("fb6a"),e("d3b7"),e("2909")),a=e("d7e3"),s={name:"conflictLogDetail",props:{rowData:String},data:function(){return{conflictTrxLogId:0,recordLoading:!1,logTableLoading:!1,srcRecords:[],dstRecords:[],srcTable:{data:[],columns:[]},dstTable:{data:[],columns:[]},trxLog:{srcDc:"",dstDc:"",trxResult:null,trxResultStr:"",hasDiff:null,recordEqual:null,diffStr:"",tableData:[],columns:[{type:"expand",width:40,render:function(t,r){var e=r.row;return t(a["default"],{props:{rowData:"/*原始SQL*/\n"+e.rawSql+"\n/*原始SQL处理结果: "+e.rawSqlResult+"*/\n\n/*冲突时行记录*/\n"+e.dstRowRecord+"\n\n/*冲突处理SQL*/\n"+e.handleSql+"\n/*冲突处理SQL处理结果: "+e.handleSqlResult+"*/"}})}},{title:"执行顺序",key:"rowId",width:100},{title:"原始sql",key:"rawSql",tooltip:!0},{title:"处理结果",key:"rowResult",width:120,align:"center",render:function(t,r){var e=r.row,n=0===e.rowResult?"blue":"volcano",o=0===e.rowResult?"commit":"rollback";return t("Tag",{props:{color:n}},o)}}],total:0,current:1,size:10,pageSizeOpts:[10,20,50,100]}}},methods:{handleSpan:function(t){t.row,t.column,t.rowIndex;var r=t.columnIndex;if(0===r)return[0,0]},handleChangeSize:function(t){this.trxLog.size=t},getTrxLogDetail:function(){var t=this;this.logTableLoading=!0,this.axios.get("/api/drc/v2/log/conflict/detail?conflictTrxLogId="+this.conflictTrxLogId).then((function(r){if(1===r.data.status)t.$Message.error("查询冲突详情失败!");else{var e=r.data.data;t.trxLog.srcDc=e.srcDc,t.trxLog.dstDc=e.dstDc,t.trxLog.trxResult=e.trxResult,t.trxLog.trxResultStr=0===e.trxResult?"commit":"rollback",t.trxLog.tableData=e.rowsLogDetailViews}})).finally((function(){t.logTableLoading=!1}))},getTrxRecords:function(){var t=this;this.recordLoading=!0,this.axios.get("/api/drc/v2/log/conflict/records?conflictTrxLogId="+this.conflictTrxLogId).then((function(r){if(1===r.data.status)t.trxLog.diffStr="数据比对失败";else{var e=r.data.data;t.trxLog.recordEqual=e.recordIsEqual,t.trxLog.diffStr=e.recordIsEqual?"数据一致":"数据不一致",t.srcRecords=e.srcRecords,t.dstRecords=e.dstRecords}})).finally((function(){t.recordLoading=!1}))}},computed:{dataWithPage:function(){var t=this.trxLog.tableData,r=this.trxLog.current*this.trxLog.size-this.trxLog.size,e=r+this.trxLog.size;return Object(i["a"])(t).slice(r,e)}},created:function(){this.conflictTrxLogId=this.$route.query.conflictTrxLogId,this.getTrxLogDetail(),this.getTrxRecords()}},c=s,l=e("2877"),u=Object(l["a"])(c,n,o,!1,null,"8e94a238",null);r["default"]=u.exports},fb6a:function(t,r,e){"use strict";var n=e("23e7"),o=e("861d"),i=e("e8b5"),a=e("23cb"),s=e("50c4"),c=e("fc6a"),l=e("8418"),u=e("b622"),f=e("1dde"),d=e("ae40"),g=f("slice"),p=d("slice",{ACCESSORS:!0,0:0,1:2}),v=u("species"),h=[].slice,b=Math.max;n({target:"Array",proto:!0,forced:!g||!p},{slice:function(t,r){var e,n,u,f=c(this),d=s(f.length),g=a(t,d),p=a(void 0===r?d:r,d);if(i(f)&&(e=f.constructor,"function"!=typeof e||e!==Array&&!i(e.prototype)?o(e)&&(e=e[v],null===e&&(e=void 0)):e=void 0,e===Array||void 0===e))return h.call(f,g,p);for(n=new(void 0===e?Array:e)(b(p-g,0)),u=0;g<p;g++,u++)g in f&&l(n,u,f[g]);return n.length=u,n}})},fdbc:function(t,r){t.exports={CSSRuleList:0,CSSStyleDeclaration:0,CSSValueList:0,ClientRectList:0,DOMRectList:0,DOMStringList:0,DOMTokenList:1,DataTransferItemList:0,FileList:0,HTMLAllCollection:0,HTMLCollection:0,HTMLFormElement:0,HTMLSelectElement:0,MediaList:0,MimeTypeArray:0,NamedNodeMap:0,NodeList:1,PaintRequestList:0,Plugin:0,PluginArray:0,SVGLengthList:0,SVGNumberList:0,SVGPathSegList:0,SVGPointList:0,SVGStringList:0,SVGTransformList:0,SourceBufferList:0,StyleSheetList:0,TextTrackCueList:0,TextTrackList:0,TouchList:0}}}]);
//# sourceMappingURL=chunk-77a8808e.85041261.js.map
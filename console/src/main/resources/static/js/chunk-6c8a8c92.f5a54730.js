(window["webpackJsonp"]=window["webpackJsonp"]||[]).push([["chunk-6c8a8c92"],{1276:function(t,e,r){"use strict";var n=r("d784"),a=r("44e7"),i=r("825a"),o=r("1d80"),s=r("4840"),c=r("8aa5"),l=r("50c4"),u=r("14c3"),h=r("9263"),p=r("d039"),f=[].push,d=Math.min,g=4294967295,m=!p((function(){return!RegExp(g,"y")}));n("split",2,(function(t,e,r){var n;return n="c"=="abbc".split(/(b)*/)[1]||4!="test".split(/(?:)/,-1).length||2!="ab".split(/(?:ab)*/).length||4!=".".split(/(.?)(.?)/).length||".".split(/()()/).length>1||"".split(/.?/).length?function(t,r){var n=String(o(this)),i=void 0===r?g:r>>>0;if(0===i)return[];if(void 0===t)return[n];if(!a(t))return e.call(n,t,i);var s,c,l,u=[],p=(t.ignoreCase?"i":"")+(t.multiline?"m":"")+(t.unicode?"u":"")+(t.sticky?"y":""),d=0,m=new RegExp(t.source,p+"g");while(s=h.call(m,n)){if(c=m.lastIndex,c>d&&(u.push(n.slice(d,s.index)),s.length>1&&s.index<n.length&&f.apply(u,s.slice(1)),l=s[0].length,d=c,u.length>=i))break;m.lastIndex===s.index&&m.lastIndex++}return d===n.length?!l&&m.test("")||u.push(""):u.push(n.slice(d)),u.length>i?u.slice(0,i):u}:"0".split(void 0,0).length?function(t,r){return void 0===t&&0===r?[]:e.call(this,t,r)}:e,[function(e,r){var a=o(this),i=void 0==e?void 0:e[t];return void 0!==i?i.call(e,a,r):n.call(String(a),e,r)},function(t,a){var o=r(n,t,this,a,n!==e);if(o.done)return o.value;var h=i(t),p=String(this),f=s(h,RegExp),v=h.unicode,y=(h.ignoreCase?"i":"")+(h.multiline?"m":"")+(h.unicode?"u":"")+(m?"y":"g"),w=new f(m?h:"^(?:"+h.source+")",y),x=void 0===a?g:a>>>0;if(0===x)return[];if(0===p.length)return null===u(w,p)?[p]:[];var b=0,k=0,D=[];while(k<p.length){w.lastIndex=m?k:0;var L,M=u(w,m?p:p.slice(k));if(null===M||(L=d(l(w.lastIndex+(m?0:k)),p.length))===b)k=c(p,k,v);else{if(D.push(p.slice(b,k)),D.length===x)return D;for(var E=1;E<=M.length-1;E++)if(D.push(M[E]),D.length===x)return D;k=b=L}}return D.push(p.slice(b)),D}]}),!m)},"145a":function(t,e,r){},"14c3":function(t,e,r){var n=r("c6b6"),a=r("9263");t.exports=function(t,e){var r=t.exec;if("function"===typeof r){var i=r.call(t,e);if("object"!==typeof i)throw TypeError("RegExp exec method returned something other than an Object or null");return i}if("RegExp"!==n(t))throw TypeError("RegExp#exec called on incompatible receiver");return a.call(t,e)}},"1da1":function(t,e,r){"use strict";r.d(e,"a",(function(){return a}));r("d3b7"),r("e6cf");function n(t,e,r,n,a,i,o){try{var s=t[i](o),c=s.value}catch(l){return void r(l)}s.done?e(c):Promise.resolve(c).then(n,a)}function a(t){return function(){var e=this,r=arguments;return new Promise((function(a,i){var o=t.apply(e,r);function s(t){n(o,a,i,s,c,"next",t)}function c(t){n(o,a,i,s,c,"throw",t)}s(void 0)}))}}},"44e7":function(t,e,r){var n=r("861d"),a=r("c6b6"),i=r("b622"),o=i("match");t.exports=function(t){var e;return n(t)&&(void 0!==(e=t[o])?!!e:"RegExp"==a(t))}},7252:function(t,e,r){"use strict";t.exports=function(t){if("number"!==typeof t)throw new TypeError("Expected a number");var e=t>0?Math.floor:Math.ceil;return{days:e(t/864e5),hours:e(t/36e5)%24,minutes:e(t/6e4)%60,seconds:e(t/1e3)%60,milliseconds:e(t)%1e3}}},"86aa":function(t){t.exports=JSON.parse('{"addendum":"addenda","aircraft":"aircraft","alga":"algae","alumna":"alumnae","alumnus":"alumni","amoeba":"amoebae","analysis":"analyses","antenna":"antennae","antithesis":"antitheses","apex":"apices","appendix":"appendices","automaton":"automata","axis":"axes","bacillus":"bacilli","bacterium":"bacteria","barracks":"barracks","basis":"bases","beau":"beaux","bison":"bison","buffalo":"buffalo","bureau":"bureaus","cactus":"cacti","calf":"calves","carp":"carp","census":"censuses","chassis":"chassis","cherub":"cherubim","child":"children","château":"châteaus","cod":"cod","codex":"codices","concerto":"concerti","corpus":"corpora","crisis":"crises","criterion":"criteria","curriculum":"curricula","datum":"data","deer":"deer","diagnosis":"diagnoses","die":"dice","dwarf":"dwarfs","echo":"echoes","elf":"elves","elk":"elk","ellipsis":"ellipses","embargo":"embargoes","emphasis":"emphases","erratum":"errata","faux pas":"faux pas","fez":"fezes","firmware":"firmware","fish":"fish","focus":"foci","foot":"feet","formula":"formulae","fungus":"fungi","gallows":"gallows","genus":"genera","goose":"geese","graffito":"graffiti","grouse":"grouse","half":"halves","hero":"heroes","hoof":"hooves","hovercraft":"hovercraft","hypothesis":"hypotheses","index":"indices","kakapo":"kakapo","knife":"knives","larva":"larvae","leaf":"leaves","libretto":"libretti","life":"lives","loaf":"loaves","locus":"loci","louse":"lice","man":"men","matrix":"matrices","means":"means","medium":"media","memorandum":"memoranda","millennium":"millennia","minutia":"minutiae","moose":"moose","mouse":"mice","nebula":"nebulae","nemesis":"nemeses","neurosis":"neuroses","news":"news","nucleus":"nuclei","oasis":"oases","offspring":"offspring","opus":"opera","ovum":"ova","ox":"oxen","paralysis":"paralyses","parenthesis":"parentheses","person":"people","phenomenon":"phenomena","phylum":"phyla","pike":"pike","polyhedron":"polyhedra","potato":"potatoes","prognosis":"prognoses","quiz":"quizzes","radius":"radii","referendum":"referenda","salmon":"salmon","scarf":"scarves","self":"selves","series":"series","sheep":"sheep","shelf":"shelves","shrimp":"shrimp","spacecraft":"spacecraft","species":"species","spectrum":"spectra","squid":"squid","stimulus":"stimuli","stratum":"strata","swine":"swine","syllabus":"syllabi","symposium":"symposia","synopsis":"synopses","synthesis":"syntheses","tableau":"tableaus","that":"those","thesis":"theses","thief":"thieves","this":"these","tomato":"tomatoes","tooth":"teeth","trout":"trout","tuna":"tuna","vertebra":"vertebrae","vertex":"vertices","veto":"vetoes","vita":"vitae","vortex":"vortices","watercraft":"watercraft","wharf":"wharves","wife":"wives","wolf":"wolves","woman":"women"}')},"8aa5":function(t,e,r){"use strict";var n=r("6547").charAt;t.exports=function(t,e,r){return e+(r?n(t,e).length:1)}},9263:function(t,e,r){"use strict";var n=r("ad6d"),a=r("9f7f"),i=RegExp.prototype.exec,o=String.prototype.replace,s=i,c=function(){var t=/a/,e=/b*/g;return i.call(t,"a"),i.call(e,"a"),0!==t.lastIndex||0!==e.lastIndex}(),l=a.UNSUPPORTED_Y||a.BROKEN_CARET,u=void 0!==/()??/.exec("")[1],h=c||u||l;h&&(s=function(t){var e,r,a,s,h=this,p=l&&h.sticky,f=n.call(h),d=h.source,g=0,m=t;return p&&(f=f.replace("y",""),-1===f.indexOf("g")&&(f+="g"),m=String(t).slice(h.lastIndex),h.lastIndex>0&&(!h.multiline||h.multiline&&"\n"!==t[h.lastIndex-1])&&(d="(?: "+d+")",m=" "+m,g++),r=new RegExp("^(?:"+d+")",f)),u&&(r=new RegExp("^"+d+"$(?!\\s)",f)),c&&(e=h.lastIndex),a=i.call(p?r:h,m),p?a?(a.input=a.input.slice(g),a[0]=a[0].slice(g),a.index=h.lastIndex,h.lastIndex+=a[0].length):h.lastIndex=0:c&&a&&(h.lastIndex=h.global?a.index+a[0].length:e),u&&a&&a.length>1&&o.call(a[0],r,(function(){for(s=1;s<arguments.length-2;s++)void 0===arguments[s]&&(a[s]=void 0)})),a}),t.exports=s},"96cf":function(t,e,r){var n=function(t){"use strict";var e,r=Object.prototype,n=r.hasOwnProperty,a="function"===typeof Symbol?Symbol:{},i=a.iterator||"@@iterator",o=a.asyncIterator||"@@asyncIterator",s=a.toStringTag||"@@toStringTag";function c(t,e,r,n){var a=e&&e.prototype instanceof g?e:g,i=Object.create(a.prototype),o=new _(n||[]);return i._invoke=L(t,r,o),i}function l(t,e,r){try{return{type:"normal",arg:t.call(e,r)}}catch(n){return{type:"throw",arg:n}}}t.wrap=c;var u="suspendedStart",h="suspendedYield",p="executing",f="completed",d={};function g(){}function m(){}function v(){}var y={};y[i]=function(){return this};var w=Object.getPrototypeOf,x=w&&w(w(S([])));x&&x!==r&&n.call(x,i)&&(y=x);var b=v.prototype=g.prototype=Object.create(y);function k(t){["next","throw","return"].forEach((function(e){t[e]=function(t){return this._invoke(e,t)}}))}function D(t){function e(r,a,i,o){var s=l(t[r],t,a);if("throw"!==s.type){var c=s.arg,u=c.value;return u&&"object"===typeof u&&n.call(u,"__await")?Promise.resolve(u.__await).then((function(t){e("next",t,i,o)}),(function(t){e("throw",t,i,o)})):Promise.resolve(u).then((function(t){c.value=t,i(c)}),(function(t){return e("throw",t,i,o)}))}o(s.arg)}var r;function a(t,n){function a(){return new Promise((function(r,a){e(t,n,r,a)}))}return r=r?r.then(a,a):a()}this._invoke=a}function L(t,e,r){var n=u;return function(a,i){if(n===p)throw new Error("Generator is already running");if(n===f){if("throw"===a)throw i;return R()}r.method=a,r.arg=i;while(1){var o=r.delegate;if(o){var s=M(o,r);if(s){if(s===d)continue;return s}}if("next"===r.method)r.sent=r._sent=r.arg;else if("throw"===r.method){if(n===u)throw n=f,r.arg;r.dispatchException(r.arg)}else"return"===r.method&&r.abrupt("return",r.arg);n=p;var c=l(t,e,r);if("normal"===c.type){if(n=r.done?f:h,c.arg===d)continue;return{value:c.arg,done:r.done}}"throw"===c.type&&(n=f,r.method="throw",r.arg=c.arg)}}}function M(t,r){var n=t.iterator[r.method];if(n===e){if(r.delegate=null,"throw"===r.method){if(t.iterator["return"]&&(r.method="return",r.arg=e,M(t,r),"throw"===r.method))return d;r.method="throw",r.arg=new TypeError("The iterator does not provide a 'throw' method")}return d}var a=l(n,t.iterator,r.arg);if("throw"===a.type)return r.method="throw",r.arg=a.arg,r.delegate=null,d;var i=a.arg;return i?i.done?(r[t.resultName]=i.value,r.next=t.nextLoc,"return"!==r.method&&(r.method="next",r.arg=e),r.delegate=null,d):i:(r.method="throw",r.arg=new TypeError("iterator result is not an object"),r.delegate=null,d)}function E(t){var e={tryLoc:t[0]};1 in t&&(e.catchLoc=t[1]),2 in t&&(e.finallyLoc=t[2],e.afterLoc=t[3]),this.tryEntries.push(e)}function T(t){var e=t.completion||{};e.type="normal",delete e.arg,t.completion=e}function _(t){this.tryEntries=[{tryLoc:"root"}],t.forEach(E,this),this.reset(!0)}function S(t){if(t){var r=t[i];if(r)return r.call(t);if("function"===typeof t.next)return t;if(!isNaN(t.length)){var a=-1,o=function r(){while(++a<t.length)if(n.call(t,a))return r.value=t[a],r.done=!1,r;return r.value=e,r.done=!0,r};return o.next=o}}return{next:R}}function R(){return{value:e,done:!0}}return m.prototype=b.constructor=v,v.constructor=m,v[s]=m.displayName="GeneratorFunction",t.isGeneratorFunction=function(t){var e="function"===typeof t&&t.constructor;return!!e&&(e===m||"GeneratorFunction"===(e.displayName||e.name))},t.mark=function(t){return Object.setPrototypeOf?Object.setPrototypeOf(t,v):(t.__proto__=v,s in t||(t[s]="GeneratorFunction")),t.prototype=Object.create(b),t},t.awrap=function(t){return{__await:t}},k(D.prototype),D.prototype[o]=function(){return this},t.AsyncIterator=D,t.async=function(e,r,n,a){var i=new D(c(e,r,n,a));return t.isGeneratorFunction(r)?i:i.next().then((function(t){return t.done?t.value:i.next()}))},k(b),b[s]="Generator",b[i]=function(){return this},b.toString=function(){return"[object Generator]"},t.keys=function(t){var e=[];for(var r in t)e.push(r);return e.reverse(),function r(){while(e.length){var n=e.pop();if(n in t)return r.value=n,r.done=!1,r}return r.done=!0,r}},t.values=S,_.prototype={constructor:_,reset:function(t){if(this.prev=0,this.next=0,this.sent=this._sent=e,this.done=!1,this.delegate=null,this.method="next",this.arg=e,this.tryEntries.forEach(T),!t)for(var r in this)"t"===r.charAt(0)&&n.call(this,r)&&!isNaN(+r.slice(1))&&(this[r]=e)},stop:function(){this.done=!0;var t=this.tryEntries[0],e=t.completion;if("throw"===e.type)throw e.arg;return this.rval},dispatchException:function(t){if(this.done)throw t;var r=this;function a(n,a){return s.type="throw",s.arg=t,r.next=n,a&&(r.method="next",r.arg=e),!!a}for(var i=this.tryEntries.length-1;i>=0;--i){var o=this.tryEntries[i],s=o.completion;if("root"===o.tryLoc)return a("end");if(o.tryLoc<=this.prev){var c=n.call(o,"catchLoc"),l=n.call(o,"finallyLoc");if(c&&l){if(this.prev<o.catchLoc)return a(o.catchLoc,!0);if(this.prev<o.finallyLoc)return a(o.finallyLoc)}else if(c){if(this.prev<o.catchLoc)return a(o.catchLoc,!0)}else{if(!l)throw new Error("try statement without catch or finally");if(this.prev<o.finallyLoc)return a(o.finallyLoc)}}}},abrupt:function(t,e){for(var r=this.tryEntries.length-1;r>=0;--r){var a=this.tryEntries[r];if(a.tryLoc<=this.prev&&n.call(a,"finallyLoc")&&this.prev<a.finallyLoc){var i=a;break}}i&&("break"===t||"continue"===t)&&i.tryLoc<=e&&e<=i.finallyLoc&&(i=null);var o=i?i.completion:{};return o.type=t,o.arg=e,i?(this.method="next",this.next=i.finallyLoc,d):this.complete(o)},complete:function(t,e){if("throw"===t.type)throw t.arg;return"break"===t.type||"continue"===t.type?this.next=t.arg:"return"===t.type?(this.rval=this.arg=t.arg,this.method="return",this.next="end"):"normal"===t.type&&e&&(this.next=e),d},finish:function(t){for(var e=this.tryEntries.length-1;e>=0;--e){var r=this.tryEntries[e];if(r.finallyLoc===t)return this.complete(r.completion,r.afterLoc),T(r),d}},catch:function(t){for(var e=this.tryEntries.length-1;e>=0;--e){var r=this.tryEntries[e];if(r.tryLoc===t){var n=r.completion;if("throw"===n.type){var a=n.arg;T(r)}return a}}throw new Error("illegal catch attempt")},delegateYield:function(t,r,n){return this.delegate={iterator:S(t),resultName:r,nextLoc:n},"next"===this.method&&(this.arg=e),d}},t}(t.exports);try{regeneratorRuntime=n}catch(a){Function("r","regeneratorRuntime = r")(n)}},"9f7f":function(t,e,r){"use strict";var n=r("d039");function a(t,e){return RegExp(t,e)}e.UNSUPPORTED_Y=n((function(){var t=a("a","y");return t.lastIndex=2,null!=t.exec("abcd")})),e.BROKEN_CARET=n((function(){var t=a("^r","gy");return t.lastIndex=2,null!=t.exec("str")}))},a15b:function(t,e,r){"use strict";var n=r("23e7"),a=r("44ad"),i=r("fc6a"),o=r("a640"),s=[].join,c=a!=Object,l=o("join",",");n({target:"Array",proto:!0,forced:c||!l},{join:function(t){return s.call(i(this),void 0===t?",":t)}})},a3b8:function(t,e,r){"use strict";const n=r("7252"),a=r("afa5");t.exports=(t,e)=>{if(!Number.isFinite(t))throw new TypeError("Expected a finite number");if(e=e||{},t<1e3){const r="number"===typeof e.msDecimalDigits?e.msDecimalDigits:0;return(r?t.toFixed(r):Math.ceil(t))+(e.verbose?" "+a("millisecond",Math.ceil(t)):"ms")}const r=[],i=(t,n,i,o)=>{if(0===t)return;const s=e.verbose?" "+a(n,t):i;r.push((o||t)+s)},o=n(t);if(i(Math.trunc(o.days/365),"year","y"),i(o.days%365,"day","d"),i(o.hours,"hour","h"),i(o.minutes,"minute","m"),e.compact)return i(o.seconds,"second","s"),"~"+r[0];const s=t/1e3%60,c="number"===typeof e.secDecimalDigits?e.secDecimalDigits:1,l=s.toFixed(c).replace(/\.0$/,"");return i(s,"second","s",l),r.join(" ")}},a640:function(t,e,r){"use strict";var n=r("d039");t.exports=function(t,e){var r=[][t];return!!r&&n((function(){r.call(null,e||function(){throw 1},1)}))}},ac1f:function(t,e,r){"use strict";var n=r("23e7"),a=r("9263");n({target:"RegExp",proto:!0,forced:/./.exec!==a},{exec:a})},ad6d:function(t,e,r){"use strict";var n=r("825a");t.exports=function(){var t=n(this),e="";return t.global&&(e+="g"),t.ignoreCase&&(e+="i"),t.multiline&&(e+="m"),t.dotAll&&(e+="s"),t.unicode&&(e+="u"),t.sticky&&(e+="y"),e}},afa5:function(t,e,r){"use strict";var n=r("86aa");t.exports=function(t,e,r){return"number"===typeof e&&(r=e),t in n?e=n[t]:"string"!==typeof e&&(e=(t.replace(/(?:s|x|z|ch|sh)$/i,"$&e").replace(/([^aeiou])y$/i,"$1ie")+"s").replace(/i?e?s$/i,(function(e){var r=t.slice(-1)===t.slice(-1).toLowerCase();return r?e.toLowerCase():e.toUpperCase()}))),1===r?t:e}},b0c0:function(t,e,r){var n=r("83ab"),a=r("9bf2").f,i=Function.prototype,o=i.toString,s=/^\s*function ([^ (]*)/,c="name";n&&!(c in i)&&a(i,c,{configurable:!0,get:function(){try{return o.call(this).match(s)[1]}catch(t){return""}}})},d784:function(t,e,r){"use strict";r("ac1f");var n=r("6eeb"),a=r("d039"),i=r("b622"),o=r("9263"),s=r("9112"),c=i("species"),l=!a((function(){var t=/./;return t.exec=function(){var t=[];return t.groups={a:"7"},t},"7"!=="".replace(t,"$<a>")})),u=function(){return"$0"==="a".replace(/./,"$0")}(),h=i("replace"),p=function(){return!!/./[h]&&""===/./[h]("a","$0")}(),f=!a((function(){var t=/(?:)/,e=t.exec;t.exec=function(){return e.apply(this,arguments)};var r="ab".split(t);return 2!==r.length||"a"!==r[0]||"b"!==r[1]}));t.exports=function(t,e,r,h){var d=i(t),g=!a((function(){var e={};return e[d]=function(){return 7},7!=""[t](e)})),m=g&&!a((function(){var e=!1,r=/a/;return"split"===t&&(r={},r.constructor={},r.constructor[c]=function(){return r},r.flags="",r[d]=/./[d]),r.exec=function(){return e=!0,null},r[d](""),!e}));if(!g||!m||"replace"===t&&(!l||!u||p)||"split"===t&&!f){var v=/./[d],y=r(d,""[t],(function(t,e,r,n,a){return e.exec===o?g&&!a?{done:!0,value:v.call(e,r,n)}:{done:!0,value:t.call(r,e,n)}:{done:!1}}),{REPLACE_KEEPS_$0:u,REGEXP_REPLACE_SUBSTITUTES_UNDEFINED_CAPTURE:p}),w=y[0],x=y[1];n(String.prototype,t,w),n(RegExp.prototype,d,2==e?function(t,e){return x.call(t,this,e)}:function(t){return x.call(t,this)})}h&&s(RegExp.prototype[d],"sham",!0)}},dab3:function(t,e,r){"use strict";r.r(e);var n=function(){var t=this,e=t.$createElement,r=t._self._c||e;return r("base-component",[r("Breadcrumb",{style:{margin:"15px 0 15px 185px",position:"fixed"}},[r("BreadcrumbItem",{attrs:{to:"/home"}},[t._v("首页")]),r("BreadcrumbItem",{attrs:{to:"/v2/migration"}},[t._v("DB 搬迁任务")])],1),r("Content",{staticClass:"content",style:{padding:"10px",background:"#fff",margin:"50px 0 1px 185px",zIndex:"1"}},[r("div",{staticStyle:{padding:"1px 1px"}},[r("Row",{attrs:{gutter:10,align:"middle"}},[r("Col",{attrs:{span:"15"}},[r("Card",{attrs:{padding:5},scopedSlots:t._u([{key:"title",fn:function(){return[t._v("查询条件")]},proxy:!0}])},[r("Row",{attrs:{gutter:10}},[r("Col",{attrs:{span:"5"}},[r("Input",{attrs:{prefix:"ios-search",placeholder:"老集群"},on:{"on-enter":t.getMigrationTasks},model:{value:t.queryParam.oldMha,callback:function(e){t.$set(t.queryParam,"oldMha",e)},expression:"queryParam.oldMha"}})],1),r("Col",{attrs:{span:"5"}},[r("Input",{attrs:{prefix:"ios-search",placeholder:"新集群"},on:{"on-enter":t.getMigrationTasks},model:{value:t.queryParam.newMha,callback:function(e){t.$set(t.queryParam,"newMha",e)},expression:"queryParam.newMha"}})],1),r("Col",{attrs:{span:"5"}},[r("Input",{attrs:{prefix:"ios-search",placeholder:"操作人"},on:{"on-enter":t.getMigrationTasks},model:{value:t.queryParam.operator,callback:function(e){t.$set(t.queryParam,"operator",e)},expression:"queryParam.operator"}})],1),r("Col",{attrs:{span:"5"}},[r("Select",{attrs:{filterable:"",prefix:"ios-home",clearable:"",placeholder:"状态"},on:{"on-change":t.getMigrationTasks},model:{value:t.queryParam.status,callback:function(e){t.$set(t.queryParam,"status",e)},expression:"queryParam.status"}},t._l(t.status,(function(e){return r("Option",{key:e,attrs:{value:e}},[t._v(t._s(e))])})),1)],1)],1)],1)],1),r("Col",{attrs:{span:"1"}},[r("Button",{attrs:{type:"primary",icon:"ios-search",loading:t.dataLoading},on:{click:t.getMigrationTasks}},[t._v("查询")]),r("Button",{staticStyle:{"margin-top":"20px"},attrs:{icon:"md-refresh",loading:t.dataLoading},on:{click:t.resetParam}},[t._v("重置")])],1)],1),r("br"),r("Table",{attrs:{loading:t.dataLoading,stripe:"",border:"",columns:t.columns,data:t.migrationTasks},scopedSlots:t._u([{key:"action",fn:function(e){var n=e.row,a=e.index;return[r("Button",{staticStyle:{"margin-right":"5px"},attrs:{type:"success",size:"small"},on:{click:function(e){return t.getDetail(n,a)}}},[t._v(" 查看 ")]),r("Button",{attrs:{type:"info",size:"small"},on:{click:function(e){return t.getHistoryLog(n,a)}}},[t._v(" 日志 ")])]}}])}),r("div",{staticStyle:{"text-align":"center",margin:"16px 0"}},[r("Page",{attrs:{transfer:!0,total:t.total,current:t.current,"page-size-opts":[10,20,50,100],"page-size":10,"show-total":"","show-sizer":"","show-elevator":""},on:{"update:current":function(e){t.current=e},"on-change":t.getMigrationTasks,"on-page-size-change":t.handleChangeSize}})],1),r("Modal",{attrs:{width:"1200px"},model:{value:t.replicationDetail.show,callback:function(e){t.$set(t.replicationDetail,"show",e)},expression:"replicationDetail.show"}},[r("Divider",{attrs:{orientation:"left"}},[t._v("相关 DRC 延迟")]),r("Table",{attrs:{stripe:"",loading:t.replicationDetail.mhaReplicationDataLoading,columns:t.detailColumn,data:t.replicationDetail.data,border:""}}),r("Divider",{attrs:{orientation:"left"}},[t._v("相关 Messenger 延迟")]),r("Table",{attrs:{stripe:"",loading:t.replicationDetail.messengerDataLoading,columns:t.messengerDetailColumn,data:t.replicationDetail.messengerData,border:""}})],1),r("Modal",{attrs:{width:"500px"},model:{value:t.logDetail.show,callback:function(e){t.$set(t.logDetail,"show",e)},expression:"logDetail.show"}},[r("Timeline",t._l(t.logDetail.splitLog,(function(e){return r("TimelineItem",{key:e.time},[r("span",{staticClass:"time",style:{"font-size":"14px","font-weight":"bold"}},[t._v(t._s(e.time))]),r("br"),r("span",{staticClass:"content",style:{"margin-left":"0px","margin-top":"0px","font-size":"14px","font-weight":"bold",color:"blue"}},[t._v("操作: "+t._s(e.operate))]),r("br"),r("span",{staticClass:"content",style:{"margin-left":"0px","margin-top":"0px"}},[t._v("操作人："+t._s(e.operator))])])})),1)],1)],1)])],1)},a=[],i=(r("caad"),r("a15b"),r("b0c0"),r("d3b7"),r("ac1f"),r("1276"),r("53ca")),o=(r("96cf"),r("1da1")),s=r("a3b8"),c=r.n(s),l=(r("7a7a"),r("d5e0"),r("697e"),r("cbc8"),{name:"Application",data:function(){var t=this;function e(){return function(t,e){var r,n,a=e.row.delayInfoDto,i=e.row.srcMha.monitorSwitch,o=a.delay,s="";if(null!=o&&o<1e4)r="blue",n=c()(o);else{if(r="volcano",n=o?c()(o):0===i?"查询失败, 未打开延迟监控":"查询失败",a.srcTime){var l=new Date(a.srcTime);s+="源集群: "+l.toLocaleDateString()+" "+l.toLocaleTimeString()+"\n"}if(a.dstTime){var u=new Date(a.dstTime);a.dstMha?s+="端集群: ":s+="Messenger: ",s+=u.toLocaleDateString()+" "+u.toLocaleTimeString()}}return t("div",[t("Tag",{props:{color:r}},n),t("div",{style:{"white-space":"pre-wrap"}},s)])}}return{detailColumn:[{title:"状态",key:"status",width:100,align:"center",render:function(t,e){var r=e.row,n=1===r.status?"blue":"volcano",a=1===r.status?"已接入":"未接入";return t("Tag",{props:{color:n}},a)}},{title:"srcMha",width:150,key:"id",render:function(t,e){return t("p",e.row.srcMha.name)}},{title:"dstMha",width:150,key:"id",render:function(t,e){return t("p",e.row.dstMha.name)}},{title:"delay",key:"delay",render:e()},{title:"dbs",key:"dbs",render:function(t,e){return t("p",e.row.dbs.join(","))}}],messengerDetailColumn:[{title:"状态",key:"status",width:100,align:"center",render:function(t,e){var r=e.row,n=1===r.status?"blue":"volcano",a=1===r.status?"已接入":"未接入";return t("Tag",{props:{color:n}},a)}},{title:"srcMha",width:150,key:"id",render:function(t,e){return t("p",e.row.srcMha.name)}},{title:"delay",key:"delay",render:e()},{title:"dbs",key:"dbs",render:function(t,e){return t("p",e.row.dbs.join(","))}}],columns:[{title:"状态",key:"status",width:200,align:"left",render:function(e,r){var n,a=r.row,i=a.status,s=!1;switch(["Starting","ReadyToSwitchDal"].includes(i)&&(s=!0),a.status){case"Success":n="success";break;case"Fail":n="error";break;default:n="blue";break}return a.dataLoading=!1,e("div",[e("Tag",{props:{color:n}},i),s&&e("Button",{on:{click:function(){var e=Object(o["a"])(regeneratorRuntime.mark((function e(){return regeneratorRuntime.wrap((function(e){while(1)switch(e.prev=e.next){case 0:return t.dataLoading=!0,e.next=3,t.getTaskStatus(a,null);case 3:t.dataLoading=!1;case 4:case"end":return e.stop()}}),e)})));function r(){return e.apply(this,arguments)}return r}()},props:{loading:t.dataLoading,size:"small",shape:"circle",type:"default",icon:"md-refresh"}})])}},{title:"任务ID",width:80,key:"id",render:function(t,e){return t("p",e.row.id)}},{title:"DB",key:"srcMhaName",render:function(t,e){return t("p",e.row.dbs.join(","))}},{title:"老集群名",key:"srcMhaName",render:function(t,e){return t("p",e.row.oldMha)}},{title:"新集群名",key:"dstMhaName",render:function(t,e){return t("p",e.row.newMha)}},{title:"操作人",key:"operator",render:function(t,e){return t("p",e.row.operator)}},{title:"操作",slot:"action",align:"center"}],total:0,current:1,size:10,queryParam:{dbName:"",oldMha:"",newMha:"",status:"",operator:""},migrationTasks:[],bus:[],regions:[],status:[],replicationDetail:{show:!1,row:{},mhaReplicationDataLoading:!1,data:[],messengerDataLoading:!1,messengerData:[]},logDetail:{show:!1,splitLog:[]},dataLoading:!0}},computed:{},methods:{getBus:function(){var t=this;this.axios.get("/api/drc/v2/meta/bus/all").then((function(e){t.bus=e.data.data}))},getRegions:function(){var t=this;this.axios.get("/api/drc/v2/meta/regions/all").then((function(e){t.regions=e.data.data}))},getStatus:function(){var t=this;this.axios.get("/api/drc/v2/migration/allStatus").then((function(e){t.status=e.data.data}))},flattenObj:function(t){var e={};for(var r in t)if("object"!==Object(i["a"])(t[r])||Array.isArray(t[r]))e[r]=t[r];else{var n=this.flattenObj(t[r]);for(var a in n)e[r+"."+a]=n[a]}return e},resetParam:function(){this.queryParam={},this.current=1,this.getMigrationTasks()},getMigrationTasks:function(){var t=this,e=this.flattenObj(this.queryParam);e.pageIndex=this.current,e.pageSize=this.size,t.dataLoading=!0,t.axios.get("/api/drc/v2/migration/query",{params:e}).then((function(e){var r=e.data.data;r&&0!==r.totalCount?(t.total=r.totalCount,t.current=r.pageIndex,t.migrationTasks=r.data,t.$Message.success("查询成功")):(t.total=0,t.current=1,t.migrationTasks=[],t.$Message.warning("查询结果为空"))})).catch((function(e){t.$Message.error("查询异常: "+e)})).finally((function(){t.dataLoading=!1}))},handleChangeSize:function(t){var e=this;this.size=t,this.$nextTick((function(){e.getMigrationTasks()}))},getMhaReplicationDetail:function(t){var e=this;this.replicationDetail.mhaReplicationDataLoading=!0,this.replicationDetail.data=[],this.axios.get("/api/drc/v2/replication/relatedReplicationDelay",{params:{mhas:[t.oldMha,t.newMha].join(","),dbs:t.dbs.join(",")}}).then((function(r){1!==r.data.status?(e.replicationDetail.data=r.data.data,console.log(e.replicationDetail.data),e.replicationDetail.show=!0,e.replicationDetail.row=t,e.replicationDetail.mhaReplicationDataLoading=!1):e.$Message.warning("查询异常: "+r.data.message)})).catch((function(t){e.$Message.error("查询异常: "+t)})).finally((function(){}))},getMhaMessengerDetail:function(t){var e=this;this.replicationDetail.messengerDataLoading=!0,this.replicationDetail.messengerData=[],this.axios.get("/api/drc/v2/messenger/delay",{params:{mhas:[t.oldMha,t.newMha].join(","),dbs:t.dbs.join(",")}}).then((function(t){1!==t.data.status?(e.replicationDetail.messengerData=t.data.data,console.log(e.replicationDetail.messengerData),e.replicationDetail.messengerDataLoading=!1):e.$Message.warning("查询异常: "+t.data.message)})).catch((function(t){e.$Message.error("查询异常: "+t)})).finally((function(){}))},getDetail:function(t,e){this.replicationDetail.show=!0,this.replicationDetail.row=t,this.getMhaReplicationDetail(t),this.getMhaMessengerDetail(t)},getHistoryLog:function(t,e){this.parseLog(t),this.logDetail.show=!0},parseLog:function(t){this.logDetail.splitLog=[];for(var e=t.log,r=e.split(";"),n=0;n<r.length;n++){var a=r[n].split(","),i={};i.operate=a[0].substring(9,a[0].length),i.operator=a[1].substring(10,a[1].length),i.time=a[2].substring(6,a[2].length),this.logDetail.splitLog.push(i)}},getTaskStatus:function(t,e){var r=this;return Object(o["a"])(regeneratorRuntime.mark((function e(){return regeneratorRuntime.wrap((function(e){while(1)switch(e.prev=e.next){case 0:return e.next=2,r.axios.get("/api/drc/v2/migration/status",{params:{taskId:t.id}}).then((function(e){if(1!==e.data.status){var n=e.data.data,a=t.status;a===n?r.$Message.success("状态不变"):(t.status=n,r.$Message.info({content:"刷新成功："+a+"->"+n,duration:10}))}else r.$Message.warning("查询异常: "+e.data.message)})).catch((function(t){r.$Message.error("查询异常: "+t)})).finally((function(){}));case 2:case"end":return e.stop()}}),e)})))()}},created:function(){var t=this;this.axios.get("/api/drc/v2/permission/ops/dbmigration").then((function(e){403!==e.data.status?(t.getMigrationTasks(),t.getStatus()):t.$router.push("/nopermission")}))}}),u=l,h=(r("f87a"),r("2877")),p=Object(h["a"])(u,n,a,!1,null,"236e0d4a",null);e["default"]=p.exports},f87a:function(t,e,r){"use strict";r("145a")}}]);
//# sourceMappingURL=chunk-6c8a8c92.f5a54730.js.map
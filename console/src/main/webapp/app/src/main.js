import Vue from 'vue'
import './plugins/axios'
import App from './App.vue'
import router from './router'
import store from './store'
import './plugins/iview.js'
import JsonViewer from 'vue-json-viewer'
import devArticle from './components/dev-article.vue'
import pageLayout from './components/page-layout.vue'
import baseComponent from './components/base-component'
import moment from 'moment'
import VueCodeMirror from 'vue-codemirror'
import 'codemirror/lib/codemirror.css'
import constant from './common/constant'

Vue.config.productionTip = false
Vue.component('dev-article', devArticle)
Vue.component('page-layout', pageLayout)
Vue.component('base-component', baseComponent)
Vue.use(VueCodeMirror)
Vue.use(JsonViewer)
Vue.prototype.constant = constant

Vue.filter('dateFormat', function (dateStr, pattern = 'YYYY-MM-DD HH:mm:ss') {
  return moment(dateStr).format(pattern)
})

new Vue({
  router,
  store,
  render: h => h(App)
}).$mount('#app')

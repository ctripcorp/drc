<template>
  <div class="layout">
    <Layout>
      <Header class="header">
        <Row>
          <i-col span="4">
            <img src="../assets/ctrip.png" @click="linkHome" alt="logo" class="layout-logo" to="/home">
          </i-col>
          <i-col span="16">
            <Menu mode="horizontal" theme="dark" :active-name="activeName">
              <MenuItem name="/home" to="/home">
                <Icon type="ios-navigate"></Icon>
                监控大盘
              </MenuItem>
              <MenuItem name="/apply" to="/apply">
                <Icon type="ios-keypad"></Icon>
                集群列表
              </MenuItem>
              <Submenu name="1">
                <template slot="title">
                  <Icon type="ios-analytics"></Icon>
                  自助运维
                </template>
                <MenuItem name="/monitor" to="/monitor">日志展示</MenuItem>
              </Submenu>
              <MenuItem name="/manage" to="/manage">
                <Icon type="ios-paper"></Icon>
                审批管理
              </MenuItem>
            </Menu>
          </i-col>
          <i-col span="4" style="">
            <Menu mode="horizontal" theme="dark" :active-name="activeName">
              <Submenu name="2">
                <template slot="title">
                  帮助
                </template>
                <MenuItem>帮助1</MenuItem>
                <MenuItem>帮助2</MenuItem>
              </Submenu>
              <Submenu name="3">
                <template slot="title">
                  {{ userName }}
                </template>
                <MenuItem>
                  <a class="a" :href=logoutUrl>注销</a>
                </MenuItem>
              </Submenu>
            </Menu>
          </i-col>
        </Row>
      </Header>
      <Layout>
        <div class="side-title">
          <h2 :style="{margin: '15px 0 0 15px'}">我的应用</h2>
        </div>
        <Sider  class="sider" hide-trigger :style="{background: '#fff'}" collapsible v-model="isCollapsed" :class="{ 'sider-hide': isCollapsed }">
          <ul id=userName class="group-list">
            <li v-for="item in appData" :key="item.appId" @click="sendMsgToParent(item.appId, index)" :class="{active:categoryIndex==item.appId}">
              <div class="group-box">
                <div class="group-text">
                  <div>
                    <div class="group-name">{{ item.appId }}</div>
                    <div class="group-des">{{ item.appName }}</div>
                  </div>
                </div>
              </div>
            </li>
          </ul>
        </Sider>
        <Layout :style="{padding: '60px 12px 24px 24px',height: '100%'}">
          <slot></slot>
        </Layout>
      </Layout>
    </Layout>
  </div>
</template>
<script>
export default {
  data () {
    return {
      activeName: this.$route.path,
      isCollapsed: false,
      appTotal: 0,
      categoryIndex: 0,
      logoutUrlFirstLogin: '',
      userNameFirstLogin: '',
      appDataFirstLogin: ''
    }
  },
  created () {
    this.activeName = this.$route.path
    this.getUserName()
    this.getAppData()
    this.getLogoutUrl()
  },
  methods: {
    linkHome () {
      this.$router.push({ path: '/home' })
    },
    getUserName () {
      if (!sessionStorage.getItem('userName')) {
        const that = this
        this.axios.get('/api/sso/v1')
          .then(res => {
            console.log(res.data.data)
            that.userNameFirstLogin = res.data.data
            sessionStorage.setItem('userName', res.data.data)
          })
      }
    },
    getAppData () {
      if (!sessionStorage.getItem('appData')) {
        const that = this
        this.axios.get('/api/sso/v1')
          .then(res => {
            console.log(res.data.data)
            var userName = res.data.data
            this.axios.post('/api/app/v1/' + userName).then(function (response) {
              console.log('start test')
              console.log(response.data.data.data)
              that.appTotal = response.data.data.total
              that.appDataFirstLogin = response.data.data.data
              sessionStorage.setItem('appData', JSON.stringify(response.data.data.data))
              console.log(that.appTotal)
              console.log(that.appData)
            })
          })
      }
    },
    sendMsgToParent: function (appId) {
      this.categoryIndex = appId
      this.$emit('listenToChildEvent', appId)
    },
    getLogoutUrl () {
      if (!sessionStorage.getItem('logoutUrl')) {
        const that = this
        this.axios.get('/api/sso/v1/logout')
          .then(res => {
            console.log(res.data.data)
            that.logoutUrlFirstLogin = res.data.data
            sessionStorage.setItem('logoutUrl', res.data.data)
          })
      }
    }
  },
  computed: {
    userName: function () {
      if (!sessionStorage.getItem('userName')) { return this.userNameFirstLogin }
      return sessionStorage.getItem('userName')
    },
    appData: function () {
      if (!sessionStorage.getItem('appData')) { return this.appDataFirstLogin }
      return JSON.parse(sessionStorage.getItem('appData'))
    },
    logoutUrl: function () {
      if (!sessionStorage.getItem('logoutUrl')) { return this.logoutUrlFirstLogin }
      return sessionStorage.getItem('logoutUrl')
    }
  },
  mounted () {
    console.log('mounted')
    console.log(this.appData)
  }
}
</script>
<style scoped>
  .layout{
    border: 1px solid #d7dde4;
    background: #f5f7f9;
    border-radius: 4px;
  }
  .layout-logo{
    width: 100px;
    height: 30px;
    background: #5b6270;
    border-radius: 3px;
    float: left;
    position: relative;
    top: 15px;
    cursor: pointer;
  }
  .sider-hide {
    display: none;
  }
  .header{
    width: 100%;
    height: 60px;
    background: #535a6d;
    position: fixed;
    z-index: 2;
    padding: 0 0 0 50px;
  }
  .sider{
    position: fixed;
    height: 100%;
    left: 0;
    overflow: auto;
    z-index: 1;
  }
  .content{
  }
  .a{
    text-decoration: none;
    color:#535a6d;
  }
  .side-title{
    margin: 59px 0 0 0;
    height: 50px;
    width: 200px;
    background-color: #fff;
    font-size: 10px;
    color: #616161;
    line-height: 1.5;
    position: fixed;
    z-index: 3;
    font-family: PingFangSC-Regular,tahoma,arial,"Hiragino Sans GB","\5FAE\8F6F\96C5\9ED1",simsun,sans-serif;
  }
  .group-list{
    margin-top: 110px;
    width: auto;
    color: #616161;
    line-height: 1.5;
    font-size: 12px;
    padding: 0 5px 0 5px;
    font-family: PingFangSC-Regular,tahoma,arial,"Hiragino Sans GB",simsun,sans-serif;
  }
  .group-box{
    display: flex;
    align-items: center;
    border-bottom: 1px #eee solid;
    border-top: 1px #eee solid;
    padding: 6px;
  }
  .group-text{
    display: flex;
    align-items: center;
    min-height: 45px;
    cursor: pointer;
  }
  .group-name{
    width: 166px;
    color: #000;
    flex-grow: 1;
    font-size: 14px;
    overflow:auto;
  }
  .group-des{
    white-space:nowrap;
  }
  .active {
    background: #e7eef7;
  }
</style>

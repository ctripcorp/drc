<template>
  <div class="layout">
    <Layout>
      <Sider class="sider" breakpoint="md">
        <img src="../assets/ctrip.png" @click="linkHome" alt="logo" class="layout-logo" to="/home">
        <Menu :active-name="isFather?fatherMenu:activeName" :open-names="isFather?subMenuName:openNames" theme="dark" width="auto" :style="{margin: '60px 0 0 0'}">
          <MenuItem name="/home" to="/home">
            <Icon type="ios-navigate"></Icon>
            <span>监控大盘</span>
          </MenuItem>
          <Submenu name="-1">
            <template slot="title">
              <Icon type="ios-apps"></Icon>
              集群管理 v2
            </template>
            <MenuItem name="/mhaReplications" to="/mhaReplications">
              <span>MHA复制</span>
            </MenuItem>
            <MenuItem name="/drcV2" to="/drcV2">
              <span>DRC配置</span>
            </MenuItem>
          </Submenu>
          <Submenu name="0">
            <template slot="title">
              <Icon type="ios-apps"></Icon>
              集群管理
            </template>
            <MenuItem name="/clusters" to="/clusters">
              <span>MHA集群</span>
            </MenuItem>
            <MenuItem name="/drcclusters" to="/drcclusters">
              <span>DRC集群</span>
            </MenuItem>
            <MenuItem name="/deletedDrcClusters" to="/deletedDrcClusters">
              <span>DRC回滚</span>
            </MenuItem>
            <MenuItem name="/apply" to="/apply">
              <span>DAL集群</span>
            </MenuItem>
<!--            <MenuItem name="/access" to="/access">-->
<!--              <span>搭建DRC</span>-->
<!--            </MenuItem>-->
            <MenuItem name="/accessV2" to="/accessV2">
              <span>DRC配置</span>
            </MenuItem>
            <MenuItem name="/messengers" to="/messengers">
              <span>Messenger集群</span>
            </MenuItem>
            <MenuItem name="/buildMhaMessenger" to="/buildMhaMessenger">
              <span>Messenger配置</span>
            </MenuItem>
            <MenuItem name="/proxyRouteCluster" to="/proxyRouteCluster">
              <span>Proxy路由</span>
            </MenuItem>
            <MenuItem name="/metaMessage" to="/metaMessage">
              <span>行过滤元信息配置</span>
            </MenuItem>
          </Submenu>
          <Submenu name="1">
            <template slot="title">
              <Icon type="ios-analytics"></Icon>
              自助运维
            </template>
            <MenuItem name="/monitor" to="/monitor">
              <span>冲突处理</span>
            </MenuItem>
            <MenuItem name="/incrementDataConsistencyResult" to="/incrementDataConsistencyResult">
              <span>实时数据一致性校验</span>
            </MenuItem>
            <MenuItem name="/fullDataConsistencyCluster" to="/fullDataConsistencyCluster">
              <span>手工数据一致性校验</span>
            </MenuItem>
            <MenuItem name="/unitRouteVerificationCluster" to="/unitRouteVerificationCluster">
              <span>单元化路由校验</span>
            </MenuItem>
          </Submenu>
          <Submenu name="2">
            <template slot="title">
              <Icon type="ios-paper"></Icon>
              资源管理
            </template>
            <MenuItem name="/drcResource" to="/drcResource">
              <span>DRC资源</span>
            </MenuItem>
            <MenuItem name="/proxyResource" to="/proxyResource">
              <span>Proxy资源</span>
            </MenuItem>
          </Submenu>
          <MenuItem name="/manage" to="/manage">
            <Icon type="ios-paper"></Icon>
            <span>审批管理</span>
          </MenuItem>
        </Menu>
      </Sider>
      <Layout>
        <Header class="header">
          <Row>
            <i-col span="4" style="" push="21">
              <Menu mode="horizontal" :active-name="activeName">
                <Submenu name="2">
                  <template slot="title">
                    帮助
                  </template>
                  <MenuItem name="help1">帮助1</MenuItem>
                  <MenuItem name="help2">帮助2</MenuItem>
                </Submenu>
                <Submenu name="3">
                  <template slot="title">
                    {{ userName }}
                  </template>
                  <MenuItem name="logout">
                    <a class="a" :href=logoutUrl>注销</a>
                  </MenuItem>
                </Submenu>
              </Menu>
            </i-col>
          </Row>
        </Header>
        <Layout :style="{padding: '60px 12px 24px 24px',height: '100%'}">
          <slot></slot>
        </Layout>
      </Layout>
    </Layout>
  </div>
</template>
<script>
export default {
  props: {
    isFather: {
      type: Boolean,
      default: false
    },
    subMenuName: {
      type: Array,
      default: function () {
        return []
      }
    },
    fatherMenu: {
      type: String,
      default: ''
    }
  },
  data () {
    return {
      activeName: this.$route.path,
      openNames: [],
      appTotal: 0,
      categoryIndex: 0,
      logoutUrlFirstLogin: '',
      userNameFirstLogin: '',
      appDataFirstLogin: ''
    }
  },
  created () {
    switch (this.$route.path) {
      default : this.openNames = ['']; break
      case '/apply':
      case '/clusters':
      case '/drcclusters':
      case '/deletedDrcClusters':
      case '/proxyRouteCluster':
      case '/proxyRouteManagement':
      case '/accessV2':
      case '/messengers':
      case '/buildMhaMessenger':
      case '/access': this.openNames = ['0']; break
      case '/incrementDataConsistencyResult':
      case '/incrementDataConsistencyHandle':
      case '/incrementDataConsistencyCluster':
      case '/incrementDataConsistencyCheck':
      case '/monitor': this.openNames = ['1']; break
      case '/drcResource': this.openNames = ['2']; break
      case '/proxyResource': this.openNames = ['2']; break
    }
    this.activeName = this.$route.path
    this.getUserName()
    this.getLogoutUrl()
  },
  methods: {
    linkHome () {
      this.$router.push({ path: '/home' })
    },
    getUserName () {
      if (!sessionStorage.getItem('userName')) {
        const that = this
        this.axios.get('/api/drc/v2/user/current')
          .then(res => {
            console.log(res.data.data)
            that.userNameFirstLogin = res.data.data
            sessionStorage.setItem('userName', res.data.data)
          })
      }
    },
    getLogoutUrl () {
      if (!sessionStorage.getItem('logoutUrl')) {
        const that = this
        this.axios.get('/api/drc/v2/user/logout')
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
    logoutUrl: function () {
      if (!sessionStorage.getItem('logoutUrl')) { return this.logoutUrlFirstLogin }
      return sessionStorage.getItem('logoutUrl')
    }
  }
}</script>
<style scoped>
  .layout{
    border: 1px solid #d7dde4;
    background: #f5f7f9;
    position: relative;
    border-radius: 4px;
    overflow: hidden;
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
    z-index: 4;
    margin-left: 50px;
  }
  .layout-header-bar{
    background: #fff;
    box-shadow: 0 1px 1px rgba(0,0,0,.1);
  }
  .menu-item span{
    display: inline-block;
    overflow: hidden;
    width: 69px;
    text-overflow: ellipsis;
    white-space: nowrap;
    vertical-align: bottom;
    transition: width .2s ease .2s;
  }
  .menu-item i{
    transform: translateX(0px);
    transition: font-size .2s ease, transform .2s ease;
    vertical-align: middle;
    font-size: 16px;
  }
  .collapsed-menu span{
    width: 0px;
    transition: width .2s ease;
  }
  .collapsed-menu i{
    transform: translateX(5px);
    transition: font-size .2s ease .2s, transform .2s ease .2s;
    vertical-align: middle;
    font-size: 22px;
  }
  .sider{
    z-index: 3;
  }
  .header{
    z-index: 2;
  }
</style>

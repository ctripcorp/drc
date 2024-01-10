<template>
  <div class="layout">
    <Layout>
      <Sider class="sider" breakpoint="md">
        <img src="../assets/ctrip.png" @click="linkHome" alt="logo" class="layout-logo" to="/home">
        <Menu :active-name="isFather?fatherMenu:activeName" :open-names="isFather?subMenuName:openNames" theme="dark"
              width="auto" :style="{margin: '60px 0 0 0'}">
          <MenuItem name="/home" to="/home">
            <Icon type="ios-navigate"></Icon>
            <span>监控大盘</span>
          </MenuItem>
          <Submenu name="v2-0">
            <template slot="title">
              <Icon type="ios-apps"></Icon>
              元信息管理
            </template>
            <MenuItem name="/v2/mhaReplications" to="/v2/mhaReplications">
              <span>MHA 同步</span>
            </MenuItem>
            <MenuItem name="/v2/mhaDbReplications" to="/v2/mhaDbReplications">
              <span>DB 粒度同步</span>
            </MenuItem>
            <MenuItem name="/v2/messengersV2" to="/v2/messengersV2">
              <span>Messenger 同步</span>
            </MenuItem>
            <MenuItem name="/metaMessage" to="/metaMessage">
              <span>行过滤标识</span>
            </MenuItem>
          </Submenu>
          <Submenu name="1">
            <template slot="title">
              <Icon type="ios-analytics"></Icon>
              自助运维
            </template>
            <MenuItem name="/v2/migration" to="/v2/migration">
              <span>DB 搬迁任务</span>
            </MenuItem>
            <MenuItem name="/conflictLog" to="/conflictLog">
              <span>冲突处理</span>
            </MenuItem>
            <MenuItem name="/v2/operationlog" to="/v2/operationlog">
              <span>操作日志</span>
            </MenuItem>
<!--            <MenuItem name="/monitor" to="/monitor">-->
<!--              <span>冲突处理</span>-->
<!--            </MenuItem>-->
<!--            <MenuItem name="/incrementDataConsistencyResult" to="/incrementDataConsistencyResult">-->
<!--              <span>实时数据一致性校验</span>-->
<!--            </MenuItem>-->
<!--            <MenuItem name="/fullDataConsistencyCluster" to="/fullDataConsistencyCluster">-->
<!--              <span>手工数据一致性校验</span>-->
<!--            </MenuItem>-->
<!--            <MenuItem name="/unitRouteVerificationCluster" to="/unitRouteVerificationCluster">-->
<!--              <span>单元化路由校验</span>-->
<!--            </MenuItem>-->
          </Submenu>
          <Submenu name="2">
            <template slot="title">
              <Icon type="ios-paper"></Icon>
              资源管理
            </template>
            <MenuItem name="/v2/resourceV2" to="/v2/resourceV2">
              <span>DRC机器资源</span>
            </MenuItem>
            <MenuItem name="/proxyResource" to="/proxyResource">
              <span>Proxy资源</span>
            </MenuItem>
            <MenuItem name="/proxyRouteCluster" to="/proxyRouteCluster">
              <span>Proxy路由</span>
            </MenuItem>
          </Submenu>
          <Submenu name="3">
            <template slot="title">
              <Icon type="ios-paper"></Icon>
              审批管理
            </template>
            <MenuItem name="/conflictApproval" to="/conflictApproval">
              <span>冲突处理审批</span>
            </MenuItem>
          </Submenu>
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
                  <MenuItem name="help1">
                    <a class="a" href='http://conf.ctripcorp.com/pages/viewpage.action?pageId=2130543490'>冲突自助处理</a></MenuItem>
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
      default :
        this.openNames = ['']
        break
      case '/v2/mhaReplications':
      case '/v2/mhaDbReplications':
      case '/drcV2':
      case '/v2/buildMessengerV2':
      case '/v2/messengersV2':
      case '/v2/dbDrcBuild':
      case '/metaMessage':
        this.openNames = ['v2-0']
        break
      case '/apply':
      case '/clusters':
      case '/drcclusters':
      case '/deletedDrcClusters':
      case '/proxyRouteManagement':
      case '/accessV2':
      case '/messengers':
      case '/buildMhaMessenger':
      case '/access':
        this.openNames = ['0']
        break
      case '/v2/migration':
      case '/incrementDataConsistencyResult':
      case '/incrementDataConsistencyHandle':
      case '/incrementDataConsistencyCluster':
      case '/incrementDataConsistencyCheck':
      case '/conflictLog':
      case '/v2/operationlog':
      case '/monitor':
        this.openNames = ['1']
        break
      case '/drcResource':
      case '/proxyResource':
      case '/v2/resourceV2':
      case '/proxyRouteCluster':
        this.openNames = ['2']
        break
      case '/conflictApproval':
        this.openNames = ['3']
        break
    }
    let activeName = this.$route.path
    if (['/drcV2', '/v2/dbDrcBuild'].includes(this.$route.path)) {
      activeName = '/v2/mhaReplications'
    } else if (['/v2/buildMessengerV2'].includes(this.$route.path)) {
      activeName = '/v2/messengersV2'
    } else if (['/drcResource'].includes(this.$route.path)) {
      activeName = '/v2/resourceV2'
    }
    this.activeName = activeName
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
      if (!sessionStorage.getItem('userName')) {
        return this.userNameFirstLogin
      }
      return sessionStorage.getItem('userName')
    },
    logoutUrl: function () {
      if (!sessionStorage.getItem('logoutUrl')) {
        return this.logoutUrlFirstLogin
      }
      return sessionStorage.getItem('logoutUrl')
    }
  }
}</script>
<style scoped>
.layout {
  border: 1px solid #d7dde4;
  background: #f5f7f9;
  position: relative;
  border-radius: 4px;
  overflow: hidden;
}

.layout-logo {
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

.layout-header-bar {
  background: #fff;
  box-shadow: 0 1px 1px rgba(0, 0, 0, .1);
}

.menu-item span {
  display: inline-block;
  overflow: hidden;
  width: 69px;
  text-overflow: ellipsis;
  white-space: nowrap;
  vertical-align: bottom;
  transition: width .2s ease .2s;
}

.menu-item i {
  transform: translateX(0px);
  transition: font-size .2s ease, transform .2s ease;
  vertical-align: middle;
  font-size: 16px;
}

.collapsed-menu span {
  width: 0px;
  transition: width .2s ease;
}

.collapsed-menu i {
  transform: translateX(5px);
  transition: font-size .2s ease .2s, transform .2s ease .2s;
  vertical-align: middle;
  font-size: 22px;
}

.sider {
  z-index: 3;
}

.header {
  z-index: 2;
}
</style>

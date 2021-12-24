<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/proxyRouteCluster">路由列表</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Card>
        BU名：<Input :style="{width: '180px', marginRight: '10px'}" v-model="searchCondition.routeOrgName" />
        源端机房：<Input :style="{width: '180px', marginRight: '10px'}" v-model="searchCondition.srcDcName" />
        目标机房：<Input :style="{width: '180px', marginRight: '10px'}" v-model="searchCondition.dstDcName" />
        tag：<Input :style="{width: '180px', marginRight: '10px'}" v-model="searchCondition.tag" />
          <Button :style="{marginLeft: '10px'}" type="primary" @click="getRoutes">查询</Button>
          <Button :style="{marginLeft: '10px'}" type="primary" @click="goToRouteManagementLink">添加路由</Button>
          <Table stripe :columns="columns" :data="dataWithPage">
            <template slot-scope="{ row, index }" slot="action">
              <Button type="primary" size="small" style="margin-right: 5px" @click="goToLink(row, index)">修改</Button>
            </template>
          </Table>
          <div style="text-align: center;margin: 16px 0">
            <Page
                :transfer="true"
                :total="routes.length"
                :current.sync="current"
                show-sizer
                show-elevator
                @on-page-size-change="handleChangeSize"></Page>
          </div>
        </Card>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'proxyRouteCluster',
  data () {
    return {
      columns: [
        {
          title: '序号',
          width: 75,
          align: 'center',
          render: (h, params) => {
            return h(
              'span',
              params.index + 1 + (this.current - 1) * this.size
            )
          }
        },
        {
          title: 'BU',
          key: 'routeOrgName'
        },
        {
          title: '源端机房',
          key: 'srcDcName'
        },
        {
          title: '目标机房',
          key: 'dstDcName'
        },
        {
          title: '源端Proxy',
          key: 'srcProxyUris'
        },
        {
          title: '目标Proxy',
          key: 'dstProxyUris'
        },
        {
          title: '中继Proxy',
          key: 'relayProxyUris'
        },
        {
          title: 'tag',
          key: 'tag'
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center'
        }
      ],
      searchCondition: {
        routeOrgName: '',
        srcDcName: '',
        dstDcName: '',
        tag: ''
      },
      routes: [],
      total: 0,
      current: 1,
      size: 10
    }
  },
  computed: {
    dataWithPage () {
      const data = this.routes
      const start = this.current * this.size - this.size
      const end = start + this.size
      return [...data].slice(start, end)
    }
  },
  methods: {
    getRoutes () {
      const that = this
      let uri = '/api/drc/v1/meta/routes'
      let flag = false
      if (this.searchCondition.routeOrgName !== '') {
        if (flag === false) {
          uri = uri + '?routeOrgName=' + this.searchCondition.routeOrgName
          flag = true
        } else {
          uri = uri + '&routeOrgName=' + this.searchCondition.routeOrgName
        }
      }
      if (this.searchCondition.srcDcName !== '') {
        if (flag === false) {
          uri = uri + '?srcDcName=' + this.searchCondition.srcDcName
          flag = true
        } else {
          uri = uri + '&srcDcName=' + this.searchCondition.srcDcName
        }
      }
      if (this.searchCondition.dstDcName !== '') {
        if (flag === false) {
          uri = uri + '?dstDcName=' + this.searchCondition.dstDcName
          flag = true
        } else {
          uri = uri + '&dstDcName=' + this.searchCondition.dstDcName
        }
      }
      if (this.searchCondition.tag !== '') {
        if (flag === false) {
          uri = uri + '?tag=' + this.searchCondition.tag
          flag = true
        } else {
          uri = uri + '&tag=' + this.searchCondition.tag
        }
      }
      console.log('uri: {}', uri)
      that.axios.get(uri)
        .then(response => {
          console.log(response)
          that.total = response.data.data.length
          that.routes = response.data.data
        })
    },
    handleChangeSize (val) {
      this.size = val
    },
    goToLink (row, index) {
      console.log('go to manage route for' + row.routeOrgName + '-' + row.tag + ', from ' + row.srcDcName + ' to ' + row.dstDcName)
      this.$router.push({ path: '/proxyRouteManagement', query: { routeOrgName: row.routeOrgName, srcDcName: row.srcDcName, dstDcName: row.dstDcName, tag: row.tag } })
    },
    goToRouteManagementLink () {
      console.log('go to manage route')
      this.$router.push({ path: '/proxyRouteManagement' })
    }
  },
  created () {
    this.getRoutes()
  }
}
</script>

<style scoped>

</style>

<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem>Mha 集群</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Row :gutter=10 align="middle">
          <Col span="16">
            <Row :gutter=10 align="middle">
              <Col span="24">
                <Card :padding=5>
                  <template #title>查询条件</template>
                  <Row :gutter=10>
                    <Col span="10">
                      <Input prefix="ios-search" v-model="mhaQuery.mhaName" placeholder="集群名↵"
                             @on-enter="getData()">
                      </Input>
                    </Col>
                    <Col span="8">
                      <Input prefix="ios-search" v-model="mhaQuery.dbName" placeholder="DB名↵"
                             @on-enter="getData()">
                      </Input>
                    </Col>
                    <Col span="6">
                      <Select filterable prefix="ios-pin" clearable v-model="mhaQuery.regionName"
                              placeholder="地域"
                              @on-change="getData()">
                        <Option v-for="item in regions" :value="item.regionName" :key="item.regionName">{{
                          item.regionName
                          }}
                        </Option>
                      </Select>
                    </Col>
                  </Row>
                </Card>
              </Col>
            </Row>
          </Col>
          <Col span="4">
            <Row :gutter=10 align="middle">
              <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getData()">查询</Button>
            </Row>
            <Row :gutter=10 align="middle" style="margin-top: 20px">
              <Button icon="md-refresh" @click="resetParam" :loading="dataLoading">重置</Button>
            </Row>
          </Col>
        </Row>
        <br>
        <Table :loading="dataLoading" stripe :columns="columns" :data="dataWithPage" border>
          <template slot-scope="{ row, index }" slot="action">
            <Button type="success" size="small" style="margin-right: 5px" @click="checkConfig(row, index)">查看
            </Button>
            <Button type="primary" size="small" style="margin-right: 5px" @click="goToLink(row, index)">修改</Button>
          </template>
        </Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :transfer="true"
            :total="total"
            :current.sync="current"
            :page-size-opts="[20,50,100]"
            :page-size="20"
            show-total
            show-sizer
            show-elevator
            @on-change="getData"
            @on-page-size-change="handleChangeSize"></Page>
        </div>
      </div>
      <Drawer title="DRC配置" width="80" :closable="true" v-model="cluster.drawer.show">
        <template #header>
          查看详情
          <div style="float:right;margin-right: 100px">
            自动换行
            <i-switch v-model="cluster.drawer.lineWrap"/>
            黑夜模式
            <i-switch v-model="cluster.drawer.darkMode"/>
          </div>
        </template>
        <div id="xmlCode">
          <codemirror
            v-model="cluster.drawer.data"
            class="code"
            :options="{
                  mode: 'xml',
                  theme: cluster.drawer.darkMode? 'monokai':'default',
                  autofocus: true,
                  lineWrapping: cluster.drawer.lineWrap,
                  readOnly: true,
                  lineNumbers: true,
                  foldGutter: true,
                  styleActiveLine: true,
                  gutters: ['CodeMirror-linenumbers', 'CodeMirror-foldgutter']
            }">
          </codemirror>
        </div>
      </Drawer>
    </Content>
  </base-component>
</template>

<script>
import 'codemirror/theme/monokai.css'
import 'codemirror/mode/xml/xml.js'

import 'codemirror/addon/fold/foldgutter.css'
import 'codemirror/addon/fold/foldgutter.js'

export default {
  name: 'mha',
  data () {
    return {
      cluster: {
        modal: {
          remove: false,
          show: false,
          data: ''
        },
        drawer: {
          lineWrap: false,
          darkMode: true,
          show: false,
          data: ''
        },
        mhaToBeRemoved: ''
      },
      mhaQuery: {
        mhaName: null,
        dbName: null,
        regionName: null
      },
      dbNames: null,
      mha: {
        name: this.$route.query.mhaName,
        buId: null,
        regionId: null
      },
      total: 0,
      current: 1,
      size: 20,
      pageSizeOpts: [20, 50, 100],
      tableData: [],
      bus: [],
      regions: [],
      switchOneInfo: {},
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
          title: '集群名',
          key: 'mhaName'
        },
        {
          title: '监控',
          key: 'monitorSwitch',
          width: 200,
          align: 'center',
          render: (h, params) => {
            const row = params.row

            return h('i-switch', {
              props: {
                size: 'large',
                value: row.monitorSwitch === 1,
                beforeChange: this.handleBeforeChange
              },
              scopedSlots: {
                open: () => h('span', '开启'),
                close: () => h('span', '关闭')
              },
              on: {
                'on-change': () => {
                  this.switchMonitor(row)
                }
              },
              nativeOn: {
                mousedown: () => { // 监听组件原生事件mousedown,此事件在click之前触发
                  this.switchOneInfo = {
                    monitorSwitch: row.monitorSwitch
                  }
                }
              }
            })
          }
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center'
        }
      ],

      dataLoading: false
    }
  },
  computed: {
    dataWithPage () {
      const data = this.tableData
      const start = this.current * this.size - this.size
      const end = start + this.size
      return [...data].slice(start, end)
    }
  },
  methods: {
    getRegions () {
      this.axios.get('/api/drc/v2/meta/regions/all')
        .then(response => {
          this.regions = response.data.data
        })
    },
    flattenObj (ob) {
      const result = {}
      for (const i in ob) {
        if ((typeof ob[i]) === 'object' && !Array.isArray(ob[i])) {
          const temp = this.flattenObj(ob[i])
          for (const j in temp) {
            result[i + '.' + j] = temp[j]
          }
        } else {
          result[i] = ob[i]
        }
      }
      return result
    },
    resetParam () {
      this.mhaQuery = {
        mhaName: null,
        dbName: null,
        regionName: null
      }
      this.getData()
    },
    getParams () {
      const params = {
        mhaName: this.mhaQuery.mhaName,
        dbName: this.mhaQuery.dbName,
        regionName: this.mhaQuery.regionName,
        pageReq: {
          pageSize: this.size,
          pageIndex: this.current
        }
      }
      return this.flattenObj(params)
    },
    getData () {
      this.dataLoading = true
      const reqParam = this.getParams()
      this.axios.get('/api/drc/v2/mha/', { params: reqParam })
        .then(response => {
          const data = response.data
          const pageResult = data.pageReq
          if (data.status === 1) {
            this.$Message.error('查询失败')
          } else if (data.data.length === 0 || pageResult.totalCount === 0) {
            this.total = 0
            this.current = 1
            this.tableData = data.data
            this.$Message.warning('查询结果为空')
          } else {
            this.total = pageResult.totalCount
            this.current = pageResult.pageIndex
            this.tableData = data.data
            this.$Message.success('查询成功')
          }
        })
        .finally(() => {
          this.dataLoading = false
        })
    },
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getData()
      })
    },
    goToLink (row, index) {
      console.log('go to change messenger config for ' + row.mhaName)
      const detail = this.$router.resolve({
        path: '/mha/replicatorConfig',
        query: {
          mhaName: row.mhaName
        }
      })
      window.open(detail.href, '_blank')
    },
    checkConfig (row, index) {
      console.log(row.mhaName)
      this.dataLoading = true
      this.axios.get('/api/drc/v2/meta/queryConfig/mha', {
        params: {
          mhaName: row.mhaName
        }
      }).then(response => {
        if (response.data.status === 1) {
          this.$Message.error('查询失败：' + response.message)
          return
        }
        this.cluster.drawer.data = response.data.data
        this.cluster.drawer.show = true
        this.$Message.success('查询成功')
      }).catch(message => {
        this.$Message.error('查询异常: ' + message)
      }).finally(() => {
        this.dataLoading = false
      })
    },
    handleBeforeChange () {
      console.log('handleBeforeChange:', this.switchOneInfo)
      return new Promise((resolve) => {
        this.$Modal.confirm({
          title: '切换确认',
          content: '您确认要切换开关状态吗？',
          onOk: () => {
            resolve()
          }
        })
      })
    },
    switchMonitor (row) {
      // 取反
      const switchStatus = row.monitorSwitch === 0 ? 'on' : 'off'
      this.switchMonitors(row.mhaName, switchStatus)
    },
    switchMonitors (mhaName, status) {
      console.log(mhaName)
      this.axios.post('/api/drc/v2/monitor/switch/' + mhaName + '/' + status).then(res => {
        if (res.data.status === 0) {
          console.log(status)
          if (status === 'on') {
            this.$Message.success('监控开启成功')
          } else {
            this.$Message.success('监控关闭成功')
          }
        } else {
          this.$Message.warning('监控操作失败')
        }
        this.getData()
      })
    }
  },
  created () {
    this.getRegions()
    this.getData()
  }
}
</script>

<style scoped>

</style>
<style lang="scss">
#xmlCode {
  .CodeMirror {
    overscroll-y: scroll !important;
    height: auto !important;
  }
}
</style>

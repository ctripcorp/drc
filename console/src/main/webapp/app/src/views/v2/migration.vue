<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/v2/migration">DB 搬迁任务</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px ">
        <Row :gutter=10 align="middle">
          <Col span="15">
            <Card :padding=5>
              <template #title>查询条件</template>
              <Row :gutter=10>
                <Col span="5">
                  <Input prefix="ios-search" v-model="queryParam.oldMha" placeholder="老集群" @on-enter="getMigrationTasks"/>
                </Col>
                <Col span="5">
                  <Input prefix="ios-search" v-model="queryParam.newMha" placeholder="新集群" @on-enter="getMigrationTasks"/>
                </Col>
                <Col span="5">
                  <Input prefix="ios-search" v-model="queryParam.operator" placeholder="操作人" @on-enter="getMigrationTasks"/>
                </Col>
                <Col span="5">
                  <Select filterable prefix="ios-home" clearable v-model="queryParam.status" placeholder="状态"
                          @on-change="getMigrationTasks">
                    <Option v-for="item in status" :value="item" :key="item">{{ item}}</Option>
                  </Select>
                </Col>
              </Row>
            </Card>
          </Col>
          <Col span="3">
            <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getMigrationTasks">查询</Button>
            <Button icon="md-refresh" @click="resetParam" :loading="dataLoading" style="margin-top: 20px">重置</Button>
          </Col>
        </Row>
        <br>
        <Table :loading="dataLoading" stripe border :columns="columns" :data="migrationTasks" >
        </Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :transfer="true"
            :total="total"
            :current.sync="current"
            :page-size-opts="[10,20,50,100]"
            :page-size="10"
            show-total
            show-sizer
            show-elevator
            @on-change="getMigrationTasks"
            @on-page-size-change="handleChangeSize"></Page>
        </div>
        <Drawer title="Basic Drawer" width="80" :closable="true" v-model="replicationDetail.show">
          <template #header>
            查看详情
            <div style="float:right;margin-right: 100px">
              自动换行
              <i-switch v-model="replicationDetail.lineWrap"/>
              黑夜模式
              <i-switch v-model="replicationDetail.darkMode"
                        on-change="(status)=>{this.$Message.info('开关状态：'+status)}"/>
            </div>
          </template>
          <div id="xmlCode">
            <codemirror
              v-model="replicationDetail.data"
              class="code"
              :options="{
                  mode: 'xml',
                  theme: replicationDetail.darkMode? 'monokai':'default',
                  autofocus: true,
                  lineWrapping: replicationDetail.lineWrap,
                  readOnly: true,
                  lineNumbers: true,
                  foldGutter: true,
                  styleActiveLine: true,
                  gutters: ['CodeMirror-linenumbers', 'CodeMirror-foldgutter']
            }">
            </codemirror>
          </div>
        </Drawer>
      </div>
    </Content>
  </base-component>
</template>

<script>
// eslint-disable-next-line no-unused-vars
import MhaGraph from '@/views/v2/mhaReplicationDetails.vue'
import 'codemirror/theme/monokai.css'
import 'codemirror/mode/xml/xml.js'

import 'codemirror/addon/fold/foldgutter.css'
import 'codemirror/addon/fold/foldgutter.js'

export default {
  name: 'Application',
  data () {
    return {
      columns: [
        {
          title: '状态',
          key: 'status',
          width: 100,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.status
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: 'DB',
          key: 'srcMhaName',
          render: (h, params) => {
            return h('p', params.row.dbs)
          }
        },
        {
          title: '老集群名',
          key: 'srcMhaName',
          render: (h, params) => {
            return h('p', params.row.oldMha)
          }
        },
        {
          title: '新集群名',
          key: 'dstMhaName',
          render: (h, params) => {
            return h('p', params.row.newMha)
          }
        },
        {
          title: '操作人',
          key: 'operator',
          render: (h, params) => {
            return h('p', params.row.operator)
          }
        }
      ],
      // page
      total: 0,
      current: 1,
      size: 10,
      // query param
      queryParam: {
        oldMha: '',
        newMha: '',
        status: '',
        operator: ''
      },
      // get from backend
      migrationTasks: [],
      bus: [],
      regions: [],
      status: [],
      // for detail show
      replicationDetail: {
        show: false,
        data: null,
        darkMode: true,
        lineWrap: false,
        row: {}
      },
      dataLoading: true
    }
  },
  computed: {},
  methods: {
    getBus () {
      this.axios.get('/api/drc/v2/meta/bus/all')
        .then(response => {
          this.bus = response.data.data
        })
    },
    getRegions () {
      this.axios.get('/api/drc/v2/meta/regions/all')
        .then(response => {
          this.regions = response.data.data
        })
    },
    getStatus () {
      this.axios.get('/api/drc/v2/dbmigration/allStatus')
        .then(response => {
          this.status = response.data.data
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
      this.queryParam = {}
      this.getMigrationTasks()
    },
    getMigrationTasks () {
      const that = this

      const reqParam = this.flattenObj(this.queryParam)
      that.dataLoading = true
      that.axios.get('/api/drc/v2/dbmigration/query', { params: reqParam })
        .then(response => {
          const pageResult = response.data.data
          if (!pageResult || pageResult.totalCount === 0) {
            that.total = 0
            that.current = 1
            that.migrationTasks = []
            that.$Message.warning('查询结果为空')
          } else {
            that.total = pageResult.totalCount
            that.current = pageResult.pageIndex
            that.migrationTasks = pageResult.data
            that.$Message.success('查询成功')
          }
        })
        .catch(message => {
          that.$Message.error('查询异常: ' + message)
        })
        .finally(() => {
          that.dataLoading = false
        })
    },
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getMigrationTasks()
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
    switchMonitor (mhaName, status) {
      // 求反
      const switchStatus = status === 0 ? 'on' : 'off'
      this.doSwitchMonitor(mhaName, switchStatus)
    },
    doSwitchMonitor (mhaName, status) {
      console.log(mhaName)
      this.axios.post('/api/drc/v1/monitor/switch/' + mhaName + '/' + status).then(res => {
        if (res.data.status === 0) {
          console.log(status)
          if (status === 'on') {
            this.$Message.info('监控开启成功')
          } else {
            this.$Message.info('监控关闭成功')
          }
        } else {
          this.$Message.info('监控操作失败')
        }
        this.getMigrationTasks()
      })
    }
  },
  created () {
    this.getMigrationTasks()
    this.getStatus()
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

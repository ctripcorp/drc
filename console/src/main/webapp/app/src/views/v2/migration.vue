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
          <template slot-scope="{ row, index }" slot="action">
            <Button type="success" size="small" style="margin-right: 5px" @click="getDetail(row, index)">
              查看
            </Button>
          </template>
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
        <Modal v-model="replicationDetail.show" title="相关 DRC 延迟" width="1200px">
          <Table stripe :loading="dataLoading" :columns="detailColumn" :data="replicationDetail.data" border></Table>
        </Modal>
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
      detailColumn: [
        {
          title: '状态',
          key: 'status',
          width: 100,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const color = row.status === 1 ? 'blue' : 'volcano'
            const text = row.status === 1 ? '已接入' : '未接入'
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: 'id',
          width: 80,
          key: 'id',
          render: (h, params) => {
            return h('p', params.row.replicationId)
          }
        },
        {
          title: 'srcMha',
          width: 150,
          key: 'id',
          render: (h, params) => {
            return h('p', params.row.srcMha.name)
          }
        },
        {
          title: 'dstMha',
          width: 150,
          key: 'id',
          render: (h, params) => {
            return h('p', params.row.dstMha.name)
          }
        },
        {
          title: 'delay',
          key: 'delay',
          render: (h, params) => {
            const delayInfo = params.row.delayInfoDto
            const delay = delayInfo.delay
            let color
            let text
            let extraInfo
            if (delay != null) {
              if (delay < 10000) {
                color = 'blue'
                text = delay + 'ms'
              } else {
                color = 'volcano'
                text = delay + 'ms'
                const srcTime = new Date(delayInfo.srcTime)
                const dstTime = new Date(delayInfo.dstTime)
                extraInfo = '源集群: ' + srcTime.toLocaleDateString() + ' ' + srcTime.toLocaleTimeString() + '\n' +
                  '端集群: ' + dstTime.toLocaleDateString() + ' ' + dstTime.toLocaleTimeString()
              }
            } else {
              color = 'volcano'
              text = '查询失败'
            }
            return h('div', [
              h('Tag', {
                props: {
                  color: color
                }
              }, text),
              h('div', {
                style: {
                  'white-space': 'pre-wrap'
                }
              }, extraInfo)
            ])
          }
        },
        {
          title: 'dbs',
          key: 'dbs',
          render: (h, params) => {
            return h('p', params.row.dbs.join(','))
          }
        }
      ],
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
          title: 'ID',
          width: 80,
          key: 'id',
          render: (h, params) => {
            return h('p', params.row.id)
          }
        },
        {
          title: 'DB',
          key: 'srcMhaName',
          render: (h, params) => {
            return h('p', params.row.dbs.join(','))
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
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center'
        }
      ],
      // page
      total: 0,
      current: 1,
      size: 10,
      // query param
      queryParam: {
        dbName: '',
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
        data: [],
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
      this.axios.get('/api/drc/v2/migration/allStatus')
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
      that.axios.get('/api/drc/v2/migration/query', { params: reqParam })
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
    getDetail (row, index) {
      this.dataLoading = true
      this.replicationDetail.data = []
      this.axios.get('/api/drc/v2/replication/relatedReplicationDelay', {
        params: {
          mhas: [row.oldMha, row.newMha].join(','),
          dbs: row.dbs.join(',')
        }
      }).then(response => {
        if (response.data.status === 1) {
          this.$Message.warning('查询异常: ' + response.data.message)
          return
        }
        this.replicationDetail.data = response.data.data
        console.log(this.replicationDetail.data)
        this.replicationDetail.show = true
        this.replicationDetail.row = row
      }).catch(message => {
        this.$Message.error('查询异常: ' + message)
      }).finally(() => {
        this.dataLoading = false
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

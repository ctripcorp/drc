<!--<style scoped>-->
<!--.time{-->
<!--  font-size: 14px;-->
<!--  font-weight: bold;-->
<!--}-->
<!--.content{-->
<!--  padding-left: 0;-->
<!--}-->
<!--</style>-->
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
          <Col span="1">
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
            <Button type="info" size="small" @click="getHistoryLog(row, index)">
              日志
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
        <Modal v-model="replicationDetail.show"  width="1200px">
          <Divider orientation="left">相关 DRC 延迟</Divider>
          <Table stripe :loading="replicationDetail.mhaReplicationDataLoading" :columns="detailColumn" :data="replicationDetail.data" border></Table>
          <Divider orientation="left">相关 Messenger 延迟</Divider>
          <Table stripe :loading="replicationDetail.messengerDataLoading" :columns="messengerDetailColumn" :data="replicationDetail.messengerData" border></Table>
        </Modal>
        <Modal v-model="logDetail.show"  width="500px">
          <Timeline>
            <TimelineItem v-for="item in logDetail.splitLog" :key="item.time">
              <span class="time" :style="{'font-size': '14px','font-weight': 'bold'}">{{item.time}}</span>
              <br/>
              <span class="content" :style="{'margin-left': '0px', 'margin-top': '0px','font-size': '14px','font-weight': 'bold', 'color': 'blue'}">操作: {{item.operate}}</span>
              <br/>
              <span class="content" :style="{'margin-left': '0px', 'margin-top': '0px'}">操作人：{{item.operator}}</span>
            </TimelineItem>
          </Timeline>
        </Modal>
      </div>
    </Content>
  </base-component>
</template>

<script>
// eslint-disable-next-line no-unused-vars
import prettyMilliseconds from 'pretty-ms'
import 'codemirror/theme/monokai.css'
import 'codemirror/mode/xml/xml.js'

import 'codemirror/addon/fold/foldgutter.css'
import 'codemirror/addon/fold/foldgutter.js'

export default {
  name: 'Application',
  data () {
    function renderDelay () {
      return (h, params) => {
        const delayInfo = params.row.delayInfoDto
        const srcMhaMonitorSwitch = params.row.srcMha.monitorSwitch
        const delay = delayInfo.delay
        let color
        let text
        let extraInfo = ''
        if (delay != null && delay < 10000) {
          color = 'blue'
          text = prettyMilliseconds(delay)
        } else {
          color = 'volcano'
          if (delay) {
            text = prettyMilliseconds(delay)
          } else if (srcMhaMonitorSwitch === 0) {
            text = '查询失败, 未打开延迟监控'
          } else {
            text = '查询失败'
          }
          if (delayInfo.srcTime) {
            const srcTime = new Date(delayInfo.srcTime)
            extraInfo += '源集群: ' + srcTime.toLocaleDateString() + ' ' + srcTime.toLocaleTimeString() + '\n'
          }
          if (delayInfo.dstTime) {
            const dstTime = new Date(delayInfo.dstTime)
            if (delayInfo.dstMha) {
              extraInfo += '端集群: '
            } else {
              extraInfo += 'Messenger: '
            }
            extraInfo += dstTime.toLocaleDateString() + ' ' + dstTime.toLocaleTimeString()
          }
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
    }

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
          render: renderDelay()
        },
        {
          title: 'dbs',
          key: 'dbs',
          render: (h, params) => {
            return h('p', params.row.dbs.join(','))
          }
        }
      ],
      messengerDetailColumn: [
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
          title: 'srcMha',
          width: 150,
          key: 'id',
          render: (h, params) => {
            return h('p', params.row.srcMha.name)
          }
        },
        {
          title: 'delay',
          key: 'delay',
          render: renderDelay()
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
          width: 200,
          align: 'left',
          render: (h, params) => {
            const row = params.row
            const text = row.status
            let needRefreshButton = false
            let color
            if (['Starting', 'ReadyToSwitchDal'].includes(text)) {
              needRefreshButton = true
            }
            switch (row.status) {
              case 'Success':
                color = 'success'
                break
              case 'Fail':
                color = 'error'
                break
              default:
                color = 'blue'
                break
            }
            row.dataLoading = false
            return h('div', [
              h('Tag', {
                props: {
                  color: color
                }
              }, text),
              needRefreshButton && h('Button', {
                on: {
                  click: async () => {
                    this.dataLoading = true
                    await this.getTaskStatus(row, null)
                    this.dataLoading = false
                  }
                },
                props: {
                  loading: this.dataLoading,
                  size: 'small',
                  shape: 'circle',
                  type: 'default',
                  icon: 'md-refresh'
                }
              })
            ])
          }
        },
        {
          title: '任务ID',
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
        row: {},
        mhaReplicationDataLoading: false,
        data: [],
        messengerDataLoading: false,
        messengerData: []
      },
      logDetail: {
        show: false,
        splitLog: []
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
      this.current = 1
      this.getMigrationTasks()
    },
    getMigrationTasks () {
      const that = this

      const reqParam = this.flattenObj(this.queryParam)
      reqParam.pageIndex = this.current
      reqParam.pageSize = this.size

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
    getMhaReplicationDetail: function (row) {
      this.replicationDetail.mhaReplicationDataLoading = true
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
        this.replicationDetail.mhaReplicationDataLoading = false
      }).catch(message => {
        this.$Message.error('查询异常: ' + message)
      }).finally(() => {
      })
    },
    getMhaMessengerDetail: function (row) {
      this.replicationDetail.messengerDataLoading = true
      this.replicationDetail.messengerData = []
      this.axios.get('/api/drc/v2/messenger/delay', {
        params: {
          mhas: [row.oldMha, row.newMha].join(','),
          dbs: row.dbs.join(',')
        }
      }).then(response => {
        if (response.data.status === 1) {
          this.$Message.warning('查询异常: ' + response.data.message)
          return
        }
        this.replicationDetail.messengerData = response.data.data
        console.log(this.replicationDetail.messengerData)
        this.replicationDetail.messengerDataLoading = false
      }).catch(message => {
        this.$Message.error('查询异常: ' + message)
      }).finally(() => {
      })
    },
    getDetail (row, index) {
      this.replicationDetail.show = true
      this.replicationDetail.row = row
      this.getMhaReplicationDetail(row)
      this.getMhaMessengerDetail(row)
    },
    getHistoryLog (row, index) {
      this.parseLog(row)
      this.logDetail.show = true
    },
    parseLog (row) {
      this.logDetail.splitLog = []
      const text = row.log // "Operate: %s,Operator: %s,Time: %s";
      // const text = 'Operate: Init,Operator: phd,Time: 2019-11-26T13:57:57.440;Operate: Init1,Operator: phd,Time: 2019-11-26T13:57:57.440;Operate: Init2,Operator: phd,Time: 2019-11-26T13:57:57.440;Operate: Init3,Operator: phd,Time: 2019-11-26T13:57:57.440'
      const myArray = text.split(';')
      for (let i = 0; i < myArray.length; i++) {
        const log = myArray[i].split(',')
        const logObj = {}
        logObj.operate = log[0].substring(9, log[0].length)
        logObj.operator = log[1].substring(10, log[1].length)
        logObj.time = log[2].substring(6, log[2].length)
        this.logDetail.splitLog.push(logObj)
      }
    },
    async getTaskStatus (row, index) {
      await this.axios.get('/api/drc/v2/migration/status', {
        params: {
          taskId: row.id,
          careNewMha: row.status === 'Starting'
        }
      }).then(response => {
        if (response.data.status === 1) {
          this.$Message.warning('查询异常: ' + response.data.message)
          return
        }
        const newStatus = response.data.data
        const oldStatus = row.status
        if (oldStatus === newStatus) {
          this.$Message.success('状态不变')
        } else {
          row.status = newStatus
          this.$Message.info({
            content: '刷新成功：' + oldStatus + '->' + newStatus,
            duration: 10
          })
        }
      }).catch(message => {
        this.$Message.error('查询异常: ' + message)
      }).finally(() => {
      })
    }
  },
  created () {
    this.axios.get('/api/drc/v2/permission/ops/dbmigration').then((response) => {
      if (response.data.status === 403) {
        this.$router.push('/nopermission')
        return
      }
      this.getMigrationTasks()
      this.getStatus()
    })
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

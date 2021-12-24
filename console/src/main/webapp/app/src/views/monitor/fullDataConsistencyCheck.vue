<template>
  <base-component :isFather="isFather" :subMenuName="['1']" :fatherMenu="fatherMenu">
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/fullDataConsistencyCluster">校验集群</BreadcrumbItem>
      <BreadcrumbItem>校验表配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <div class="ivu-list-item-meta-title">
          <p>DRC集群：{{$route.query.clusterA}}(集群A)---{{$route.query.clusterB}}(集群B)</p>
        </div>
        <Table stripe style="margin-top: 20px" :columns="columns" :data="dataWithPage" size="small"></Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :total="records.length"
            :current.sync="current"
            show-sizer
            show-elevator
            placement="top"
            @on-page-size-change="handleChangeSize">
          </Page>
        </div>
      </div>
      <Modal
        v-model="this.confirmCheckModal"
        width="900px"
        title="一致性校验确认"
        @on-ok="okCheckConsistency"
        @on-cancel="cancelCheckConsistency">
        <Form :label-width="200">
          <FormItem label="库名">
            <Input style="width: 500px" v-model="this.chooseTableConfig.schema" readonly/>
          </FormItem>
          <FormItem label="表名">
            <Input style="width: 500px" v-model="this.chooseTableConfig.table" readonly/>
          </FormItem>
          <FormItem label="key">
            <Input style="width: 500px" v-model="this.chooseTableConfig.key" readonly/>
          </FormItem>
          <FormItem label="onUpdate">
            <Input style="width: 500px" v-model="this.chooseTableConfig.onUpdate" readonly/>
          </FormItem>
          <FormItem label="startTimestamp">
            <Input style="width: 500px" v-model="this.chooseTableConfig.startTimestamp" readonly/>
          </FormItem>
          <FormItem label="endTimeStamp">
            <Input style="width: 500px" v-model="this.chooseTableConfig.endTimeStamp" readonly/>
          </FormItem>
        </Form>
      </Modal>
      <Modal
        v-model="this.executeModal"
        width="900px"
        title="执行结果"
        @on-ok="okExecuteModal"
        @on-cancel="okExecuteModal">
        <Form>
          <FormItem label="执行结果">
            <Alert :type="this.executeStatus" show-icon>{{ this.executeMsg }}</Alert>
          </FormItem>
        </Form>
      </Modal>
    </Content>
  </base-component>
</template>

<script>
import { formatDate } from '../../common/date'

export default {
  name: 'fullDataConsistencyCheck',
  data () {
    return {
      isFather: true,
      fatherMenu: '/fullDataConsistencyCluster',
      executeStatus: '',
      executeMsg: '',
      executeModal: false,
      confirmCheckModal: false,
      chooseTableConfig: {
        tableId: '',
        schema: '',
        table: '',
        key: '',
        onUpdate: '',
        startTimestamp: '',
        endTimeStamp: ''
      },
      total: 0,
      current: 1,
      size: 10,
      records: [],
      startTime: [],
      endTime: [],
      columns: [
        {
          title: '库名',
          key: 'monitorSchemaName',
          resizable: true
        },
        {
          title: '表名',
          key: 'monitorTableName',
          resizable: true
        },
        // {
        //   title: 'primary key',
        //   key: 'monitorTableKey',
        //   align: 'center',
        //   resizable: true
        // },
        // {
        //   title: 'on update',
        //   key: 'monitorTableOnUpdate',
        //   align: 'center',
        //   resizable: true
        // },
        {
          title: '开始时间',
          align: 'center',
          resizable: true,
          render: (h, params) => {
            // const row = params.row
            return h('DatePicker', {
              props: {
                type: 'datetime',
                format: 'yyyy-MM-dd HH:mm',
                transfer: true,
                placeholder: 'start time'
              },
              on: {
                'on-change': (value) => {
                  this.startTime[params.index + (this.current - 1) * this.size] = value
                }
              }
            })
          }
        },
        {
          title: '结束时间',
          align: 'center',
          resizable: true,
          render: (h, params) => {
            // const row = params.row
            return h('DatePicker', {
              props: {
                type: 'datetime',
                format: 'yyyy-MM-dd HH:mm',
                transfer: true,
                placeholder: 'end time'
              },
              on: {
                'on-change': (value) => {
                  this.endTime[params.index + (this.current - 1) * this.size] = value
                }
              }
            })
          }
        },
        {
          title: '操作',
          align: 'center',
          resizable: true,
          render: (h, params) => {
            return h('div', [
              h('Button', {
                props: {
                  type: 'primary',
                  size: 'small'
                },
                style: {
                  marginRight: '5px'
                },
                on: {
                  click: () => {
                    this.confirmModalCheck(params)
                    // this.checkDataConsistency(params)
                  }
                }
              }, '执行')
            ])
          }
        },
        {
          title: '校验状态',
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const color = row.fullDataCheckStatus === 0 ? 'error' : (row.fullDataCheckStatus === 1 ? 'warning' : (row.fullDataCheckStatus === 2 ? 'success' : 'warning'))
            const text = row.fullDataCheckStatus === 0 ? '未校验' : (row.fullDataCheckStatus === 1 ? '校验中' : (row.fullDataCheckStatus === 2 ? '已校验' : '校验失败'))

            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '最近校验时间',
          align: 'center',
          render: (h, params) => {
            return h('div',
              params.row.fullDataCheckLasttime == null ? '' : formatDate(new Date(params.row.fullDataCheckLasttime), 'yyyy-MM-dd hh:mm:ss')
            )
          }
        },
        {
          title: '最近校验结果',
          align: 'center',
          render: (h, params) => {
            return h('div', [
              h('Button', {
                props: {
                  type: 'info',
                  size: 'small'
                },
                style: {
                  marginRight: '5px'
                },
                on: {
                  click: () => {
                    this.showCheckResult(params.row)
                  }
                }
              }, '查看')
            ])
          }
        }
      ]
    }
  },
  methods: {
    getConfiguredTables () {
      const that = this
      this.axios.get('/api/drc/v1/monitor/consistency/data/' + this.$route.query.clusterA + '/' + this.$route.query.clusterB)
        .then(response => {
          that.records = response.data.data
          console.log(response.data.data)
        })
    },
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getData()
      })
    },
    confirmModalCheck (params) {
      this.chooseTableConfig.tableId = params.row.id
      this.chooseTableConfig.schema = params.row.monitorSchemaName
      this.chooseTableConfig.table = params.row.monitorTableName
      this.chooseTableConfig.key = params.row.monitorTableKey
      this.chooseTableConfig.onUpdate = params.row.monitorTableOnUpdate
      this.chooseTableConfig.startTimestamp = this.startTime[params.index + (this.current - 1) * this.size]
      this.chooseTableConfig.endTimeStamp = this.endTime[params.index + (this.current - 1) * this.size]
      this.confirmCheckModal = true
    },
    okCheckConsistency () {
      this.checkDataConsistency()
      this.confirmCheckModal = false
    },
    cancelCheckConsistency () {
      this.confirmCheckModal = false
    },
    checkDataConsistency () {
      const that = this
      this.axios.post('/api/drc/v1/monitor/consistency/data/full/' + this.$route.query.clusterA + '/' + this.$route.query.clusterB, {
        tableId: this.chooseTableConfig.tableId,
        schema: this.chooseTableConfig.schema,
        table: this.chooseTableConfig.table,
        key: this.chooseTableConfig.key,
        onUpdate: this.chooseTableConfig.onUpdate,
        startTimestamp: this.chooseTableConfig.startTimestamp,
        endTimeStamp: this.chooseTableConfig.endTimeStamp,
        mhaAName: this.$route.query.clusterA,
        mhaBName: this.$route.query.clusterB
      }).then(res => {
        console.log('add result')
        console.log(res.data)
        if (res.data.status === 0) {
          that.executeStatus = 'success'
          that.executeMsg = res.data.message
        } else {
          that.executeStatus = 'error'
          that.executeMsg = res.data.data
        }
        that.executeModal = true
      })
    },
    okExecuteModal () {
      this.executeModal = false
      this.confirmCheckModal = false
    },
    showCheckResult (row) {
      this.$router.push({
        name: 'fullDataConsistencyResult',
        query: {
          dbName: row.monitorSchemaName,
          tableName: row.monitorTableName,
          mhaA: this.$route.query.clusterA,
          mhaB: this.$route.query.clusterB,
          key: row.monitorTableKey,
          checkTime: formatDate(new Date(row.fullDataCheckLasttime), 'yyyy-MM-dd hh:mm:ss')
        }
      })
    }
  },
  computed: {
    dataWithPage () {
      const data = this.records
      const start = this.current * this.size - this.size
      const end = start + this.size
      return [...data].slice(start, end)
    }
  },
  created () {
    this.getConfiguredTables()
  }
}
</script>

<style scoped>

</style>

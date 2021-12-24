<template>
  <base-component :isFather="isFather" :subMenuName="['1']" :fatherMenu="fatherMenu">
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/incrementDataConsistencyResult">校验结果</BreadcrumbItem>
      <BreadcrumbItem to="/incrementDataConsistencyCluster">校验集群</BreadcrumbItem>
      <BreadcrumbItem>校验配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <div class="ivu-list-item-meta-title">
          <p>DRC集群：{{$route.query.clusterA}}(集群A)---{{$route.query.clusterB}}(集群B)</p>
        </div>
        <Table stripe :columns="columns" :data="dataWithPage">
        </Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :transfer="true"
            :total="tables.length"
            :current.sync="current"
            show-sizer
            show-elevator
            @on-page-size-change="handleChangeSize"></Page>
        </div>
      </div>
    </Content>
  </base-component>
</template>

<script>

export default {
  name: 'consistencyMonitorConfig',
  data () {
    return {
      isFather: true,
      fatherMenu: '/incrementDataConsistencyResult',
      confirmAddTableModal: false,
      resultConfirmAddTableModal: false,
      confirmAddTableResult: '',
      addTableStatus: '',
      addTableSMsg: '',
      commitItem: {
        monitorTableName: '',
        monitorTableKey: '',
        monitorTableOnUpdate: ''
      },
      confirmDeleteTableModal: false,
      resultConfirmDeleteTableModal: false,
      confirmDeleteTableResult: '',
      deleteTableStatus: '',
      deleteTableSMsg: '',
      commitDeleteItem: {
        monitorTableName: '',
        monitorTableKey: '',
        monitorTableOnUpdate: ''
      },
      deleteIndex: 0,
      tables: [],
      current: 1,
      size: 10,
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
          title: '库名',
          key: 'monitorSchemaName'
        },
        {
          title: '表名',
          key: 'monitorTableName'
        },
        {
          title: 'key',
          width: 100,
          key: 'monitorTableKey',
          align: 'center'
        },
        {
          title: 'onUpdate',
          key: 'monitorTableOnUpdate',
          align: 'center'
        },
        // {
        //   title: '操作',
        //   key: 'monitorSwitch',
        //   width: 200,
        //   align: 'center',
        //   render: (h, params) => {
        //     const row = params.row
        //     const type = row.monitorSwitch === 0 ? 'success' : 'error'
        //     const text = row.monitorSwitch === 0 ? '开启监控' : '取消监控'
        //
        //     return h('Button', {
        //       props: {
        //         type: type,
        //         size: 'small'
        //       },
        //       on: {
        //         click: () => {
        //           this.switchDataConsistencyMonitor(row)
        //         }
        //       }
        //     }, text)
        //   }
        // },
        {
          title: '操作',
          key: 'monitorSwitch',
          width: 200,
          align: 'center',
          render: (h, params) => {
            const row = params.row

            return h('i-switch', {
              props: {
                size: 'large',
                value: row.monitorSwitch === 1
              },
              scopedSlots: {
                open: () => h('span', '开启'),
                close: () => h('span', '关闭')
              },
              on: {
                'on-change': () => {
                  this.switchDataConsistencyMonitor(row)
                }
              }
            })
          }
        },
        {
          title: '校验结果',
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
  computed: {
    dataWithPage () {
      const data = this.tables
      const start = this.current * this.size - this.size
      const end = start + this.size
      return [...data].slice(start, end)
    }
  },
  methods: {
    handleDataConsistencyAdd () {
      this.tables.push({ monitorTableName: '', monitorTableKey: '', monitorTableOnUpdate: '', status: 0 })
    },
    handleDataConsistencyAddCommit (item) {
      this.confirmAddTableModal = true
      this.commitItem.monitorTableName = item.monitorTableName
      this.commitItem.monitorTableKey = item.monitorTableKey
      this.commitItem.monitorTableOnUpdate = item.monitorTableOnUpdate
    },
    okHandleDataConsistencyAddCommit () {
      const that = this
      this.axios.post('/api/drc/v1/monitor/consistency/data/' + this.$route.query.clusterA + '/' + this.$route.query.clusterB, {
        table: that.commitItem.monitorTableName,
        key: that.commitItem.monitorTableKey,
        onUpdate: that.commitItem.monitorTableOnUpdate
      }).then(res => {
        console.log('add result')
        console.log(res.data)
        if (res.data.status === 0) {
          that.addTableStatus = 'success'
          that.addTableSMsg = res.data.message
        } else {
          that.addTableStatus = 'error'
          that.addTableSMsg = res.data.data.message
        }
      })
      this.resultConfirmAddTableModal = true
    },
    cancelHandleDataConsistencyAddCommit () {
      this.confirmAddTableModal = false
    },
    okAddTableResult () {
      this.$router.go(0)
      this.confirmAddTableModal = false
      this.resultConfirmAddTableModal = false
    },

    handleDataConsistencyDeleteCommit (item) {
      this.confirmDeleteTableModal = true
      this.commitDeleteItem.monitorTableName = item.monitorTableName
      this.commitDeleteItem.monitorTableKey = item.monitorTableKey
      this.commitDeleteItem.monitorTableOnUpdate = item.monitorTableOnUpdate
      this.deleteIndex = item.id
    },
    okHandleDataConsistencyDeleteCommit () {
      const that = this
      this.axios.delete('/api/drc/v1/monitor/consistency/data/' + this.deleteIndex)
        .then(res => {
          console.log(res.data)
          if (res.data.status === 0) {
            that.deleteTableStatus = 'success'
            that.deleteTableSMsg = res.data.message
          } else {
            that.deleteTableStatus = 'error'
            that.deleteTableSMsg = res.data.data.message
          }
        })
      this.resultConfirmDeleteTableModal = true
    },
    cancelHandleDataConsistencyDeleteCommit () {
      this.confirmDeleteTableModal = false
    },
    okDeleteTableResult () {
      this.$router.go(0)
      this.confirmDeleteTableModal = false
      this.resultConfirmDeleteTableModal = false
    },
    getConfiguredTables () {
      const that = this
      this.axios.get('/api/drc/v1/monitor/consistency/data/' + this.$route.query.clusterA + '/' + this.$route.query.clusterB)
        .then(response => {
          that.tables = response.data.data
          console.log(response.data)
        })
    },
    handleChangeSize (val) {
      this.size = val
    },
    switchDataConsistencyMonitor (row) {
      const that = this
      const switchStatus = row.monitorSwitch === 0 ? 'on' : 'off'
      this.axios.post('/api/drc/v1/monitor/consistency/switches/' + switchStatus, [
        {
          srcMha: that.$route.query.clusterA,
          dstMha: that.$route.query.clusterB,
          schema: row.monitorSchemaName,
          table: row.monitorTableName,
          key: row.monitorTableKey,
          onUpdate: row.monitorTableOnUpdate
        }
      ]).then(res => {
        console.log('show result')
        console.log(res.data)
        if (res.data.status === 0) {
          row.monitorSwitch = 1 - row.monitorSwitch
        }
      })
    },
    showCheckResult (row) {
      this.$router.push({
        name: 'incrementDataConsistencyResult'
      })
    }
  },
  created () {
    this.getConfiguredTables()
  }
}
</script>

<style scoped>

</style>

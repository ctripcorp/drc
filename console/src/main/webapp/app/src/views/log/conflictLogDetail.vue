<template>
  <base-component :isFather="isFather" :subMenuName="['1']" :fatherMenu="fatherMenu">
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/conflictLog">冲突事务</BreadcrumbItem>
      <BreadcrumbItem>冲突详情</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div :style="{padding: '1px 1px',height: '100%'}">
        <Card>
          <p slot="title">
            自动冲突处理结果
          </p>
          <div class="ivu-list-item-meta-title">事务提交结果：
            <Tag :loading="logTableLoading" :color="trxLog.trxResult==0?'success':'error'" size="medium">{{trxLog.trxResultStr}}</Tag>
          </div>
          <div class="ivu-list-item-meta-title">所有机房当前冲突事务记录：
            <Tag :color="trxLog.recordEqual==true?'success':'error'" size="medium">{{trxLog.diffStr}}</Tag>
          </div>
          <Divider/>
          <div class="ivu-list-item-meta-title">源机房({{trxLog.srcDc}})</div>
          <Table size="small" stripe v-for="(item, index) in srcRecords" :key="index" :columns="item.columns" :data="item.records"
                 border></Table>
          <Divider/>
          <div class="ivu-list-item-meta-title">目标机房({{trxLog.dstDc}})</div>
          <Table size="small" stripe v-for="(item, index) in dstRecords" :key="index" :columns="item.columns" :data="item.records"
                 border></Table>
          <Divider/>
        </Card>
        <Divider/>
        <Card>
          <p slot="title">
            自动冲突处理流程
          </p>
          <Table stripe :loading="logTableLoading" :columns="trxLog.columns" :data="dataWithPage" ></Table>
          <div>
            <Page
              :transfer="true"
              :total="trxLog.tableData.length"
              :current.sync="trxLog.current"
              :page-size-opts="trxLog.pageSizeOpts"
              :page-size="this.trxLog.size"
              show-total
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
import conflictRowsLogDetail from './conflictRowsLogDetail.vue'

export default {
  name: 'conflictLogDetail',
  props: {
    rowData: String
  },
  data () {
    return {
      conflictTrxLogId: 0,
      recordLoading: false,
      logTableLoading: false,
      srcRecords: [],
      dstRecords: [],
      srcTable: {
        data: [],
        columns: []
      },
      dstTable: {
        data: [],
        columns: []
      },
      trxLog: {
        srcDc: '',
        dstDc: '',
        trxResult: null,
        trxResultStr: '',
        hasDiff: null,
        recordEqual: null,
        diffStr: '',
        tableData: [],
        columns: [
          {
            type: 'expand',
            width: 40,
            render: (h, params) => {
              const row = params.row
              return h(conflictRowsLogDetail, {
                props: {
                  rowData: '/*原始SQL*/\n' + row.rawSql + '\n/*原始SQL处理结果: ' + row.rawSqlResult + '*/\n\n' + '/*冲突时行记录*/\n' +
                    row.dstRowRecord + '\n\n' + '/*冲突时行记录*/\n' + row.handleSql + '\n/*冲突时行记录处理结果: ' + row.handleSqlResult + '*/'
                }
              })
            }
          },
          {
            title: '序号',
            width: 60,
            // align: 'left',
            render: (h, params) => {
              return h(
                'span',
                params.index + 1
              )
            }
          },
          {
            title: '原始sql',
            key: 'rawSql',
            tooltip: true
          },
          {
            title: '处理结果',
            key: 'rowResult',
            width: 120,
            align: 'center',
            render: (h, params) => {
              const row = params.row
              const color = row.rowResult === 0 ? 'blue' : 'volcano'
              const text = row.rowResult === 0 ? 'commit' : 'rollback'
              return h('Tag', {
                props: {
                  color: color
                }
              }, text)
            }
          }
        ],
        total: 0,
        current: 1,
        size: 10,
        pageSizeOpts: [10, 20, 50, 100]
      }
    }
  },
  methods: {
    handleSpan ({ row, column, rowIndex, columnIndex }) {
      if (columnIndex === 0) {
        return [0, 0]
      }
    },
    handleChangeSize (val) {
      this.trxLog.size = val
    },
    getTrxLogDetail () {
      this.logTableLoading = true
      this.axios.get('/api/drc/v2/conflict/log/detail?conflictTrxLogId=' + this.conflictTrxLogId)
        .then(response => {
          if (response.data.status === 1) {
            this.$Message.error('查询冲突详情失败!')
          } else {
            const data = response.data.data
            this.trxLog.srcDc = data.src
            this.trxLog.dstDc = data.dstDc
            this.trxLog.trxResult = data.trxResult
            this.trxLog.trxResultStr = data.trxResult === 0 ? 'commit' : 'rollback'
            this.trxLog.tableData = data.rowsLogDetailViews
          }
        })
        .finally(() => {
          this.logTableLoading = false
        })
    },
    getTrxRecords () {
      this.axios.get('/api/drc/v2/conflict/log/records?conflictTrxLogId=' + this.conflictTrxLogId)
        .then(response => {
          if (response.data.status === 1) {
            this.$Message.error('查询当前行记录失败!')
          } else {
            const data = response.data.data
            this.trxLog.recordEqual = data.recordIsEqual
            this.trxLog.diffStr = data.recordIsEqual === 0 ? '数据一致' : '数据不一致'
            this.srcRecords = data.srcRecords
            this.dstRecords = data.dstRecords
          }
        })
    }
  },
  computed: {
    dataWithPage () {
      const data = this.trxLog.tableData
      const start = this.trxLog.current * this.trxLog.size - this.trxLog.size
      const end = start + this.trxLog.size
      return [...data].slice(start, end)
    }
  },
  created () {
    this.conflictTrxLogId = this.$route.query.conflictTrxLogId
    this.getTrxLogDetail()
    this.getTrxRecords()
  }
}
</script>

<style scoped>

</style>

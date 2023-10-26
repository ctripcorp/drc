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
            <Button :loading="logTableLoading" size="small" :type="trxLog.trxResult==0?'success':'error'">{{trxLog.trxResultStr}}</Button>
          </div>
          <div class="ivu-list-item-meta-title">所有机房当前冲突事务记录：
            <Tooltip content="数据一致性比对忽略字段过滤的列">
              <Button :loading="recordLoading" size="small" :type="trxLog.recordEqual==true?'success':'error'">{{trxLog.diffStr}}</Button>
            </Tooltip >
          </div>
          <Divider/>
          <div class="ivu-list-item-meta-title">源机房({{trxLog.srcDc}})</div>
          <Card v-for="(item, index) in srcRecords" :key="index">
            <div class="ivu-list-item-meta-title">表名：{{item.tableName}}
              <Tooltip :content="item.doubleSync==true?'双向同步':'单向同步'">
                <Button size="small" :type="item.doubleSync==true?'success':'primary'">{{item.doubleSync==true?'双向同步':'单向同步'}}</Button>
              </Tooltip >
            </div>
            <Table  size="small" stripe :columns="item.columns" :data="item.records" border></Table>
          </Card>
          <Divider/>
          <div class="ivu-list-item-meta-title">目标机房({{trxLog.dstDc}})</div>
          <Card v-for="(item, index) in dstRecords" :key="index">
            <div class="ivu-list-item-meta-title">表名：{{item.tableName}}
              <Tooltip :content="item.doubleSync==true?'双向同步':'单向同步'">
                <Button size="small" :type="item.doubleSync==true?'success':'primary'">{{item.doubleSync==true?'双向同步':'单向同步'}}</Button>
              </Tooltip >
            </div>
            <Table  size="small" stripe :columns="item.columns" :data="item.records" border></Table>
          </Card>
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
                    row.dstRowRecord + '\n\n' + '/*冲突处理SQL*/\n' + row.handleSql + '\n/*冲突处理SQL处理结果: ' + row.handleSqlResult + '*/'
                }
              })
            }
          },
          {
            title: '执行顺序',
            key: 'rowId',
            width: 100
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
      this.axios.get('/api/drc/v2/log/conflict/detail?conflictTrxLogId=' + this.conflictTrxLogId)
        .then(response => {
          if (response.data.status === 1) {
            this.$Message.error('查询冲突详情失败!')
          } else {
            const data = response.data.data
            this.trxLog.srcDc = data.srcDc
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
      this.recordLoading = true
      this.axios.get('/api/drc/v2/log/conflict/records?conflictTrxLogId=' + this.conflictTrxLogId + '&columnSize=12')
        .then(response => {
          if (response.data.status === 1) {
            // this.$Message.error('查询当前行记录失败!')
            this.trxLog.diffStr = '数据比对失败'
          } else {
            const data = response.data.data
            this.trxLog.recordEqual = data.recordIsEqual
            this.trxLog.diffStr = data.recordIsEqual ? '数据一致' : '数据不一致'
            this.srcRecords = data.srcRecords
            // data.srcRecords.forEach(e => this.srcRecords.push(e))
            this.dstRecords = data.dstRecords
            // data.dstRecords.forEach(e => this.dstRecords.push(e))
          }
        })
        .finally(() => {
          this.recordLoading = false
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

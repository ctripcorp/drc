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
          <div v-if="!byRowLogIds" class="ivu-list-item-meta-title">事务提交结果：
            <Button :loading="logTableLoading" size="small" :type="trxLog.trxResult==0?'success':'error'">
              {{trxLog.trxResultStr}}
            </Button>
          </div>
          <div class="ivu-list-item-meta-title">所有机房当前冲突行记录：
            <Tooltip content="数据一致性比对忽略字段过滤的列">
              <Button :loading="recordLoading" size="small" @click="getRecords" :type="trxLog.recordEqual==true?'success':'error'">{{trxLog.diffStr}}</Button>
            </Tooltip>
          </div>
          <Divider/>
          <div class="ivu-list-item-meta-title">源机房({{trxLog.srcRegion}})</div>
          <Card v-for="(item, index) in srcRecords" :key="index">
            <div class="ivu-list-item-meta-title">表名：{{item.tableName}}
              <Tooltip :content="item.doubleSync==true?'双向同步':'单向同步'">
                <Button size="small" :type="item.doubleSync==true?'success':'primary'">
                  {{item.doubleSync == true ? '双向同步' : '单向同步'}}
                </Button>
              </Tooltip>
            </div>
            <Table size="small" stripe :columns="item.columns" :data="item.records" border></Table>
          </Card>
          <Divider/>
          <div class="ivu-list-item-meta-title">目标机房({{trxLog.dstRegion}})</div>
          <Card v-for="(item, index) in dstRecords" :key="index">
            <div class="ivu-list-item-meta-title">表名：{{item.tableName}}
              <Tooltip :content="item.doubleSync==true?'双向同步':'单向同步'">
                <Button size="small" :type="item.doubleSync==true?'success':'primary'">
                  {{item.doubleSync == true ? '双向同步' : '单向同步'}}
                </Button>
              </Tooltip>
            </div>
            <Table size="small" stripe :columns="item.columns" :data="item.records" border></Table>
          </Card>
        </Card>
        <Divider/>
        <Card>
          <p slot="title">
            DRC冲突处理流程
          </p>
          <Table stripe :loading="logTableLoading" :columns="trxLog.columns" :data="dataWithPage"
                 ref="multipleTable" @on-selection-change="changeSelection"></Table>
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
        <Divider/>
        <Card>
          <p slot="title">
            选择冲突行自动冲突处理
          </p>
          <Row :gutter=10>
            <Col span="3">
              <Select filterable clearable v-model="writeSide" placeholder="选择写入region">
                <Option v-for="item in regionOpt" :value="item.key" :key="item.key">{{ item.val }}</Option>
              </Select>
            </Col>
            <Col span="4">
              <Button :loading="sqlLoading" size="middle" @click="generateHandleSql" type="success">生成SQL</Button>
            </Col>
          </Row>
          <br>
          <div :loading="sqlLoading">
            <codemirror  v-model="handleSql" :options="options"></codemirror>
          </div>
          <br>
          <Button :loading="approvalLoading" size="middle" @click="generateApproval" type="primary">提交审批</Button>
        </Card>
        <Divider/>
      </div>
    </Content>
  </base-component>
</template>

<script>
import conflictRowsLogDetail from './conflictRowsLogDetail.vue'
import { codemirror } from 'vue-codemirror'
import 'codemirror/theme/ambiance.css'
import 'codemirror/mode/sql/sql.js'

export default {
  name: 'conflictLogDetail',
  props: {
    rowData: String
  },
  components: {
    codemirror
  },
  data () {
    return {
      approvalLoading: false,
      handleSql: '',
      handleSqlList: [],
      showHandleSql: false,
      multiData: [],
      sqlLoading: false,
      writeSide: 0,
      regionOpt: [],
      byRowLogIds: false,
      rowLogIds: [],
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
        srcRegion: '',
        dstRegion: '',
        trxResult: null,
        trxResultStr: '',
        hasDiff: null,
        recordEqual: null,
        diffStr: '',
        tableData: [],
        columns: [
          {
            type: 'selection',
            width: 40,
            align: 'center'
          },
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
            title: '表名',
            key: 'tableName',
            width: 250,
            tooltip: true,
            render: (h, params) => {
              const row = params.row
              const text = row.dbName + '.' + row.tableName
              return h('div', text)
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
      },
      options: {
        value: '',
        mode: 'text/x-mysql',
        theme: 'ambiance',
        lineWrapping: true,
        height: 100,
        readOnly: true,
        lineNumbers: true
      }
    }
  },
  methods: {
    generateApproval () {
      if (this.handleSqlList.length === 0) {
        this.$Message.warning('未生成SQL！')
        return
      }
      this.approvalLoading = true
      const SqlDataList = []
      this.handleSqlList.forEach(sql => {
        const sqlData = {
          rowLogId: sql.rowLogId,
          handleSql: sql.autoHandleSql
        }
        SqlDataList.push(sqlData)
      })
      this.axios.post('/api/drc/v2/log/approval/create', {
        writeSide: this.writeSide,
        handleSqlDtos: SqlDataList
      }).then(res => {
        if (res.data.status === 1) {
          this.$Message.error('提交审批单失败！')
        } else {
          this.$Message.success('提交审批单成功')
        }
      }).finally(() => {
        this.approvalLoading = false
      })
    },
    generateHandleSql () {
      const multiData = this.multiData
      if (multiData === undefined || multiData === null || multiData.length === 0) {
        this.$Message.warning('请勾选！')
        return
      }
      const rowLogIds = []
      const srcRecords = []
      const dstRecords = []
      multiData.forEach(data => rowLogIds.push(data.rowLogId))
      this.srcRecords.forEach(srcRecord => {
        srcRecord.records.forEach(record => srcRecords.push(record))
      })
      this.dstRecords.forEach(dstRecord => {
        dstRecord.records.forEach(record => dstRecords.push(record))
      })
      this.sqlLoading = true
      this.handleSql = ''
      this.axios.post('/api/drc/v2/log/conflict/rows/handleSql', {
        writeSide: this.writeSide,
        srcRecords: srcRecords,
        dstRecords: dstRecords,
        rowLogIds: rowLogIds
      }).then(res => {
        const data = res.data
        if (data.status === 0) {
          this.handleSqlList = data.data
          const handleSqlList = []
          data.data.forEach(e => handleSqlList.push(e.autoHandleSql))
          this.handleSql = handleSqlList.join('\n')
        } else {
          this.$Message.error('自动生成SQL失败: ' + data.message)
        }
      }).finally(() => {
        this.sqlLoading = false
      })
    },
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
            this.trxLog.srcRegion = data.srcRegion
            this.trxLog.dstRegion = data.dstRegion
            this.trxLog.trxResult = data.trxResult
            this.trxLog.trxResultStr = data.trxResult === 0 ? 'commit' : 'rollback'
            this.trxLog.tableData = data.rowsLogDetailViews
          }
        })
        .finally(() => {
          this.logTableLoading = false
        })
    },
    getTrxLogDetail1 () {
      this.logTableLoading = true
      this.axios.get('/api/drc/v2/log/conflict/rows/detail?conflictRowLogIds=' + this.rowLogIds)
        .then(response => {
          if (response.data.status === 1) {
            this.$Message.error('查询冲突详情失败!')
          } else {
            this.trxLog.tableData = response.data.data
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
    },
    getTrxRecords1 () {
      this.recordLoading = true
      this.axios.get('/api/drc/v2/log/conflict/rows/records?conflictRowLogIds=' + this.rowLogIds)
        .then(response => {
          if (response.data.status === 1) {
            // this.$Message.error('查询当前行记录失败!')
            this.trxLog.diffStr = '数据比对失败'
          } else {
            const data = response.data.data
            this.trxLog.recordEqual = data.recordIsEqual
            this.trxLog.diffStr = data.recordIsEqual ? '数据一致' : '数据不一致'
            this.srcRecords = data.srcRecords
            this.dstRecords = data.dstRecords
          }
        })
        .finally(() => {
          this.recordLoading = false
        })
    },
    getRecords () {
      if (this.byRowLogIds) {
        this.getTrxRecords1()
      } else {
        this.getTrxRecords()
      }
    },
    changeSelection (val) {
      this.multiData = val
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
    this.byRowLogIds = this.$route.query.byRowLogIds
    this.rowLogIds = this.$route.query.rowLogIds
    this.trxLog.srcRegion = this.$route.query.srcRegion
    this.trxLog.dstRegion = this.$route.query.dstRegion
    if (this.byRowLogIds) {
      this.getTrxLogDetail1()
      this.getTrxRecords1()
    } else {
      this.getTrxLogDetail()
      this.getTrxRecords()
    }
    this.regionOpt = [
      {
        key: 0,
        val: this.trxLog.srcRegion
      },
      {
        key: 1,
        val: this.trxLog.dstRegion
      }
    ]
  }
}
</script>

<style scoped>
.ivu-table .table-info-cell-extra-column-add {
  background-color: #2db7f5;
  color: #fff;
}

.ivu-table .table-info-cell-extra-column-diff {
  background-color: #ff6600;
  color: #fff;
}

</style>

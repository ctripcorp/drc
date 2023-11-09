<template>
  <div style="padding: 1px 1px ">
    <Row :gutter=10 align="middle">
      <Col span="20">
        <Card :padding=5>
          <template #title>查询条件</template>
          <Row :gutter=10 v-show="!searchMode">
            <Col span="3">
              <Select filterable clearable v-model="queryParam.srcRegion" placeholder="源region">
                <Option v-for="item in regions" :value="item.regionName" :key="item.regionName">
                  {{ item.regionName }}
                </Option>
              </Select>
            </Col>
            <Col span="3">
              <Select filterable clearable v-model="queryParam.dstRegion" placeholder="目标region">
                <Option v-for="item in regions" :value="item.regionName" :key="item.regionName">
                  {{ item.regionName }}
                </Option>
              </Select>
            </Col>
            <Col span="4">
              <Input prefix="ios-search" v-model="queryParam.dbName" placeholder="库名"></Input>
            </Col>
            <Col span="4">
              <Input prefix="ios-search" v-model="queryParam.tableName" placeholder="表名"></Input>
            </Col>
            <Col span="4">
              <DatePicker type="datetime" :editable="editable" v-model="queryParam.beginHandleTime"
                          :clearable="false" placeholder="起始日期"></DatePicker>
            </Col>
            <Col span="4">
              <DatePicker type="datetime" :editable="editable" v-model="queryParam.endHandleTime"
                          :clearable="false" placeholder="结束日期"></DatePicker>
            </Col>
            <Col span="2">
              <Select filterable clearable v-model="queryParam.rowResult" placeholder="执行结果">
                <Option v-for="item in resultOpts" :value="item.val" :key="item.val">{{ item.name }}</Option>
              </Select>
            </Col>
          </Row>
          <Row :gutter=10 v-show="searchMode">
            <Col span="12">
              <Input prefix="ios-search" v-model="queryParam.gtid" placeholder="事务id"></Input>
            </Col>
            <Col span="4">
              <DatePicker type="datetime" :editable="editable" v-model="queryParam.beginHandleTime"
                          :clearable="false" placeholder="起始日期"></DatePicker>
            </Col>
            <Col span="4">
              <DatePicker type="datetime" :editable="editable" v-model="queryParam.endHandleTime"
                          :clearable="false" placeholder="结束日期"></DatePicker>
            </Col>
            <Col span="4">
              <Select filterable clearable v-model="queryParam.rowResult" placeholder="执行结果">
                <Option v-for="item in resultOpts" :value="item.val" :key="item.val">{{ item.name }}</Option>
              </Select>
            </Col>
          </Row>
        </Card>
      </Col>
      <Col span="4">
        <Row :gutter=10 align="middle">
          <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getData">查询</Button>
          <i-switch v-model="searchMode" size="large" style="margin-left: 10px">进阶
            <template #open>
              <span>进阶</span>
            </template>
            <template #close>
              <span>进阶</span>
            </template>
          </i-switch>
        </Row>
        <Row :gutter=10 align="middle" style="margin-top: 20px">
          <Button icon="md-refresh" @click="resetParam">重置</Button>
        </Row>
      </Col>
    </Row>
    <br>
    <Row  style="background: #fdfdff; border: 1px solid #e8eaec;">
      <Col span="2" style="display: flex;float: left;margin: 5px" >
        <Dropdown placement="bottom-start">
          <Button type="default" icon="ios-hammer">
            批量操作
            <Icon type="ios-arrow-down"></Icon>
          </Button>
          <template #list>
            <DropdownMenu >
              <DropdownItem @click.native="compareRecords">数据比对</DropdownItem>
              <DropdownItem @click.native="getLogDetails">冲突行详情</DropdownItem>
            </DropdownMenu>
          </template>
        </Dropdown>
      </Col>
    </Row>
    <Table stripe border :columns="columns" :data="tableData" ref="multipleTable"
           @on-selection-change="changeSelection">
      <template slot-scope="{ row, index }" slot="action">
        <Button type="primary" size="small" @click="getLogDetail1(row, index)" style="margin-right: 5px">
          详情
        </Button>
        <Button type="success" size="small" @click="queryTrxLog(row, index)" style="margin-right: 5px">
          冲突事务
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
        @on-change="getData"
        @on-page-size-change="handleChangeSize"></Page>
    </div>
    <Modal
      v-model="detailModal"
      title="冲突行详情"
      width="1200px" :scrollable="true" :draggable="true">
      <div :style="{padding: '1px 1px',height: '100%'}">
        <Card>
          <div class="ivu-list-item-meta-title">冲突行提交结果：
            <Button :loading="modalLoading" size="small" :type="logDetail.result==0?'success':'error'">{{logDetail.resultStr}}</Button>
          </div>
          <div class="ivu-list-item-meta-title">冲突行数据一致性比较结果：
            <Tooltip content="数据一致性比对忽略字段过滤的列">
              <Button :loading="modalLoading" size="small" :type="logDetail.recordEqual==true?'success':'error'">{{logDetail.diffStr}}</Button>
            </Tooltip >
          </div>
          <Divider/>
          <div class="ivu-list-item-meta-title">源机房({{logDetail.srcRegion}})</div>
          <Card v-for="(item, index) in logDetail.srcRecords" :key="index">
            <div class="ivu-list-item-meta-title">表名：{{item.tableName}}
              <Tooltip :content="item.doubleSync==true?'双向同步':'单向同步'">
                <Button size="small" :type="item.doubleSync==true?'success':'primary'">{{item.doubleSync==true?'双向同步':'单向同步'}}</Button>
              </Tooltip >
            </div>
            <Table :loading="modalLoading" size="small" stripe :columns="item.columns" :data="item.records" border></Table>
          </Card>
          <Divider/>
          <div class="ivu-list-item-meta-title">目标机房({{logDetail.dstRegion}})</div>
          <Card v-for="(item, index) in logDetail.dstRecords" :key="index">
            <div class="ivu-list-item-meta-title">表名：{{item.tableName}}
              <Tooltip :content="item.doubleSync==true?'双向同步':'单向同步'">
                <Button size="small" :type="item.doubleSync==true?'success':'primary'">{{item.doubleSync==true?'双向同步':'单向同步'}}</Button>
              </Tooltip >
            </div>
            <Table :loading="modalLoading" size="small" stripe :columns="item.columns" :data="item.records" border></Table>
          </Card>
          <Divider/>
          <Card>
            <codemirror v-model="rowData" :options="options"></codemirror>
          </Card>
        </Card>
      </div>
    </Modal>
    <Modal
      v-model="compareModal"
      title="数据一致性比对结果"
      width="800px">
      <div v-if="this.rowLogIds.length>0" class="ivu-list-item-meta-title">存在数据比对不一致的冲突行，点击查询
        <Tooltip content="冲突行仅限当前页面">
          <Button size="middle" type="success" @click="getUnEqualRecords">冲突行</Button>
        </Tooltip >
      </div>
      <Table stripe border :loading="compareLoading" :columns="compareData.columns" :data="compareData.compareRowRecords">
      </Table>
    </Modal>
  </div>
</template>

<script>
import { codemirror } from 'vue-codemirror'
import 'codemirror/theme/ambiance.css'
import 'codemirror/mode/sql/sql.js'
export default {
  name: 'conflictRowsLog',
  props: {
    gtid: String,
    searchMode: Boolean
  },
  components: {
    codemirror
  },
  data () {
    return {
      compareModal: false,
      multiData: [],
      detailModal: false,
      modalLoading: false,
      compareLoading: false,
      rowLogIds: [],
      compareData: {
        compareRowRecords: [],
        columns: [
          {
            title: '表名',
            key: 'tableName'
          },
          {
            title: '比对结果',
            key: 'recordIsEqual',
            render: (h, params) => {
              const row = params.row
              const color = row.recordIsEqual ? 'blue' : 'volcano'
              const text = row.recordIsEqual ? '数据一致' : '数据不一致'
              return h('Tag', {
                props: {
                  color: color
                }
              }, text)
            }
          }
        ]
      },
      logDetail: {
        srcRecords: [],
        dstRecords: [],
        srcRegion: '',
        dstRegion: '',
        result: null,
        resultStr: '',
        hasDiff: null,
        recordEqual: null,
        diffStr: '',
        rowData: ''
      },
      regions: [],
      // searchMode: this.searchMode1,
      editable: false,
      dataLoading: false,
      queryParam: {
        srcRegion: null,
        dstRegion: null,
        dbName: null,
        tableName: null,
        gtid: this.gtid,
        beginHandleTime: '2023-11-07 13:00:00',
        endHandleTime: '2023-11-07 15:00:00',
        rowResult: null
      },
      tableData: [],
      columns: [
        // {
        //   title: '序号',
        //   width: 75,
        //   align: 'center',
        //   // fixed: 'left',
        //   render: (h, params) => {
        //     return h(
        //       'span',
        //       params.index + 1
        //     )
        //   }
        // },
        {
          type: 'selection',
          width: 60,
          align: 'center'
        },
        {
          title: '同步方向',
          key: 'region',
          width: 160,
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.srcRegion + ' -> ' + row.dstRegion
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
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
          title: '冲突处理sql',
          key: 'handleSql',
          tooltip: true
        },
        {
          title: '事务提交时间',
          key: 'handleTime',
          width: 180,
          sortable: true
        },
        {
          title: '执行结果',
          key: 'rowResult',
          width: 150,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const color = row.rowResult === 0 ? 'blue' : 'volcano'
            const text = row.rowResult === 0 ? 'commit' : 'rollBack'
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '操作',
          slot: 'action',
          width: 165,
          align: 'center'
        }
      ],
      total: 0,
      current: 1,
      size: 10,
      pageSizeOpts: [10, 20, 50, 100],
      resultOpts: [
        {
          name: 'commit',
          val: 0
        },
        {
          name: 'rollBack',
          val: 1
        }
      ],
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
    compareRecords () {
      const multiData = this.multiData
      if (multiData === undefined || multiData === null || multiData.length === 0) {
        this.$Message.warning('请勾选！')
        return
      }
      this.compareLoading = true
      this.compareModal = true
      const rowLogIds = []
      multiData.forEach(data => rowLogIds.push(data.conflictRowsLogId))
      this.axios.get('/api/drc/v2/log/conflict/records/compare?conflictRowLogIds=' + rowLogIds)
        .then(response => {
          if (response.data.status === 1) {
            this.compareData.compareRowRecords = []
            this.$Message.error({
              content: '数据比对失败! ' + response.data.message,
              duration: 5
            })
          } else {
            const data = response.data.data
            this.compareData.compareRowRecords = data.recordDetailList
            this.rowLogIds = []
            data.rowLogIds.forEach(e => this.rowLogIds.push(e))
          }
        })
        .finally(() => {
          this.compareLoading = false
        })
    },
    getLogDetails () {
      const multiData = this.multiData
      if (multiData === undefined || multiData === null || multiData.length === 0) {
        this.$Message.warning('请勾选！')
        return
      }
      const rowLogIds = []
      const row = multiData[0]
      multiData.forEach(data => rowLogIds.push(data.conflictRowsLogId))
      this.axios.get('/api/drc/v2/log/conflict/rows/check?conflictRowLogIds=' + rowLogIds)
        .then(response => {
          if (response.data.status === 1) {
            this.$Message.error(response.data.message)
          } else {
            const detail = this.$router.resolve({
              path: '/conflictLogDetail',
              query: {
                byRowLogIds: true,
                rowLogIds: rowLogIds,
                srcRegion: row.srcRegion,
                dstRegion: row.dstRegion
              }
            })
            window.open(detail.href, '_blank')
          }
        })
    },
    changeSelection (val) {
      this.multiData = val
      console.log(this.multiData)
    },
    getLogDetail1 (row, index) {
      this.modalLoading = true
      this.detailModal = true
      this.logDetail.result = row.rowResult
      this.logDetail.resultStr = row.rowResult === 0 ? 'commit' : 'rollBack'
      this.logDetail.srcRegion = row.srcRegion
      this.logDetail.dstRegion = row.dstRegion
      this.axios.get('/api/drc/v2/log/conflict/row/record?conflictRowLogId=' + row.conflictRowsLogId + '&columnSize=12')
        .then(response => {
          if (response.data.status === 1) {
            this.$Message.error({
              content: '查询失败! ' + response.data.message,
              duration: 5
            })
            // this.$Message.error(response.data.message)
            this.logDetail.recordEqual = false
            this.logDetail.diffStr = '数据比对失败'
            this.logDetail.srcRecords = []
            this.logDetail.dstRecords = []
          } else {
            const data = response.data.data
            this.logDetail.recordEqual = data.recordIsEqual
            this.logDetail.diffStr = data.recordIsEqual ? '数据一致' : '数据不一致'
            this.logDetail.srcRecords = data.srcRecords
            this.logDetail.dstRecords = data.dstRecords
            this.rowData = '/*原始SQL*/\n' + row.rawSql + '\n/*原始SQL处理结果: ' + row.rawSqlResult + '*/\n\n' + '/*冲突时行记录*/\n' +
              row.dstRowRecord + '\n\n' + '/*冲突处理SQL*/\n' + row.handleSql + '\n/*冲突处理SQL处理结果: ' + row.handleSqlResult + '*/'
          }
        })
        .finally(() => {
          this.modalLoading = false
        })
    },
    getRegions () {
      this.axios.get('/api/drc/v2/meta/regions/all')
        .then(response => {
          this.regions = response.data.data
        })
    },
    queryTrxLog (row, index) {
      this.$emit('tabValueChanged', 'trxLog')
      this.$emit('gtidChanged', row.gtid)
      // this.tabVal = 'rowsLog'
    },
    getUnEqualRecords () {
      this.multiData = []
      this.compareModal = false
      this.dataLoading = true
      this.axios.get('/api/drc/v2/log/conflict/rows/rowLogIds?rowLogIds=' + this.rowLogIds)
        .then(response => {
          const data = response.data
          if (data.status === 1) {
            this.$Message.error('查询失败')
          } else {
            this.tableData = data.data
            this.total = data.data.length
            this.current = 1
            this.tableData = data.data
            if (this.total === 0) {
              this.$Message.warning('查询结果为空')
            } else {
              this.$Message.success('以下冲突行数据对比不一致')
            }
          }
        })
        .finally(() => {
          this.dataLoading = false
        })
    },
    getData () {
      this.multiData = []
      this.compareData.compareRowRecords = []
      this.rowLogIds = []
      const beginTime = this.queryParam.beginHandleTime
      const endTime = this.queryParam.endHandleTime
      const beginHandleTime = new Date(beginTime).getTime()
      const endHandleTime = new Date(endTime).getTime()
      // const beginHandleTime = beginTime === null || isNaN(beginTime) ? null : new Date(beginTime).getTime()
      // const endHandleTime = endTime === null || isNaN(endTime) ? null : new Date(endTime).getTime()
      console.log('beginTime: ' + beginTime)
      console.log('endTime: ' + endTime)
      const params = {
        gtid: this.queryParam.gtid,
        dbName: this.queryParam.dbName,
        tableName: this.queryParam.tableName,
        rowResult: this.queryParam.rowResult,
        srcRegion: this.queryParam.srcRegion,
        dstRegion: this.queryParam.dstRegion,
        beginHandleTime: beginHandleTime,
        endHandleTime: endHandleTime,
        pageReq: {
          pageSize: this.size,
          pageIndex: this.current
        }
      }
      console.log('params')
      console.log(params)
      const reqParam = this.flattenObj(params)
      this.dataLoading = true
      this.axios.get('/api/drc/v2/log/conflict/rows', { params: reqParam })
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
      this.queryParam = {
        dbName: null,
        tableName: null,
        gtId: null,
        beginHandleTime: '2023-11-07 13:00:00',
        endHandleTime: '2023-11-07 15:00:00',
        rowResult: null,
        srcRegion: null,
        dstRegion: null
      }
      this.rowLogIds = null
    },
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getData()
      })
    }
  },
  created () {
    this.resetParam()
    this.getData()
    this.getRegions()
  }
}
</script>

<style scoped>

</style>

<template>
  <div style="padding: 1px 1px">
    <Row :gutter=10 align="middle">
      <Col span="22">
        <Card :padding=5>
          <template #title>查询条件</template>
          <Row :gutter=10>
            <Col span="8">
              <Input prefix="ios-search" v-model="queryParam.gtid" placeholder="事务id"></Input>
            </Col>
            <Col span="4">
              <Input prefix="ios-search" v-model="queryParam.srcMhaName" placeholder="源MHA"></Input>
            </Col>
            <Col span="4">
              <Input prefix="ios-search" v-model="queryParam.dstMhaName" placeholder="目标MHA"></Input>
            </Col>
            <Col span="3">
              <DatePicker type="datetime" :editable="editable"  v-model="queryParam.beginHandleTime"
                          placeholder="起始日期"></DatePicker>
            </Col>
            <Col span="3">
              <DatePicker type="datetime" :editable="editable" v-model="queryParam.endHandleTime"
                          placeholder="结束日期"></DatePicker>
            </Col>
            <Col span="2">
              <Select filterable clearable v-model="queryParam.trxResult" placeholder="执行结果">
                <Option v-for="item in resultOpts" :value="item.val" :key="item.val">{{ item.name }}</Option>
              </Select>
            </Col>
          </Row>
        </Card>
      </Col>
      <Col span="1">
        <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getTrxData">查询</Button>
        <Button icon="md-refresh"  @click="resetParam" style="margin-top: 20px">重置
        </Button>
      </Col>
    </Row>
    <br>
    <Table stripe border :columns="columns" :data="tableData">
      <template slot-scope="{ row, index }" slot="action">
        <Button type="primary" size="small" @click="getLogDetail(row, index)" style="margin-right: 5px">
          详情
        </Button>
        <Button type="success" size="small" @click="queryRowsLog(row, index)" style="margin-right: 5px">
          冲突行
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
        @on-change="getTrxData"
        @on-page-size-change="handleChangeSize"></Page>
    </div>
  </div>
</template>

<script>

export default {
  name: 'conflictTrxLog',
  props: {
    gtid: String,
    beginHandleTime: String,
    endHandleTime: String
  },
  data () {
    return {
      editable: false,
      clearable: false,
      dataLoading: false,
      queryParam: {
        srcMhaName: null,
        dstMhaName: null,
        gtid: this.gtid,
        beginHandleTime: this.beginHandleTime,
        endHandleTime: this.endHandleTime,
        trxResult: null
      },
      tableData: [],
      columns: [
        {
          title: '事务ID',
          key: 'gtid',
          tooltip: true
        },
        {
          title: '源MHA',
          key: 'srcMhaName',
          width: 200
        },
        {
          title: '目标MHA',
          key: 'dstMhaName',
          width: 200
        },
        {
          title: '事务影响行数',
          key: 'trxRowsNum',
          width: 150,
          sortable: true
        },
        {
          title: '冲突行数',
          key: 'cflRowsNum',
          width: 150,
          sortable: true
        },
        {
          title: '事务提交时间',
          key: 'handleTime',
          width: 200,
          sortable: true
        },
        {
          title: '执行结果',
          key: 'trxResult',
          width: 150,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const color = row.trxResult === 0 ? 'blue' : 'volcano'
            const text = row.trxResult === 0 ? 'commit' : 'rollBack'
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
          width: 150,
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
      ]
    }
  },
  methods: {
    getTrxData () {
      const beginTime = this.queryParam.beginHandleTime
      const endTime = this.queryParam.endHandleTime
      const beginHandleTime = new Date(beginTime).getTime()
      const endHandleTime = new Date(endTime).getTime()
      console.log('beginTime: ' + beginTime)
      console.log('endTime: ' + endTime)
      if (isNaN(beginHandleTime) || isNaN(endHandleTime)) {
        this.$Message.warning('请选择时间范围!')
        return
      }
      const params = {
        gtId: this.queryParam.gtid,
        srcMhaName: this.queryParam.srcMhaName,
        dstMhaName: this.queryParam.dstMhaName,
        trxResult: this.queryParam.trxResult,
        beginHandleTime: beginHandleTime,
        endHandleTime: endHandleTime,
        pageReq: {
          pageSize: this.size,
          pageIndex: this.current
        }
      }
      const reqParam = this.flattenObj(params)
      this.dataLoading = true
      this.axios.get('/api/drc/v2/log/conflict/trx', { params: reqParam })
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
        srcMhaName: null,
        dstMhaName: null,
        gtid: null,
        beginHandleTime: this.beginHandleTime,
        endHandleTime: this.endHandleTime,
        trxResult: null
      }
    },
    queryRowsLog (row, index) {
      this.$emit('tabValueChanged', 'rowsLog')
      this.$emit('gtidChanged', row.gtid)
      this.$emit('searchModeChanged', true)
      this.$emit('beginHandleTimeChanged', this.queryParam.beginHandleTime)
      this.$emit('endHandleTimeChanged', this.queryParam.endHandleTime)
      // this.tabVal = 'rowsLog'
    },
    getLogDetail (row, index) {
      const detail = this.$router.resolve({
        path: '/conflictLogDetail',
        query: {
          conflictTrxLogId: row.conflictTrxLogId,
          queryType: '0'
        }
      })
      window.open(detail.href, '_blank')
    },
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getTrxData()
      })
    }
  },
  created () {
    this.getTrxData()
  }
}
</script>

<style scoped>

</style>

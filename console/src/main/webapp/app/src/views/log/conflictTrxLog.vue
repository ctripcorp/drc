<template>
  <div style="padding: 1px 1px ">
    <Row :gutter=10 align="middle">
      <Col span="22">
        <Card :padding=5>
          <template #title>查询条件</template>
          <Row :gutter=10>
            <Col span="8">
              <Input prefix="ios-search" v-model="queryParam.gtid" placeholder="事务id"
                     @on-enter="getTrxData"></Input>
            </Col>
            <Col span="4">
              <Input prefix="ios-search" v-model="queryParam.srcMhaName" placeholder="源MHA"
                     @on-enter="getTrxData"></Input>
            </Col>
            <Col span="4">
              <Input prefix="ios-search" v-model="queryParam.dstMhaName" placeholder="目标MHA"
                     @on-enter="getTrxData"></Input>
            </Col>
            <Col span="3">
              <DatePicker type="date" :editable="editable" value-format="timestamp" v-model="queryParam.beginHandleTime"
                          placeholder="起始日期"></DatePicker>
            </Col>
            <Col span="3">
              <DatePicker type="date" :editable="editable" v-model="queryParam.endHandleTime"
                          placeholder="结束日期"></DatePicker>
            </Col>
            <Col span="2">
              <Select filterable clearable v-model="queryParam.trxResult" placeholder="执行结果"
                      @on-change="getTrxData">
                <Option v-for="item in resultOpts" :value="item.val" :key="item.val">{{ item.name }}</Option>
              </Select>
            </Col>
          </Row>
        </Card>
      </Col>
      <Col span="1">
        <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getTrxData">查询</Button>
        <Button icon="md-refresh" :loading="dataLoading" @click="resetParam" style="margin-top: 20px">重置
        </Button>
      </Col>
    </Row>
    <br>
    <Table stripe border :columns="columns" :data="tableData">
      <template slot-scope="{ row, index }" slot="action">
        <Button type="success" size="small" @click="queryRowsLog(row, index)" style="margin-right: 5px">
          查看
        </Button>
        <Button type="primary" size="small" @click="getLogDetail(row, index)" style="margin-right: 5px">
          详情
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
    gtid: String
  },
  data () {
    return {
      editable: false,
      dataLoading: false,
      queryParam: {
        srcMhaName: null,
        dstMhaName: null,
        gtid: this.gtid,
        beginHandleTime: null,
        endHandleTime: null,
        trxResult: null
      },
      tableData: [
        {
          srcMhaName: 'testA',
          dstMhaName: 'testB',
          gtid: 'c5ed128d-5b8b-11ee-ae5e-fa163e4168ae',
          trxRowsNum: 0,
          cflRowsNum: 1,
          trxResult: 0,
          handleTime: '2023-09-23 10:00:00'
        },
        {
          srcMhaName: 'testB',
          dstMhaName: 'testA',
          gtid: 'c5ed128d-5b8b-11ee-ae5e',
          trxRowsNum: 0,
          cflRowsNum: 1,
          trxResult: 1,
          handleTime: '2023-09-23 11:00:00'
        }
      ],
      columns: [
        {
          title: '事务ID',
          key: 'gtid'
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
      const beginHandleTime = new Date(this.queryParam.beginHandleTime).getTime()
      const endHandleTime = new Date(this.queryParam.beginHandleTime).getTime()
      const params = {
        beginHandleTime: beginHandleTime,
        endHandleTime: endHandleTime,
        gtId: this.queryParam.gtid,
        srcMhaName: this.queryParam.srcMhaName,
        dstMhaName: this.queryParam.dstMhaName,
        trxResult: this.queryParam.trxResult,
        pageReq: {
          pageSize: this.size,
          pageIndex: this.current
        }
      }
      const reqParam = this.flattenObj(params)
      this.axios.get('/api/drc/v2/conflict/log/trx', { params: reqParam })
        .then(response => {
          const data = response.data
          const pageResult = data.pageReq
          if (data.status === 1) {
            this.$Message.error('查询失败')
          } else if (data.data.length === 0 || pageResult.totalCount === 0) {
            this.total = 0
            this.current = 1
            this.tableData = []
            this.$Message.warning('查询结果为空')
          } else {
            this.total = pageResult.totalCount
            this.current = pageResult.pageIndex
            this.tableData = data.data
            this.$Message.success('查询成功')
          }
        })
    },
    resetParam () {
      this.queryParam = {
        srcMhaName: null,
        dstMhaName: null,
        gtid: null,
        beginHandleTime: null,
        endHandleTime: null,
        trxResult: null
      }
    },
    queryRowsLog (row, index) {
      this.$emit('tabValueChanged', 'rowsLog')
      this.$emit('gtidChanged', row.gtid)
      // alert(this.tabVal)
      this.tabVal = 'rowsLog'
      // alert(this.tabVal)
    },
    getLogDetail (row, index) {
      this.$router.push({
        path: '/conflictLogDetail',
        query: {
          conflictTrxLogId: row.conflictTrxLogId
        }
      })
    },
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getTrxData()
      })
    }
  }
}
</script>

<style scoped>

</style>

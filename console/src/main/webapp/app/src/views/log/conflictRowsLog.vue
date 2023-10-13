<template>
  <div style="padding: 1px 1px ">
    <Row :gutter=10 align="middle">
      <Col span="22">
        <Card :padding=5>
          <template #title>查询条件</template>
          <Row :gutter=10>
            <Col span="8">
              <Input prefix="ios-search" v-model="queryParam.gtid" placeholder="事务id"
                     @on-enter="getData"></Input>
            </Col>
            <Col span="4">
              <Input prefix="ios-search" v-model="queryParam.dbName" placeholder="库名"
                     @on-enter="getData"></Input>
            </Col>
            <Col span="4">
              <Input prefix="ios-search" v-model="queryParam.tableName" placeholder="表名"
                     @on-enter="getData"></Input>
            </Col>
            <Col span="3">
              <DatePicker type="date" :editable="editable" v-model="queryParam.beginHandleTime"
                          placeholder="起始日期"></DatePicker>
            </Col>
            <Col span="3">
              <DatePicker type="date" :editable="editable" v-model="queryParam.endHandleTime"
                          placeholder="结束日期"></DatePicker>
            </Col>
            <Col span="2">
              <Select filterable clearable v-model="queryParam.rowResult" placeholder="执行结果"
                      @on-change="getData">
                <Option v-for="item in resultOpts" :value="item.val" :key="item.val">{{ item.name }}</Option>
              </Select>
            </Col>
          </Row>
        </Card>
      </Col>
      <Col span="1">
        <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getData">查询</Button>
        <Button icon="md-refresh" @click="resetParam" style="margin-top: 20px">重置</Button>
      </Col>
    </Row>
    <br>
    <Table stripe border :columns="columns" :data="tableData">
      <template slot-scope="{ row, index }" slot="action">
        <Button type="success" size="small" @click="queryRowsLog(row, index)" style="margin-right: 5px">
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
        @on-change="getData"
        @on-page-size-change="handleChangeSize"></Page>
    </div>
  </div>
</template>

<script>
export default {
  name: 'conflictRowsLog',
  props: {
    gtid: String
  },
  data () {
    return {
      editable: false,
      dataLoading: false,
      queryParam: {
        dbName: null,
        tableName: null,
        gtid: this.gtid,
        beginHandleTime: null,
        endHandleTime: null,
        rowResult: null
      },
      tableData: [],
      columns: [
        {
          title: '事务ID',
          key: 'gtid',
          tooltip: true
        },
        {
          title: '同步方向',
          key: 'dc',
          width: 200,
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.srcDc + ' -> ' + row.dstDc
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
          title: '事务提交时间',
          key: 'handleTime',
          width: 200,
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
        }
        // {
        //   title: '操作',
        //   slot: 'action',
        //   width: 150,
        //   align: 'center'
        // }
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
    getData () {
      const beginTime = this.queryParam.beginHandleTime
      const endTime = this.queryParam.endHandleTime
      const beginHandleTime = beginTime === null || isNaN(beginTime) ? null : new Date(beginTime).getTime()
      const endHandleTime = endTime === null || isNaN(endTime) ? null : new Date(endTime).getTime()
      console.log('beginTime: ' + beginTime)
      console.log('endTime: ' + endTime)
      const params = {
        gtid: this.queryParam.gtid,
        dbName: this.queryParam.dbName,
        tableName: this.queryParam.tableName,
        rowResult: this.queryParam.rowResult,
        pageReq: {
          pageSize: this.size,
          pageIndex: this.current
        }
      }
      if (!isNaN(beginHandleTime)) {
        params.beginHandleTime = beginHandleTime
      }
      if (!isNaN(endHandleTime) && endHandleTime !== null) {
        params.endHandleTime = endHandleTime + 24 * 60 * 60 * 1000 - 1
      }
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
        beginHandleTime: null,
        endHandleTime: null,
        rowResult: null
      }
    },
    queryRowsLog (row, index) {
    },
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getData()
      })
    }
  },
  created () {
    this.getData()
  }
}
</script>

<style scoped>

</style>

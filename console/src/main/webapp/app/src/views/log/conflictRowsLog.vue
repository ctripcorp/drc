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
              <Select filterable clearable v-model="queryParam.trxResult" placeholder="执行结果"
                      @on-change="getData">
                <Option v-for="item in resultOpts" :value="item.val" :key="item.val">{{ item.name }}</Option>
              </Select>
            </Col>
          </Row>
        </Card>
      </Col>
      <Col span="1">
        <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getData">查询</Button>
        <Button icon="md-refresh" :loading="dataLoading" @click="resetParam" style="margin-top: 20px">重置</Button>
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
      tableData: [{
        dbName: 'db',
        tableName: 'table',
        gtid: 'c5ed128d-5b8b-11ee-ae5e-fa163e4168ae',
        rowResult: 0,
        handleTime: '2023-09-23 10:00:00',
        srcDc: 'shaxy',
        dstDc: 'sinaws'
      }],
      columns: [
        {
          title: '事务ID',
          key: 'gtid'
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
          title: '库名',
          key: 'dbName',
          width: 200
        },
        {
          title: '表名',
          key: 'tableName',
          width: 200
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
    getData () {
    },
    getRowsData () {
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
  }
}
</script>

<style scoped>

</style>

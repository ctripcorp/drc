<template>
  <base-component :isFather="isFather" :subMenuName="['1']" :fatherMenu="fatherMenu">
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem>校验结果</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Card>
          数据库名：<Input :style="{width: '180px', marginRight: '10px'}" v-model="searchCondition.dbName" />
          表名：<Input :style="{width: '180px', marginRight: '10px'}" v-model="searchCondition.tableName" />
          开始时间：<DatePicker type="datetime" :style="{width: '160px', marginRight: '10px'}" :transfer="true" format="yyyy-MM-dd HH:mm" :value="searchCondition.startTime" :key="searchCondition.startTime" @on-change="searchCondition.startTime=$event"></DatePicker>
          结束时间：<DatePicker type="datetime" :style="{width: '160px', marginRight: '10px'}" :transfer="true" format="yyyy-MM-dd HH:mm" :value="searchCondition.endTime" :key="searchCondition.endTime" @on-change="searchCondition.endTime=$event"></DatePicker>
          <Button :style="{marginLeft: '10px'}" type="primary" @click="searchRecord">查询</Button>
          <Button :style="{marginLeft: '10px'}" type="primary" @click="switchConsistencyMonitor">配置</Button>
          <Table style="margin-top: 20px" :columns="columns" :data="records" stripe size="small"></Table>
          <div style="text-align: center;margin: 16px 0">
            <Page
              :total="total"
              :current.sync="current"
              show-sizer
              show-elevator
              :transfer="true"
              @on-change="getCurrentRecord2"
              @on-page-size-change="handleChangeSize"></Page>
          </div>
        </Card>
      </div>
    </Content>
  </base-component>
</template>

<script>
import { formatDate } from '../../common/date'

export default {
  name: 'incrementDataConsistencyResult',
  data () {
    return {
      isFather: true,
      fatherMenu: '/incrementDataConsistencyResult',
      srcTableItems: [],
      destTableItems: [],
      srcDc: '',
      destDc: '',
      srcMhaName: '',
      destMhaName: '',
      isDiffTransaction: 'error',
      diffTransaction: '',
      records: [],
      total: 0,
      current: 1,
      size: 10,
      searchCondition: {
        dbName: '',
        tableName: '',
        startTime: '',
        endTime: ''
      },
      columns: [
        {
          title: '数据库',
          key: 'monitorSchemaName',
          align: 'center',
          tooltip: true
        },
        {
          title: '表名',
          key: 'monitorTableName',
          align: 'center',
          tooltip: true
        },
        {
          title: '校验key',
          key: 'monitorTableKey',
          align: 'center',
          tooltip: true
        },
        {
          title: '校验keyValue',
          key: 'monitorTableKeyValue',
          align: 'center',
          tooltip: true
        },
        {
          title: '校验时间',
          key: 'createTime',
          align: 'center',
          tooltip: true,
          render: (h, params) => {
            return h('div',
              formatDate(new Date(params.row.createTime), 'yyyy-MM-dd hh:mm:ss')
            )
          }
        },
        {
          title: '操作',
          key: 'createTime',
          align: 'center',
          tooltip: true,
          render: (h, params) => {
            return h('div', [
              h('Button', {
                props: {
                  type: 'success',
                  size: 'small'
                },
                style: {
                  marginLeft: '5px'
                },
                on: {
                  click: () => {
                    this.handleInconsistency(params.row)
                  }
                }
              }, '处理')
            ])
          }
        }
      ]
    }
  },
  methods: {
    getCurrentRecord () {
      const that = this
      this.axios.get('api/drc/v1/monitor/consistency/data/increment/history/inconsistency/' + this.$route.query.mhaA + '/' + this.$route.query.mhaB + '/' + this.$route.query.tableName + '/' + this.$route.query.key)
        .then(res => {
          console.log('test record data')
          console.log(res)
          that.srcDc = res.data.data.srcDc
          that.destDc = res.data.data.destDc
          that.srcMhaName = res.data.data.srcMhaName
          that.destMhaName = res.data.data.destMhaName
          that.srcTableItems = res.data.data.srcTableItems
          that.destTableItems = res.data.data.destTableItems
          that.isDiffTransaction = res.data.data.diffTransaction === true ? 'error' : 'success'
          that.diffTransaction = res.data.data.diffTransaction === true ? '数据不一致' : '数据一致'
          console.log('test column')
          console.log(that.destTableItems[0].column)
        })
    },
    getCurrentRecord2 () {
      const that = this
      this.axios.get('api/drc/v1/monitor/consistency/increment/history?' +
        'pageNo=' + this.current + '&pageSize=' + this.size +
        '&dbName=' + this.searchCondition.dbName + '&tableName=' + this.searchCondition.tableName +
        '&startTime=' + this.searchCondition.startTime + '&endTime=' + this.searchCondition.endTime)
        .then(res => {
          console.log('test record data')
          console.log(res.data.data)
          that.records = res.data.data
          console.log('test column')
        })
    },
    getCurrentRecordCount () {
      const that = this
      this.axios.get('api/drc/v1/monitor/consistency/increment/history/count?' +
        '&dbName=' + this.searchCondition.dbName + '&tableName=' + this.searchCondition.tableName +
        '&startTime=' + this.searchCondition.startTime + '&endTime=' + this.searchCondition.endTime)
        .then(res => {
          console.log('test record count')
          console.log(res.data.data)
          that.total = res.data.data
          console.log('test column')
        })
    },
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getCurrentRecord2()
      })
    },
    searchRecord () {
      this.getCurrentRecord2()
      this.getCurrentRecordCount()
    },
    handleInconsistency (row) {
      this.$router.push({
        name: 'incrementDataConsistencyHandle',
        query: { mhaGroupId: row.mhaGroupId, dbName: row.monitorSchemaName, tableName: row.monitorTableName, key: row.monitorTableKey, keyValue: row.monitorTableKeyValue }
      })
    },
    switchConsistencyMonitor () {
      this.$router.push({
        name: 'incrementDataConsistencyCluster'
      })
    }
  },
  created () {
    // this.getCurrentRecord()
    this.getCurrentRecord2()
    this.getCurrentRecordCount()
  }
}
</script>

<style>
  .ivu-table .table-info-cell-extra-column-add {
    background-color: #2db7f5;
    color: #fff;
  }
  .ivu-table .table-info-cell-extra-column-diff {
    background-color: #ff6600;
    color: #fff;
  }
</style>

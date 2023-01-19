<template>
  <base-component :isFather="isFather" :subMenuName="['1']" :fatherMenu="fatherMenu">
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem>单元化路由校验</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Card>
          数据库名：<Input :style="{width: '180px', marginRight: '10px'}" v-model="searchCondition.schemaName" />
          表名：<Input :style="{width: '180px', marginRight: '10px'}" v-model="searchCondition.tableName" />
          <Button :style="{marginLeft: '10px'}" type="primary" @click="getVerificationResults">查询</Button>
          <!-- <div class="ivu-list-item-meta-title">
            <p>DRC集群：{{$route.query.clusterA}}(集群A)---{{$route.query.clusterB}}(集群B)</p>
          </div> -->
          <Table stripe style="margin-top: 20px" :columns="columns" :data="dataWithPage" size="small"></Table>
          <Modal
            v-model="verificationResultDetail"
            title="校验详情"
            width="900px"
            @on-ok="clearDetail"
            @on-cancel="clearDetail">
            {{ this.detail.querySql }}
            <Row :gutter="5">
              <i-col span="12">
                <List border>
                  <ListItem>字段名</ListItem>
                  <ListItem>原始值</ListItem>
                  <ListItem>更新值</ListItem>
                </List>
              </i-col>
              <i-col span="12">
                <List border>
                  <ListItem>{{ this.detail.columns }}</ListItem>
                  <ListItem>{{ this.detail.beforeValues }}</ListItem>
                  <ListItem>{{ this.detail.afterValues }}</ListItem>
                </List>
              </i-col>
            </Row>
            单元化路由机房：{{ this.detail.expectedDc }}（根据用户id名[{{ this.detail.uidName }}]对应值和ucs策略号[{{ this.detail.ucsStrategyId }}]获得）
            <br>
            实际写入机房：{{ this.detail.actualDc }}（根据GTID[{{ this.detail.gtid }}]获得）
          </Modal>
          <div style="text-align: center;margin: 16px 0">
            <Page
              :transfer="true"
              :total="verificationResults.length"
              :current.sync="current"
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
import { formatDate } from '../../common/date.js'

export default {
  name: 'unitRouteVerificationResult',
  data () {
    return {
      isFather: true,
      fatherMenu: '/unitRouteVerificationCluster',
      total: 0,
      current: 1,
      size: 10,
      verificationResults: [],
      verificationResultDetail: false,
      detail: {
        gtid: '',
        querySql: '',
        expectedDc: '',
        actualDc: '',
        columns: '',
        beforeValues: '',
        afterValues: '',
        uidName: '',
        ucsStrategyId: ''
      },
      searchCondition: {
        schemaName: '',
        tableName: ''
      },
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
          key: 'schemaName',
          align: 'center',
          resizable: true
        },
        {
          title: '表名',
          key: 'tableName',
          align: 'center',
          resizable: true
        },
        {
          title: 'SQL',
          key: 'querySql',
          align: 'center',
          resizable: true
        },
        {
          title: '单元化路由机房',
          key: 'expectedDc',
          align: 'center',
          resizable: true
        },
        {
          title: '实际写入机房',
          key: 'actualDc',
          align: 'center',
          resizable: true
        },
        {
          title: '执行时间',
          key: 'executeTime',
          align: 'center',
          resizable: true,
          render: (h, params) => {
            return h('div',
              formatDate(new Date(params.row.executeTime), 'yyyy-MM-dd hh:mm:ss')
              // Date是后台时间戳参数字段
              // 'yyyy-MM-dd hh:mm' 对应的时间格式 2018-12-21 18:46
              // 格式可以自行修改，例如 'yyyy-MM-dd' -> 2018-12-21,  'yyyy-MM' -> 2018-12
            )
          }
        },
        {
          title: '详情',
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
                    this.showDetail(params.row)
                  }
                }
              }, '查看')
            ])
          }
        }
      ]
    }
  },
  methods: {
    getVerificationResults () {
      const that = this
      that.axios.get('/api/drc/v1/logs/unit/mhas/' + that.$route.query.clusterA + '?' +
      'schemaName=' + this.searchCondition.schemaName +
      '&tableName=' + this.searchCondition.tableName)
        .then(response => {
          console.log(response)
          that.total = response.data.data.length
          that.verificationResults = response.data.data
        })
    },
    handleChangeSize (val) {
      this.size = val
    },
    showDetail (row) {
      this.verificationResultDetail = true
      this.detail.gtid = row.gtid
      this.detail.querySql = row.querySql
      this.detail.expectedDc = row.expectedDc
      this.detail.actualDc = row.actualDc
      this.detail.columns = row.columns
      this.detail.beforeValues = row.beforeValues
      this.detail.afterValues = row.afterValues
      this.detail.uidName = row.uidName
      this.detail.ucsStrategyId = row.ucsStrategyId
    },
    clearDetail () {
      this.detail.gtid = ''
      this.detail.querySql = ''
      this.detail.expectedDc = ''
      this.detail.actualDc = ''
      this.detail.columns = ''
      this.detail.beforeValues = ''
      this.detail.afterValues = ''
      this.detail.uidName = ''
      this.detail.ucsStrategyId = ''
    }
  },
  computed: {
    dataWithPage () {
      const data = this.verificationResults
      const start = this.current * this.size - this.size
      const end = start + this.size
      return [...data].slice(start, end)
    }
  },
  created () {
    this.getVerificationResults()
  }
}
</script>

<style scoped>

</style>

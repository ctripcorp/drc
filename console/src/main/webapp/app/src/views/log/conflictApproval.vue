<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem>冲突处理审批</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Row :gutter=10 align="middle">
          <Col span="20">
            <Card :padding=5>
              <template #title>查询条件</template>
              <Row :gutter=10>
                <Col span="8">
                  <Input prefix="ios-search" v-model="queryParam.dbName" placeholder="库名"
                         @on-enter="getData"></Input>
                </Col>
                <Col span="8">
                  <Input prefix="ios-search" v-model="queryParam.tableName" placeholder="表名"
                         @on-enter="getData"></Input>
                </Col>
                <Col span="4">
                  <Input prefix="ios-search" v-model="queryParam.applicant" placeholder="申请人"
                         @on-enter="getData"></Input>
                </Col>
                <Col span="4">
                  <Select filterable clearable v-model="queryParam.approvalResult" placeholder="审批结果"
                          @on-change="getData">
                    <Option v-for="item in resultOpts" :value="item.val" :key="item.val">{{ item.name }}</Option>
                  </Select>
                </Col>
              </Row>
            </Card>
          </Col>
          <Col span="2">
            <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getData">查询</Button>
            <Button icon="md-refresh" @click="resetParam" style="margin-top: 20px">重置
            </Button>
          </Col>
        </Row>
        <br>
        <Table stripe border :columns="columns" :data="tableData">
          <template slot-scope="{ row, index }" slot="action">
            <Button type="primary" size="small" @click="getDetail(row, index)" style="margin-right: 5px">
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
            @on-change="getData"
            @on-page-size-change="handleChangeSize"></Page>
        </div>
      </div>
    </Content>
  </base-component>
</template>

<script>

export default {
  name: 'conflictApproval',
  data () {
    return {
      editable: false,
      clearable: false,
      dataLoading: false,
      queryParam: {
        dbName: null,
        tableName: null,
        applicant: null,
        approvalResult: null
      },
      tableData: [],
      columns: [
        {
          title: '库名',
          key: 'dbName'
        },
        {
          title: '表名',
          key: 'tableName',
          tooltip: true
        },
        {
          title: '申请人',
          key: 'applicant',
          width: 200
        },
        {
          title: '提交时间',
          key: 'createTime',
          width: 200,
          sortable: true
        },
        {
          title: '备注',
          key: 'remark',
          tooltip: true
        },
        {
          title: '审批结果',
          key: 'approvalResult',
          width: 150,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const color = row.approvalResult === 0 ? 'blue' : 'volcano'
            const text = row.approvalResult === 0 ? 'commit' : 'rollBack'
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
          name: '审批中',
          val: 0
        },
        {
          name: '审批通过',
          val: 1
        },
        {
          name: '审批未通过',
          val: 2
        }
      ]
    }
  },
  methods: {
    getData () {
      const params = {
        dbName: this.queryParam.dbName,
        tableName: this.queryParam.tableName,
        applicant: this.queryParam.applicant,
        approvalResult: this.queryParam.approvalResult,
        pageReq: {
          pageSize: this.size,
          pageIndex: this.current
        }
      }
      const reqParam = this.flattenObj(params)
      this.dataLoading = true
      this.axios.get('/api/drc/v2/log/conflict/approval', { params: reqParam })
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
        applicant: null,
        approvalResult: null
      }
    },
    getDetail (row, index) {
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

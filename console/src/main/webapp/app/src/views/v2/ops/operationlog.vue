
<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/v2/operationlog">操作日志</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px ">
        <Row :gutter=10 align="middle">
          <Col span="20">
            <Card :padding=5>
              <template #title>查询条件</template>
              <Row :gutter=10 >
                <Col span="5">
                  <Select filterable clearable v-model="queryParam.type" placeholder="操作类型">
                    <Option v-for="item in operateOptions" :value="item.val" :key="item.val">{{ item.name }}</Option>
                  </Select>
                </Col>
                <Col span="3">
                  <Select filterable clearable v-model="queryParam.attr" placeholder="操作属性">
                    <Option v-for="item in attrOptions" :value="item.val" :key="item.val">{{ item.name }}</Option>
                  </Select>
                </Col>
                <Col span="3">
                  <Input prefix="ios-search" v-model="queryParam.operator" placeholder="操作人"></Input>
                </Col>
                <Col span="3">
                  <Input prefix="ios-search" v-model="queryParam.operationKeyword" placeholder="日志内容"></Input>
                </Col>
                <Col span="4">
                  <DatePicker type="datetime" :editable="false" v-model="queryParam.beginCreateTime"
                              :clearable="false" placeholder="起始日期"></DatePicker>
                </Col>
                <Col span="4">
                  <DatePicker type="datetime" :editable="false" v-model="queryParam.endCreatTime"
                              :confirm="false" :clearable="false" placeholder="结束日期"></DatePicker>
                </Col>
                <Col span="2">
                  <Select filterable clearable v-model="queryParam.fail" placeholder="操作结果">
                    <Option v-for="item in resIsFail" :value="item.val" :key="item.val">{{ item.name }}</Option>
                  </Select>
                </Col>
              </Row>
            </Card>
          </Col>
          <Col span="4">
            <Row :gutter=10 align="middle">
              <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getTotalData">查询</Button>
            </Row>
            <Row :gutter=10 align="middle" style="margin-top: 20px">
              <Button icon="md-refresh" @click="resetParam">重置</Button>
            </Row>
          </Col>
        </Row>
        <Table border :columns="columns" :data="tableData"  ref="multipleTable">
          <template slot-scope="{ row, index }" slot="action">
            <Button type="primary" size="small" @click="showLogDetail(row, index)" style="margin-right: 5px">
              详情
            </Button>
          </template>
        </Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            v-if="!countLoading"
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
          <div v-else>
            Total Loading...
          </div>
        </div>
        <Modal
          v-model="detail.modal"
          title="操作日志详情"
          width="800px" :scrollable="true" :draggable="true">
            <Form :label-width="80">
              <Row>
                <Col span="10">
                  <FormItem label="操作人">
                    <Input v-model="detail.operator" :disabled="true"></Input>
                  </FormItem>
                </Col>
                <Col span="14">
                  <FormItem label="时间">
                    <Input v-model="detail.createTime" :disabled="true"></Input>
                  </FormItem>
                </Col>
              </Row>
              <Row>
                <Col span="14">
                  <FormItem label="类型">
                    <Input v-model="detail.type" :disabled="true"></Input>
                  </FormItem>
                </Col>
                <Col span="10">
                  <FormItem label="属性">
                    <Input v-model="detail.attr" :disabled="true"></Input>
                  </FormItem>
                </Col>
              </Row>
              <FormItem label="结果">
                <Input v-model="detail.fail" :disabled="true"></Input>
              </FormItem>
              <FormItem label="日志">
                <Input type="textarea" v-model="detail.operation" :autosize="true" :disabled="true"></Input>
              </FormItem>
            </Form>
        </Modal>
      </div>
    </Content>
  </base-component>
</template>

<script>

export default {
  name: 'operationlog',
  data () {
    return {
      total: 0,
      current: 1,
      size: 10,
      pageSizeOpts: [10, 20, 50, 100],
      tableData: [],
      dataLoading: false,
      countLoading: false,
      columns: [
        {
          title: '操作人',
          key: 'operator',
          align: 'center',
          width: 150
        },
        {
          title: '操作类型',
          key: 'type',
          align: 'center',
          width: 250,
          render: (h, params) => {
            const text = this.getOperateNameByVal(params.row.type)
            return h('Tag', {
              props: {
                color: 'primary'
              }
            }, text)
          }
        },
        {
          title: '操作属性',
          key: 'attr',
          align: 'center',
          width: 100,
          render: (h, params) => {
            const row = params.row
            const color = row.attr === 'query' ? 'success' : row.attr === 'add' ? 'blue' : row.attr === 'update' ? 'warning' : 'error'
            const text = this.getAttrNameByVal(row.attr)
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '操作日志',
          key: 'operation',
          tooltip: true
        },
        {
          title: '操作结果',
          key: 'fail',
          align: 'center',
          width: 100,
          render: (h, params) => {
            const row = params.row
            const color = row.fail === 1 ? 'volcano' : 'blue'
            const text = row.fail === 1 ? '异常' : '成功'
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '操作时间',
          key: 'createTime',
          align: 'center',
          width: 200
        },
        {
          title: '操作详情',
          key: 'action',
          align: 'center',
          width: 100,
          render: (h, params) => {
            return h('div', [
              h('Button', {
                props: {
                  type: 'primary',
                  size: 'small'
                },
                style: {
                  marginRight: '5px'
                },
                on: {
                  click: () => {
                    this.showLogDetail(params.row, params.index)
                  }
                }
              }, '详情')
            ])
          }
        }
      ],
      resIsFail: [
        {
          name: '成功',
          val: 0
        },
        {
          name: '失败',
          val: 1
        }
      ],
      operateOptions: [],
      attrOptions: [],
      queryParam: {
        type: null,
        attr: null,
        fail: null,
        operator: null,
        operationKeyword: null,
        beginCreateTime: null,
        endCreatTime: null
      },
      detail: {
        modal: false,
        type: null,
        attr: null,
        fail: null,
        operation: null,
        operator: null,
        createTime: null
      }
    }
  },
  methods: {
    getTotalData () {
      this.getData()
      this.getCount()
    },
    getData () {
      const beginTime = this.queryParam.beginCreateTime
      const endTime = this.queryParam.endCreatTime
      const beginCreateTime = new Date(beginTime).getTime()
      const endCreatTime = new Date(endTime).getTime()
      const params = {
        type: this.queryParam.type,
        attr: this.queryParam.attr,
        operator: this.queryParam.operator,
        operationKeyword: this.queryParam.operationKeyword,
        fail: this.queryParam.fail,
        beginCreateTime: isNaN(beginCreateTime) ? null : beginCreateTime,
        endCreatTime: isNaN(endCreatTime) ? null : endCreatTime,
        pageReq: {
          pageSize: this.size,
          pageIndex: this.current
        }
      }
      console.log('params')
      console.log(params)
      const reqParam = this.flattenObj(params)
      this.dataLoading = true
      this.axios.get('/api/drc/v2/log/operation/query', { params: reqParam })
        .then(response => {
          const data = response.data
          const pageResult = data.pageReq
          if (data.status === 1) {
            this.$Message.error(data.message)
          } else if (data.data.length === 0) {
          // this.total = 0
            this.current = 1
            this.tableData = data.data
            this.$Message.warning('查询结果为空')
          } else {
          // this.total = pageResult.totalCount
            this.current = pageResult.pageIndex
            this.tableData = data.data
            this.$Message.success('查询成功')
          }
        })
        .finally(() => {
          this.dataLoading = false
        })
    },
    getCount: function () {
      const beginTime = this.queryParam.beginCreateTime
      const endTime = this.queryParam.endCreatTime
      const beginCreateTime = new Date(beginTime).getTime()
      const endCreatTime = new Date(endTime).getTime()
      const params = {
        type: this.queryParam.type,
        operator: this.queryParam.operator,
        operationKeyword: this.queryParam.operationKeyword,
        fail: this.queryParam.fail,
        beginCreateTime: isNaN(beginCreateTime) ? null : beginCreateTime === 0 ? null
          : beginCreateTime,
        endCreatTime: isNaN(endCreatTime) ? null : endCreatTime === 0 ? null : endCreatTime
      }
      const reqParam = this.flattenObj(params)
      this.countLoading = true
      this.axios.get('/api/drc/v2/log/operation/count', { params: reqParam })
        .then(response => {
          const data = response.data
          if (data.status === 1) {
            this.$Message.error(data.message)
          } else {
            this.total = data.data
          }
        })
        .finally(() => {
          this.countLoading = false
        })
    },
    resetParam () {
      this.queryParam = {
        type: null,
        fail: null,
        operator: null,
        beginCreateTime: null,
        endCreatTime: null
      }
    },
    getAllQueryOptions () {
      this.getAllType()
      this.getAllTypeAttr()
    },
    getAllType () {
      this.axios.get('/api/drc/v2/log/operation/type/all')
        .then(response => {
          this.operateOptions = response.data.data
        })
    },
    getAllTypeAttr () {
      this.axios.get('/api/drc/v2/log/operation/attr/all')
        .then(response => {
          this.attrOptions = response.data.data
        })
    },
    getOperateNameByVal (val) {
      const attr = this.operateOptions.find(item => {
        return item.val === val
      })
      return attr ? attr.name : 'error'
    },
    getAttrNameByVal (val) {
      const attr = this.attrOptions.find(item => {
        return item.val === val
      })
      return attr ? attr.name : 'error'
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
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getData()
      })
    },
    showLogDetail (row, index) {
      this.detail.type = this.getOperateNameByVal(row.type)
      this.detail.attr = this.getAttrNameByVal(row.attr)
      this.detail.fail = row.fail === 0 ? '成功' : '异常'
      this.detail.operation = row.operation
      this.detail.operator = row.operator
      this.detail.createTime = row.createTime
      this.detail.modal = true
    }
  },
  created () {
    this.getAllQueryOptions()
    this.getTotalData()
  }
}
</script>

<style>
.ivu-table .cell-class-type {
  background-color: #2db7f5;
  color: #fff;
}
</style>

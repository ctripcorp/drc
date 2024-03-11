<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem>DRC审批</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Row :gutter=10 align="middle">
          <Col span="20">
            <Card :padding=5>
              <template #title>查询条件</template>
              <Row :gutter=10>
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
                <Col span="6">
                  <Input prefix="ios-search" v-model="queryParam.dbName" placeholder="库名"
                         @on-enter="getData"></Input>
                </Col>
                <Col span="6">
                  <Input prefix="ios-search" v-model="queryParam.tableName" placeholder="表名"
                         @on-enter="getData"></Input>
                </Col>
                <Col span="3">
                  <Select filterable clearable v-model="queryParam.filterType" placeholder="过滤类型"
                          @on-change="getData">
                    <Option v-for="item in filterTypeOpts" :value="item.val" :key="item.val">{{ item.val }}</Option>
                  </Select>
                </Col>
                <Col span="3">
                  <Select filterable clearable v-model="queryParam.approvalResult" placeholder="审批结果"
                          @on-change="getData">
                    <Option v-for="item in approvalResultOpts" :value="item.val" :key="item.val">{{ item.name }}</Option>
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
        <Row style="background: #fdfdff; border: 1px solid #e8eaec;">
          <Col span="2" style="display: flex;float: left;margin: 5px">
            <Button type="default" @click="applicationBuild" icon="md-create">
              申请DRC
            </Button>
          </Col>
        </Row>
        <Table stripe border :columns="columns" :data="tableData">
          <template slot-scope="{ row, index }" slot="action">
            <Dropdown :transfer="true" placement="bottom-start">
              <Button type="default" icon="ios-hammer">
                操作
                <Icon type="ios-arrow-down"></Icon>
              </Button>
              <template #list>
                <DropdownMenu >
                  <DropdownItem>
                    <Button type="primary" size="small" @click="getDetail(row, index)" style="margin-right: 5px">
                      详情
                    </Button>
                  </DropdownItem>
                  <DropdownItem>
                    <Button type="success" size="small" @click="copy(row, index)" style="margin-right: 5px">
                      复制
                    </Button>
                  </DropdownItem>
                  <DropdownItem>
                    <Button type="error" size="small" @click="preDelete(row, index)" style="margin-right: 5px">
                      废弃
                    </Button>
                  </DropdownItem>
                  <DropdownItem>
                    <Button type="warning" size="small" @click="sendEmail(row, index)" style="margin-right: 5px">
                      发送邮件
                    </Button>
                  </DropdownItem>
                </DropdownMenu>
              </template>
            </Dropdown>
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
          v-model="deleteInfo.deleteModal"
          title="确认删除以下申请单"
          width="800px"
          @on-ok="deleteForm"
          @on-cancel="clearDeleteInfo">
          <p>同步DB: {{deleteInfo.dbName}}</p>
          <p>同步表: {{deleteInfo.tableName}}</p>
          <p>同步方向: {{deleteInfo.srcRegion}} => {{deleteInfo.dstRegion}}</p>
        </Modal>
      </div>
    </Content>
  </base-component>
</template>

<script>

export default {
  name: 'applicationForm',
  data () {
    return {
      deleteInfo: {
        deleteModal: false,
        dbName: null,
        tableName: null,
        srcRegion: null,
        dstRegion: null,
        applicationFormId: null
      },
      editable: false,
      clearable: false,
      dataLoading: false,
      regions: [],
      queryParam: {
        dbName: null,
        tableName: null,
        srcRegion: null,
        dstRegion: null,
        approvalResult: null,
        filterType: null
      },
      tableData: [],
      columns: [
        {
          title: '同步DB',
          key: 'dbName',
          tooltip: true,
          width: 200
        },
        {
          title: '同步表',
          key: 'tableName',
          tooltip: true
        },
        {
          title: 'BU',
          key: 'buName',
          width: 150,
          align: 'center'
        },
        {
          title: '同步方向',
          key: 'region',
          width: 150,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const text = row.srcRegion + '->' + row.dstRegion
            return h('Tag', {
              props: {
                color: 'blue'
              }
            }, text)
          }
        },
        {
          title: '过滤方式',
          key: 'filterType',
          width: 150,
          align: 'center',
          render: (h, params) => {
            let color = 'blue'
            const text = params.row.filterType
            if (text === 'ALL') {
              color = 'blue'
            } else if (text === 'UDL') {
              color = 'volcano'
            }
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '是否刷存量',
          key: 'flushExistingData',
          width: 150,
          align: 'center',
          render: (h, params) => {
            const result = params.row.flushExistingData
            let color = 'blue'
            let text = ''
            if (result === 0) {
              color = 'blue'
              text = '否'
            } else if (result === 1) {
              color = 'volcano'
              text = '是'
            }
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '是否使用给定位点',
          key: 'useGivenGtid',
          width: 150,
          align: 'center',
          render: (h, params) => {
            const result = params.row.useGivenGtid
            let color = ''
            let text = ''
            if (result === 0) {
              color = 'blue'
              text = '否'
            } else if (result === 1) {
              color = 'volcano'
              text = '是'
            }
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '提交时间',
          key: 'createTime',
          width: 200,
          sortable: true
        },
        {
          title: '审批结果',
          key: 'approvalResult',
          width: 150,
          align: 'center',
          render: (h, params) => {
            const result = params.row.approvalResult
            let color = 'cyan'
            let text = ''
            if (result === 1) {
              color = 'green'
              text = '通过'
            } else if (result === 2) {
              color = 'volcano'
              text = '未通过'
            } else if (result === 3) {
              color = 'blue'
              text = '未审批'
            } else if (result === 0) {
              text = '审批中'
            }
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
          width: 200,
          align: 'center'
        }
      ],
      total: 0,
      current: 1,
      size: 10,
      pageSizeOpts: [10, 20, 50, 100],
      approvalResultOpts: [
        {
          name: '审批中',
          val: 0
        },
        {
          name: '通过',
          val: 1
        },
        {
          name: '未通过',
          val: 2
        },
        {
          name: '未审批',
          val: 3
        }
      ],
      filterTypeOpts: [
        {
          val: 'ALL'
        },
        {
          val: 'UDL'
        }
      ]
    }
  },
  methods: {
    sendEmail (row, index) {
      this.axios.post('/api/drc/v2/application/email?applicationFormId=' + row.applicationFormId).then(res => {
        const data = res.data
        if (data.data === false) {
          this.$Message.error('发送邮件失败！')
        } else {
          this.$Message.success('发送邮件成功')
        }
      })
    },
    preDelete (row, index) {
      this.deleteInfo = {
        deleteModal: true,
        dbName: row.dbName,
        tableName: row.tableName,
        srcRegion: row.srcRegion,
        dstRegion: row.dstRegion,
        applicationFormId: row.applicationFormId
      }
    },
    clearDeleteInfo () {
      this.deleteInfo = {
        deleteModal: false,
        dbName: null,
        tableName: null,
        srcRegion: null,
        dstRegion: null,
        applicationFormId: null
      }
    },
    deleteForm () {
      this.axios.delete('/api/drc/v2/application/?applicationFormId=' + this.deleteInfo.applicationFormId).then(res => {
        const data = res.data
        if (data.status === 1) {
          this.$Message.error('操作失败！' + data.message)
        } else {
          this.getData()
        }
      })
    },
    applicationBuild () {
      const detail = this.$router.resolve({
        path: '/applicationBuild'
      })
      window.open(detail.href, '_blank')
    },
    getData () {
      const params = {
        dbName: this.queryParam.dbName,
        tableName: this.queryParam.tableName,
        srcRegion: this.queryParam.srcRegion,
        dstRegion: this.queryParam.dstRegion,
        approvalResult: this.queryParam.approvalResult,
        filterType: this.queryParam.filterType,
        pageReq: {
          pageSize: this.size,
          pageIndex: this.current
        }
      }
      const reqParam = this.flattenObj(params)
      this.dataLoading = true
      this.axios.get('/api/drc/v2/application/list', { params: reqParam })
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
        srcRegion: null,
        dstRegion: null,
        approvalResult: null,
        filterType: null
      }
    },
    getDetail (row, index) {
      const detail = this.$router.resolve({
        path: '/applicationBuild',
        query: {
          applicationFormId: row.applicationFormId,
          showDetail: true,
          dbName: row.dbName,
          tableName: row.tableName,
          srcRegion: row.srcRegion,
          dstRegion: row.dstRegion,
          filterType: row.filterType,
          buName: row.buName,
          tps: row.tps,
          description: row.description,
          disruptionImpact: row.disruptionImpact,
          tag: row.tag,
          flushExistingData: row.flushExistingData,
          orderRelated: row.orderRelated,
          gtidInit: row.gtidInit,
          remark: row.remark,
          applicant: row.applicant
        }
      })
      window.open(detail.href, '_blank')
    },
    copy (row, index) {
      const detail = this.$router.resolve({
        path: '/applicationBuild',
        query: {
          dbName: row.dbName,
          tableName: row.tableName,
          srcRegion: row.srcRegion,
          dstRegion: row.dstRegion,
          filterType: row.filterType,
          buName: row.buName,
          tps: row.tps,
          description: row.description,
          disruptionImpact: row.disruptionImpact,
          tag: row.tag,
          flushExistingData: row.flushExistingData,
          orderRelated: row.orderRelated,
          // gtidInit: row.gtidInit,
          remark: row.remark,
          applicant: row.applicant,
          applicationFormId: row.applicationFormId
        }
      })
      window.open(detail.href, '_blank')
    },
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getData()
      })
    },
    getRegions () {
      this.axios.get('/api/drc/v2/meta/regions/all')
        .then(response => {
          this.regions = response.data.data
        })
    }
  },
  created () {
    this.getRegions()
    this.getData()
  }
}
</script>

<style scoped>

</style>

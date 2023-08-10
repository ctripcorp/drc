<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem :to="{
          path: '/drcV2',query :{
          step: 3,
          srcMhaName: this.commonInfo.srcMhaName,
          dstMhaName: this.commonInfo.dstMhaName,
          order: this.commonInfo.order
        }
      }">DRC配置</BreadcrumbItem>
      <BreadcrumbItem :to="{
        path: '/dbTables',query :{
          srcMhaName: commonInfo.srcMhaName,
          dstMhaName: commonInfo.dstMhaName,
          srcDc: commonInfo.srcDc,
          dstDc: commonInfo.dstDc
        }
      }">同步表</BreadcrumbItem>
      <BreadcrumbItem >配置流程</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#ffffff', margin: '50px 0 1px 185px', zIndex: '1', top: '500px'}">
      <span style="margin-top: 10px;color:#464c5b;font-weight:600">{{commonInfo.srcMhaName}}({{commonInfo.srcDc}})==>{{commonInfo.dstMhaName}}}({{commonInfo.dstDc}})</span>
      <template>
        <Steps :current="currentStep" style="width: 90%; margin-left: 50px; margin-bottom: 15px; margin-top: 50px">
          <Step title="配置表" content="添加同步表" @click.native="jumpTo(0)" :style="{cursor: 'pointer'}"></Step>
          <Step title="行过滤" content="配置行过滤（非必要）" @click.native="jumpTo(1)" :style="{cursor: 'pointer'}"></Step>
          <Step title="字段过滤" content="配置字段过滤(非必要）" @click.native="jumpTo(2)" :style="{cursor: 'pointer'}"></Step>
          <Step title="完成" content="完成该表同步配置" @click.native="jumpTo(3)" :style="{cursor: 'pointer'}"></Step>
        </Steps>
      </template>
      <Card dis-hover>
        <Row >
          <Col span="10">
            <Card>
              <div slot="title">
                <span>相关表</span>
              </div>
              <Table stripe :columns="nameFilterCheck.columns" :data="dataWithPage" border></Table>
              <div >
                <Page
                  :transfer="true"
                  :total="commonInfo.tableData.length"
                  :current.sync="nameFilterCheck.current"
                  :page-size-opts="nameFilterCheck.pageSizeOpts"
                  :page-size="this.nameFilterCheck.size"
                  show-total
                  show-sizer
                  show-elevator
                  @on-page-size-change="handleChangeSize"></Page>
              </div>
            </Card>
          </Col>
          <Col span="14">
            <Card>
              <div v-if="currentStep === 0">
                <div slot="title">
                  <span>库表配置</span>
                </div>
                <Form ref="commonInfo" :model="commonInfo" :rules="ruleInline" :label-width="100">
                  <FormItem prop="dbName" label="库名" style="width: 350px">
                    <Input type="text" v-model="commonInfo.dbName" placeholder="请输入库名（支持正则）"/>
                  </FormItem>
                  <FormItem prop="tableName" label="表名" style="width: 350px">
                    <Input type="text" v-model="commonInfo.tableName" placeholder="请输入表名（支持正则）">
                    </Input>
                  </FormItem>
                  <FormItem>
                    <Row>
                      <Col span="10">
                        <Button type="success" @click="checkMysqlTablesInSrcMha"  style="margin-left: 10px">校验</Button>
                      </Col>
                      <Col span="8">
                        <Button type="primary" @click="submitConfig">保存</Button>
                      </Col>
                    </Row>
                  </FormItem>
                </Form>
              </div>
              <rows-filter-config v-bind="commonInfo" v-if="currentStep === 1"></rows-filter-config>
              <columns-filter-v2 v-bind="commonInfo" v-if="currentStep === 2"></columns-filter-v2>
              <complete v-bind="commonInfo" v-if="currentStep === 3"></complete>
            </Card>
          </Col>
          <Divider/>
          <div style="padding: 1px 1px; height: 100px; margin-top: 75px">
            <div>
              <Button type="primary" @click="prev" style="position: absolute; left: 465px" v-if="currentStep > 0 ">
                上一步
              </Button>
              <Button type="primary" @click="next" style="position: absolute; left: 790px" v-if="currentStep < 5">
                下一步
              </Button>
            </div>
          </div>
        </Row>
      </Card>
    </Content>
  </base-component>
</template>

<script>
import rowsFilterConfig from '@/components/configs/flow/rowsFilterConfig'
import columnsFilterV2 from './columnsFilterV2'
import complete from '@/components/configs/flow/complete'

export default {
  name: 'dbReplicationConfig',
  components: {
    rowsFilterConfig,
    columnsFilterV2,
    complete
  },
  data () {
    return {
      currentStep: 0,
      commonInfo: {
        srcMhaName: '',
        srcMhaId: 0,
        dstMhaName: '',
        srcDc: '',
        dstDc: '',
        order: true,
        dbReplicationIds: [],
        dbReplicationId: 0,
        dbName: '',
        tableName: '',
        tableData: []
      },
      ruleInline: {
        dbName: [
          { required: true, message: 'Please fill in the schema', trigger: 'blur' }
        ],
        tableName: [
          { required: true, message: 'Please fill in the table.', trigger: 'blur' }
        ]
      },
      nameFilterCheck: {
        columns: [
          {
            title: '序号',
            width: 75,
            align: 'center',
            fixed: 'left',
            render: (h, params) => {
              return h(
                'span',
                params.index + 1
              )
            }
          },
          {
            title: '库名',
            key: 'schema',
            resizable: true
          },
          {
            title: '表名',
            key: 'table',
            resizable: true
          },
          {
            title: '结果',
            align: 'center',
            render: (h, params) => {
              const row = params.row
              const color = row.res !== 'ok' ? 'volcano' : 'green'
              const text = row.res
              return h('Tag', {
                props: {
                  color: color
                }
              }, text)
            }
          }
        ],
        total: 0,
        current: 1,
        size: 5,
        pageSizeOpts: [5, 10, 20, 100]
      }
    }
  },
  methods: {
    jumpTo (n) {
      this.currentStep = n
    },
    next () {
      this.currentStep = this.currentStep + 1
    },
    prev () {
      this.currentStep = this.currentStep - 1
    },
    handleSubmit () {
      if (this.commonInfo.dataMediaId === 0) {
        // insert
        this.submitConfig()
      } else {
        // update
        this.submitConfig()
      }
    },
    submitConfig () {
      console.log(this.commonInfo)
      this.axios.post('/api/drc/v2/config/dbReplication', {
        srcMhaName: this.commonInfo.srcMhaName,
        dstMhaName: this.commonInfo.dstMhaName,
        dbName: this.commonInfo.dbName,
        tableName: this.commonInfo.tableName
      }).then(response => {
        if (response.data.status === 1) {
          alert('提交失败!')
        } else {
          this.commonInfo.dbReplicationIds = response.data.data
          alert('提交成功！')
          alert(this.dbReplicationIds)
        }
      })
    },
    checkMysqlTablesInSrcMha () {
      this.checkMySqlTables(this.commonInfo.srcMhaName, this.commonInfo.dbName + '\\.' + this.commonInfo.tableName)
    },
    checkMySqlTables (mha, nameFilter) {
      console.log('nameFilter:' + nameFilter)
      if (nameFilter == null) {
        nameFilter = ''
      }
      this.$Spin.show({
        render: (h) => {
          return h('div', [
            h('Icon', {
              class: 'demo-spin-icon-load',
              props: {
                size: 18
              }
            }),
            h('div', '检测中，请稍等...')
          ])
        }
      })
      setTimeout(() => {
        this.$Spin.hide()
      }, 80000)
      this.axios.get('/api/drc/v1/build/preCheckMySqlTables?mha=' + mha +
        '&' + 'nameFilter=' + nameFilter)
        .then(response => {
          this.commonInfo.tableData = response.data.data
          this.$Spin.hide()
        })
    },
    handleChangeSize (val) {
      this.size = val
    }
  },
  computed: {
    dataWithPage () {
      const data = this.commonInfo.tableData
      const start = this.nameFilterCheck.current * this.nameFilterCheck.size - this.nameFilterCheck.size
      const end = start + this.nameFilterCheck.size
      return [...data].slice(start, end)
    }
  },
  created () {
    const curStep = this.$route.query.step
    if (curStep == null) {
      console.log('curStep is null, do nothing')
    } else {
      this.jumpTo(Number(curStep))
    }
    this.commonInfo = {
      srcMhaName: this.$route.query.srcMhaName,
      srcMhaId: this.$route.query.srcMhaId,
      dstMhaName: this.$route.query.dstMhaName,
      srcDc: this.$route.query.srcDc,
      dstDc: this.$route.query.dstDc,
      order: this.$route.query.order,
      dbName: this.$route.query.dbName,
      tableName: this.$route.query.tableName,
      dbReplicationId: this.$route.query.dbReplicationId,
      dbReplicationIds: [],
      tableData: []
    }
    alert('ok')
    this.commonInfo.dbReplicationIds.push(this.$route.query.dbReplicationId)
    alert(this.commonInfo.dbReplicationIds)
    if (this.commonInfo.dbName !== '' && this.commonInfo.tableName !== '') {
      this.checkMysqlTablesInSrcMha()
    }
    console.log('commonInfo')
    console.log(this.commonInfo)
  }
}
</script>

<style scoped>

</style>

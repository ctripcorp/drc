<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem :to="{
        path: '/accessV2',query :{
          step: 3,
          clustername: this.commonInfo.srcMha,
          newclustername: this.commonInfo.destMha,
          order: this.commonInfo.order
        }
      }">DRC配置</BreadcrumbItem>
      <BreadcrumbItem :to="{
        path: '/tables',query :{
                srcMha: commonInfo.srcMha,
                destMha: commonInfo.destMha,
                applierGroupId: commonInfo.applierGroupId,
                srcDc: commonInfo.srcDc,
                destDc: commonInfo.destDc,
                order: commonInfo.order
        }
      }">同步表</BreadcrumbItem>
      <BreadcrumbItem >配置流程</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#ffffff', margin: '50px 0 1px 185px', zIndex: '1', top: '500px'}">
      <span style="margin-top: 10px;color:#464c5b;font-weight:600">{{commonInfo.srcMha}}({{commonInfo.srcDc}})==>{{commonInfo.destMha}}({{commonInfo.destDc}})</span>
      <template>
        <Steps :current="currentStep" style="width: 90%; margin-left: 50px; margin-bottom: 15px; margin-top: 50px">
          <Step title="配置表" content="添加同步表" @click.native="jumpTo(0)" :style="{cursor: 'pointer'}"></Step>
          <Step title="表映射" content="配置表映射(非必要）" @click.native="jumpTo(1)" :style="{cursor: 'pointer'}"></Step>
          <Step title="字段映射" content="配置字段映射(非必要）" @click.native="jumpTo(2)" :style="{cursor: 'pointer'}"></Step>
          <Step title="行过滤" content="配置行过滤（非必要）" @click.native="jumpTo(3)" :style="{cursor: 'pointer'}"></Step>
          <Step title="字段过滤" content="配置字段过滤(非必要）" @click.native="jumpTo(4)" :style="{cursor: 'pointer'}"></Step>
          <Step title="完成" content="完成该表同步配置" @click.native="jumpTo(5)" :style="{cursor: 'pointer'}"></Step>
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
                  <FormItem prop="namespace" label="库名" style="width: 350px">
                    <Input type="text" v-model="commonInfo.namespace" placeholder="请输入库名（支持正则）"/>
                  </FormItem>
                  <FormItem prop="name" label="表名" style="width: 350px">
                    <Input type="text" v-model="commonInfo.name" placeholder="请输入表名（支持正则）">
                    </Input>
                  </FormItem>
                  <FormItem>
                    <Row>
                      <Col span="10">
                        <Button type="success" @click="checkMysqlTablesInSrcMha"  style="margin-left: 10px">校验</Button>
                      </Col>
                      <Col span="8">
                        <Button type="primary" @click="handleSubmit('commonInfo')">保存</Button>
                      </Col>
                    </Row>
                  </FormItem>
                </Form>
              </div>
              <table-mapping-config v-bind="commonInfo" v-if="currentStep === 1"></table-mapping-config>
              <columns-mapping-config v-bind="commonInfo" v-if="currentStep === 2"></columns-mapping-config>
              <rows-filter-config v-bind="commonInfo" v-if="currentStep === 3"></rows-filter-config>
              <columns-filter-config v-bind="commonInfo" v-if="currentStep === 4"></columns-filter-config>
              <complete v-bind="commonInfo" v-if="currentStep === 5"></complete>
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
import tableMappingConfig from '@/components/configs/flow/tableMappingConfig'
import columnsMappingConfig from '@/components/configs/flow/columnsMappingConfig'
import rowsFilterConfig from '@/components/configs/flow/rowsFilterConfig'
import columnsFilterConfig from '@/components/configs/flow/columnsFilterConfig'
import complete from '@/components/configs/flow/complete'

export default {
  name: 'configFlow',
  components: {
    tableMappingConfig,
    columnsMappingConfig,
    rowsFilterConfig,
    columnsFilterConfig,
    complete
  },
  data () {
    return {
      currentStep: 0,
      // init by route query
      commonInfo: {
        srcMha: '',
        srcMhaId: 0,
        destMha: '',
        applierGroupId: 0,
        srcDc: '',
        destDc: '',
        order: true,
        dataMediaId: 0,
        namespace: '',
        name: '',
        tableData: []
      },
      ruleInline: {
        namespace: [
          { required: true, message: 'Please fill in the schema', trigger: 'blur' }
        ],
        name: [
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
      this.axios.post('/api/drc/v1/dataMedia/dataMediaConfig', {
        id: this.commonInfo.dataMediaId,
        applierGroupId: this.commonInfo.applierGroupId,
        namespace: this.commonInfo.namespace,
        name: this.commonInfo.name,
        type: 0,
        dataMediaSourceId: this.commonInfo.srcMhaId
      }).then(response => {
        if (response.data.status === 1) {
          alert('提交失败!')
        } else {
          this.commonInfo.dataMediaId = response.data.data
          alert('提交成功！')
        }
      })
    },
    checkMysqlTablesInSrcMha () {
      this.checkMySqlTables(this.commonInfo.srcMha, this.commonInfo.namespace + '\\.' + this.commonInfo.name)
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
      console.log('/api/drc/v1/build/preCheckMySqlTables?mha=' + mha +
        '&' + 'nameFilter=' + nameFilter)
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
      srcMha: this.$route.query.srcMha,
      srcMhaId: this.$route.query.srcMhaId,
      destMha: this.$route.query.destMha,
      applierGroupId: this.$route.query.applierGroupId,
      srcDc: this.$route.query.srcDc,
      destDc: this.$route.query.destDc,
      order: this.$route.query.order,
      dataMediaId: this.$route.query.dataMediaId,
      namespace: this.$route.query.namespace,
      name: this.$route.query.name,
      tableData: []
    }
    if (this.commonInfo.namespace !== '' && this.commonInfo.name !== '') {
      this.checkMysqlTablesInSrcMha()
    }
    console.log('commonInfo')
    console.log(this.commonInfo)
  }
}
</script>

<style scoped>

</style>

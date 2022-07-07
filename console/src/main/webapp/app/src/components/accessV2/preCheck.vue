<template v-if="current === 0" :key="0">
  <div>
    <Row>
      <i-col span="12">
        <Form  :model="srcMha"  :label-width="250" style="float: left; margin-top: 50px">
          <FormItem label="源集群名" prop="mhaName" style="width: 600px">
            <Row>
              <Col span="16">
                <Input v-model="srcMha.mhaName"  readonly placeholder="请输入源集群名"/>
              </Col>
              <Col span="5">
                <Button  type="primary" @click="checkMySqlConfig(srcMha)" ghost style="margin-left: 10px">配置校验</Button>
              </Col>
            </Row>
          </FormItem>
          <FormItem label="同步表" prop="ip">
            <Row>
              <Col span="16">
                <Input v-model="srcMha.nameFilter"   placeholder="支持正则，默认全部"/>
              </Col>
              <Col span="5">
                <Button  type="primary" @click="checkMySqlTables(srcMha)" ghost style="margin-left: 10px">表校验</Button>
              </Col>
            </Row>
          </FormItem>
        </Form>
      </i-col>
      <i-col span="12">
        <Form  :model="destMha"  :label-width="250" style="float: left; margin-top: 50px">
          <FormItem label="目标集群名" prop="mhaName" style="width: 600px">
            <Row>
              <Col span="16">
                <Input v-model="destMha.mhaName"  readonly placeholder="请输入源集群名"/>
              </Col>
              <Col span="5">
                <Button type="primary" @click="checkMySqlConfig(destMha)"  ghost style="margin-left: 10px">配置校验</Button>
              </Col>
            </Row>
          </FormItem>
          <FormItem label="同步表" prop="ip">
            <Row>
              <Col span="16">
                <Input v-model="destMha.nameFilter"  placeholder="支持正则，默认全部"/>
              </Col>
              <Col span="5">
                <Button  type="primary" @click="checkMySqlTables(destMha)" ghost style="margin-left: 10px">表校验</Button>
              </Col>
            </Row>
          </FormItem>
        </Form>
      </i-col>
    </Row>
    <Modal
      v-model="configCheck.modal"
      title="预检测结果"
      width="900px">
      <Form style="width: 100%">
        <FormItem label="BINLOG [SHOULD BE: ON]">
          <Input  v-model="configCheck.binlogMode" readonly/>
        </FormItem>
        <FormItem label="BINLOG_FORMAT  [SHOULD BE: ROW]">
          <Input  v-model="configCheck.binlogFormat" readonly/>
        </FormItem>
        <FormItem label="binlogVersion1  [SHOULD BE: OFF]">
          <Input  v-model="configCheck.binlogVersion1" readonly/>
        </FormItem>
        <FormItem label="BTDT [SHOULD BE: writeSet]">
          <Input v-model="configCheck.binlogTransactionDependency" readonly/>
        </FormItem>
        <FormItem label="GTID_MODE [SHOULD BE: ON]">
          <Input v-model="configCheck.gtidMode" readonly/>
        </FormItem>
        <FormItem label="DRC系统表数量 [SHOULD BE: 1/2]">
          <Input v-model="configCheck.drcTables" readonly/>
        </FormItem>
        <FormItem label="自增步长">
          <Input v-model="configCheck.autoIncrementStep" readonly />
        </FormItem>
        <FormItem label="自增offSet">
          <Input v-model="configCheck.autoIncrementOffset" readonly />
        </FormItem>
        <FormItem label="DRC3对帐号 [SHOULD BE: three accounts ready]">
          <Input v-model="configCheck.drcAccounts" readonly />
        </FormItem>
      </Form>
    </Modal>
    <Modal
      v-model="tablesCheckModal"
      title="表检验"
      width="1000px">
      <Card>
        <div slot="title">
          <span>相关表</span>
        </div>
        <Table stripe :columns="columns" :data="dataWithPage" border ></Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :transfer="true"
            :total="tableData.length"
            :current.sync="current"
            :page-size-opts="pageSizeOpts"
            :page-size="this.size"
            show-total
            show-sizer
            show-elevator
            @on-page-size-change="handleChangeSize"></Page>
        </div>
      </Card>
    </Modal>
  </div>
</template>
<script>
export default {
  name: 'preCheck',
  props: {
    oldClusterName: String,
    newClusterName: String,
    oldDrcZone: String,
    newDrcZone: String
  },
  data () {
    return {
      srcMha: {
        mhaName: this.oldClusterName,
        nameFilter: ''
      },
      destMha: {
        mhaName: this.newClusterName,
        nameFilter: ''
      },
      configCheck: {
        binlogMode: '',
        binlogFormat: '',
        binlogVersion1: '',
        binlogTransactionDependency: '',
        gtidMode: '',
        drcTables: 0,
        autoIncrementStep: 0,
        autoIncrementOffset: 0,
        drcAccounts: '',
        modal: false
      },
      tablesCheckModal: false,
      tableData: [],
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
          key: 'schema'
        },
        {
          title: '表名',
          key: 'table'
        },
        {
          title: '无OnUpdate字段',
          key: 'noOnUpdateColumn',
          width: 100,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const text = row.noOnUpdateColumn ? 'True' : ''
            return h('span', text)
          }
        },
        {
          title: '无OnUpdate字段索引',
          key: 'noOnUpdateKey',
          width: 100,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const text = row.noOnUpdateKey ? 'True' : ''
            return h('span', text)
          }
        },
        {
          title: '无PkUk',
          key: 'noPkUk',
          width: 100,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const text = row.noPkUk ? 'True' : ''
            return h('span', text)
          }
        },
        {
          title: '支持Truncate',
          key: 'approveTruncate',
          width: 100,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const text = row.approveTruncate ? 'True' : ''
            return h('span', text)
          }
        },
        {
          title: '存在DefaultTime为0',
          key: 'timeDefaultZero',
          width: 100,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const text = row.timeDefaultZero ? 'True' : ''
            return h('span', text)
          }
        },
        {
          title: '结果',
          width: 100,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const flag = row.noOnUpdateColumn || row.noOnUpdateKey || row.noPkUk || row.approveTruncate || row.timeDefaultZero
            const color = flag ? 'volcano' : 'green'
            const text = flag ? '错误' : '正常'
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
  },
  computed: {
    dataWithPage () {
      const data = this.tableData
      const start = this.current * this.size - this.size
      const end = start + this.size
      return [...data].slice(start, end)
    }
  },
  methods: {
    checkMySqlConfig (mha) {
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
      this.axios.get('/api/drc/v1/build/preCheckMySqlConfig?mha=' + mha.mhaName)
        .then(response => {
          console.log(response)
          const result = response.data.data
          console.log(result)
          this.configCheck.binlogMode = result.binlogMode
          this.configCheck.binlogFormat = result.binlogFormat
          this.configCheck.binlogVersion1 = result.binlogVersion1
          this.configCheck.binlogTransactionDependency = result.binlogTransactionDependency
          this.configCheck.gtidMode = result.gtidMode
          this.configCheck.drcTables = result.drcTables
          this.configCheck.autoIncrementStep = result.autoIncrementStep
          this.configCheck.autoIncrementOffset = result.autoIncrementOffset
          this.configCheck.drcAccounts = result.drcAccounts
          this.$Spin.hide()
          this.configCheck.modal = true
        })
    },
    checkMySqlTables (mha) {
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
      this.axios.get('/api/drc/v1/build/preCheckMySqlTables?mha=' + mha.mhaName +
        '&' + 'nameFilter=' + mha.nameFilter)
        .then(response => {
          this.tableData = response.data.data
          this.$Spin.hide()
          this.tablesCheckModal = true
        })
    },
    handleChangeSize (val) {
      this.size = val
    }
  }
}
</script>

<style scoped>

</style>

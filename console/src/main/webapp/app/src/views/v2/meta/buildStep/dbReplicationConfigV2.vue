<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem :to="{
          path: '/v2/mhaReplications',query :{
          srcMhaName: this.commonInfo.srcMhaName,
          dstMhaName: this.commonInfo.dstMhaName,
          preciseSearchMode: true
        }
      }">首页
      </BreadcrumbItem>
      <BreadcrumbItem :to="{
          path: '/drcV2',query :{
          step: 3,
          srcMhaName: this.commonInfo.srcMhaName,
          dstMhaName: this.commonInfo.dstMhaName,
          srcDc: this.commonInfo.srcDc,
          dstDc: this.commonInfo.dstDc,
          order: this.commonInfo.order
        }
      }">DRC配置V2
      </BreadcrumbItem>
      <BreadcrumbItem :to="{
        path: '/dbTables',query :{
          srcMhaName: this.commonInfo.srcMhaName,
          dstMhaName: this.commonInfo.dstMhaName,
          srcDc: this.commonInfo.srcDc,
          dstDc: this.commonInfo.dstDc
        }
      }">同步表
      </BreadcrumbItem>
      <BreadcrumbItem>同步表配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#ffffff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <span
        style="margin-top: 10px;color:#464c5b;font-weight:600">{{commonInfo.srcMhaName}}({{commonInfo.srcDc}}) ==> {{commonInfo.dstMhaName}}({{commonInfo.dstDc}})</span><br><br>
      <Alert type="warning" show-icon v-if="showMsg" closable>
        {{title}}
        <template #desc>{{message}}</template>
      </Alert>
      <Row :gutter=10 align="middle">
        <Col span="13">
          <Form ref="commonInfo" :model="commonInfo" :rules="ruleInline" :label-width="100">
            <FormItem prop="dbName" label="库名" :required=true>
              <Input type="text" v-model="commonInfo.dbName" :readonly="update" placeholder="请输入库名（支持正则）"/>
            </FormItem>
            <FormItem prop="tableName" label="表名" :required=true>
              <Input type="text" v-model="commonInfo.tableName" :readonly="show" placeholder="请输入表名（支持正则）">
              </Input>
            </FormItem>
            <FormItem label="行过滤">
              <i-switch v-model="formItem.switch.rowsFilter" :disabled="show" size="large">
                <template #open>
                  <span>On</span>
                </template>
                <template #close>
                  <span>Off</span>
                </template>
              </i-switch>
            </FormItem>
            <Card v-if="formItem.switch.rowsFilter" style="margin-left: 100px">
              <template #title>
                <Icon type="md-settings"/>
                行过滤配置
              </template>
              <FormItem label="模式">
                <Select v-model="rowsFilterConfig.mode" style="width: 200px" placeholder="选择行过滤模式">
                  <Option v-for="item in rowsFilterConfig.modes" :value="item.mode" :key="item.mode">{{ item.name
                    }}
                  </Option>
                </Select>
              </FormItem>
              <div v-if="useTripUdlOrUidMode">
                <FormItem label="规则内容" v-if="fillContextByRegion">
                  <Select v-model="configInTripUid.regionsChosen" multiple style="width: 200px" placeholder="Region 选择">
                    <Option v-for="item in constants.regionsForChose" :value="item" :key="item">{{ item }}</Option>
                  </Select>
                </FormItem>
                <div v-if="showUdlConfigDetail">
                  <Divider>UDL配置</Divider>
                  <FormItem label="UDL字段" >
                    <Select v-model="rowsFilterConfig.udlColumns" filterable allow-create multiple style="width: 200px"
                            @on-create="handleCreateUDLColumn" placeholder="不选默认则无UDL配置">
                      <Option v-for="item in columnsForChose" :value="item" :key="item">{{ item }}</Option>
                    </Select>
                  </FormItem>
                  <FormItem label="DRC UDL策略id" v-if="hasUdlColumn">
                    <Select v-model="rowsFilterConfig.drcStrategyId" filterable allow-create style="width: 200px"
                            placeholder="请选择ucs策略id">
                      <Option v-for="item in constants.drcStrategyIdsForChose" :value="item" :key="item">{{ item }}
                      </Option>
                    </Select>
                  </FormItem>
                </div>
                <div v-if="showUidConfigDetail">
                  <Divider>UID配置</Divider>
                  <FormItem label="UID字段" >
                    <Select v-model="rowsFilterConfig.columns" filterable allow-create multiple style="width: 200px"
                            @on-create="handleCreateUDLColumn" placeholder="不选默认则无UID配置">
                      <Option v-for="item in columnsForChose" :value="item" :key="item">{{ item }}</Option>
                    </Select>
                  </FormItem>
                  <FormItem label="fetchMode" v-if="hasUidColumn">
                    <Select v-model="rowsFilterConfig.fetchMode" style="width: 200px" placeholder="选择"
                            @on-change="fetchModeChange()">
                      <Option v-for="item in rowsFilterConfig.fetchModeForChose" :value="item.v" :key="item.k">{{ item.k }}
                      </Option>
                    </Select>
                  </FormItem>
                  <FormItem label="空处理" v-if="hasUidColumn">
                    <Checkbox v-model="rowsFilterConfig.illegalArgument">【字段为空时】同步</Checkbox>
                  </FormItem>
                </div>
              </div>
              <div v-else>
                <FormItem label="规则内容">
                  <Input type="textarea" v-model="rowsFilterConfig.context"
                         style="width: 250px" placeholder="请输入行过滤内容"/>
                </FormItem>
                <FormItem label="相关字段">
                  <Select v-model="rowsFilterConfig.columns" filterable allow-create multiple style="width: 200px"
                          @on-create="handleCreateUDLColumn" placeholder="选择相关字段">
                    <Option v-for="item in columnsForChose" :value="item" :key="item" :lable="item"></Option>
                  </Select>
                </FormItem>
              </div>
            </Card>
            <FormItem label="字段过滤">
              <i-switch v-model="formItem.switch.columnsFilter" :disabled="show" size="large">
                <template #open>
                  <span>On</span>
                </template>
                <template #close>
                  <span>Off</span>
                </template>
              </i-switch>
            </FormItem>
            <Card v-if="formItem.switch.columnsFilter" style="margin-left: 100px">
              <template #title>
                <Icon type="md-settings"/>
                字段过滤配置
              </template>
              <FormItem label="模式">
                <Select v-model="columnsFilterConfig.mode" style="width: 200px" placeholder="选择字段过滤模式">
                  <Option v-for="item in columnsFilterConfig.modes" :value="item.mode" :key="item.mode">{{ item.name }}
                  </Option>
                </Select>
              </FormItem>
              <FormItem label="字段">
                <Select v-model="columnsFilterConfig.columns" filterable allow-create
                        @on-create="handleCreateUDLColumn" multiple style="width: 200px" placeholder="选择相关的字段">
                  <Option v-for="item in columnsForChose" :value="item" :key="item">{{ item }}</Option>
                </Select>
              </FormItem>
            </Card>
            <br>
            <FormItem v-if="!show">
              <Button type="primary" :loading="dataLoading" @click="submitAll">提交</Button>
            </FormItem>
          </Form>
        </Col>
        <Col span="11">
          <Divider style="margin-top: 1px">预览：同步表</Divider>
          <Button type="primary" :loading="tableLoading" @click="checkMysqlTablesInSrcMha">检查同步表</Button>
          <Table size="small" :height="tableHeight" :loading="tableLoading" stripe :columns="nameFilterCheck.columns"
                 :data="dataWithPage"
                 border></Table>
          <div>
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
        </Col>
      </Row>
    </Content>
  </base-component>
</template>
<script>

export default {
  data () {
    return {
      currentStep: 0,
      dataLoading: false,
      tableLoading: false,
      update: false,
      batchUpdate: false,
      show: false,
      message: '',
      showMsg: false,
      title: '',
      tableHeight: 80,
      submit: true,
      constants: {
        regionsForChose: [
          'SIN',
          'SH',
          'FRA'
        ],
        drcStrategyIdsForChose: [
          2000000002
        ],
        filterMode: {
          JAVA_REGEX: 0,
          TRIP_UDL: 1,
          AVIATOR_REGEX: 3,
          CUSTOM: 4,
          TRIP_UDL_UID: 5
        },
        fetchMode: {
          RPC: 0,
          BlackList: 1,
          WhiteList: 2,
          BlackList_Global: 3
        }
      },
      rowsFilterConfig: {
        mode: 1,
        drcStrategyId: 2000000002,
        routeStrategyId: 0,
        udlColumns: [],
        columns: [],
        context: '',
        illegalArgument: false,
        fetchMode: 0,
        modes: [
          {
            name: 'trip_udl',
            mode: 1
          },
          {
            name: 'java_regex',
            mode: 0
          },
          {
            name: 'aviator_regex',
            mode: 3
          },
          {
            name: 'custom',
            mode: 4
          },
          {
            name: 'trip_udl_uid',
            mode: 5
          }
        ],
        fetchModeForChose: [
          {
            k: 'RPC调用',
            v: 0
          },
          {
            k: 'BlackList',
            v: 1
          },
          {
            k: 'WhiteList',
            v: 2
          },
          {
            k: 'BlackListGlobal',
            v: 3
          }
        ]
      },
      columnsForChose: [],
      configInTripUid: {
        regionsChosen: []
      },
      columnsFilterConfig: {
        mode: Number,
        columns: [],
        modes: [
          {
            name: 'exclude',
            mode: 0
          },
          {
            name: 'include',
            mode: 1
          },
          {
            name: 'regex',
            mode: 2
          }
        ]
      },
      formItem: {
        switch: {
          rowsFilter: false,
          columnsFilter: false
        }
      },
      commonInfo: {
        srcMhaName: '',
        srcMhaId: 0,
        dstMhaName: '',
        srcDc: '',
        dstDc: '',
        order: true,
        testIds: [],
        dbReplicationIds: [],
        dbReplicationId: 0,
        dbName: '',
        tableName: '',
        tableData: [],
        rowsFilterDetail: {
          mode: 'trip-uld',
          row: null
        },
        colsFilterDetail: {
          mode: null,
          row: null
        },
        switch: {
          rowsFilter: false,
          columnsFilter: false
        }
      },
      ruleInline: {
        dbName: [
          { required: true, message: '请输入库名', trigger: 'blur' }
        ],
        tableName: [
          { required: true, message: '请输入表名', trigger: 'blur' }
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
        size: 10,
        pageSizeOpts: [10, 20, 50, 100]
      }
    }
  },
  methods: {
    submitAll () {
      console.log('dbReplicationId: ' + this.commonInfo.dbReplicationId)
      console.log('dbReplicationIds: ' + this.commonInfo.dbReplicationIds)
      console.log('testIds: ' + this.commonInfo.testIds)
      this.submit = true
      const rowsFilterParam = this.getRowsFilterParam()
      const columnsFilterParam = this.getColumnsFilterParam()
      if (!this.submit) {
        return
      }
      this.dataLoading = true
      const requestParam = {
        dbReplicationIds: this.commonInfo.dbReplicationIds,
        srcMhaName: this.commonInfo.srcMhaName,
        dstMhaName: this.commonInfo.dstMhaName,
        dbName: this.commonInfo.dbName,
        tableName: this.commonInfo.tableName,
        rowsFilterCreateParam: rowsFilterParam,
        columnsFilterCreateParam: columnsFilterParam
      }
      this.axios.post('/api/drc/v2/config/dbReplications', requestParam).then(res => {
        this.dataLoading = false
        if (res.data.status === 1) {
          this.$Message.error({
            content: '提交失败! ' + res.data.message,
            duration: 2
          })
        } else {
          this.update = true
          this.show = true
          this.$Message.success('提交成功！')
        }
      })
    },
    checkMysqlTablesInSrcMha () {
      this.nameFilterCheck.current = 1
      this.getCommonColumns()
      this.checkMySqlTables(this.commonInfo.srcMhaName, this.commonInfo.dbName + '\\.' + this.commonInfo.tableName)
    },
    checkMySqlTables (mha, nameFilter) {
      console.log('nameFilter:' + nameFilter)
      if (nameFilter == null) {
        nameFilter = ''
      }
      this.tableLoading = true
      this.axios.get('/api/drc/v2/mysql/preCheckMySqlTables?mha=' + mha +
        '&' + 'nameFilter=' + nameFilter)
        .then(response => {
          this.tableLoading = false
          this.commonInfo.tableData = response.data.data
          if (this.commonInfo.tableData.length < 10) {
            this.tableHeight = (this.commonInfo.tableData.length + 1) * 40
          } else {
            this.tableHeight = 440
          }
        })
    },
    handleChangeSize (val) {
      this.nameFilterCheck.size = val
    },
    getRowsFilterParam () {
      if (this.formItem.switch.rowsFilter) {
        if (this.useTripUdlOrUidMode) {
          // TRIP_UDL
          if (!this.hasUidColumn && !this.hasUdlColumn) {
            this.$Message.warning('uid 与 uld字段不能同时为空！')
            this.submit = false
            return
          }
          if (this.fillContextByRegion) {
            this.rowsFilterConfig.context = this.configInTripUid.regionsChosen.join(',')
          } else {
            this.rowsFilterConfig.context = '//filter by config'
          }
          if (this.fillContextByRegion &&
            (
              this.rowsFilterConfig.context === '' ||
              this.rowsFilterConfig.context === undefined ||
              this.rowsFilterConfig.context === '//filter by config'
            )
          ) {
            this.$Message.warning('context 不能为空！')
            this.submit = false
            return
          }
        }
        if (this.rowsFilterConfig.mode === '' || this.rowsFilterConfig.mode === undefined || (this.rowsFilterConfig.columns.length === 0 && this.rowsFilterConfig.udlColumns.length === 0)) {
          this.$Message.warning('缺少行过滤配置 禁止提交')
          this.submit = false
        } else {
          // alert('requestParam')
          const requestParam = {
            dbName: this.dbName,
            tableName: this.tableName,
            mode: this.rowsFilterConfig.mode,
            columns: this.rowsFilterConfig.columns.length === 0 ? null : this.rowsFilterConfig.columns,
            udlColumns: this.rowsFilterConfig.udlColumns.length === 0 ? null : this.rowsFilterConfig.udlColumns,
            drcStrategyId: this.rowsFilterConfig.udlColumns.length === 0 ? null : this.rowsFilterConfig.drcStrategyId,
            routeStrategyId: this.rowsFilterConfig.routeStrategyId,
            illegalArgument: this.rowsFilterConfig.illegalArgument,
            fetchMode: this.rowsFilterConfig.fetchMode,
            context: this.rowsFilterConfig.context === '' ? null : this.rowsFilterConfig.context
          }
          return requestParam
        }
      } else {
        return null
      }
    },
    getRowsFilterConfig () {
      this.axios.get('/api/drc/v2/config/rowsFilter?dbReplicationId=' + this.commonInfo.dbReplicationId)
        .then(response => {
          if (response.data.status === 1) {
            this.$Message.error('查询行过滤配置失败!')
          } else {
            const res = response.data.data
            if (res == null) {
              // empty config
            } else {
              this.formItem.switch.rowsFilter = true
              // rowsFilterConfig.context和onfigInTripUid.regionsChosen要放在前面
              // 不然会失效
              this.rowsFilterConfig.context = res.context
              this.rowsFilterConfig.mode = res.mode
              if (this.useTripUdlOrUidMode) {
                this.configInTripUid.regionsChosen = res.context.split(',')
              } else {
                this.configInTripUid = {
                  regionsChosen: []
                }
              }
              this.rowsFilterConfig.columns = res.columns === null ? [] : res.columns
              if (res.udlColumns !== null) {
                this.rowsFilterConfig.udlColumns = res.udlColumns
              }
              this.rowsFilterConfig.drcStrategyId = res.drcStrategyId === 0 ? null : res.drcStrategyId
              this.rowsFilterConfig.routeStrategyId = res.routeStrategyId
              this.rowsFilterConfig.illegalArgument = res.illegalArgument
              this.rowsFilterConfig.fetchMode = res.fetchMode
            }
          }
        })
    },
    getCommonColumns () {
      this.axios.get('/api/drc/v2/mysql/commonColumns?' +
        '&mhaName=' + this.commonInfo.srcMhaName +
        '&namespace=' + this.commonInfo.dbName +
        '&name=' + this.commonInfo.tableName)
        .then(response => {
          if (response.data.status === 1) {
            this.$Message.error('查询公共列名失败，请手动添加！')
            this.columnsForChose = []
          } else {
            console.log(response.data.data)
            this.columnsForChose = response.data.data
            if (this.columnsForChose.length === 0) {
              this.$Message.warning('查询无公共字段！')
            }
          }
        })
    },
    fetchModeChange () {
      if (this.rowsFilterConfig.fetchMode === 1 || this.rowsFilterConfig.fetchMode === 3) {
        this.rowsFilterConfig.illegalArgument = true
      } else {
        this.rowsFilterConfig.illegalArgument = false
      }
    },
    getColumnsFilterParam () {
      if (this.formItem.switch.columnsFilter) {
        const columnsFilterParam = {
          mode: this.columnsFilterConfig.mode,
          columns: this.columnsFilterConfig.columns
        }
        return columnsFilterParam
      } else {
        return null
      }
    },
    getColumnsFilterConfig () {
      // console.log(this.dbReplicationId)
      this.axios.get('/api/drc/v2/config/columnsFilter?dbReplicationId=' + this.commonInfo.dbReplicationId)
        .then(response => {
          if (response.data.status === 1) {
            this.$Message.error('查询字段过滤配置失败!')
          } else {
            const res = response.data.data
            if (res == null) {
              // empty config
            } else {
              this.columnsFilterConfig.mode = res.mode
              this.columnsFilterConfig.columns = res.columns
              this.formItem.switch.columnsFilter = true
            }
          }
        })
    },
    contains (a, obj) {
      var i = a.length
      while (i--) {
        if (a[i] === obj) {
          return true
        }
      }
      return false
    },
    handleCreateUDLColumn (val) {
      this.showMsg = false
      if (val === '无UDL' || this.contains(this.columnsForChose, val)) {
        alert('已有项禁止创建')
        return
      }
      if (val === '' || val === undefined || val === null) {
        alert('字段不能为空')
        return
      }
      this.axios.get(
        '/api/drc/v2/mysql//columnCheck?' +
        'mhaName=' + this.commonInfo.srcMhaName +
        '&namespace=' + this.commonInfo.dbName +
        '&name=' + this.commonInfo.tableName +
        '&column=' + val)
        .then(response => {
          if (response.data.status === 1) {
            this.$Message.error('查询字段:' + val + '失败！' + response.data.data)
            this.columnsForChose.push(val)
          } else {
            const tablesWithoutColumn = response.data.data
            if (tablesWithoutColumn.length !== 0) {
              this.showMsg = true
              this.title = '以下表无字段' + val
              this.message = tablesWithoutColumn
              // alert('以下表无字段' + val + '如下:' + tablesWithoutColumn)
            }
            this.columnsForChose.push(val)
          }
        })
    },
    handleCreateColumn (val) {
      if (this.contains(this.columnsForChose, val)) {
        alert('已有项禁止创建')
        return
      }
      if (val === '' || val === undefined || val === null) {
        alert('字段不能为空')
        return
      }
      this.columnsForChose.push(val)
    },
    notice () {
      if (this.batchUpdate && this.commonInfo.dbReplicationIds !== undefined) {
        this.$Message.info('共选中' + this.commonInfo.dbReplicationIds.length + '行')
      }
    },
    init () {
      this.notice()
      if (this.commonInfo.dbReplicationId !== undefined) {
        this.getRowsFilterConfig()
        this.getColumnsFilterConfig()
      }
    }
  },
  computed: {
    dataWithPage () {
      const data = this.commonInfo.tableData
      const start = this.nameFilterCheck.current * this.nameFilterCheck.size - this.nameFilterCheck.size
      const end = start + this.nameFilterCheck.size
      return [...data].slice(start, end)
    },
    useTripUdlOrUidMode () {
      return [this.constants.filterMode.TRIP_UDL, this.constants.filterMode.TRIP_UDL_UID].includes(this.rowsFilterConfig.mode)
    },
    hasUdlColumn () {
      return this.rowsFilterConfig.udlColumns.length !== 0
    },
    fillContextByRegion () {
      return this.rowsFilterConfig.fetchMode === this.constants.fetchMode.RPC
    },
    hasUidColumn () {
      return this.rowsFilterConfig.columns.length !== 0
    },
    showUdlConfigDetail () {
      return this.rowsFilterConfig.mode === this.constants.filterMode.TRIP_UDL_UID || this.hasUdlLegalConfig || !this.hasUidLegalConfig
    },
    showUidConfigDetail () {
      return this.rowsFilterConfig.mode === this.constants.filterMode.TRIP_UDL_UID || this.hasUidLegalConfig || !this.hasUdlLegalConfig
    },
    hasUdlLegalConfig () {
      return this.rowsFilterConfig.udlColumns.length !== 0 && this.rowsFilterConfig.drcStrategyId != null && this.rowsFilterConfig.drcStrategyId > 0
    },
    hasUidLegalConfig () {
      return this.hasUidColumn
    }
  },
  created () {
    this.axios.get('/api/drc/v2/permission/meta/mhaReplication/modify').then((response) => {
      if (response.data.status === 403) {
        this.$router.push('/nopermission')
        return
      }
      let dbReplicationIds = []
      if (this.$route.query.dbReplicationIds !== undefined) {
        dbReplicationIds = JSON.parse(this.$route.query.dbReplicationIds)
      }
      // this.$route.query.dbReplicationIds.forEach(e => dbReplicationIds.push(e))
      console.log('dbReplicationIds:' + dbReplicationIds)
      this.commonInfo = {
        srcMhaName: this.$route.query.srcMhaName,
        srcMhaId: this.$route.query.srcMhaId,
        dstMhaName: this.$route.query.dstMhaName,
        srcDc: this.$route.query.srcDc,
        dstDc: this.$route.query.dstDc,
        dbName: this.$route.query.dbName,
        tableName: this.$route.query.tableName,
        dbReplicationId: this.$route.query.dbReplicationId,
        dbReplicationIds: dbReplicationIds,
        tableData: []
      }
      this.update = this.$route.query.update === true || this.$route.query.update === 'true'
      this.batchUpdate = this.$route.query.batchUpdate === true || this.$route.query.batchUpdate === 'true'
      this.show = this.$route.query.show === true || this.$route.query.show === 'true'
      const queryMysqlTable = this.commonInfo.dbName !== undefined && this.commonInfo.tableName !== undefined
      if (queryMysqlTable) {
        this.checkMysqlTablesInSrcMha()
      }
      console.log('commonInfo')
      console.log(this.commonInfo)
      this.init()
    })
  }
}
</script>

<template>
    <base-component>
      <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
        <BreadcrumbItem to="/home">首页</BreadcrumbItem>
        <BreadcrumbItem to="/rowsFilterConfigs">行过滤配置</BreadcrumbItem>
      </Breadcrumb>
      <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
        <Row>
          <Col span="22">
            <span style="margin-top: 10px;color:#464c5b;font-weight:600">{{drc.srcMha}}({{drc.srcDc}})==>{{drc.destMha}}({{drc.destDc}})</span>
          </Col>
          <Col span="2">
            <Button style="margin-top: 10px;text-align: right" type="primary" ghost @click="goToAddRowsFilter">添加</Button>
          </Col>
        </Row>
        <div :style="{padding: '1px 1px',height: '100%'}">
          <template v-if="display.rowsFilterConfigs">
            <Table style="margin-top: 20px" stripe :columns="columns" :data="rowsFilterConfigsData" border>
              <template slot-scope="{ row, index }" slot="action">
                <Button type="success" size="small" style="margin-right: 5px" @click="goToShowConfig(row, index)">查看</Button>
                <Button type="primary" size="small" style="margin-right: 5px" @click="goToUpdateConfig(row, index)">修改</Button>
                <Button type="error" size="small" style="margin-right: 10px" @click="deleteConfig(row, index)">删除</Button>
              </template>
            </Table>
          </template>
        </div>
      </Content>
      <Modal
        v-model="display.rowsFilterModal"
        title="行过滤配置"
        width="1000px"
        >
        <Row :gutter="10">
          <Col span="12">
            <Card>
              <div slot="title">
                  <span>配置</span>
              </div>
              <Form ref="rowsFilterConfig" :model="rowsFilterConfig" :label-width="100"
                    style="margin-top: 10px">
                <FormItem label="数据源" >
                  <Input v-model="rowsFilterConfig.dataMediaSourceName" readonly style="width: 200px"/>
                </FormItem>
                <FormItem label="库名">
                  <Input v-model="rowsFilterConfig.namespace" style="width:200px" placeholder="支持正则  .*匹配全部"/>
                </FormItem>
                <FormItem label="表名">
                  <Row>
                    <Col span="16">
                      <Input v-model="rowsFilterConfig.name" style="width:200px" placeholder="支持正则  .*匹配全部"/>
                    </Col>
                    <Col span="4">
                      <Button  type="success" @click="checkTable"  style="margin-left: 10px">校验</Button>
                    </Col>
                  </Row>
                </FormItem>
                <FormItem label="模式">
                  <Select v-model="rowsFilterConfig.mode" style="width: 200px" placeholder="选择行过滤模式">
                    <Option v-for="item in modesForChose" :value="item" :key="item">{{ item }}</Option>
                  </Select>
                </FormItem>
                <FormItem  label="规则内容" v-if="rowsFilterConfig.mode !== 'trip_udl' || rowsFilterConfig.fetchMode === 0">
                  <Input v-if="rowsFilterConfig.mode !== 'trip_udl'" type="textarea" v-model="rowsFilterConfig.context" style="width: 250px" placeholder="请输入行过滤内容"/>
                  <Select v-if="rowsFilterConfig.mode === 'trip_udl' && rowsFilterConfig.fetchMode === 0"  v-model="configInTripUid.regionsChosen" multiple style="width: 200px" placeholder="Region 选择">
                    <Option v-for="item in regionsForChose" :value="item" :key="item">{{ item }}</Option>
                  </Select>
                </FormItem>
                <FormItem v-if="rowsFilterConfig.mode === 'trip_udl'" label="空处理" >
                  <Checkbox v-model="rowsFilterConfig.illegalArgument">【字段为空时】同步</Checkbox>
                </FormItem>
                <Divider v-if="rowsFilterConfig.mode === 'trip_udl'">UDL配置</Divider>
                <FormItem label="UDL字段" v-if="rowsFilterConfig.mode === 'trip_udl'">
                  <Select   v-model="rowsFilterConfig.udlColumns"  filterable allow-create @on-create="handleCreateUDLColumn" multiple style="width: 200px" placeholder="不选默认则无UDL配置">
                    <Option v-for="item in columnsForChose" :value="item" :key="item">{{ item }}</Option>
                  </Select>
                </FormItem>
                <FormItem label="DRC UDL策略id" v-if="rowsFilterConfig.mode === 'trip_udl' && rowsFilterConfig.udlColumns.length !== 0">
                  <Input v-model="rowsFilterConfig.drcStrategyId" style="width:200px" placeholder="Integer类型"/>
                </FormItem>
<!--                <FormItem label="路由 UDL策略id" v-if="rowsFilterConfig.mode === 'trip_udl' && rowsFilterConfig.udlColumns.length !== 0">-->
<!--                  <Input v-model="rowsFilterConfig.routeStrategyId" style="width:200px" placeholder="Integer类型"/>-->
<!--                </FormItem>-->
                <Divider v-if="rowsFilterConfig.mode === 'trip_udl'">UID配置</Divider>
                <FormItem label="相关字段" v-if="rowsFilterConfig.mode !== 'trip_udl'">
                  <Select v-model="rowsFilterConfig.columns" filterable allow-create @on-create="handleCreateColumn" multiple style="width: 200px" placeholder="选择相关字段">
                    <Option v-for="item in columnsForChose" :value="item" :key="item" :lable="item"></Option>
                  </Select>
                </FormItem>
                <FormItem label="UID字段" v-if="rowsFilterConfig.mode === 'trip_udl'">
                  <Select  v-model="rowsFilterConfig.columns"   filterable allow-create @on-create="handleCreateUIDColumn" multiple style="width: 200px" placeholder="不选默认则无UID配置">
                    <Option v-for="item in columnsForChose" :value="item" :key="item">{{ item }}</Option>
                  </Select>
                </FormItem>
                <FormItem label="fetchMode" v-if="rowsFilterConfig.mode === 'trip_udl'">
                  <Select  v-model="rowsFilterConfig.fetchMode" style="width: 200px" placeholder="选择" @on-change="fetchModeChange()">
                    <Option v-for="item in fetchModeForChose" :value="item.v" :key="item.k">{{ item.k }}</Option>
                  </Select>
                </FormItem>
              </Form>
            </Card>
          </Col>
          <Col span="12">
            <Card>
              <div slot="title">
                <span>相关表</span>
              </div>
              <Table stripe :columns="columnsForTableCheck" :data="dataWithPage" border ></Table>
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
          </Col>
        </Row>
        <template #footer>
          <Button type="text" size="large"  @click="cancelSubmit">取消</Button>
          <Button type="primary"  @click="submitConfig">提交</Button>
        </template>
      </Modal>
    </base-component>
</template>

<script>
export default {
  name: 'rowsFilterConfigs',
  data () {
    return {
      display: {
        rowsFilterConfigs: true,
        rowsFilterModal: false,
        showOnly: false
      },
      drc: {
        srcMha: this.$route.query.srcMha,
        destMha: this.$route.query.destMha,
        srcDc: '',
        destDc: '',
        applierGroupId: 0,
        srcMhaId: 0
      },
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
          key: 'namespace'
        },
        {
          title: '表名',
          key: 'name'
        },
        {
          title: '模式',
          key: 'mode'
        },
        {
          title: 'udl列（udl专用)',
          key: 'udlColumns'
        },
        {
          title: '相关列/uid列',
          key: 'columns'
        },
        {
          title: '内容',
          key: 'context'
        },
        // {
        //   title: 'Route UDL策略（udl专用)',
        //   key: 'routeStrategyId'
        // },
        {
          title: 'DRC UDL策略（udl专用)',
          key: 'drcStrategyId'
        },
        {
          title: '默认同步（udl专用)',
          key: 'illegalArgument'
        },
        {
          title: '校验模式（udl专用)',
          key: 'fetchMode',
          width: 100,
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.fetchMode === 0 ? 'RPC' : row.fetchMode === 1 ? 'BlackList' : 'WhiteList'
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
          align: 'center',
          width: 200,
          fixed: 'right'
        }
      ],
      rowsFilterConfigsData: [],
      tableData: [],
      total: 0,
      current: 1,
      size: 5,
      pageSizeOpts: [5, 10, 20, 100],
      columnsForTableCheck: [
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
          key: 'schema'
        },
        {
          title: '表名',
          key: 'name'
        }
      ],
      rowsFilterConfig: {
        mappingId: 0,
        dataMediaId: 0,
        namespace: '',
        name: '',
        dataMediaSourceId: 0,
        dataMediaSourceName: '',
        rowsFilterId: 0,
        mode: 'trip_udl',
        drcStrategyId: 0,
        routeStrategyId: 0,
        udlColumns: [],
        columns: [],
        context: '',
        illegalArgument: false,
        fetchMode: 0
      },
      configInTripUid: {
        regionsChosen: []
      },
      modesForChose: [
        'aviator_regex',
        'java_regex',
        'trip_udl',
        'custom'
      ],
      columnsForChose: [],
      regionsForChose: [
        'SIN',
        'SH'
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
        }
      ],
      columnForAdd: '',
      forceCommit: false,
      conflictTables: []
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
    getOrInitSimplexDrc () {
      console.log(this.drc.srcMha + '/' + this.drc.destMha)
      this.axios.post('/api/drc/v1/build/simplexDrc/' + this.drc.srcMha + '/' + this.drc.destMha)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('获取或创建该方向同步失败!')
          } else {
            const vo = response.data.data
            this.drc.srcDc = vo.srcDc
            this.drc.destDc = vo.destDc
            this.drc.applierGroupId = vo.destApplierGroupId
            this.drc.srcMhaId = vo.srcMhaId
            this.getRowsFilterConfigs()
          }
        })
    },
    getRowsFilterConfigs () {
      console.log(this.drc.applierGroupId)
      this.axios.get('/api/drc/v1/build/rowsFilterMappings/' + this.drc.applierGroupId)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('查询行过滤配置失败!')
          } else {
            this.rowsFilterConfigsData = response.data.data
          }
        })
    },
    goToShowConfig (row, index) {
      this.rowsFilterConfigInitFormRow(row, index)
      this.showMatchTables()
      this.columnsForChose = row.columns
      this.display.rowsFilterModal = true
      this.display.showOnly = true
    },
    goToUpdateConfig  (row, index) {
      this.rowsFilterConfigInitFormRow(row, index)
      this.columnsForChose = row.columns
      this.forceCommit = false
      this.display.showOnly = false
      this.display.rowsFilterModal = true
    },
    deleteConfig (row, index) {
      this.axios.delete('/api/drc/v1/build/rowsFilterConfig/' + row.id).then(response => {
        console.log(response.data)
        console.log(response.data.data)
        if (response.data.status === 0) {
          alert('删除成功！')
          this.getRowsFilterConfigs()
        } else {
          alert('操作失败！')
        }
      })
    },
    goToAddRowsFilter () {
      this.rowsFilterConfigInit()
      this.columnsForChose = []
      this.checkTable()
      this.forceCommit = false
      this.display.showOnly = false
      this.display.rowsFilterModal = true
    },
    rowsFilterConfigInitFormRow (row, index) {
      if (row.mode === 'trip_udl') {
        this.configInTripUid = {
          regionsChosen: row.context.split(',')
        }
      } else {
        this.configInTripUid = {
          regionsChosen: []
        }
      }
      this.rowsFilterConfig = {
        mappingId: row.id,
        dataMediaId: row.dataMediaId,
        namespace: row.namespace,
        name: row.name,
        dataMediaSourceId: row.dataMediaSourceId,
        dataMediaSourceName: this.drc.srcMha,
        rowsFilterId: row.rowsFilterId,
        mode: row.mode,
        columns: row.columns,
        context: row.context,
        udlColumns: row.udlColumns,
        drcStrategyId: row.drcStrategyId,
        routeStrategyId: row.routeStrategyId,
        illegalArgument: row.illegalArgument,
        fetchMode: row.fetchMode
      }
      this.tableData = []
    },
    rowsFilterConfigInit () {
      this.configInTripUid = {
        regionsChosen: []
      }
      this.rowsFilterConfig = {
        mappingId: 0,
        dataMediaId: 0,
        namespace: '.*',
        name: '.*',
        dataMediaSourceId: 0,
        dataMediaSourceName: this.drc.srcMha,
        rowsFilterId: 0,
        mode: 'trip_udl',
        columns: [],
        context: '',
        udlColumns: [],
        drcStrategyId: 0,
        routeStrategyId: 0,
        illegalArgument: false,
        fetchMode: 0
      }
      this.tableData = []
    },
    submitConfig () {
      console.log('before:')
      console.log(this.rowsFilterConfig)
      if (this.rowsFilterConfig.mode === 'trip_udl') {
        if (this.columns.length === 0 && this.udlColumns.length === 0) {
          alert('uid 与 uld字段不能同时为空！')
          return
        }
        if (this.rowsFilterConfig.fetchMode === 1 || this.rowsFilterConfig.fetchMode === 2) {
          this.rowsFilterConfig.context = '//filter by config'
        } else {
          this.rowsFilterConfig.context = this.configInTripUid.regionsChosen.join(',')
        }
        if (this.rowsFilterConfig.fetchMode === 0 &&
            (
              this.rowsFilterConfig.context === '' ||
              this.rowsFilterConfig.context === undefined ||
              this.rowsFilterConfig.context === '//filter by config'
            )
        ) {
          alert('context 不能为空！')
          return
        }
      }
      console.log('after:')
      console.log(this.rowsFilterConfig)
      if (this.display.showOnly) {
        window.alert('查看状态，禁止提交！')
      } else if (
        this.rowsFilterConfig.namespace === '' ||
        this.rowsFilterConfig.namespace === undefined ||
        this.rowsFilterConfig.name === '' ||
        this.rowsFilterConfig.name === undefined) {
        alert('缺少表配置 禁止提交！')
      } else if (
        this.rowsFilterConfig.mode === '' ||
        this.rowsFilterConfig.mode === undefined ||
        (this.rowsFilterConfig.columns.length === 0 && this.rowsFilterConfig.udlColumns.length === 0)) {
        alert('缺少行过滤配置 禁止提交')
      } else if (this.conflictTables.length !== 0) {
        alert('存在匹配表已经了行过滤，禁止提交')
      } else if (this.tableData.length === 0 && !this.forceCommit) {
        this.forceCommit = true
        alert('无匹配表 下一次提交将强制执行！！')
      } else {
        const dto = {
          id: this.rowsFilterConfig.mappingId === 0 ? null : this.rowsFilterConfig.mappingId,
          applierGroupId: this.drc.applierGroupId,
          dataMediaId: this.rowsFilterConfig.dataMediaId === 0 ? null : this.rowsFilterConfig.dataMediaId,
          namespace: this.rowsFilterConfig.namespace === '' ? null : this.rowsFilterConfig.namespace,
          name: this.rowsFilterConfig.name === '' ? null : this.rowsFilterConfig.name,
          type: 0,
          dataMediaSourceId: this.drc.srcMhaId,
          dataMediaSourceName: this.drc.srcMha,
          rowsFilterId: this.rowsFilterConfig.rowsFilterId === 0 ? null : this.rowsFilterConfig.rowsFilterId,
          mode: this.rowsFilterConfig.mode,
          columns: this.rowsFilterConfig.columns.length === 0 ? null : this.rowsFilterConfig.columns,
          udlColumns: this.rowsFilterConfig.udlColumns.length === 0 ? null : this.rowsFilterConfig.udlColumns,
          drcStrategyId: this.rowsFilterConfig.drcStrategyId,
          routeStrategyId: this.rowsFilterConfig.routeStrategyId,
          illegalArgument: this.rowsFilterConfig.illegalArgument,
          fetchMode: this.rowsFilterConfig.fetchMode,
          context: this.rowsFilterConfig.context === '' ? null : this.rowsFilterConfig.context
        }
        console.log('dto:')
        console.log(dto)
        this.axios.post('/api/drc/v1/build/rowsFilterConfig', dto).then(response => {
          if (response.data.status === 1) {
            window.alert('提交失败!' + response.data.data)
          } else {
            window.alert('提交成功!' + response.data.data)
            this.display.rowsFilterModal = false
            this.getRowsFilterConfigs()
          }
        })
      }
    },
    cancelSubmit () {
      this.display.rowsFilterModal = false
    },
    showMatchTables () {
      console.log('/api/drc/v1/build/dataMedia/check?' +
        'namespace=' + this.rowsFilterConfig.namespace +
        '&name=' + this.rowsFilterConfig.name +
        '&mhaName=' + this.drc.srcMha +
        '&type=' + 0)
      this.axios.get('/api/drc/v1/build/dataMedia/check?' +
        'namespace=' + this.rowsFilterConfig.namespace +
        '&name=' + this.rowsFilterConfig.name +
        '&mhaName=' + this.drc.srcMha +
        '&type=' + 0)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('查询匹配表失败' + response.data.data)
          } else {
            console.log(response.data.data)
            this.tableData = response.data.data
            if (this.tableData.length === 0) {
              window.alert('无匹配表 或 查询匹配表失败')
            }
          }
        })
    },
    conflictCheck () {
      console.log('/api/drc/v1/build/dataMedia/conflictCheck?' +
        'applierGroupId=' + this.drc.applierGroupId +
        '&dataMediaId=' + this.rowsFilterConfig.dataMediaId +
        '&mhaName=' + this.drc.srcMha +
        '&namespace=' + this.rowsFilterConfig.namespace +
        '&name=' + this.rowsFilterConfig.name)
      this.axios.get(
        '/api/drc/v1/build/dataMedia/conflictCheck?' +
        'applierGroupId=' + this.drc.applierGroupId +
        '&dataMediaId=' + this.rowsFilterConfig.dataMediaId +
        '&mhaName=' + this.drc.srcMha +
        '&namespace=' + this.rowsFilterConfig.namespace +
        '&name=' + this.rowsFilterConfig.name)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('表匹配冲突校验失败，请勿配置重复表！！')
          } else {
            console.log(response.data.data)
            this.conflictTables = response.data.data
            if (this.conflictTables.length !== 0) {
              window.alert('存在表已经配置行过滤规则,如下:' +
                this.conflictTables)
            }
          }
        })
    },
    getCommonColumns () {
      console.log('/api/drc/v1/build/rowsFilter/commonColumns?' +
        '&mhaName=' + this.drc.srcMha +
        '&namespace=' + this.rowsFilterConfig.namespace +
        '&name=' + this.rowsFilterConfig.name)
      this.axios.get('/api/drc/v1/build/rowsFilter/commonColumns?' +
        '&mhaName=' + this.drc.srcMha +
        '&namespace=' + this.rowsFilterConfig.namespace +
        '&name=' + this.rowsFilterConfig.name)
        .then(response => {
          if (response.data.status === 1) {
            alert('查询公共列名失败，请手动添加！' + response.data.data)
            this.columnsForChose = []
          } else {
            console.log(response.data.data)
            this.columnsForChose = response.data.data
            if (this.columnsForChose.length === 0) {
              alert('查询无公共字段！')
            }
          }
        })
    },
    checkTable () {
      if (this.rowsFilterConfig.namespace === '' || this.rowsFilterConfig.name === '') {
        window.alert('库名表名不能为空！')
        return
      }
      console.log('showMatchTables')
      this.showMatchTables()
      console.log('conflictCheck')
      this.conflictCheck()
      console.log('getCommonColumns')
      this.getCommonColumns()
    },
    handleChangeSize (val) {
      this.size = val
    },
    handleCreateUDLColumn (val) {
      if (val === '无UDL' || this.contains(this.columnsForChose, val)) {
        alert('已有项禁止创建')
        return
      }
      if (val === '' || val === undefined || val === null) {
        alert('字段不能为空')
        return
      }
      console.log('/api/drc/v1/build/dataMedia/columnCheck?' +
        'srcDc=' + this.drc.srcDc +
        '&mhaName=' + this.drc.srcMha +
        '&namespace=' + this.rowsFilterConfig.namespace +
        '&name=' + this.rowsFilterConfig.name +
        '&column=' + val)
      this.axios.get(
        '/api/drc/v1/build/dataMedia/columnCheck?' +
        'srcDc=' + this.drc.srcDc +
        '&mhaName=' + this.drc.srcMha +
        '&namespace=' + this.rowsFilterConfig.namespace +
        '&name=' + this.rowsFilterConfig.name +
        '&column=' + val)
        .then(response => {
          if (response.data.status === 1) {
            alert('查询字段:' + val + '失败！' + response.data.data)
            this.columnsForChose.push(val)
          } else {
            const tablesWithoutColumn = response.data.data
            if (tablesWithoutColumn.length !== 0) {
              alert('以下表无字段' + val + '如下:' + tablesWithoutColumn)
            }
            this.columnsForChose.push(val)
          }
        })
    },
    handleCreateUIDColumn (val) {
      if (val === '无UID' || this.contains(this.columnsForChose, val)) {
        alert('已有项禁止创建')
        return
      }
      if (val === '' || val === undefined || val === null) {
        alert('字段不能为空')
        return
      }
      console.log('/api/drc/v1/build/dataMedia/columnCheck?' +
        'srcDc=' + this.drc.srcDc +
        '&mhaName=' + this.drc.srcMha +
        '&namespace=' + this.rowsFilterConfig.namespace +
        '&name=' + this.rowsFilterConfig.name +
        '&column=' + val)
      this.axios.get(
        '/api/drc/v1/build/dataMedia/columnCheck?' +
        'srcDc=' + this.drc.srcDc +
        '&mhaName=' + this.drc.srcMha +
        '&namespace=' + this.rowsFilterConfig.namespace +
        '&name=' + this.rowsFilterConfig.name +
        '&column=' + val)
        .then(response => {
          if (response.data.status === 1) {
            alert('查询字段:' + val + '失败！' + response.data.data)
            this.columnsForChose.push(val)
          } else {
            const tablesWithoutColumn = response.data.data
            if (tablesWithoutColumn.length !== 0) {
              alert('以下表无字段' + val + '如下:' + tablesWithoutColumn)
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
      console.log('/api/drc/v1/build/dataMedia/columnCheck?' +
        'srcDc=' + this.drc.srcDc +
        '&mhaName=' + this.drc.srcMha +
        '&namespace=' + this.rowsFilterConfig.namespace +
        '&name=' + this.rowsFilterConfig.name +
        '&column=' + val)
      this.axios.get(
        '/api/drc/v1/build/dataMedia/columnCheck?' +
        'srcDc=' + this.drc.srcDc +
        '&mhaName=' + this.drc.srcMha +
        '&namespace=' + this.rowsFilterConfig.namespace +
        '&name=' + this.rowsFilterConfig.name +
        '&column=' + val)
        .then(response => {
          if (response.data.status === 1) {
            alert('查询字段:' + val + '失败！' + response.data.data)
            this.columnsForChose.push(val)
          } else {
            const tablesWithoutColumn = response.data.data
            if (tablesWithoutColumn.length !== 0) {
              alert('以下表无字段' + val + '如下:' +
                tablesWithoutColumn)
            }
            this.columnsForChose.push(val)
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
    fetchModeChange () {
      if (this.rowsFilterConfig.fetchMode === 1) {
        this.rowsFilterConfig.illegalArgument = true
      } else {
        this.rowsFilterConfig.illegalArgument = false
      }
    }
  },
  created () {
    console.log(this.$route.query.srcMha)
    console.log(this.$route.query.destMha)
    this.drc.srcMha = this.$route.query.srcMha
    this.drc.destMha = this.$route.query.destMha
    this.getOrInitSimplexDrc()
  }

}

</script>

<style scoped>

</style>

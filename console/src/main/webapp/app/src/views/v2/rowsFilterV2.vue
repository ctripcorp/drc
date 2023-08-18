<template>
  <div>
    <Card dis-hover>
      <Form ref="rowsFilterConfig" :model="rowsFilterConfig" :label-width="100"
            style="margin-top: 10px">
        <FormItem label="库名">
          <Input v-model="this.dbName" style="width:200px"  disabled/>
        </FormItem>
        <FormItem label="表名">
          <Input v-model="this.tableName" style="width:200px" disabled/>
        </FormItem>
        <FormItem label="模式">
<!--          <Select v-model="rowsFilterConfig.mode" style="width: 200px" placeholder="选择行过滤模式"  @input="selectMode">-->
          <Select v-model="rowsFilterConfig.mode" style="width: 200px" placeholder="选择行过滤模式" >
            <Option v-for="item in modes" :value="item.mode" :key="item.mode">{{ item.name }}</Option>
          </Select>
        </FormItem>
        <FormItem  label="规则内容" v-if="rowsFilterConfig.mode !== 1 || rowsFilterConfig.fetchMode === 0">
          <Input v-if="rowsFilterConfig.mode !== 1" type="textarea" v-model="rowsFilterConfig.context" style="width: 250px" placeholder="请输入行过滤内容"/>
          <Select v-if="rowsFilterConfig.mode === 1 && rowsFilterConfig.fetchMode === 0"  v-model="configInTripUid.regionsChosen" multiple style="width: 200px" placeholder="Region 选择">
            <Option v-for="item in regionsForChose" :value="item" :key="item">{{ item }}</Option>
          </Select>
        </FormItem>
        <FormItem v-if="rowsFilterConfig.mode === 1" label="空处理" >
          <Checkbox v-model="rowsFilterConfig.illegalArgument">【字段为空时】同步</Checkbox>
        </FormItem>
        <Divider v-if="rowsFilterConfig.mode === 1">UDL配置</Divider>
        <FormItem label="UDL字段" v-if="rowsFilterConfig.mode === 1">
          <Select   v-model="rowsFilterConfig.udlColumns"  filterable allow-create  multiple style="width: 200px" placeholder="不选默认则无UDL配置">
            <Option v-for="item in columnsForChose" :value="item" :key="item">{{ item }}</Option>
          </Select>
        </FormItem>
        <FormItem label="DRC UDL策略id" v-if="rowsFilterConfig.mode === 1 && rowsFilterConfig.udlColumns.length !== 0">
          <Select   v-model="rowsFilterConfig.drcStrategyId"  filterable allow-create  style="width: 200px" placeholder="请选择ucs策略id">
            <Option v-for="item in drcStrategyIdsForChose" :value="item" :key="item">{{ item }}</Option>
          </Select>
        </FormItem>
        <Divider v-if="rowsFilterConfig.mode === 1">UID配置</Divider>
        <FormItem label="相关字段" v-if="rowsFilterConfig.mode !== 1">
          <Select v-model="rowsFilterConfig.columns" filterable allow-create  multiple style="width: 200px" placeholder="选择相关字段">
            <Option v-for="item in columnsForChose" :value="item" :key="item" :lable="item"></Option>
          </Select>
        </FormItem>
        <FormItem label="UID字段" v-if="rowsFilterConfig.mode === 1">
          <Select  v-model="rowsFilterConfig.columns"   filterable allow-create  multiple style="width: 200px" placeholder="不选默认则无UID配置">
            <Option v-for="item in columnsForChose" :value="item" :key="item">{{ item }}</Option>
          </Select>
        </FormItem>
        <FormItem label="fetchMode" v-if="rowsFilterConfig.mode === 1">
          <Select  v-model="rowsFilterConfig.fetchMode" style="width: 200px" placeholder="选择" @on-change="fetchModeChange()">
            <Option v-for="item in fetchModeForChose" :value="item.v" :key="item.k">{{ item.k }}</Option>
          </Select>
        </FormItem>
        <FormItem>
          <Row>
            <Col span="10">
              <Button type="error" @click="deleteConfig" style="margin-left: 10px">删除</Button>
            </Col>
            <Col span="8">
              <Button type="primary" @click="submitConfig">保存</Button>
            </Col>
          </Row>
        </FormItem>
      </Form>
    </Card>
  </div>
</template>

<script>
export default {
  name: 'rowsFilterV2',
  props: {
    srcMhaName: String,
    dstMhaName: String,
    srcDc: String,
    destDc: String,
    dbName: String,
    tableName: String,
    dbReplicationId: Number,
    dbReplicationIds: [],
    update: Number
  },
  data () {
    return {
      rowsFilterConfig: {
        mode: 1,
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
        }
      ],
      columnsForChose: [],
      regionsForChose: [
        'SIN',
        'SH',
        'FRA'
      ],
      drcStrategyIdsForChose: [
        2000000002,
        20001
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
    }
  },
  methods: {
    selectMode () {
      if (this.rowsFilterConfig.mode === 1) {
        this.rowsFilterConfig = {
          mode: 1,
          drcStrategyId: 0,
          routeStrategyId: 0,
          udlColumns: [],
          columns: [],
          context: '',
          illegalArgument: false,
          fetchMode: 0
        }
        this.configInTripUid = {
          regionsChosen: []
        }
      }
    },
    submitConfig () {
      // if (!this.update) {
      //   alert('查看状态不能修改')
      // }
      console.log(this.rowsFilterConfig)
      if (this.rowsFilterConfig.mode === 1) {
        if (this.rowsFilterConfig.columns.length === 0 && this.rowsFilterConfig.udlColumns.length === 0) {
          alert('uid 与 uld字段不能同时为空！')
          return
        }
        if (this.rowsFilterConfig.fetchMode === 1 || this.rowsFilterConfig.fetchMode === 2 || this.rowsFilterConfig.fetchMode === 3) {
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
      // alert('after')
      if (this.rowsFilterConfig.mode === '' || this.rowsFilterConfig.mode === undefined || (this.rowsFilterConfig.columns.length === 0 && this.rowsFilterConfig.udlColumns.length === 0)) {
        alert('缺少行过滤配置 禁止提交')
      } else {
        // alert('requestParam')
        const requestParam = {
          dbReplicationIds: this.dbReplicationIds,
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
        this.axios.post('/api/drc/v2/config/rowsFilter', requestParam).then(response => {
          if (response.data.status === 1) {
            window.alert('提交失败!')
          } else {
            window.alert('提交成功!')
            this.getConfig()
          }
        })
      }
    },
    getConfig () {
      console.log(this.dbReplicationId)
      this.axios.get('/api/drc/v2/config/rowsFilter?dbReplicationId=' + this.dbReplicationId)
        .then(response => {
          if (response.data.status === 1) {
            alert('查询行过滤配置失败!')
          } else {
            const res = response.data.data
            if (res == null) {
              // empty config
            } else {
              // rowsFilterConfig.context和onfigInTripUid.regionsChosen要放在前面
              // 不然会失效
              this.rowsFilterConfig.context = res.context
              if (res.mode === 1) {
                this.configInTripUid.regionsChosen = res.context.split(',')
              } else {
                this.configInTripUid = {
                  regionsChosen: []
                }
              }
              this.rowsFilterConfig.mode = res.mode
              this.rowsFilterConfig.columns = res.columns
              if (res.udlColumns.length !== 0) {
                this.rowsFilterConfig.udlColumns = res.udlColumns
              }
              this.rowsFilterConfig.drcStrategyId = res.drcStrategyId === 0 ? null : res.drcStrategyId
              this.rowsFilterConfig.routeStrategyId = res.routeStrategyId
              this.rowsFilterConfig.illegalArgument = res.illegalArgument
              this.rowsFilterConfig.fetchMode = res.fetchMode
              // if (res.mode === 1) {
              //   alert('ok')
              //   this.configInTripUid.regionsChosen = res.context.split(',')
              // } else {
              //   this.configInTripUid = {
              //     regionsChosen: []
              //   }
              // }
            }
          }
        })
    },
    getCommonColumns () {
      this.axios.get('/api/drc/v2/mysql/commonColumns?' +
        '&mhaName=' + this.srcMhaName +
        '&namespace=' + this.dbName +
        '&name=' + this.tableName)
        .then(response => {
          if (response.data.status === 1) {
            alert('查询公共列名失败，请手动添加！')
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
    fetchModeChange () {
      if (this.rowsFilterConfig.fetchMode === 1 || this.rowsFilterConfig.fetchMode === 3) {
        this.rowsFilterConfig.illegalArgument = true
      } else {
        this.rowsFilterConfig.illegalArgument = false
      }
    }
  },
  created () {
    this.getConfig()
    this.getCommonColumns()
  }

}

</script>

<style scoped>

</style>

<template>
  <div :style="{padding: '1px 1px',height: '100%'}">
    <Row :gutter=10 align="middle">
      <Col span="13">
        <Form :model="formItem" :disabled="formAction === 'delete'" :label-width="100"
              style="margin-right: 20px;margin-top: 10px">
          <Alert type="warning" show-icon v-if="alertInfo.show" closable>
            {{ alertInfo.title }}
            <template #desc>{{ alertInfo.message }}</template>
          </Alert>
          <Alert type="success" show-icon v-if="alertInfo.successShow" closable>
            {{ alertInfo.title }}
            <template #desc>{{ alertInfo.message }}</template>
          </Alert>
          <FormItem label="部门">
            <Select v-model="formItem.bu" filterable placeholder="选择部门">
              <Option v-for="item in formItem.constants.buForChosen" :value="item" :key="item">{{ item }}</Option>
            </Select>
          </FormItem>
          <FormItem prop="dbName" label="库名" :required=true>
            <Input type="textarea" v-model="formItem.dbName" :autosize="true" :readonly="true" :border="false"
                   placeholder="请输入库名（支持正则）"/>
          </FormItem>
          <FormItem label="同步表" :required=true>
            <Input @on-keydown.space.prevent v-model="formItem.tableName" placeholder="请输入正则表达式"
                   @on-blur="afterEnterTableName"></Input>
          </FormItem>

          <FormItem label="MQ主题">
            <Input v-model="formItem.topic" style="width:350px" placeholder="请输入自定义Topic"
                   @on-blur="refreshTopicBu"
            />
          </FormItem>
          <FormItem label="exclude类型">
            <Select v-model="formItem.excludeFilterTypes" filterable  multiple style="width: 200px" placeholder="选择过滤类型" :disabled="filterReadOnly">
              <Option v-for="item in excludeFilterTypesForChose" :value="item.value" :key="item.value" >{{ item.label }}</Option>
            </Select>
          </FormItem>
          <FormItem v-if="mqType === 'qmq'" label="延迟投递(s)">
            <Input v-model="formItem.delayTime"  style="width:200px" :disabled="filterReadOnly" placeholder="qmq延迟投递时间,单位:秒"/>
          </FormItem>
          <FormItem label="有序消息">
            <i-switch v-model="formItem.switch.order" size="large">
              <template #open>
                <span>On</span>
              </template>
              <template #close>
                <span>Off</span>
              </template>
            </i-switch>
          </FormItem>
          <Card v-if="formItem.switch.order" style="margin-left: 100px">
            <template #title>
              <Icon type="md-settings"/>
              有序消息字段
              <Button icon="ios-refresh" size="small" type="primary" :loading="commonColumnLoading"
                      @click="getCommonColumns" style="margin-left: 50px">获取公共字段
              </Button>
            </template>
            <FormItem label="字段">
              <Select v-model="formItem.orderKey" filterable allow-create @on-create="handleCreateColumn"
                      style="width: 200px" placeholder="不选表示按主键投递" clearable>
                <Option v-for="item in formItem.constants.columnsForChose" :value="item" :key="item">{{ item }}</Option>
              </Select>
            </FormItem>
          </Card>
          <FormItem label="消息包括字段">
            <template v-if="filterReadOnly">
              <div v-if="formItem.filterFields != null && formItem.filterFields.length > 0">
                {{ formItem.filterFields.join(', ') }}
              </div>
              <div v-else class="array-item">
                <tag color="blue">全部列</tag>
              </div>
            </template>
            <template v-else>
              <i-switch v-model="formItem.switch.fields" size="large">
                <template #open>
                  <span>On</span>
                </template>
                <template #close>
                  <span>Off</span>
                </template>
              </i-switch>
            </template>
          </FormItem>
          <template v-if="filterReadOnly">
            <FormItem label="仅在字段有更新时投递">
              <div>
                <span v-if="formItem.sendOnlyUpdated">是</span>
                <span v-else>否</span>
              </div>
            </FormItem>
            <FormItem label="投递方式">
              <div>
                <span v-if="formItem.excludeColumn">排除所选字段</span>
                <span v-else>包含所选字段</span>
              </div>
            </FormItem>
          </template>
          <Card v-if="formItem.switch.fields" style="margin-left: 100px">
            <template #title>
              <Icon type="md-settings"/>
              消息包括字段
              <Button icon="ios-refresh" size="small" type="primary" :loading="commonColumnLoading2"
                      @click="() => getCommonColumns('fields')" style="margin-left: 50px">获取公共字段
              </Button>
            </template>
            <FormItem label="字段">
              <Select v-model="formItem.filterFields" filterable multiple
                      style="width: 200px" placeholder="不选表示消息包括全部字段" clearable>
                <Option v-for="item in formItem.constants.columnsForChose" :value="item" :key="item">{{ item }}</Option>
              </Select>
            </FormItem>
            <Row>
              <FormItem label="仅在字段有更新时投递">
                <Checkbox v-model="formItem.sendOnlyUpdated"  :disabled="filterReadOnly"></Checkbox>
              </FormItem>
              <FormItem label="是否仅排除所选字段">
                <Checkbox v-model="formItem.excludeColumn"  :disabled="filterReadOnly"></Checkbox>
              </FormItem>
            </Row>
          </Card>
        </Form>
      </Col>
      <Col span="11">
        <db-tables-preview :src-region-name="srcRegion"
                           :dst-region-name="null"
                           :db-name="formItem.dbName"
                           :table-names="formItem.tableName"
                           :mode="formItem.buildMode"
                           :replication-type="Number(1)"
        />
      </Col>
    </Row>
    <Divider></Divider>
    <Button :type="buttonTypeMap.get(formAction)" @click="submitAll" style="margin-left: 50px"
            :loading="dataLoading || commonColumnLoading || commonColumnLoading2">{{
        buttonTextMap.get(formAction) + ': ' + mqType
      }}
    </Button>
  </div>
</template>

<script>

import DbTablesPreview from '@/components/v2/dbDrcBuild/dbTablesPreview.vue'

export default {
  components: { DbTablesPreview },
  FORM_ACTION_OPTION: {
    CREATE: 'create',
    EDIT: 'edit',
    DELETE: 'delete'
  },
  name: 'dbMqConfig',
  props: {
    configData: {},
    mqType: String,
    srcRegion: String,
    dstRegion: String,
    dalclusterName: String,
    dbNames: Array,
    formAction: String,
    filterReadOnly: Boolean
  },
  emits: ['finished'],
  data () {
    return {
      meta: {
        rowsFilterId: null,
        colsFilterId: null,
        dbReplicationIds: [],
        originLogicTableConfig: {}
      },
      formItem: {
        bu: null,
        buildMode: 2,
        dbName: null,
        tableName: null,
        topic: null,
        orderKey: null,
        excludeFilterTypes: [],
        filterFields: [],
        sendOnlyUpdated: false,
        excludeColumn: false,
        switch: {
          order: false
        },
        constants: {
          columnsForChose: [],
          buForChosen: []
        },
        delayTime: 0
      },
      checkTableDataList: [],
      table: {
        dbMhaTableLoading: false,
        dbMhaTableColumn: [
          {
            title: 'DB名',
            key: 'dbName'
          },
          {
            title: '源集群',
            key: 'srcMhaName',
            render: (h, params) => {
              const row = params.row
              const mha = row.srcMha
              if (mha != null) {
                return h('div', [
                  h('span', mha.name),
                  h('span', {
                    style:
                        {
                          float: 'right',
                          color: '#ababab'
                        }
                  }, mha.regionName + '(' + mha.dcName + ')')
                ])
              }
            }
          },
          {
            title: '目标集群',
            key: 'dstMhaName',
            render: (h, params) => {
              const row = params.row
              const mha = row.dstMha
              if (mha != null) {
                return h('div', [
                  h('span', mha.name),
                  h('span', {
                    style:
                        {
                          float: 'right',
                          color: '#ababab'
                        }
                  }, mha.regionName + '(' + mha.dcName + ')')
                ])
              }
            }
          }
        ],
        dbMhaTablePage: {
          total: 0,
          current: 1,
          size: 5,
          pageSizeOpts: [5, 10, 20, 100]
        },
        dbTableLoading: false,
        dbTableColumn: [
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
        dbTablePage: {
          total: 0,
          current: 1,
          size: 5,
          pageSizeOpts: [5, 10, 20, 100]
        }
      },
      alertInfo: {
        show: false,
        successShow: false,
        title: null,
        message: null
      },
      buttonTextMap: new Map([
        ['create', '新增 MQ投递'],
        ['edit', '更新 MQ投递'],
        ['delete', '删除 MQ投递']
      ]),
      buttonTypeMap: new Map([
        ['create', 'success'],
        ['edit', 'primary'],
        ['delete', 'error']
      ]),
      dataLoading: false,
      successSubmit: false,
      commonColumnLoading: false,
      commonColumnLoading2: false,
      excludeFilterTypesForChose: [
        {
          value: 'D',
          label: 'DELETE'
        },
        {
          value: 'U',
          label: 'UPDATE'
        },
        {
          value: 'I',
          label: 'INSERT'
        }
      ]
    }
  },
  methods: {
    getParams: function () {
      const param = {}
      param.buName = this.formItem.buName
      param.tag = this.formItem.tag
      param.mode = this.formItem.buildMode
      param.srcRegionName = this.srcRegion
      param.replicationType = 1

      if (this.gtidConfigurable) {
        param.gtidInit = this.formItem.gtidInit
      }

      param.tblsFilterDetail = {
        tableNames: this.formItem.tableName
      }
      param.dbName = this.formItem.dbName
      return param
    },
    getEditParams: function () {
      const param = {}
      param.dalclusterName = this.dalclusterName
      param.dbReplicationIds = this.meta.dbReplicationIds
      param.dbNames = this.dbNames
      param.srcRegionName = this.srcRegion

      param.logicTableConfig = {
        logicTable: this.formItem.tableName,
        dstLogicTable: this.formItem.topic
      }
      param.originLogicTableConfig = this.meta.originLogicTableConfig
      param.mqConfig = {
        bu: this.formItem.bu,
        mqType: this.mqType,
        serialization: 'json',
        persistent: false,
        order: this.formItem.switch.order,
        orderKey: this.formItem.switch.orderKey === '' ? null : this.formItem.orderKey,
        excludeFilterTypes: this.formItem.excludeFilterTypes,
        delayTime: this.formItem.delayTime,
        filterFields: this.formItem.filterFields,
        sendOnlyUpdated: this.formItem.sendOnlyUpdated,
        excludeColumn: this.formItem.excludeColumn
      }
      console.log(param)
      return param
    },
    async getCommonColumns (method) {
      const params = this.getParams()
      this.formItem.constants.columnsForChose = []
      if (method === 'fields') {
        this.commonColumnLoading2 = true
      } else {
        this.commonColumnLoading = true
      }
      await this.axios.get('/api/drc/v2/autoconfig/commonColumns', {
        params: this.flattenObj(params)
      }).then(response => {
        if (response.data.status === 1) {
          this.$Message.error('查询公共列名失败，请手动添加：' + response.data.message)
        } else {
          this.formItem.constants.columnsForChose = response.data.data
          this.$Message.info('查询公共列名数：' + response.data.data.length)
        }
      }).catch(message => {
        this.$Message.error('查询公共列名异常: ' + message)
      }).finally(() => {
        if (method === 'fields') {
          this.commonColumnLoading2 = false
        } else {
          this.commonColumnLoading = false
        }
      })
    },
    handleCreateColumn (val) {
      if (this.contains(this.formItem.constants.columnsForChose, val)) {
        alert('已有项禁止创建')
        return
      }
      if (val === '' || val === undefined || val === null) {
        alert('字段不能为空')
        return
      }
      this.formItem.constants.columnsForChose.push(val)
    },
    refreshTopicBu () {
      if (this.formItem.topic) {
        this.formItem.bu = this.formItem.topic.split('.')[0]
      }
    },
    async afterEnterTableName () {
      await this.getTableInfo()
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
    async refresh () {
      this.formItem.dbName = this.dbNames.join(',')
      if (this.configData) {
        console.log('edit panel ', JSON.stringify(this.configData))
        this.formItem.tableName = this.configData.config.logicTable
        this.formItem.topic = this.configData.config.dstLogicTable
        this.formItem.orderKey = this.configData.orderKey
        this.formItem.switch.order = this.configData.order
        this.formItem.excludeFilterTypes = this.configData.excludeFilterTypes
        this.formItem.delayTime = this.configData.delayTime
        this.formItem.filterFields = this.configData.filterFields
        this.formItem.sendOnlyUpdated = this.configData.sendOnlyUpdated
        this.formItem.excludeColumn = this.configData.excludeColumn
        this.refreshTopicBu()
        this.getCommonColumns()
        this.meta.dbReplicationIds = this.configData.dbReplicationIds
        this.meta.originLogicTableConfig = this.configData.config
        this.meta.originMyConfig = this.configData.config
      } else {
        console.log('insert new config')
      }
    },
    getBuListFromQmq () {
      this.axios.get('/api/drc/v2/messenger/qmq/bus')
        .then(response => {
          if (response.data.status === 1) {
            window.alert('从查询qmq 部门信息失败!')
          } else {
            this.formItem.constants.buForChosen = response.data.data
          }
        })
    },
    async submitAll () {
      const that = this
      that.alertInfo.show = false
      that.alertInfo.successShow = false
      that.successSubmit = false
      const params = this.getEditParams()
      // if (!this.checkParam(params)) {
      //   return
      // }
      that.dataLoading = true
      await that.axios.post('/api/drc/v2/autoconfig/dbMq/' + this.formAction, params)
        .then(response => {
          const data = response.data
          const success = data.status === 0
          if (success) {
            that.$Message.success('提交成功')
            that.successSubmit = true
            that.alertInfo.successShow = true
            that.alertInfo.message = null
            that.alertInfo.title = '提交成功'
            that.$emit('finished')
          } else {
            that.$Message.warning('提交失败: ' + data.message)
            that.alertInfo.show = true
            that.alertInfo.title = '提交失败'
            that.alertInfo.message = data.message
          }
        })
        .catch(message => {
          that.$Message.error('提交异常: ' + message)
        })
        .finally(() => {
          that.dataLoading = false
        })
    },
    rowsFilterCreateParamInit () {
      return {
        mode: 1,
        drcStrategyId: 2000000002,
        routeStrategyId: 0,
        udlColumns: [],
        columns: [],
        context: '',
        illegalArgument: false,
        fetchMode: 0
      }
    },
    columnsFilterCreateParamInit () {
      return {
        mode: null,
        columns: []
      }
    }
  },
  computed: {
    preCheckTablePage () {
      const data = this.checkTableDataList
      const start = this.table.dbTablePage.current * this.table.dbTablePage.size - this.table.dbTablePage.size
      const end = start + this.table.dbTablePage.size
      return [...data].slice(start, end)
    },
    useTripUdlOrUidMode () {
      return [this.formItem.constants.rowsFilter.filterMode.TRIP_UDL, this.formItem.constants.rowsFilter.filterMode.TRIP_UDL_UID].includes(this.formItem.rowsFilterCreateParam.mode)
    },
    hasUdlColumn () {
      return this.formItem.rowsFilterCreateParam.udlColumns.length !== 0
    },
    fillContextByRegion () {
      return this.formItem.rowsFilterCreateParam.fetchMode === this.formItem.constants.rowsFilter.fetchMode.RPC
    },
    hasUidColumn () {
      return this.formItem.rowsFilterCreateParam.columns.length !== 0
    },
    showUdlConfigDetail () {
      return this.formItem.rowsFilterCreateParam.mode === this.formItem.constants.rowsFilter.filterMode.TRIP_UDL_UID || this.hasUdlLegalConfig || !this.hasUidLegalConfig
    },
    showUidConfigDetail () {
      return this.formItem.rowsFilterCreateParam.mode === this.formItem.constants.rowsFilter.filterMode.TRIP_UDL_UID || this.hasUidLegalConfig || !this.hasUdlLegalConfig
    },
    hasUdlLegalConfig () {
      return this.formItem.rowsFilterCreateParam.udlColumns.length !== 0 && this.formItem.rowsFilterCreateParam.drcStrategyId != null && this.formItem.rowsFilterCreateParam.drcStrategyId > 0
    },
    hasUidLegalConfig () {
      return this.hasUidColumn
    }
  },
  created () {
    this.getBuListFromQmq()
    this.refresh()
  }
}
</script>

<style scoped>

</style>

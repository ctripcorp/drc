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
          <FormItem prop="dbName" label="库名" :required=true>
            <Input type="textarea" v-model="formItem.dbName" :autosize="true" :readonly="true" :border="false"
                   placeholder="请输入库名（支持正则）"/>
          </FormItem>
          <FormItem label="同步表" :required=true>
            <Input @on-keydown.space.prevent v-model="formItem.tableName" placeholder="请输入正则表达式"
                   @on-blur="afterEnterTableName"></Input>
          </FormItem>
          <FormItem label="行过滤">
            <i-switch v-model="formItem.switch.rowsFilter" size="large">
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
              <Button icon="ios-refresh" size="small" type="primary" :loading="commonColumnLoading"
                      @click="getCommonColumns" style="margin-left: 50px">获取公共字段
              </Button>
            </template>
            <FormItem label="模式">
              <Select v-model="formItem.rowsFilterCreateParam.mode" style="width: 200px" placeholder="选择行过滤模式">
                <Option v-for="item in formItem.constants.rowsFilter.modes" :value="item.mode" :key="item.mode">
                  {{ item.name }}
                </Option>
              </Select>
            </FormItem>
            <div v-if="useTripUdlOrUidMode">
              <FormItem label="规则内容" v-if="fillContextByRegion">
                <Select v-model="formItem.rowsFilter.configInTripUid.regionsChosen" multiple
                        style="width: 200px" placeholder="Region 选择">
                  <Option v-for="item in formItem.constants.rowsFilter.regionsForChose" :value="item" :key="item">
                    {{ item }}
                  </Option>
                </Select>
              </FormItem>
              <div v-if="showUdlConfigDetail">
                <Divider>UDL配置</Divider>
                <FormItem label="UDL字段">
                  <Select v-model="formItem.rowsFilterCreateParam.udlColumns" filterable allow-create multiple
                          :disabled="commonColumnLoading"
                          style="width: 200px" placeholder="不选默认则无UDL配置">
                    <Option v-for="item in formItem.constants.columnsForChose" :value="item" :key="item">{{
                        item
                      }}
                    </Option>
                  </Select>
                </FormItem>
                <FormItem label="DRC UDL策略id" v-if="hasUdlColumn">
                  <Select v-model="formItem.rowsFilterCreateParam.drcStrategyId" filterable allow-create
                          style="width: 200px"
                          placeholder="请选择ucs策略id">
                    <Option v-for="item in formItem.constants.rowsFilter.drcStrategyIdsForChose" :value="item"
                            :key="item">{{ item }}
                    </Option>
                  </Select>
                </FormItem>
              </div>
              <div v-if="showUidConfigDetail">
                <Divider>UID配置</Divider>
                <FormItem label="UID字段">
                  <Select v-model="formItem.rowsFilterCreateParam.columns" filterable allow-create multiple
                          :disabled="commonColumnLoading"
                          style="width: 200px" placeholder="不选默认则无UID配置">
                    <Option v-for="item in formItem.constants.columnsForChose" :value="item" :key="item">{{
                        item
                      }}
                    </Option>
                  </Select>
                </FormItem>
                <FormItem label="fetchMode" v-if="hasUidColumn">
                  <Select v-model="formItem.rowsFilterCreateParam.fetchMode" style="width: 200px" placeholder="选择"
                          @on-change="fetchModeChange()">
                    <Option v-for="item in formItem.constants.rowsFilter.fetchModeForChose" :value="item.v"
                            :key="item.k">
                      {{ item.k }}
                    </Option>
                  </Select>
                </FormItem>
                <FormItem v-if="hasUidColumn" label="空处理">
                  <Checkbox v-model="formItem.rowsFilterCreateParam.illegalArgument">【字段为空时】同步</Checkbox>
                </FormItem>
              </div>
            </div>
            <div v-else-if="useCustomSoaMode">
              <FormItem label="相关字段">
                <Select v-model="formItem.rowsFilterCreateParam.columns" filterable allow-create multiple style="width: 200px"
                        @on-create="handleCreateColumn" placeholder="选择相关字段">
                  <Option v-for="item in formItem.constants.columnsForChose" :value="item" :key="item" :lable="item"></Option>
                </Select>
              </FormItem>
              <FormItem label="ServiceCode">
                <Select v-model="formItem.rowsFilterCreateParam.drcStrategyId" filterable allow-create @on-create="handleCreateSoaServiceCode"
                        style="width: 200px"
                        placeholder="请选择soaServiceCode">
                  <Option v-for="item in formItem.constants.serviceCodeForChose" :value="item"
                          :key="item">{{ item }}
                  </Option>
                </Select>
              </FormItem>
              <FormItem label="服务名">
                <Select v-model="formItem.rowsFilterCreateParam.context" filterable allow-create @on-create="handleCreateSoaServiceName"
                        style="width: 200px"
                        placeholder="请选择服务名">
                  <Option v-for="item in formItem.constants.serviceNameForChose" :value="item"
                          :key="item">{{ item }}
                  </Option>
                </Select>
              </FormItem>
            </div>
            <div v-else>
              <FormItem label="规则内容">
                <Input type="textarea"
                       v-model="formItem.rowsFilterCreateParam.context" style="width: 250px"
                       placeholder="请输入行过滤内容"/>
              </FormItem>
              <FormItem label="相关字段">
                <Select v-model="formItem.rowsFilterCreateParam.columns" filterable allow-create multiple
                        style="width: 200px" placeholder="选择相关字段">
                  <Option v-for="item in formItem.constants.columnsForChose" :value="item" :key="item"
                          :lable="item"></Option>
                </Select>
              </FormItem>
            </div>
          </Card>

          <FormItem label="列过滤">
            <i-switch v-model="formItem.switch.colsFilter" size="large">
              <template #open>
                <span>On</span>
              </template>
              <template #close>
                <span>Off</span>
              </template>
            </i-switch>
          </FormItem>
          <Card v-if="formItem.switch.colsFilter" style="margin-left: 100px">
            <template #title>
              <Icon type="md-settings"/>
              列过滤配置
              <Button icon="ios-refresh" size="small" type="primary" :loading="commonColumnLoading"
                      @click="getCommonColumns" style="margin-left: 50px">获取公共字段
              </Button>
            </template>
            <FormItem label="模式">
              <Select v-model="formItem.columnsFilterCreateParam.mode" style="width: 200px"
                      placeholder="选择字段过滤模式">
                <Option v-for="item in formItem.constants.colsFilter.modes" :value="item.mode" :key="item.mode">
                  {{ item.name }}
                </Option>
              </Select>
            </FormItem>
            <FormItem label="字段">
              <Select v-model="formItem.columnsFilterCreateParam.columns" filterable allow-create
                      @on-create="handleCreateColumn" multiple style="width: 200px" placeholder="选择相关的字段"
                      :disabled="commonColumnLoading">
                <Option v-for="item in formItem.constants.columnsForChose" :value="item" :key="item">{{
                    item
                  }}
                </Option>
              </Select>
            </FormItem>
          </Card>
          <br/>
          <Row>
            <Col span="6" offset="8"></Col>
            <Col span="6" offset="4">
            </Col>
          </Row>
        </Form>
      </Col>
      <Col span="11">
        <db-tables-preview :src-region-name="srcRegion"
                           :dst-region-name="dstRegion"
                           :db-name="formItem.dbName"
                           :table-names="formItem.tableName"
                           :mode="formItem.buildMode"
                           :replication-type="Number(0)"
        />
      </Col>
    </Row>
    <Divider></Divider>
    <Button :type="buttonTypeMap.get(formAction)" @click="submitAll" style="margin-left: 50px"
            :loading="dataLoading || commonColumnLoading">{{
        buttonTextMap.get(formAction)
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
  name: 'dbReplicationConfig',
  props: {
    configData: {},
    srcRegion: String,
    dstRegion: String,
    dbNames: Array,
    formAction: String
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
        buildMode: 2,
        dbName: null,
        tableName: null,
        rowsFilterCreateParam: this.rowsFilterCreateParamInit(),
        columnsFilterCreateParam: this.columnsFilterCreateParamInit(),
        rowsFilter: {
          configInTripUid: {
            regionsChosen: []
          }
        },
        switch: {
          rowsFilter: false,
          colsFilter: false
        },
        constants: {
          rowsFilter: {
            filterMode: {
              JAVA_REGEX: 0,
              TRIP_UDL: 1,
              AVIATOR_REGEX: 3,
              TRIP_UDL_UID: 5,
              CUSTOM_SOA: 6
            },
            fetchMode: {
              RPC: 0,
              BlackList: 1,
              WhiteList: 2,
              BlackList_Global: 3
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
                name: 'trip_udl_uid',
                mode: 5
              },
              {
                name: 'custom_soa',
                mode: 6
              }
            ],
            regionsForChose: [
              'SGP',
              'SIN',
              'SH',
              'FRA'
            ],
            drcStrategyIdsForChose: [
              2000000002
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
          colsFilter: {
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
          columnsForChose: [],
          serviceCodeForChose: [
            32578
          ],
          serviceNameForChose: [
            'DataSyncService',
            'FilterRowService'
          ]
        }
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
        ['create', '新增表同步'],
        ['edit', '更新表同步'],
        ['delete', '删除表同步']
      ]),
      buttonTypeMap: new Map([
        ['create', 'success'],
        ['edit', 'primary'],
        ['delete', 'error']
      ]),
      dataLoading: false,
      successSubmit: false,
      commonColumnLoading: false
    }
  },
  methods: {
    getParams: function () {
      const param = {}
      param.buName = this.formItem.buName
      param.tag = this.formItem.tag
      param.mode = this.formItem.buildMode
      param.srcRegionName = this.srcRegion
      param.dstRegionName = this.dstRegion

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
      param.dbReplicationIds = this.meta.dbReplicationIds
      param.dbNames = this.dbNames
      param.srcRegionName = this.srcRegion
      param.dstRegionName = this.dstRegion

      param.logicTableConfig = {
        logicTable: this.formItem.tableName,
        rowsFilterId: this.formItem.switch.rowsFilter ? this.meta.rowsFilterId : null,
        colsFilterId: this.formItem.switch.colsFilter ? this.meta.colsFilterId : null
      }
      param.originLogicTableConfig = this.meta.originLogicTableConfig
      if (this.formItem.switch.rowsFilter) {
        if (this.useTripUdlOrUidMode) {
          if (this.fillContextByRegion) {
            this.formItem.rowsFilterCreateParam.context = this.formItem.rowsFilter.configInTripUid.regionsChosen.join(',')
          } else {
            this.formItem.rowsFilterCreateParam.context = '//filter by config'
          }
        }
        param.openRowsFilterConfig = true
        param.rowsFilterCreateParam = this.formItem.rowsFilterCreateParam
      }
      if (this.formItem.switch.colsFilter) {
        param.openColsFilterConfig = true
        param.columnsFilterCreateParam = this.formItem.columnsFilterCreateParam
      }
      console.log(param)
      return param
    },
    async getRowsFilterConfig () {
      await this.axios.get('/api/drc/v2/autoconfig/getRowsFilterView?rowsFilterId=' + this.meta.rowsFilterId)
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
              this.formItem.rowsFilterCreateParam.context = res.context
              this.formItem.rowsFilterCreateParam.mode = res.mode
              if (this.useTripUdlOrUidMode) {
                this.formItem.rowsFilter.configInTripUid.regionsChosen = res.context.split(',')
              } else {
                this.formItem.rowsFilter.configInTripUid = {
                  regionsChosen: []
                }
              }
              this.formItem.rowsFilterCreateParam.columns = res.columns === null ? [] : res.columns
              if (res.udlColumns !== null) {
                this.formItem.rowsFilterCreateParam.udlColumns = res.udlColumns
              }
              this.formItem.rowsFilterCreateParam.drcStrategyId = res.drcStrategyId === 0 ? null : res.drcStrategyId
              this.formItem.rowsFilterCreateParam.routeStrategyId = res.routeStrategyId
              this.formItem.rowsFilterCreateParam.illegalArgument = res.illegalArgument
              this.formItem.rowsFilterCreateParam.fetchMode = res.fetchMode
            }
          }
        })
    },
    async getColsFilterConfig () {
      await this.axios.get('/api/drc/v2/autoconfig/getColsFilterView?colsFilterId=' + this.meta.colsFilterId)
        .then(response => {
          if (response.data.status === 1) {
            this.$Message.error('查询字段过滤配置失败!')
          } else {
            const res = response.data.data
            if (res == null) {
              // empty config
            } else {
              this.formItem.switch.colsFilter = true
              this.formItem.columnsFilterCreateParam.mode = res.mode
              this.formItem.columnsFilterCreateParam.columns = res.columns
            }
          }
        })
    },
    async getCommonColumns () {
      const params = this.getParams()
      this.formItem.constants.columnsForChose = []
      this.commonColumnLoading = true
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
        this.commonColumnLoading = false
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
    handleCreateSoaServiceCode (val) {
      if (this.contains(this.formItem.constants.serviceCodeForChose, val)) {
        alert('已有项禁止创建')
        return
      }
      if (val === '' || val === undefined || val === null || val === 0) {
        alert('serviceCode不能为空')
        return
      }
      this.formItem.constants.serviceCodeForChose.push(val)
    },
    handleCreateSoaServiceName (val) {
      if (this.contains(this.formItem.constants.serviceNameForChose, val)) {
        alert('已有项禁止创建')
        return
      }
      if (val === '' || val === undefined || val === null) {
        alert('serviceName不能为空')
        return
      }
      this.formItem.constants.serviceNameForChose.push(val)
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
        console.log('edit panel')
        this.formItem.tableName = this.configData.config.logicTable
        this.meta.rowsFilterId = this.configData.config.rowsFilterId
        this.meta.colsFilterId = this.configData.config.colsFilterId
        this.getCommonColumns()
        if (this.meta.rowsFilterId) {
          await this.getRowsFilterConfig(this.meta.rowsFilterId)
        } else {
          this.formItem.switch.rowsFilter = false
          this.formItem.rowsFilterCreateParam = this.rowsFilterCreateParamInit()
        }
        if (this.meta.colsFilterId) {
          await this.getColsFilterConfig(this.meta.colsFilterId)
        } else {
          this.formItem.switch.colsFilter = false
          this.formItem.columnsFilterCreateParam = this.columnsFilterCreateParamInit()
        }
        this.meta.dbReplicationIds = this.configData.dbReplicationIds
        this.meta.originLogicTableConfig = this.configData.config
      } else {
        console.log('insert new config')
      }
    },
    async submitAll () {
      const that = this
      that.dataLoading = true
      that.alertInfo.show = false
      that.alertInfo.successShow = false
      that.successSubmit = false
      const params = this.getEditParams()
      // if (!this.checkParam(params)) {
      //   return
      // }
      await that.axios.post('/api/drc/v2/autoconfig/dbReplication/' + this.formAction, params)
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
    useCustomSoaMode () {
      return [this.formItem.constants.rowsFilter.filterMode.CUSTOM_SOA].includes(this.formItem.rowsFilterCreateParam.mode)
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
    this.refresh()
  }
}
</script>

<style scoped>

</style>

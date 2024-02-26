<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/v2/dbDrcBuild">建立 DRC 同步</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content"
             :style="{padding: '10px', background: '#ffffff', margin: '50px 0 111px 185px', zIndex: '1'}">
      <Row :gutter=10 align="middle">
        <Col span="13">
          <Form :model="formItem" :label-width="100" style="margin-right: 20px;margin-top: 10px">
            <FormItem label="配置模式" :required=true>
              <Select clearable v-model="formItem.buildMode" placeholder="模式">
                <Option :value=0>单库</Option>
                <Option :value=1>分库</Option>
                <Option :value=2>自定义多库</Option>
              </Select>
            </FormItem>
            <div v-if="formItem.buildMode === 0">
              <FormItem label="数据库" :required=true>
                <Select
                  v-model="formItem.dbName"
                  filterable
                  placeholder="请搜索数据库信息"
                  @on-change="selectDb"
                  :remote-method="getExistDb"
                  :loading="dataLoading">
                  <Option v-for="(option, index) in meta.dbOptions" :value="option.dbName" :key="index">
                    {{ option.dbName }}
                  </Option>
                </Select>
              </FormItem>
            </div>
            <div v-if="formItem.buildMode === 1">
              <FormItem label="dalcluster" :required=true>
                <Input @on-keydown.space.prevent v-model="formItem.dalClusterModeOption.dalClusterName"
                       placeholder="请输入 dalCluster 名称" @on-blur="afterEnterDalCluster"/>
              </FormItem>
            </div>
            <div v-if="formItem.buildMode === 2">
              <FormItem label="数据库列表" :required=true>
                <Input @on-keydown.space.prevent v-model="formItem.dbName" placeholder="请输入数据库列表，英文逗号分隔"
                       @on-blur="afterEnterDalCluster"/>
              </FormItem>
            </div>
            <FormItem label="同步方向" :required=true>
              <Row>
                <Col span="11">
                  <Card :bordered="true">
                    <template #title>
                      <Icon type="ios-pin"/>
                      源 region
                    </template>
                    <FormItem>
                      <Select filterable clearable v-model="formItem.srcRegionName" placeholder="地域"
                              @on-change="afterSwitchRegion">
                        <Option v-for="region in meta.regionOptions" :value="region" :key="region" :label="region">
                          {{ region }}
                        </Option>
                      </Select>
                    </FormItem>
                  </Card>
                </Col>
                <Col span="2" style="text-align: center">
                  <Button size="small" shape="circle" type="default" :loading="dataLoading" @click="getRegionOptions">
                    ->
                  </Button>
                </Col>
                <Col span="11">
                  <Card :bordered="true">
                    <template #title>
                      <Icon type="ios-pin"/>
                      目标 region
                    </template>
                    <FormItem>
                      <Select filterable clearable v-model="formItem.dstRegionName" placeholder="地域"
                              @on-change="afterSwitchRegion">
                        <Option v-for="region in meta.regionOptions" :value="region" :key="region" :label="region">
                          {{ region }}
                        </Option>
                      </Select>
                    </FormItem>
                  </Card>
                </Col>
              </Row>
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
                <Select v-model="formItem.rowsFilterDetail.mode" style="width: 200px" placeholder="选择行过滤模式">
                  <Option v-for="item in formItem.constants.rowsFilter.modes" :value="item.mode" :key="item.mode">
                    {{ item.name }}
                  </Option>
                </Select>
              </FormItem>
              <div v-if="useTripUdlOrUidMode">
                <FormItem label="规则内容" v-if="fillContextByRegion">
                  <Select v-model="formItem.constants.rowsFilter.configInTripUid.regionsChosen" multiple
                          style="width: 200px" placeholder="Region 选择">
                    <Option v-for="item in formItem.constants.rowsFilter.regionsForChose" :value="item" :key="item">
                      {{ item }}
                    </Option>
                  </Select>
                </FormItem>
                <div v-if="showUdlConfigDetail">
                  <Divider>UDL配置</Divider>
                  <FormItem label="UDL字段" >
                    <Select v-model="formItem.rowsFilterDetail.udlColumns" filterable allow-create multiple
                            style="width: 200px" placeholder="不选默认则无UDL配置">
                      <Option v-for="item in formItem.constants.columnsForChose" :value="item" :key="item">{{
                          item
                        }}
                      </Option>
                    </Select>
                  </FormItem>
                  <FormItem label="DRC UDL策略id" v-if="hasUdlColumn">
                    <Select v-model="formItem.rowsFilterDetail.drcStrategyId" filterable allow-create style="width: 200px"
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
                    <Select v-model="formItem.rowsFilterDetail.columns" filterable allow-create multiple
                            style="width: 200px" placeholder="不选默认则无UID配置">
                      <Option v-for="item in formItem.constants.columnsForChose" :value="item" :key="item">{{
                          item
                        }}
                      </Option>
                    </Select>
                  </FormItem>
                  <FormItem label="fetchMode" v-if="hasUidColumn">
                    <Select v-model="formItem.rowsFilterDetail.fetchMode" style="width: 200px" placeholder="选择"
                            @on-change="fetchModeChange()">
                      <Option v-for="item in formItem.constants.rowsFilter.fetchModeForChose" :value="item.v" :key="item.k">
                        {{ item.k }}
                      </Option>
                    </Select>
                  </FormItem>
                  <FormItem v-if="hasUidColumn" label="空处理">
                    <Checkbox v-model="formItem.rowsFilterDetail.illegalArgument">【字段为空时】同步</Checkbox>
                  </FormItem>
                </div>
              </div>
              <div v-else>
                <FormItem label="规则内容">
                  <Input type="textarea"
                         v-model="formItem.rowsFilterDetail.context" style="width: 250px" placeholder="请输入行过滤内容"/>
                </FormItem>
                <FormItem label="相关字段">
                  <Select v-model="formItem.rowsFilterDetail.columns" filterable allow-create multiple
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
                <Select v-model="formItem.colsFilterDetail.mode" style="width: 200px" placeholder="选择字段过滤模式">
                  <Option v-for="item in formItem.constants.colsFilter.modes" :value="item.mode" :key="item.mode">
                    {{ item.name }}
                  </Option>
                </Select>
              </FormItem>
              <FormItem label="字段">
                <Select v-model="formItem.colsFilterDetail.columns" filterable allow-create
                        @on-create="handleCreateColumn" multiple style="width: 200px" placeholder="选择相关的字段">
                  <Option v-for="item in formItem.constants.columnsForChose" :value="item" :key="item">{{
                      item
                    }}
                  </Option>
                </Select>
              </FormItem>
            </Card>
            <FormItem label="业务部门" :required=true>
              <Select filterable prefix="ios-home"
                      @on-change="autoSetTag"
                      clearable v-model="formItem.buName" placeholder="部门">
                <Option v-for="item in meta.bus" :value="item.buName" :key="item.buName">{{ item.buName }}</Option>
              </Select>
            </FormItem>
            <FormItem label="Tag" prop="tag" :required=true>
              <Select v-model="formItem.tag" filterable allow-create style="width: 200px" placeholder="选择tag"
                      @on-create="handleCreateTag">
                <Option v-for="item in meta.tags" :value="item" :key="item">{{ item }}</Option>
              </Select>
            </FormItem>
            <FormItem label="初始同步位点" prop="gtidInit"
                      v-if="gtidConfigurable">
              <Input v-model="formItem.gtidInit" style="width: 80%;margin-right: 10px" :border="false"
                     placeholder="请输入binlog拉取位点"/>
              <Button type="success" @click="querySrcMhaGtidCheckRes">位点校验</Button>
            </FormItem>
            <FormItem>
              <Button type="primary" :loading="dataLoading"
                      :disabled="previewDataList.length === 0 || !formItem.tableName" @click="beforeSubmit">提交
              </Button>
            </FormItem>
          </Form>
        </Col>
        <Col span="11">
          <Divider>预览：同步集群</Divider>
          <Button type="primary" :loading="table.dbMhaTableLoading" @click="getDalInfo" style="margin-bottom: 5px">
            检查同步集群
          </Button>
          <Table size="small" :loading="table.dbMhaTableLoading" stripe :columns="table.dbMhaTableColumn"
                 :data="preCheckMhaReplicationPage" border></Table>
          <div>
            <Page
              :transfer="true"
              :total="previewDataList.length"
              :current.sync="table.dbMhaTablePage.current"
              :page-size-opts="table.dbMhaTablePage.pageSizeOpts"
              :page-size="table.dbMhaTablePage.size"
              show-total
              show-sizer
              show-elevator
              @on-page-size-change="(val) => {table.dbMhaTablePage.size = val}"></Page>
          </div>
          <Divider style="margin-top: 50px">预览：同步表</Divider>
          <Button type="primary" :loading="table.dbTableLoading" @click="getTableInfo" style="margin-bottom: 5px">
            检查同步表
          </Button>
          <Table size="small" :loading="table.dbTableLoading" stripe :columns="table.dbTableColumn"
                 :data="preCheckTablePage" border></Table>
          <div>
            <Page
              :transfer="true"
              :total="checkTableDataList.length"
              :current.sync="table.dbTablePage.current"
              :page-size-opts="table.dbTablePage.pageSizeOpts"
              :page-size="table.dbTablePage.size"
              show-total
              show-sizer
              show-elevator
              @on-page-size-change="(val) => {table.dbTablePage = val}"></Page>
          </div>
        </Col>
      </Row>
      <Modal
        v-model="gtidCheck.modal"
        title="gitd位点校验结果"
        width="900px">
        <Form style="width: 80%">
          <FormItem label="校验结果">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="gtidCheck.resVo.legal" readonly/>
          </FormItem>
          <FormItem label="当前Mha">
            <Input :autosize="{minRows: 1,maxRows: 30}" v-model="gtidCheck.resVo.mha" readonly/>
          </FormItem>
          <FormItem label="配置位点">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="gtidCheck.resVo.configGtid" readonly/>
          </FormItem>
          <FormItem label="purgedGtid">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="gtidCheck.resVo.purgedGtid" readonly/>
          </FormItem>
        </Form>
      </Modal>
      <Drawer
        title="请确认同步信息"
        v-model="drawer.show"
        width="80"
        :mask-closable="false"
      >
        <Alert type="warning" show-icon v-if="alertInfo.show" closable>
          {{ alertInfo.title }}
          <template #desc>{{ alertInfo.message }}</template>
        </Alert>
        <Alert type="success" show-icon v-if="alertInfo.successShow" closable>
          {{ alertInfo.title }}
          <template #desc>{{ alertInfo.message }}</template>
        </Alert>
        <Table size="small" :loading="dataLoading" stripe border :columns="drawer.tableColumns"
               :data="finalBuildParam"></Table>
        <div class="drawer-footer">
          <Divider></Divider>
          <Button style="margin-right: 8px" @click="drawer.show = false">关闭</Button>
          <Button v-show="!successSubmit" type="primary" @click="submitAll" :loading="dataLoading">提交</Button>
        </div>
      </Drawer>
    </Content>
  </base-component>
</template>
<script>

export default {
  data () {
    return {
      formItem: {
        buildMode: 0,
        srcRegionName: null,
        dstRegionName: null,
        dbName: null,
        dalClusterModeOption: {
          dalClusterName: null
        },
        buName: null,
        tag: 'COMMON',
        tableName: null,
        rowsFilterDetail: {
          mode: 1,
          drcStrategyId: 0,
          routeStrategyId: 0,
          udlColumns: [],
          columns: [],
          context: '',
          illegalArgument: false,
          fetchMode: 0
        },
        colsFilterDetail: {
          mode: null,
          columns: []
        },
        gtidInit: '',
        textarea: '',
        switch: {
          rowsFilter: false,
          colsFilter: false
        },
        constants: {
          drcStatus: {
            NOT_EXIST: -1,
            STOP: 0,
            STARTED: 1,
            PARTIAL_STARTED: 2
          },
          drcApplyMode: {
            MHA_APPLY: 0,
            DB_APPLY: 1
          },
          rowsFilter: {
            configInTripUid: {
              regionsChosen: []
            },
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
              },
              {
                name: 'trip_udl_uid',
                mode: 5
              }
            ],
            regionsForChose: [
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
          columnsForChose: []
        }
      },
      gtidCheck: {
        modal: false,
        resVo: {
          mha: '',
          legal: '',
          configGtid: '',
          purgedGtid: ''
        }
      },
      meta: {
        bus: [],
        regions: [],
        regionOptions: [],
        existReplicationRegionOptions: [],
        dbOptions: [],
        tags: this.constant.tagList,
        selectedDb: {}
      },
      drawer: {
        show: false,
        tableColumns: [
          {
            title: '状态（点击查看详情）',
            key: 'status',
            width: 200,
            align: 'center',
            render: (h, params) => {
              const row = params.row
              const applyMode = row.viewOnlyInfo.drcApplyMode
              const status = row.viewOnlyInfo.drcStatus
              const disabled = status === -1
              let type, text, tagText, tagColor
              if (applyMode === this.formItem.constants.drcApplyMode.DB_APPLY) {
                tagText = 'DB 粒度同步'
                tagColor = 'success'
                if (status === this.formItem.constants.drcStatus.STARTED) {
                  text = '已接入'
                  type = 'primary'
                } else if (status === this.formItem.constants.drcStatus.PARTIAL_STARTED) {
                  text = '部分接入'
                  type = 'primary'
                  tagColor = 'error'
                } else {
                  text = '未接入'
                  type = 'default'
                }
              } else {
                if (status === this.formItem.constants.drcStatus.STARTED) {
                  text = '已接入'
                  type = 'primary'
                } else {
                  text = '未接入'
                  type = 'default'
                }
              }

              return h('div', [
                applyMode === this.formItem.constants.drcApplyMode.DB_APPLY && h('Tag', {
                  props: {
                    color: tagColor
                  }
                }, tagText),
                h('Button', {
                  on: {
                    click: async () => {
                      const routeData = this.$router.resolve({
                        path: '/drcV2',
                        query: {
                          step: 3,
                          srcMhaName: row.srcMhaName,
                          dstMhaName: row.dstMhaName
                        }
                      })
                      window.open(routeData.href, '_blank')
                    }
                  },
                  props: {
                    type: type,
                    disabled: disabled,
                    icon: 'ios-open-outline'
                  }
                }, text)
              ])
            }
          },
          {
            title: '源集群',
            key: 'srcMhaName',
            render: (h, params) => {
              const row = params.row
              const mhaName = row.srcMhaName
              const dcName = row.srcDcName
              return h('div', [
                h('span', mhaName),
                h('span', {
                  style:
                    {
                      float: 'right',
                      color: '#ababab'
                    }
                }, dcName)
              ])
            }
          },
          {
            title: '目标集群',
            key: 'srcMhaName',
            render: (h, params) => {
              const row = params.row
              const mhaName = row.dstMhaName
              const dcName = row.dstDcName
              return h('div', [
                h('span', mhaName),
                h('span', {
                  style:
                    {
                      float: 'right',
                      color: '#ababab'
                    }
                }, dcName)
              ])
            }
          },
          {
            title: 'BU',
            key: 'buName'
          },
          {
            title: '表过滤',
            key: 'tableFilter'
          },
          {
            title: 'DB名',
            key: 'dbName',
            render: (h, params) => {
              const row = params.row
              const dbName = row.dbName.join('\r\n')
              return h('div', {
                style: {
                  'white-space': 'pre-wrap'
                }
              }, dbName)
            }
          }
        ]
      },
      alertInfo: {
        show: false,
        successShow: false,
        title: null,
        message: null
      },
      dbClusterInfoList: [],
      finalBuildParam: [],
      previewDataList: [],
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
      dataLoading: false,
      successSubmit: false,
      commonColumnLoading: false
    }
  },
  methods: {
    getBus () {
      this.axios.get('/api/drc/v2/meta/bus/all')
        .then(response => {
          this.meta.bus = response.data.data
        })
    },
    getRegions () {
      this.axios.get('/api/drc/v2/meta/regions/all')
        .then(response => {
          this.meta.regions = response.data.data.map((e) => {
            return e.regionName
          })
          this.meta.regionOptions = this.meta.regions
        })
    },
    getParams: function () {
      const param = {}
      param.buName = this.formItem.buName
      param.tag = this.formItem.tag
      param.mode = this.formItem.buildMode
      param.srcRegionName = this.formItem.srcRegionName
      param.dstRegionName = this.formItem.dstRegionName

      if (this.gtidConfigurable) {
        param.gtidInit = this.formItem.gtidInit
      }

      param.tblsFilterDetail = {
        tableNames: this.formItem.tableName
      }
      if (this.formItem.buildMode === 0) {
        param.dbName = this.formItem.dbName
      } else if (this.formItem.buildMode === 1) {
        param.dalClusterName = this.formItem.dalClusterModeOption.dalClusterName
      } else if (this.formItem.buildMode === 2) {
        param.dbName = this.formItem.dbName
      }
      if (this.formItem.switch.rowsFilter) {
        if (this.useTripUdlOrUidMode) {
          if (this.fillContextByRegion) {
            this.formItem.rowsFilterDetail.context = this.formItem.constants.rowsFilter.configInTripUid.regionsChosen.join(',')
          } else {
            this.formItem.rowsFilterDetail.context = '//filter by config'
          }
        }
        param.openRowsFilterConfig = true
        param.rowsFilterDetail = this.formItem.rowsFilterDetail
      }
      if (this.formItem.switch.colsFilter) {
        param.openColsFilterConfig = true
        param.colsFilterDetail = this.formItem.colsFilterDetail
      }
      return param
    },
    async submitAll () {
      const that = this
      that.dataLoading = true
      that.alertInfo.show = false
      that.alertInfo.successShow = false
      that.successSubmit = false
      const params = this.getParams()
      if (!this.checkParam(params)) {
        return
      }
      console.log(params)
      await that.axios.post('/api/drc/v2/autoconfig/submit', params)
        .then(response => {
          const data = response.data
          const success = data.status !== 1
          if (success) {
            that.$Message.success('提交成功')
            this.preCheckBuildParam()
            that.successSubmit = true
            that.alertInfo.successShow = true
            that.alertInfo.message = null
            that.alertInfo.title = '提交成功'
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
    async selectDb () {
      const dbName = this.formItem.dbName
      console.log(dbName)
      this.meta.selectedDb = this.meta.dbOptions.filter((item) => item.dbName === dbName)[0]
      console.log('selected', this.meta.selectedDb)
      this.formItem.buName = this.meta.selectedDb.buCode
      console.log('selected', this.formItem.tableName)
      this.autoSetTag()
      await this.getRegionOptions()
      if (this.formItem.srcRegionName && this.formItem.dstRegionName) {
        await this.getDalInfo()
      }
    },
    autoSetTag () {
      if (this.formItem.buName === 'FLT') {
        this.formItem.tag = 'FLT'
      } else if (this.formItem.buName === 'HTL') {
        this.formItem.tag = 'HTL'
      } else {
        this.formItem.tag = 'COMMON'
      }
    },
    async getDalInfo () {
      const params = this.getParams()
      const that = this
      that.previewDataList = []
      that.table.dbMhaTablePage.current = 1
      if (params.mode === 0) {
        if (!params.dbName) {
          this.$Message.warning('请先填写数据库')
          return
        }
      } else if (params.mode === 1) {
        if (!params.dalClusterName) {
          this.$Message.warning('请先填写 dalcluster')
          return
        }
      }
      if (!params.srcRegionName || !params.dstRegionName) {
        this.$Message.warning('请先填写同步方向')
        return
      }
      that.table.dbMhaTableLoading = true
      await that.axios.get('/api/drc/v2/autoconfig/preCheck', {
        params: {
          mode: params.mode,
          dbName: params.dbName,
          dalClusterName: params.dalClusterName,
          srcRegionName: params.srcRegionName,
          dstRegionName: params.dstRegionName
        }
      })
        .then(response => {
          const data = response.data.data
          const success = data && response.data.status === 0
          if (success) {
            that.previewDataList = data
            console.log('total: ' + that.previewDataList.length)
            that.$Message.success('同步集群检测成功, 共找到 ' + data.length + ' 个DB')
          } else {
            that.$Message.warning('同步集群检测失败：' + response.data.message)
          }
        })
        .catch(message => {
          that.$Message.error('查询dalcluster异常: ' + message)
        })
        .finally(() => {
          that.table.dbMhaTableLoading = false
        })
    },
    async getTableInfo () {
      const params = this.getParams()
      const that = this
      that.checkTableDataList = []
      that.table.dbTablePage.current = 1
      if (params.mode === 0) {
        if (!params.dbName) {
          this.$Message.warning('请先填写数据库')
          return
        }
      } else if (params.mode === 1) {
        if (!params.dalClusterName) {
          this.$Message.warning('请先填写 dalcluster')
          return
        }
      }
      if (!params.srcRegionName || !params.dstRegionName) {
        this.$Message.warning('请先填写同步方向')
        return
      }
      if (!params.tblsFilterDetail.tableNames) {
        console.log(params)
        this.$Message.warning('请先填写表名')
        return
      }
      that.table.dbTableLoading = true
      await that.axios.get('/api/drc/v2/autoconfig/preCheckTable', {
        params: this.flattenObj({
          mode: params.mode,
          dbName: params.dbName,
          dalClusterName: params.dalClusterName,
          srcRegionName: params.srcRegionName,
          dstRegionName: params.dstRegionName,
          tblsFilterDetail: params.tblsFilterDetail
        })
      })
        .then(response => {
          const data = response.data.data
          if (data) {
            that.checkTableDataList = data
            console.log('total: ' + that.checkTableDataList.length)
            that.$Message.success('同步表检测成功, 共找到 ' + data.length + ' 个表')
          } else {
            that.$Message.warning('同步表检测失败：' + response.data.message)
          }
        })
        .catch(message => {
          that.$Message.error('查询dalcluster异常: ' + message)
        })
        .finally(() => {
          that.table.dbTableLoading = false
        })
    },
    async preCheckBuildParam () {
      const params = this.getParams()
      if (!this.checkParam(params)) {
        return
      }
      const that = this
      that.dataLoading = true
      that.finalBuildParam = []
      that.alertInfo.show = false
      that.alertInfo.successShow = false
      that.successSubmit = false
      console.log('precheckParam', params)
      await that.axios.post('/api/drc/v2/autoconfig/preCheckBuildParam', params)
        .then(response => {
          const data = response.data.data
          if (data) {
            that.drawer.show = true
            that.finalBuildParam = data
            that.$Message.success('预检测成功, 共 ' + data.length + ' 个集群')
          } else {
            that.drawer.show = false
            that.$Message.warning('预检测失败: ' + response.data.message)
          }
        })
        .catch(message => {
          that.$Message.error('查询dalcluster异常: ' + message)
        })
        .finally(() => {
          that.dataLoading = false
        })
    },
    async afterSwitchRegion () {
      const params = this.getParams()
      if (!params.srcRegionName || !params.dstRegionName) {
        return
      }
      this.getDalInfo()
    },
    async afterEnterTableName () {
      await this.getTableInfo()
    },
    async afterEnterDalCluster () {
      await this.getRegionOptions()
    },
    async getRegionOptions () {
      const that = this
      that.dataLoading = true
      that.previewDataList = []
      that.meta.regionOptions = []
      const params = this.getParams()
      await that.axios.get('/api/drc/v2/autoconfig/regionOptions', {
        params: {
          mode: params.mode,
          dbName: params.dbName,
          dalClusterName: params.dalClusterName
        }
      })
        .then(response => {
          const data = response.data.data
          const success = data && response.data.status === 0
          if (success) {
            that.meta.regionOptions = data
            if (data.indexOf(that.formItem.srcRegionName) === -1) {
              that.formItem.srcRegionName = null
            }
            if (data.indexOf(that.formItem.dstRegionName) === -1) {
              that.formItem.dstRegionName = null
            }
            that.$Message.success('查询到 ' + that.meta.regionOptions.length + ' 个可选区域')
            console.log('total selectable region: ' + that.meta.regionOptions.length)
          } else {
            that.formItem.srcRegionName = null
            that.formItem.dstRegionName = null
            that.$Message.warning('查询可选地域失败')
          }
        })
        .catch(message => {
          that.$Message.error('查询可选地域失败: ' + message)
        })
        .finally(() => {
          that.dataLoading = false
        })
    },
    async getExistDb (dbName) {
      if (dbName === null || dbName.length === null || dbName.length <= 4) {
        return []
      }
      const that = this
      that.dataLoading = true
      that.dbClusterInfoList = []
      await that.axios.get('/api/drc/v2/autoconfig/getExistDb', {
        params: {
          dbName: dbName
        }
      })
        .then(response => {
          const data = response.data.data
          if (data) {
            that.meta.dbOptions = data
          } else {
            that.$Message.warning('查询DB失败')
          }
        })
        .catch(message => {
          that.$Message.error('查询DB异常: ' + message)
        })
        .finally(() => {
          that.dataLoading = false
        })
    },
    fetchModeChange () {
      this.formItem.rowsFilterDetail.illegalArgument = this.formItem.rowsFilterDetail.fetchMode === 1 || this.formItem.rowsFilterDetail.fetchMode === 3
    },
    async beforeSubmit () {
      await this.preCheckBuildParam()
    },
    getCommonColumns () {
      const params = this.getParams()
      if (params.rowsFilterDetail) {
        params.rowsFilterDetail.columns = []
        params.rowsFilterDetail.udlColumns = []
      }
      if (params.colsFilterDetail) {
        params.colsFilterDetail.columns = []
      }
      console.log(params)
      this.formItem.constants.columnsForChose = []
      this.commonColumnLoading = true
      this.axios.get('/api/drc/v2/autoconfig/commonColumns', {
        params: this.flattenObj(params)
      }).then(response => {
        if (response.data.status === 1) {
          this.$Message.error('查询公共列名失败，请手动添加：' + response.data.message)
        } else {
          console.log(response.data.data)
          this.formItem.constants.columnsForChose = response.data.data
          this.$Message.info('查询公共列名数：' + response.data.data.length)
        }
      }).catch(message => {
        this.$Message.error('查询公共列名异常: ' + message)
      }).finally(() => {
        this.commonColumnLoading = false
      })
    },
    querySrcMhaGtidCheckRes () {
      const gtidInit = this.formItem.gtidInit
      const mhaName = this.previewDataList[0].srcMha.name
      if (gtidInit == null || gtidInit === '') {
        alert('位点为空！')
        return
      }
      console.log(mhaName)
      this.axios.get('/api/drc/v2/mha/gtid/checkResult?mha=' + mhaName +
        '&configGtid=' + gtidInit)
        .then(response => {
          if (response.data.status === 0) {
            this.gtidCheck.resVo = {
              mha: mhaName,
              legal: response.data.data.legal === true ? '合理位点' : 'binlog已经被purge',
              configGtid: gtidInit,
              purgedGtid: response.data.data.purgedGtid
            }
            this.gtidCheck.modal = true
          } else {
            alert('位点校验失败!')
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
    handleCreateTag (val) {
      this.meta.tags.push(val)
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
    checkParam (params) {
      if (params.openRowsFilterConfig === true) {
        const rowsFilterConfig = params.rowsFilterDetail
        console.log('rowsFilterConfig', rowsFilterConfig)
        // check rows filter
        if ([this.formItem.constants.rowsFilter.filterMode.TRIP_UDL, this.formItem.constants.rowsFilter.filterMode.TRIP_UDL_UID].includes(rowsFilterConfig.mode)) {
          if (rowsFilterConfig.columns.length === 0 && rowsFilterConfig.udlColumns.length === 0) {
            this.$Message.warning('行过滤检测失败：uid 与 uld字段不能同时为空！')
            return false
          }
          if (rowsFilterConfig.udlColumns.length !== 0 && !rowsFilterConfig.drcStrategyId) {
            this.$Message.warning('行过滤检测失败：UDL策略不可为空！')
            return false
          }
          if (rowsFilterConfig.fetchMode === 0 &&
            (
              rowsFilterConfig.context === '' ||
              rowsFilterConfig.context === undefined ||
              rowsFilterConfig.context === '//filter by config'
            )
          ) {
            this.$Message.warning('行过滤检测失败：context 不能为空！')
            return false
          }
        } else {
          if (rowsFilterConfig.mode === '' || rowsFilterConfig.mode === undefined || rowsFilterConfig.columns.length === 0) {
            this.$Message.warning('缺少行过滤配置 禁止提交')
            return false
          }
        }
      }
      return true
    }
  },
  computed: {
    useTripUdlOrUidMode () {
      return [this.formItem.constants.rowsFilter.filterMode.TRIP_UDL, this.formItem.constants.rowsFilter.filterMode.TRIP_UDL_UID].includes(this.formItem.rowsFilterDetail.mode)
    },
    hasUdlColumn () {
      return this.formItem.rowsFilterDetail.udlColumns.length !== 0
    },
    fillContextByRegion () {
      return this.formItem.rowsFilterDetail.fetchMode === this.formItem.constants.rowsFilter.fetchMode.RPC
    },
    hasUidColumn () {
      return this.formItem.rowsFilterDetail.columns.length !== 0
    },
    showUdlConfigDetail () {
      return this.formItem.rowsFilterDetail.mode === this.formItem.constants.rowsFilter.filterMode.TRIP_UDL_UID || this.hasUdlLegalConfig || !this.hasUidLegalConfig
    },
    showUidConfigDetail () {
      return this.formItem.rowsFilterDetail.mode === this.formItem.constants.rowsFilter.filterMode.TRIP_UDL_UID || this.hasUidLegalConfig || !this.hasUdlLegalConfig
    },
    hasUdlLegalConfig () {
      return this.formItem.rowsFilterDetail.udlColumns.length !== 0 && this.formItem.rowsFilterDetail.drcStrategyId != null && this.formItem.rowsFilterDetail.drcStrategyId > 0
    },
    hasUidLegalConfig () {
      return this.hasUidColumn
    },
    gtidConfigurable () {
      return this.previewDataList !== null && this.previewDataList.length === 1 && [this.formItem.constants.drcStatus.NOT_EXIST, this.formItem.constants.drcStatus.STOP].includes(this.previewDataList[0].drcStatus)
    },
    preCheckTablePage () {
      const data = this.checkTableDataList
      const start = this.table.dbTablePage.current * this.table.dbTablePage.size - this.table.dbTablePage.size
      const end = start + this.table.dbTablePage.size
      return [...data].slice(start, end)
    },
    preCheckMhaReplicationPage () {
      const data = this.previewDataList
      const start = this.table.dbMhaTablePage.current * this.table.dbMhaTablePage.size - this.table.dbMhaTablePage.size
      const end = start + this.table.dbMhaTablePage.size
      return [...data].slice(start, end)
    }
  },
  created () {
    // this.getRegions()
    this.axios.get('/api/drc/v2/permission/meta/mhaReplication/modify').then((response) => {
      if (response.data.status === 403) {
        this.$router.push('/nopermission')
        return
      }
      this.getBus()
    })
  }
}
</script>

<style>
.drawer-footer {
  width: 100%;
  bottom: 0;
  left: 0;
  border-top: 1px solid #e8e8e8;
  padding: 10px 16px;
  text-align: right;
  background: #fff;
}
</style>

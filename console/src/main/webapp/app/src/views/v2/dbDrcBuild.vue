<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/v2/mhaReplications">建立 DRC 同步</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#ffffff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <Form :model="formItem" :label-width="100" style="margin-right: 100px;margin-top: 10px">
        <FormItem label="配置模式" :required=true>
          <Select clearable v-model="formItem.buildMode" placeholder="模式">
            <Option :value=0>单库</Option>
            <Option :value=1>分库</Option>
          </Select>
        </FormItem>
        <div v-if="formItem.buildMode === 0">
          <FormItem label="数据库" :required=true>
            <Select
              v-model="formItem.dbName"
              filterable
              allow-create
              placeholder="请搜索数据库信息"
              @on-change="selectDb"
              :remote-method="getExistDb"
              :loading="dataLoading">
              <Option v-for="(option, index) in meta.dbOptions" :value="option.dbName" :key="index">
                {{option.dbName}}
              </Option>
            </Select>
          </FormItem>
        </div>
        <div v-if="formItem.buildMode === 1">
          <FormItem label="dalcluster" :required=true>
            <Input v-model="formItem.dalClusterModeOption.dalClusterName" placeholder="请输入 dalCluster 名称" @on-blur="getRegionOptions"/>
          </FormItem>
        </div>
        <FormItem label="同步方向" :required=true>
          <Row>
            <Col span="8">
              <Card :bordered="true">
                <template #title>
                  <Icon type="ios-pin"/>
                  源 region
                </template>
                <FormItem>
                  <Select filterable clearable v-model="formItem.srcRegionName" placeholder="地域" @on-change="getDalInfo">
                    <Option v-for="region in meta.regionOptions" :value="region" :key="region" :label="region">
                      {{ region }}
                    </Option>
                  </Select>
                </FormItem>
              </Card>
            </Col>
            <Col span="1" style="text-align: center">-></Col>
            <Col span="8">
              <Card :bordered="true">
                <template #title>
                  <Icon type="ios-pin"/>
                  目标 region
                </template>
                <FormItem>
                  <Select filterable clearable v-model="formItem.dstRegionName" placeholder="地域" @on-change="getDalInfo">
                    <Option v-for="region in meta.regionOptions" :value="region" :key="region" :label="region">
                      {{ region }}
                    </Option>
                  </Select>
                </FormItem>
              </Card>
            </Col>
          </Row>
        </FormItem>
        <FormItem label="确认详情" :required=true>
          <Table size="small" :loading="dataLoading" stripe :columns="formItem.dalClusterModeOption.columns" :data="previewDataList" border></Table>
        </FormItem>
        <!--        todo by yongnian: 同步表过滤使用选择       -->
        <FormItem label="业务部门" :required=true>
          <Select filterable prefix="ios-home" clearable v-model="formItem.buName" placeholder="部门">
            <Option v-for="item in meta.bus" :value="item.buName" :key="item.buName">{{ item.buName }}</Option>
          </Select>
        </FormItem>
        <FormItem label="同步表" :required=true>
          <Input v-model="formItem.tableName" placeholder="请输入正则表达式"></Input>
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
          </template>
          <FormItem label="行过滤模式">
            <Select v-model="formItem.rowsFilterDetail.mode">
              <Option value="trip-uld">UDL</Option>
            </Select>
          </FormItem>
          <FormItem label="UDL字段">
            <Select v-model="formItem.rowsFilterDetail.row">
              <Option value="1">row1</Option>
              <Option value="2">row2</Option>
            </Select>
          </FormItem>
          <FormItem label="空处理">
            <CheckboxGroup v-model="formItem.rowsFilterDetail.empty">
              <Checkbox label="empty">【字段为空时】同步</Checkbox>
            </CheckboxGroup>
          </FormItem>
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
            行过滤配置
          </template>
          <FormItem label="模式">
            <Select v-model="formItem.colsFilterDetail.mode">
              <Option value="exclude">exclude</Option>
              <Option value="include">include</Option>
            </Select>
          </FormItem>
          <FormItem label="字段">
            <Select v-model="formItem.colsFilterDetail.col">
              <Option value="1">col1</Option>
              <Option value="2">col2</Option>
            </Select>
          </FormItem>
        </Card>
        <FormItem>
          <Button type="primary" :loading="dataLoading" @click="beforeSubmit">提交</Button>
          <Button style="margin-left: 8px">Cancel</Button>
          <Button style="margin-left: 8px" type="primary" :loading="dataLoading" @click="getDalInfo">getDalInfo</Button>
          <Button style="margin-left: 8px" type="primary" :loading="dataLoading" @click="preCheckBuildParam">preCheckBuildParam</Button>
        </FormItem>
      </Form>
      <Drawer
        title="Create"
        v-model="drawer.show"
        width="80"
        :mask-closable="false"
      >
        <Table size="small" :loading="dataLoading" stripe border :columns="drawer.tableColumns" :data="finalBuildParam"></Table>
        <div class="demo-drawer-footer">
          <Button style="margin-right: 8px" @click="drawer.show = false">Cancel</Button>
          <Button type="primary" @click="submitAll">Submit</Button>
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
        buildMode: 1,
        srcRegionName: null,
        dstRegionName: null,
        dbName: null,
        dalClusterModeOption: {
          dalClusterName: 'bbzaccountsshardbasedb_dalcluster',
          columns: [
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
          ]
        },
        buName: null,
        tableName: null,
        rowsFilterDetail: {
          mode: 'trip-uld',
          row: null
        },
        colsFilterDetail: {
          mode: null,
          row: null
        },
        textarea: '',
        switch: {
          rowsFilter: false,
          colsFilter: false
        }
      },
      meta: {
        bus: [],
        regions: [],
        regionOptions: [],
        dbOptions: [],
        selectedDb: {}
      },
      drawer: {
        show: false,
        tableColumns: [
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
      dbClusterInfoList: [],
      finalBuildParam: [],
      previewDataList: [],
      dataLoading: false
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
          this.meta.regions = response.data.data.map((e) => { return e.regionName })
          this.meta.regionOptions = this.meta.regions
        })
    },
    getParams: function () {
      const param = {}
      param.buName = this.formItem.buName
      param.mode = this.formItem.buildMode
      param.srcRegionName = this.formItem.srcRegionName
      param.dstRegionName = this.formItem.dstRegionName

      param.tblsFilterDetail = {
        tableNames: this.formItem.tableName
      }
      if (this.formItem.buildMode === 0) {
        param.dbName = this.formItem.dbName
      } else if (this.formItem.buildMode === 1) {
        param.dalClusterName = this.formItem.dalClusterModeOption.dalClusterName
      }
      if (this.formItem.switch.rowsFilter) {
        param.rowsFilterDetail = this.formItem.rowsFilterDetail
      }
      if (this.formItem.switch.colsFilter) {
        param.colsFilterDetail = this.formItem.colsFilterDetail
      }
      return param
    },
    async submitAll () {
      const that = this
      that.dataLoading = true
      const params = this.getParams()
      console.log(params)
      await that.axios.post('/api/drc/v2/autoconfig/submit', params)
        .then(response => {
          const data = response.data
          const success = data.status !== 1
          if (success) {
            that.$Message.success('提交成功')
          } else {
            that.$Message.warning('提交失败: ' + data.message)
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
      await this.getRegionOptions()
      if (this.formItem.srcRegionName && this.formItem.dstRegionName) {
        await this.getDalInfo()
      }
    },
    async getDalInfo () {
      const params = this.getParams()
      console.log(params.srcRegionName, params.dstRegionName)
      const that = this
      that.previewDataList = []
      if (!params.srcRegionName || !params.dstRegionName) {
        return
      }
      that.dataLoading = true
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
          if (data) {
            that.previewDataList = data
            console.log('total: ' + that.previewDataList.length)
            that.$Message.success('预检测成功, 共 ' + data.length + ' 个集群')
          } else {
            that.$Message.warning('预检测失败')
          }
        })
        .catch(message => {
          that.$Message.error('查询dalcluster异常: ' + message)
        })
        .finally(() => {
          that.dataLoading = false
        })
    },
    async preCheckBuildParam () {
      const params = this.getParams()
      const that = this
      that.dataLoading = true

      that.finalBuildParam = []
      await that.axios.get('/api/drc/v2/autoconfig/preCheckBuildParam', {
        params: this.flattenObj(params)
      })
        .then(response => {
          const data = response.data.data
          if (data) {
            that.finalBuildParam = data
            that.$Message.success('预检测成功, 共 ' + data.length + ' 个集群')
          } else {
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
          if (data) {
            that.meta.regionOptions = data
            if (data.indexOf(that.formItem.srcRegionName) === -1) {
              that.formItem.srcRegionName = null
            }
            if (data.indexOf(that.formItem.dstRegionName) === -1) {
              that.formItem.dstRegionName = null
            }
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
    async beforeSubmit () {
      this.drawer.show = true
      await this.preCheckBuildParam()
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
    }
  },
  created () {
    // this.getRegions()
    this.getBus()
  }
}
</script>

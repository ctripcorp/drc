<template>
  <div :style="{padding: '1px 1px',height: '100%'}">
    <Row>
      <Alert type="warning" show-icon v-if="alertInfo.show" closable>
        {{ alertInfo.title }}
        <template #desc>{{ alertInfo.message }}</template>
      </Alert>
      <Alert type="success" show-icon v-if="alertInfo.successShow" closable>
        {{ alertInfo.title }}
        <template #desc>{{ alertInfo.message }}</template>
      </Alert>
      <div v-if="replicationType === meta.replicationType.DB_TO_DB">
        <Col span="12">
          <Row style="margin-right: 20px">
            <Divider>1. 选择同步方向</Divider>
            <Col span="11">
              <Card :bordered="true">
                <template #title>
                  <Icon type="ios-pin"/>
                  源 region
                </template>
                <Select filterable clearable v-model="formItem.srcRegionName" placeholder="地域"
                        @on-change="afterSwitchRegion">
                  <Option v-for="region in meta.regionOptions" :value="region" :key="region" :label="region">
                    {{ region }}
                  </Option>
                </Select>
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
                <Select filterable clearable v-model="formItem.dstRegionName" placeholder="地域"
                        @on-change="afterSwitchRegion">
                  <Option v-for="region in meta.regionOptions" :value="region" :key="region" :label="region">
                    {{ region }}
                  </Option>
                </Select>
              </Card>
            </Col>
          </Row>
        </Col>
        <Col span="12">
          <Row>
            <Divider>2. 预览：同步集群</Divider>
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
          </Row>
        </Col>
      </div>
      <div v-if="replicationType === meta.replicationType.DB_TO_MQ">
        <Col span="12">
          <Row style="margin-right: 20px">
            <Divider>1. 选择消息投递Region</Divider>
            <Col span="24">
              <Card :bordered="true">
                <template #title>
                  <Icon type="ios-pin"/>
                  源 region
                  <Button size="small" shape="circle" type="default" :loading="dataLoading" style="margin-left: 50px" @click="getRegionOptions">刷新
                  </Button>
                </template>
                <Select filterable clearable v-model="formItem.srcRegionName" placeholder="地域"
                        @on-change="afterSwitchRegion">
                  <Option v-for="region in meta.regionOptions" :value="region" :key="region" :label="region">
                    {{ region }}
                  </Option>
                </Select>
              </Card>
            </Col>
          </Row>
        </Col>
        <Col span="12">
          <Row>
            <Divider>2. 预览：消息投递集群</Divider>
            <Button type="primary" :loading="table.dbMhaTableLoading" @click="getDalInfo" style="margin-bottom: 5px">
              检查同步集群
            </Button>
            <Table size="small" :loading="table.dbMhaTableLoading" stripe :columns="table.dbMhaMqTableColumn"
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
          </Row>
        </Col>
      </div>
    </Row>
    <Divider></Divider>
    <Button @click="createReplication" :loading="dataLoading" type="primary">提交</Button>

  </div>
</template>

<script>

export default {
  name: 'mhaPreview',
  components: {},
  props: {
    replicationType: Number,
    dbName: String,
    existReplicationRegionOptions: Array
  },
  emits: ['updated'],
  data () {
    return {
      meta: {
        regionOptions: [],
        existReplicationRegionOptions: [],
        replicationType: {
          DB_TO_DB: 0,
          DB_TO_MQ: 1
        }
      },
      alertInfo: {
        show: false,
        successShow: false,
        title: null,
        message: null
      },
      formItem: {
        srcRegionName: null,
        dstRegionName: null
      },
      dataLoading: false,
      constants: {
        DAL_CLUSTER_MODE: 1
      },
      previewDataList: [],
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
        dbMhaMqTableColumn: [
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
          }
        ],
        dbMhaTablePage: {
          total: 0,
          current: 1,
          size: 5,
          pageSizeOpts: [5, 10, 20, 100]
        }
      }
    }
  },
  methods: {
    async getRegionOptions () {
      this.dataLoading = true
      this.meta.regionOptions = []
      await this.axios.get('/api/drc/v2/autoconfig/regionOptions', {
        params: {
          mode: this.constants.DAL_CLUSTER_MODE,
          dbName: this.dbName
        }
      })
        .then(response => {
          const data = response.data.data
          const success = data && response.data.status === 0
          if (success) {
            this.meta.regionOptions = data
            if (data.indexOf(this.formItem.srcRegionName) === -1) {
              this.formItem.srcRegionName = null
            }
            if (data.indexOf(this.formItem.dstRegionName) === -1) {
              this.formItem.dstRegionName = null
            }
          } else {
            this.formItem.srcRegionName = null
            this.formItem.dstRegionName = null
            this.$Message.warning('查询可选地域失败')
          }
        })
        .catch(message => {
          this.$Message.error('查询可选地域失败: ' + message)
        })
        .finally(() => {
          this.dataLoading = false
        })
    },
    async getDalInfo () {
      this.previewDataList = []
      this.table.dbMhaTablePage.current = 1
      this.table.dbMhaTableLoading = true
      await this.axios.get('/api/drc/v2/autoconfig/preCheck', {
        params: {
          mode: this.constants.DAL_CLUSTER_MODE,
          dbName: this.dbName,
          srcRegionName: this.formItem.srcRegionName,
          replicationType: this.replicationType
        }
      })
        .then(response => {
          const data = response.data.data
          const success = data && response.data.status === 0
          if (success) {
            this.previewDataList = data
            console.log('total: ' + this.previewDataList.length)
            this.$Message.success('同步集群检测成功, 共找到 ' + data.length + ' 个DB')
          } else {
            this.$Message.warning('同步集群检测失败：' + response.data.message)
          }
        })
        .catch(message => {
          this.$Message.error('查询dalcluster异常: ' + message)
        })
        .finally(() => {
          this.table.dbMhaTableLoading = false
        })
    },
    async createReplication () {
      const params = {
        srcRegionName: this.formItem.srcRegionName,
        dstRegionName: this.formItem.dstRegionName,
        dbName: this.dbName,
        replicationType: this.replicationType
      }
      this.alertInfo.show = false
      this.alertInfo.successShow = false
      this.dataLoading = true
      let scene
      if (this.replicationType === this.meta.replicationType.DB_TO_MQ) {
        scene = 'mhaDbReplicationForMq'
      } else {
        scene = 'mhaDbReplication'
      }
      await this.axios.post('/api/drc/v2/autoconfig/' + scene + '/create', params)
        .then(response => {
          const data = response.data
          const success = data.status === 0
          if (success) {
            this.alertInfo.successShow = true
            this.alertInfo.message = null
            this.alertInfo.title = '提交成功'
            this.$emit('updated')
          } else {
            this.alertInfo.show = true
            this.alertInfo.title = '提交失败'
            this.alertInfo.message = data.message
            this.$Message.warning('提交失败: ' + data.message)
          }
        })
        .catch(message => {
          this.$Message.error('提交异常: ' + message)
        })
        .finally(() => {
          this.dataLoading = false
        })
    },
    async afterSwitchRegion () {
    }
  },
  computed: {
    preCheckMhaReplicationPage () {
      const data = this.previewDataList
      const start = this.table.dbMhaTablePage.current * this.table.dbMhaTablePage.size - this.table.dbMhaTablePage.size
      const end = start + this.table.dbMhaTablePage.size
      return [...data].slice(start, end)
    }
  },
  created () {
    this.getRegionOptions()
  }
}
</script>

<style scoped>

</style>

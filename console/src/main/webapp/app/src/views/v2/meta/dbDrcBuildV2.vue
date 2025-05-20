<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/v2/mhaDbReplications">DB 复制链路</BreadcrumbItem>
      <BreadcrumbItem >DRC-DB 配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content"
             :style="{padding: '10px', background: '#ffffff', margin: '50px 0 111px 185px', zIndex: '1'}">
      <Row :gutter=10 align="middle">
        <Col span="18">
          <Form :model="formItem" :label-width="100" style="margin-right: 20px;margin-top: 10px">
            <FormItem label="数据库" :required=true>
              <Select
                v-model="meta.dbName"
                filterable
                :disabled="!!meta.fixDb"
                placeholder="请搜索数据库信息"
                @on-change="selectDb"
                :remote-method="getExistDb"
                :loading="dataLoading">
                <Option v-for="(option, index) in meta.dbOptions" :value="option.dbName" :key="index">{{option.dbName}}</Option>
              </Select>
            </FormItem>
            <FormItem label="同步方向" :required=true>
              <RadioGroup v-model="selectedExistReplication" button-style="solid"
                          @on-change="afterSelectExistReplication">
                <Radio v-for="items in meta.existReplicationRegionOptions"
                       :key="items.srcRegionName+' -> '+items.dstRegionName"
                       :label="items.srcRegionName+' -> '+items.dstRegionName" border>
                  {{ items.srcRegionName + ' -> ' + items.dstRegionName }}
                </Radio>
              </RadioGroup>
              <Button type="primary" icon="md-add" ghost @click="goToCreateReplication">新增</Button>
              <Modal v-model="createModal.open" width="1200px" :footer-hide="true" title="创建同步">
                <mha-preview v-if="createModal.open" :db-name="meta.dbName" :replication-type=0 @updated="selectDb"
                             :exist-replication-region-options="meta.existReplicationRegionOptions"></mha-preview>
              </Modal>
            </FormItem>
            <FormItem label="DB 组" :required=true v-if="isShardedDb">
              <db-shard-selector  :all-drc-configs="allDrcConfigs" @change="afterSelectDbs"></db-shard-selector>
            </FormItem>
            <Divider orientation="left">同步表</Divider>
            <Card style="width:100%">
              <tables :table-data="drcConfig.logicTableSummaryDtos" :data-loading="configDataLoading"
                      :src-region="meta.srcRegionName" :dst-region="meta.dstRegionName" :db-name="meta.dbName"
                      :db-names="drcConfig.dbNames" @updated="getDrcConfig"
              />
            </Card>
            <Divider orientation="left">同步DB</Divider>
            <Card style="width:100%">
              <mha-replication-panel v-if="drcConfig.mhaReplications" :mha-replications="drcConfig.mhaReplications" @updated="getDrcConfig"/>
            </Card>
          </Form>
        </Col>
      </Row>
    </Content>
  </base-component>
</template>
<script>

import tables from '@/components/v2/dbDrcBuild/tables.vue'
import MhaReplicationPanel from '@/components/v2/dbDrcBuild/mhaReplicationPanel.vue'
import MhaPreview from '@/components/v2/dbDrcBuild/mhaPreview.vue'
import DbShardSelector from '@/components/v2/dbDrcBuild/dbShardSelector.vue'

export default {
  components: { DbShardSelector, MhaPreview, MhaReplicationPanel, tables },
  data () {
    return {
      createModal: {
        open: false
      },
      dataLoading: false,
      configDataLoading: false,
      selectedExistReplication: this.$route.query.srcRegionName + ' -> ' + this.$route.query.dstRegionName,
      selectedDbs: '',
      formItem: {
        srcRegionName: null,
        dstRegionName: null
      },
      allDrcConfigs: [],
      drcConfig: {},
      meta: {
        dbName: this.$route.query.dbName,
        srcRegionName: this.$route.query.srcRegionName,
        dstRegionName: this.$route.query.dstRegionName,
        fixDb: this.$route.query.fixDb,
        regionOptions: [],
        dbOptions: [],
        existReplicationRegionOptions: []
      }
    }
  },
  methods: {
    getParams: function () {
      const param = {}
      if (this.selectedDbs) {
        param.dbName = this.selectedDbs
        param.mode = 2
      } else {
        param.dbName = this.meta.dbName
        param.mode = 0
      }
      param.srcRegionName = this.meta.srcRegionName
      param.dstRegionName = this.meta.dstRegionName
      return param
    },
    async getExistDb (dbName) {
      if (dbName === null || dbName.length === null || dbName.length <= 0) {
        return []
      }
      const that = this
      that.dataLoading = true
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
    async selectDb () {
      this.clearDrcConfigs()
      await this.getExistReplicationRegionOptions()
      await this.getDrcConfig()
    },
    clearDrcConfigs () {
      this.drcConfig = {}
      this.allDrcConfigs = []
    },
    setDrcConfigs (allConfigs) {
      this.drcConfig = allConfigs[0] ? allConfigs[0] : {}
      if (this.allDrcConfigs.length === 0 || allConfigs.length > 1) {
        this.allDrcConfigs = allConfigs
      }
    },
    async getDrcConfig () {
      this.createModal.open = false
      this.resetPath()
      this.setDrcConfigs([])
      const params = this.getParams()
      if (!params.dbName) {
        this.$Message.warning('请先填写数据库')
        return
      }
      if (!params.srcRegionName || !params.dstRegionName) {
        return
      }
      if (params.srcRegionName === params.dstRegionName) {
        return
      }
      this.configDataLoading = true
      await this.axios.get('/api/drc/v2/autoconfig/getDbDrcConfigs', {
        params: {
          dbName: params.dbName,
          mode: params.mode,
          srcRegionName: params.srcRegionName,
          dstRegionName: params.dstRegionName
        }
      })
        .then(response => {
          const data = response.data.data
          const success = data && response.data.status === 0
          if (success) {
            this.setDrcConfigs(data)
            this.$Message.success('查询DRC配置成功 ')
          } else {
            this.$Message.warning('查询失败：' + response.data.message)
          }
        })
        .catch(message => {
          this.$Message.error('查询异常: ' + message)
        })
        .finally(() => {
          this.configDataLoading = false
        })
    },
    async afterSelectExistReplication () {
      console.log('afterSelectExistReplication ', this.selectedExistReplication)
      const ret = this.selectedExistReplication.split('->')
      this.meta.srcRegionName = ret[0].trim()
      this.meta.dstRegionName = ret[1].trim()
      this.clearDrcConfigs()
      this.getDrcConfig()
    },
    async afterSelectDbs (value) {
      console.log('afterSelectDbs ', value)
      this.selectedDbs = value
      this.getDrcConfig()
    },
    async getExistReplicationRegionOptions () {
      const that = this
      that.dataLoading = true
      that.meta.existReplicationRegionOptions = []
      const params = this.getParams()
      await that.axios.get('/api/drc/v2/autoconfig/getExistDbReplicationDirections', {
        params: {
          dbName: params.dbName
        }
      })
        .then(response => {
          const data = response.data.data
          const success = data && response.data.status === 0
          if (success) {
            that.meta.existReplicationRegionOptions = data
            if (that.meta.existReplicationRegionOptions.length > 0) {
              that.$Message.success('查询到 ' + that.meta.existReplicationRegionOptions.length + ' 个可选同步方向')
            }
          } else {
            that.meta.srcRegionName = null
            that.meta.dstRegionName = null
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
    async goToCreateReplication () {
      this.createModal.open = true
    },
    resetPath () {
      this.$router.replace({
        query: {
          dbName: this.meta.dbName,
          srcRegionName: this.meta.srcRegionName,
          dstRegionName: this.meta.dstRegionName,
          fixDb: this.meta.fixDb
        }
        // eslint-disable-next-line handle-callback-err
      }).catch(() => {
      })
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
  computed: {
    isShardedDb () {
      const data = this.allDrcConfigs
      if (!data || !(data instanceof Array) || data.length <= 0) {
        return false
      }
      if (data.length > 1) {
        return true
      }
      if (data[0].dbNames && data[0].dbNames.length > 1) {
        return true
      }
      return false
    }
  },
  created () {
    // this.getRegions()
    this.axios.get('/api/drc/v2/permission/meta/mhaReplication/modify').then((response) => {
      if (response.data.status === 403) {
        this.$router.push('/nopermission')
        return
      }
      if (this.meta.dbName) {
        this.getExistReplicationRegionOptions()
        this.getDrcConfig()
      }
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

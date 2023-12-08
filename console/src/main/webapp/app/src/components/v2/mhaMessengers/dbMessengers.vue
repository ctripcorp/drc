<template>
  <base-component>
<!--    todo by yongnian test-->
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/v2/messengersV2">Messenger 集群</BreadcrumbItem>
      <BreadcrumbItem :to="{
          path: '/v2/buildMessengerV2',query :{
          step: 3,
          mhaName: initInfo.mhaName
        }
      }">Messenger 配置
      </BreadcrumbItem>
      <BreadcrumbItem>Db messenger</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <Row>
        <Col span="4">
          <Button style="margin-top: 10px;text-align: right" type="primary" ghost @click="batchAutoConfigure()">批量配置
            applier
          </Button>
        </Col>
        <Col span="2">
          <Button :loading="dataLoading" style="margin-top: 10px;text-align: right" type="primary" ghost v-if="!submitted" @click="submitDbAppliers()">提交
          </Button>
        </Col>
      </Row>
      <div :style="{padding: '1px 1px',height: '100%'}">
        <template>
          <Table style="margin-top: 20px" stripe :columns="columns" :data="tableData" :loading="dataLoading" border ref="multipleTable"
                 @on-selection-change="changeSelection">
            <template #applier="{row, index}">
              <Select :transfer="true" v-model="tableData[index].ips" multiple style="width: 250px" placeholder="选择源集群Applier">
                <Option v-for="item in applierResourceList" :value="item.ip" :key="item.ip">{{ item.ip }} —— {{
                    item.az
                  }}
                </Option>
              </Select>
              <Button type="success" size="small"  style="margin-left: 10px" @click="autoConfigApplier(row, index)">自动录入</Button>
            </template>
            <template #gtidInit="{row, index}">
              <Input v-model="tableData[index].gtidInit" :border="false" placeholder="请输入binlog拉取位点"/>
            </template>
          </Table>
        </template>
      </div>
    </Content>
  </base-component>
</template>

<script>

import Vue from 'vue'

export default {
  name: 'tables',
  data () {
    return {
      initInfo: {
        mhaName: this.$route.query.mhaName
      },
      dataLoading: false,
      submitted: false,
      applierResourceList: [],
      batchDeleteModal: false,
      deleteData: [],
      filterMap: {
        0: '行过滤',
        1: '字段过滤'
      },
      columns: [
        {
          title: '库名',
          key: 'dbName',
          width: 200
        },
        {
          title: 'Applier',
          slot: 'applier'
        },
        {
          title: 'gtidInit',
          slot: 'gtidInit'
        }
      ],
      propertiesJson: {},
      tableData: [],
      total: 0,
      size: 5,
      pageSizeOpts: [5, 10, 20, 100]
    }
  },
  methods: {
    getFilterText (val) {
    },
    changeSelection (val) {
      this.initInfo.multiData = val
      console.log(this.initInfo.multiData)
    },
    buildApplierDto (row) {
      return this.flattenObj([{
        ips: row.ips,
        gtidInit: row.gtidInit,
        dbName: row.dbName
      }])
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
    async autoConfigApplier (row, index) {
      this.axios.get('/api/drc/v2/resource/db/auto', {
        params: {
          dstMhaName: this.initInfo.mhaName,
          type: 1,
          selectedIps: row.ips ? row.ips.join(',') : null
        }
      }).then(response => {
        row.ips = []
        response.data.data.forEach(ip => row.ips.push(ip.ip))
        Vue.set(this.tableData, index, row)
        this.tableData[index] = row
      })
    },

    submitDbAppliers () {
      const params = {
        mhaName: this.initInfo.mhaName,
        dbApplierDtos: this.tableData
      }
      this.dataLoading = true
      this.axios.post('/api/drc/v2/config/db/messenger', params)
        .then(response => {
          const data = response.data
          const success = data.status !== 1
          if (success) {
            this.submitted = true
            this.$Message.success('提交成功')
          } else {
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
    async batchAutoConfigure () {
      this.tableData.map((row, index) => this.autoConfigApplier(row, index))
    },
    getResources () {
      this.axios.get('/api/drc/v2/resource/mha/all?mhaName=' + this.initInfo.mhaName + '&type=1')
        .then(response => {
          console.log(response.data)
          this.applierResourceList = response.data.data
        })
    },
    getDbAppliers () {
      this.dataLoading = true
      this.axios.get('/api/drc/v2/config/mha/dbMessenger', {
        params: {
          mhaName: this.initInfo.mhaName
        }
      }
      ).then(response => {
        if (response.data.status === 1) {
          this.$Message.error('查询 db applier 失败!')
        } else {
          this.tableData = response.data.data
        }
      }).finally(() => {
        this.dataLoading = false
      })
    },
    clearDeleteDbReplication (row, index) {
      this.deleteData = []
      this.batchDeleteModal = false
    },
    deleteDbReplication () {
      const deleteDbReplicationIds = []
      this.deleteData.forEach(e => deleteDbReplicationIds.push(e.dbReplicationId))
      this.axios.delete('/api/drc/v2/config/dbReplications?dbReplicationIds=' + deleteDbReplicationIds).then(res => {
        if (res.data.status === 1) {
          this.$Message.error('删除失败!' + res.data.message)
        } else {
          this.$Message.success('删除成功!')
          this.getDbAppliers()
          this.initInfo.multiData = []
        }
      })
    }
  },
  created () {
    this.axios.get('/api/drc/v2/permission/meta/mhaReplication/modify').then((response) => {
      if (response.data.status === 403) {
        this.$router.push('/nopermission')
        return
      }
      this.initInfo = {
        mhaName: this.$route.query.mhaName
      }
      console.log('initInfo:')
      console.log(this.initInfo)
      this.getResources()
      this.getDbAppliers()
    })
  }
}
</script>

<style scoped>

</style>

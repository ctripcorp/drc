<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem :to="{
          path: '/v2/mhaReplications',query :{
          srcMhaName: this.initInfo.srcMhaName,
          dstMhaName: this.initInfo.dstMhaName,
          preciseSearchMode: true
        }
      }">首页
      </BreadcrumbItem>
      <BreadcrumbItem :to="{
        path: '/drcV2',query :{
          step: 3,
          srcMhaName: this.initInfo.srcMhaName,
          dstMhaName: this.initInfo.dstMhaName,
          srcDc: this.initInfo.srcDc,
          dstDc: this.initInfo.dstDc,
          order: this.initInfo.order
        }
      }">DRC配置V2
      </BreadcrumbItem>
      <BreadcrumbItem>同步表</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <Row>
        <Col span="16">
          <span
            style="margin-top: 10px;color:#464c5b;font-weight:600">{{initInfo.srcMhaName}}({{initInfo.srcDc}})==>{{initInfo.dstMhaName}}({{initInfo.dstDc}})</span>
        </Col>
        <Col span="2">
          <Button style="margin-top: 10px;text-align: right" type="primary" ghost @click="goTodbReplicationConfigV2">添加
          </Button>
        </Col>
        <Col span="2">
          <Button style="margin-top: 10px;text-align: right" type="primary" ghost @click="goToUpdate()">批量修改
          </Button>
        </Col>
      </Row>
      <Modal
        v-model="display.showPropertiesJson"
        title="Applier properties"
        width="800px"
      >
        <json-viewer
          :value="propertiesJson"
          :expand-depth=5
          copyable>
          <template v-slot:copy="{copied}">
            <span v-if="copied">复制成功</span>
            <span v-else>复制</span>
          </template>
        </json-viewer>
      </Modal>
      <div :style="{padding: '1px 1px',height: '100%'}">
        <template>
          <Table style="margin-top: 20px" stripe :columns="columns" :data="tableData" border ref="multipleTable"
                 @on-selection-change="changeSelection">
            <template slot-scope="{ row, index }" slot="action">
              <Button type="success" size="small" style="margin-right: 5px" @click="goToShowConfig(row, index)">查看
              </Button>
              <Button type="primary" size="small" style="margin-right: 5px" @click="goToUpdateConfig(row, index)">修改
              </Button>
              <Button type="error" size="small" style="margin-right: 10px" @click="preDeleteDbReplication(row, index)">
                删除
              </Button>
            </template>
          </Table>
        </template>
      </div>
      <Modal
        v-model="deleteModal"
        title="确认删除以下同步表"
        @on-ok="deleteDbReplication"
        @on-cancel="clearDeleteDbReplication">
        <p>
          <span>db: </span><span
          style="color: red;font-size: 16px; word-break: break-all; word-wrap: break-word">{{deleteDbReplicationInfo.dbName}}</span>
        </p>
        <p>
          <span>table: </span><span
          style="color: red;font-size: 16px; word-break: break-all; word-wrap: break-word">{{deleteDbReplicationInfo.logicTableName}}</span>
        </p>
      </Modal>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'tables',
  data () {
    return {
      dbReplicationIds: [],
      initInfo: {
        srcMhaName: '',
        srcMhaId: 0,
        dstMhaName: '',
        dbReplicationId: 0,
        multiData: [],
        srcDc: '',
        dstDc: '',
        order: true
      },
      deleteModal: false,
      deleteDbReplicationInfo: {
        dbReplicationId: 0,
        dbName: '',
        logicTableName: ''
      },
      columns: [
        {
          type: 'selection',
          width: 60,
          align: 'center'
        },
        {
          title: '序号',
          width: 75,
          align: 'center',
          // fixed: 'left',
          render: (h, params) => {
            return h(
              'span',
              params.index + 1
            )
          }
        },
        // {
        //   title: 'id',
        //   key: 'dbReplicationId'
        // },
        {
          title: '库名',
          key: 'dbName'
        },
        {
          title: '表名',
          key: 'logicTableName'
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center',
          width: 200,
          fixed: 'right'
        }
      ],
      display: {
        showPropertiesJson: false
      },
      propertiesJson: {},
      tableData: [],
      total: 0,
      size: 5,
      pageSizeOpts: [5, 10, 20, 100]
    }
  },
  methods: {
    changeSelection (val) {
      this.initInfo.multiData = val
      console.log(this.initInfo.multiData)
    },
    getDbReplications () {
      this.axios.get('/api/drc/v2/config/dbReplication?srcMhaName=' + this.initInfo.srcMhaName + '&dstMhaName=' + this.initInfo.dstMhaName)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('查询相关配置表失败!')
          } else {
            this.tableData = response.data.data
          }
        })
    },
    goTodbReplicationConfigV2 () {
      this.$router.push({
        path: '/dbReplicationConfigV2',
        query: {
          srcMhaName: this.initInfo.srcMhaName,
          dstMhaName: this.initInfo.dstMhaName,
          srcDc: this.initInfo.srcDc,
          dstDc: this.initInfo.dstDc,
          update: false
        }
      })
    },
    goToShowConfig (row, index) {
      const dbReplicationIds = []
      dbReplicationIds.push(row.dbReplicationId)
      this.$router.push({
        path: '/dbReplicationConfigV2',
        query: {
          srcMhaName: this.initInfo.srcMhaName,
          dstMhaName: this.initInfo.dstMhaName,
          srcDc: this.initInfo.srcDc,
          dstDc: this.initInfo.dstDc,
          dbName: row.dbName,
          tableName: row.logicTableName,
          dbReplicationId: row.dbReplicationId,
          dbReplicationIds: JSON.stringify(dbReplicationIds),
          update: true,
          show: true
        }
      })
    },
    goToUpdate () {
      const multiData = this.initInfo.multiData
      if (multiData === undefined || multiData === null || multiData.length === 0) {
        this.$Message.warning('请勾选！')
      }
      const row = multiData[0]
      const dbReplicationIds = []
      multiData.forEach(data => dbReplicationIds.push(data.dbReplicationId))
      let dbName = ''
      if (multiData.length > 1) {
        dbName = '(' + row.dbName
        for (var i = 1; i < multiData.length; i++) {
          if (multiData[i].logicTableName !== row.logicTableName) {
            this.$Message.warning('表名不一样不能勾选')
            return
          }
          dbName += '|' + multiData[i].dbName
        }
        dbName += ')'
      } else {
        dbName = row.dbName
      }
      console.log(dbReplicationIds)
      this.$router.push({
        path: '/dbReplicationConfigV2',
        query: {
          srcMhaName: this.initInfo.srcMhaName,
          dstMhaName: this.initInfo.dstMhaName,
          srcDc: this.initInfo.srcDc,
          dstDc: this.initInfo.dstDc,
          dbName: dbName,
          tableName: row.logicTableName,
          dbReplicationId: row.dbReplicationId,
          dbReplicationIds: JSON.stringify(dbReplicationIds),
          update: true
        }
      })
    },
    goToUpdateConfig (row, index) {
      // this.dbReplicationIds.push(row.dbReplicationId)
      const dbReplicationIds = []
      dbReplicationIds.push(row.dbReplicationId)
      this.$router.push({
        path: '/dbReplicationConfigV2',
        query: {
          srcMhaName: this.initInfo.srcMhaName,
          dstMhaName: this.initInfo.dstMhaName,
          srcDc: this.initInfo.srcDc,
          dstDc: this.initInfo.dstDc,
          dbName: row.dbName,
          tableName: row.logicTableName,
          dbReplicationId: row.dbReplicationId,
          dbReplicationIds: JSON.stringify(dbReplicationIds),
          update: true
        }
      })
    },
    preDeleteDbReplication (row, index) {
      this.deleteDbReplicationInfo = {
        dbReplicationId: row.dbReplicationId,
        dbName: row.dbName,
        logicTableName: row.logicTableName
      }
      this.deleteModal = true
    },
    clearDeleteDbReplication (row, index) {
      this.deleteDbReplicationInfo = {
        dbReplicationId: 0,
        dbName: '',
        logicTableName: ''
      }
      this.deleteModal = false
    },
    deleteDbReplication () {
      this.axios.delete('/api/drc/v2/config/dbReplication?dbReplicationId=' + this.deleteDbReplicationInfo.dbReplicationId).then(res => {
        if (res.data.status === 1) {
          this.$Message.warning('删除失败!' + res.data.message)
        } else {
          this.$Message.success('删除成功!')
          this.getDbReplications()
        }
      })
    }
  },
  created () {
    this.initInfo = {
      srcMhaName: this.$route.query.srcMhaName,
      srcMhaId: this.$route.query.srcMhaId,
      dstMhaName: this.$route.query.dstMhaName,
      applierGroupId: this.$route.query.applierGroupId,
      srcDc: this.$route.query.srcDc,
      dstDc: this.$route.query.dstDc,
      order: this.$route.query.order
    }
    console.log('initInfo:')
    console.log(this.initInfo)
    this.getDbReplications()
  }
}
</script>

<style scoped>

</style>

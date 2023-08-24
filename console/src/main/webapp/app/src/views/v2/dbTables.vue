<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem :to="{
        path: '/drcV2',query :{
          step: 3,
          srcMhaName: this.initInfo.srcMhaName,
          dstMhaName: this.initInfo.dstMhaName,
          srcDc: this.initInfo.srcDc,
          dstDc: this.initInfo.dstDc,
          order: this.initInfo.order
        }
      }">DRC配置V2</BreadcrumbItem>
      <BreadcrumbItem >同步表</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <Row>
        <Col span="20">
          <span style="margin-top: 10px;color:#464c5b;font-weight:600">{{initInfo.srcMhaName}}({{initInfo.srcDc}})==>{{initInfo.dstMhaName}}({{initInfo.dstDc}})</span>
        </Col>
        <Col span="2">
          <Button style="margin-top: 10px;text-align: right" type="primary" ghost @click="goTodbReplicationConfig">添加</Button>
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
          <Table style="margin-top: 20px" stripe :columns="columns" :data="tableData" border>
            <template slot-scope="{ row, index }" slot="action">
              <Button type="success" size="small" style="margin-right: 5px" @click="goToShowConfig(row, index)">查看</Button>
              <Button type="primary" size="small" style="margin-right: 5px" @click="goToUpdateConfig(row, index)">修改</Button>
              <Button type="error" size="small" style="margin-right: 10px" @click="goToDeleteConfig(row, index)">删除</Button>
            </template>
          </Table>
        </template>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'tables',
  data () {
    return {
      initInfo: {
        srcMhaName: '',
        srcMhaId: 0,
        dstMhaName: '',
        dbReplicationId: 0,
        srcDc: '',
        dstDc: '',
        order: true
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
    goTodbReplicationConfig () {
      this.$router.push({
        path: '/dbReplicationConfig',
        query: {
          srcMhaName: this.initInfo.srcMhaName,
          srcMhaId: this.initInfo.srcMhaId,
          dstMhaName: this.initInfo.dstMhaName,
          srcDc: this.initInfo.srcDc,
          dstDc: this.initInfo.dstDc,
          order: this.initInfo.order,
          dbName: '',
          tableName: ''
        }
      })
    },
    goToShowConfig (row, index) {
      // todo 展示json
    },
    goToUpdateConfig (row, index) {
      this.$router.push({
        path: '/dbReplicationConfig',
        query: {
          srcMhaName: this.initInfo.srcMhaName,
          dstMhaName: this.initInfo.dstMhaName,
          srcDc: this.initInfo.srcDc,
          dstDc: this.initInfo.dstDc,
          order: this.initInfo.order,
          dbName: row.dbName,
          tableName: row.logicTableName,
          dbReplicationId: row.dbReplicationId,
          update: 0
        }
      })
    },
    goToDeleteConfig (row, index) {
      // todo, json确认弹窗
      console.log(row)
      this.axios.delete('/api/drc/v2/config/dbReplication?dbReplicationId=' + row.dbReplicationId).then(response => {
        if (response.data.status === 1) {
          window.alert('删除失败!')
        } else {
          window.alert('删除成功!')
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

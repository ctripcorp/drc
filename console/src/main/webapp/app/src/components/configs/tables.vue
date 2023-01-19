<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem :to="{
        path: '/accessV2',query :{
          step: 3,
          clustername: this.initInfo.srcMha,
          newclustername: this.initInfo.destMha,
          order: this.initInfo.order
        }
      }">DRC配置</BreadcrumbItem>
      <BreadcrumbItem >同步表</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <Row>
        <Col span="22">
          <span style="margin-top: 10px;color:#464c5b;font-weight:600">{{initInfo.srcMha}}({{initInfo.srcDc}})==>{{initInfo.destMha}}({{initInfo.destDc}})</span>
        </Col>
        <Col span="2">
          <Button style="margin-top: 10px;text-align: right" type="primary" ghost @click="goToTableConfigFlow">添加</Button>
        </Col>
      </Row>
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
        srcMha: '',
        srcMhaId: 0,
        destMha: '',
        applierGroupId: 0,
        srcDc: '',
        destDc: '',
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
        {
          title: '库名',
          key: 'namespace'
        },
        {
          title: '表名',
          key: 'name'
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center',
          width: 200,
          fixed: 'right'
        }
      ],
      tableData: [],
      total: 0,
      size: 5,
      pageSizeOpts: [5, 10, 20, 100]
    }
  },
  methods: {
    getAllTableVosInApplierGroup () {
      console.log(this.initInfo.applierGroupId)
      this.axios.get('/api/drc/v1/dataMedia/vos?applierGroupId=' + this.initInfo.applierGroupId)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('查询相关配置表失败!')
          } else {
            this.tableData = response.data.data
          }
        })
    },
    goToTableConfigFlow () {
      this.$router.push({
        path: '/tables/configFlow',
        query: {
          srcMha: this.initInfo.srcMha,
          srcMhaId: this.initInfo.srcMhaId,
          destMha: this.initInfo.destMha,
          applierGroupId: this.initInfo.applierGroupId,
          srcDc: this.initInfo.srcDc,
          destDc: this.initInfo.destDc,
          order: this.initInfo.order,
          dataMediaId: 0,
          namespace: '',
          name: ''
          // tableData: []
        }
      })
    },
    goToShowConfig (row, index) {
      // todo 展示json
    },
    goToUpdateConfig (row, index) {
      this.$router.push({
        path: '/tables/configFlow',
        query: {
          srcMha: this.initInfo.srcMha,
          srcMhaId: this.initInfo.srcMhaId,
          destMha: this.initInfo.destMha,
          applierGroupId: this.initInfo.applierGroupId,
          srcDc: this.initInfo.srcDc,
          destDc: this.initInfo.destDc,
          order: this.initInfo.order,
          dataMediaId: row.id,
          namespace: row.namespace,
          name: row.name
          // tableData: []
        }
      })
    },
    goToDeleteConfig (row, index) {
      // todo, json确认弹窗
      console.log(row)
      this.axios.delete('/api/drc/v1/dataMedia/dataMediaConfig/' + row.id)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('删除失败!')
          } else {
            window.alert('删除成功!')
            this.getAllTableVosInApplierGroup()
          }
        })
    }
  },
  created () {
    this.initInfo = {
      srcMha: this.$route.query.srcMha,
      srcMhaId: this.$route.query.srcMhaId,
      destMha: this.$route.query.destMha,
      applierGroupId: this.$route.query.applierGroupId,
      srcDc: this.$route.query.srcDc,
      destDc: this.$route.query.destDc,
      order: this.$route.query.order
    }
    console.log('initInfo:')
    console.log(this.initInfo)
    this.getAllTableVosInApplierGroup()
  }
}
</script>

<style scoped>

</style>

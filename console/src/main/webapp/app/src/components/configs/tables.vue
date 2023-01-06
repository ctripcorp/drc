<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/accessV2">DRC配置</BreadcrumbItem>
      <BreadcrumbItem to="/tables">高级配置</BreadcrumbItem>
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
        // todo init
        srcMha: '',
        destMha: '',
        applierGroupId: 0,
        srcDc: '',
        destDc: ''
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
      // todo getAllTableVosInApplierGroup
      // console.log(this.drc.applierGroupId)
      // this.axios.get('/api/drc/v1/table/rowsFilterMappings/' + this.drc.applierGroupId)
      //   .then(response => {
      //     if (response.data.status === 1) {
      //       window.alert('查询行过滤配置失败!')
      //     } else {
      //       this.rowsFilterConfigsData = response.data.data
      //     }
      //   })
    },
    goToTableConfigFlow () {
      this.$router.push(
        this.$router.push({
          path: '/tables/configFlow',
          query: {
            commonInfo: {
              srcMha: this.initInfo.srcMha,
              destMha: this.initInfo.destMha,
              applierGroupId: this.initInfo.applierGroupId,
              srcDc: this.initInfo.srcDc,
              destDc: this.initInfo.destDc,
              dataMediaId: 0,
              schema: '',
              table: ''
            }
          }
        })
      )
    },
    goToShowConfig (row, index) {
      this.$router.push(
        this.$router.push({
          path: '/tables/configFlow',
          query: {
            commonInfo: {
              srcMha: this.initInfo.srcMha,
              destMha: this.initInfo.destMha,
              applierGroupId: this.initInfo.applierGroupId,
              srcDc: this.initInfo.srcDc,
              destDc: this.initInfo.destDc,
              dataMediaId: row.dataMediaId,
              namespace: row.namespace,
              name: row.name
            }
          }
        })
      )
    },
    goToUpdateConfig (row, index) {
      // todo
    },
    goToDeleteConfig (row, index) {
      // todo
    }
  },
  created () {
    // todo 展示优化
    this.initInfo = this.$route.query.initInfo
    this.getAllTableVosInApplierGroup()
  }
}
</script>

<style scoped>

</style>

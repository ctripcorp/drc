<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/apply">DAL集群</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Table stripe :columns="columns" :data="dataWithPage">
          <template slot-scope="{}" slot="drcStatus">
            <Tag color="default">未接入</Tag>
          </template>
          <template slot-scope="{ row, index }" slot="action">
            <Button type="primary" size="small" style="margin-right: 5px" @click="doApply(row, index)">申请接入</Button>
          </template>
        </Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :transfer="true"
            :total="total"
            :current.sync="current"
            show-sizer
            @on-change="getCluster"
            @on-page-size-change="handleChangeSize"></Page>
        </div>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'Application',
  data () {
    return {
      columns: [
        {
          title: '序号',
          width: 60,
          align: 'center',
          render: (h, params) => {
            return h(
              'span',
              params.index + 1 + (this.current - 1) * this.size
            )
          }
        },
        {
          title: '集群名称',
          key: 'clusterName'
        },
        {
          title: '状态',
          slot: 'drcStatus',
          align: 'center'
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center'
        }
      ],
      data: [],
      total: 0,
      current: 1,
      size: 10
    }
  },
  computed: {
    dataWithPage () {
      return [...this.data]
    }
  },
  methods: {
    getCluster () {
      const that = this

      this.axios.get('/api/drc/v1/clusters/all/count')
        .then(response => {
          console.log(response.data)
          that.total = response.data.data
        })

      this.axios.get('/api/drc/v1/clusters/all/pageNo/' + this.current + '/pageSize/' + this.size)
        .then(response => {
          setTimeout(() => {
            console.log(response.data)
            that.data = response.data.data
          }, 500)
        })
    },
    handleChangeSize (val) {
      this.size = val

      this.$nextTick(() => {
        this.getCluster()
      })
    },
    doApply (row, index) {
      console.log(row.clusterName)
      this.axios.get('/api/drc/v1/clusters/' + row.clusterName + '/mhas')
        .then(response => {
          setTimeout(() => {
            console.log(response.data)
          }, 500)
        })
    }
  },
  mounted () {
    this.getCluster()
  }
}
</script>

<style scoped>

</style>

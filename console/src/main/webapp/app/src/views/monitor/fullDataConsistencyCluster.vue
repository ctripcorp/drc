<template>
  <base-component :isFather="isFather" :subMenuName="['1']" :fatherMenu="fatherMenu">
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem>校验集群</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Table stripe :columns="columns" :data="dataWithPage">
        </Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :transfer="true"
            :total="mhaGroups.length"
            :current.sync="current"
            show-sizer
            show-elevator
            @on-page-size-change="handleChangeSize"></Page>
        </div>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'fullDataConsistencyCluster',
  data () {
    return {
      columns: [
        {
          title: '序号',
          width: 75,
          align: 'center',
          render: (h, params) => {
            return h(
              'span',
              params.index + 1 + (this.current - 1) * this.size
            )
          }
        },
        {
          title: '集群A',
          key: 'srcMha'
        },
        {
          title: '集群B',
          key: 'destMha'
        },
        {
          title: '全量数据校验',
          key: 'dataConsistentMonitorStatus',
          width: 120,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const type = 'success'
            const text = '详情'

            return h('Button', {
              props: {
                type: type,
                size: 'small'
              },
              on: {
                click: () => {
                  this.fullDataConsistencyCheck(row)
                }
              }
            }, text)
          }
        }
      ],
      mhaGroups: [],
      total: 0,
      current: 1,
      size: 10,
      isFather: true,
      fatherMenu: '/fullDataConsistencyCluster'
    }
  },
  computed: {
    dataWithPage () {
      const data = this.mhaGroups
      const start = this.current * this.size - this.size
      const end = start + this.size
      return [...data].slice(start, end)
    }
  },
  methods: {
    getMhaGroups () {
      const that = this
      that.axios.get('/api/drc/v1/meta/groups/all')
        .then(response => {
          console.log(response)
          that.total = response.data.data.length
          that.mhaGroups = response.data.data
        })
    },
    handleChangeSize (val) {
      this.size = val
    },
    fullDataConsistencyCheck (row) {
      this.$router.push({
        name: 'fullDataConsistencyCheck',
        query: { clusterA: row.srcMha, clusterB: row.destMha }
      })
    }
  },
  created () {
    this.getMhaGroups()
  }
}
</script>

<style scoped>

</style>

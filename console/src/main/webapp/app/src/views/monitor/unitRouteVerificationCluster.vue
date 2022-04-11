<template>
  <base-component :isFather="isFather" :subMenuName="['1']" :fatherMenu="fatherMenu">
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/unitRouteVerificationCluster">单元化路由校验</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Table stripe :columns="columns" :data="dataWithPage">
          <template slot-scope="{ row }" slot="result">
            <div>
              <Button type="success" size="small" :style="{margin: '5px 0px 20px 5px'}" @click="showCheckResult(row)">查看</Button>
              <a :href="hickwall" target="_blank">
                <img src="../../assets/hickwall.png" height="23" width="23" style="margin: 20px 0px 0px 6px">
              </a>
            </div>
          </template>
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
  name: 'unitRouteVerificationCluster',
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
          title: '操作',
          key: 'unitVerificationSwitch',
          width: 200,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const type = row.unitVerificationSwitch === 0 ? 'info' : 'warning'
            const text = row.unitVerificationSwitch === 0 ? '开启校验' : '校验中...'

            return h('Button', {
              props: {
                type: type,
                size: 'small'
              },
              on: {
                click: () => {
                  this.switchVerification(row)
                }
              }
            }, text)
          }
        },
        {
          title: '校验结果',
          slot: 'result',
          align: 'center'
          // render: (h, params) => {
          //   return h('div', [
          //     h('Button', {
          //       props: {
          //         type: 'success',
          //         size: 'small'
          //       },
          //       style: {
          //         marginRight: '5px'
          //       },
          //       on: {
          //         click: () => {
          //           this.showCheckResult(params.row)
          //         }
          //       }
          //     }, '查看')
          //   ])
          // }
        }
      ],
      mhaGroups: [],
      total: 0,
      current: 1,
      size: 10,
      isFather: true,
      fatherMenu: '/unitRouteVerificationCluster',
      hickwall: ''
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
      that.axios.get('/api/drc/v1/meta/orderedGroups/all?deleted=0')
        .then(response => {
          console.log(response)
          that.total = response.data.data.length
          that.mhaGroups = response.data.data
        })
    },
    handleChangeSize (val) {
      this.size = val
    },
    switchVerification (row) {
      const switchStatus = row.unitVerificationSwitch === 0 ? 'on' : 'off'
      this.axios.post('/api/drc/v1/monitor/unit/switches/' + switchStatus, {
        srcMha: row.srcMha,
        destMha: row.destMha
      }).then(res => {
        console.log('show result')
        console.log(res.data)
        if (res.data.status === 0) {
          row.unitVerificationSwitch = 1 - row.unitVerificationSwitch
        }
      })
    },
    showCheckResult (row) {
      this.$router.push({
        name: 'unitRouteVerificationResult',
        query: { clusterA: row.srcMha, clusterB: row.destMha }
      })
    },
    setHickwallAddress () {
      const that = this
      this.axios.get('/api/drc/v1/unit/result/hickwall').then(
        res => {
          console.log(res.data.data)
          that.hickwall = res.data.data
        }
      )
    }
  },
  created () {
    this.getMhaGroups()
    this.setHickwallAddress()
  }
}
</script>

<style scoped>

</style>

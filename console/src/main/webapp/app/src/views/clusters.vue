<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/clusters">MHA集群</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <i-input :style="{margin:'10px, 0, 0, 0', width: '200px'}" v-model="keyWord" placeholder="输入MHA集群名称" v-on:input="getCluster">
          <Icon type="ios-search" slot="prefix" />
        </i-input>
        <Table stripe :columns="columns" :data="clusters[current-1]">
          <template slot-scope="{}" slot="drcStatus">
            <Tag color="default">未接入</Tag>
          </template>
          <template slot-scope="{ row, index }" slot="action">
            <Button type="warning" size="small" style="margin-right: 5px" @click="preCheck(row, index)">预检测</Button>
            <Button type="primary" size="small" style="margin-right: 5px" @click="doApply(row, index)">显示详情</Button>
            <Modal
              v-model="cluster.modal"
              title="预检测结果"
              width="900px">
              <Form style="width: 100%">
                <FormItem label="不含ON UPDATE的表">
                  <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="cluster.noOnUpdate" readonly/>
                </FormItem>
                <FormItem label="ON UPDATE字段未加索引的表">
                  <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="cluster.noOnUpdateKey" readonly/>
                </FormItem>
                <FormItem label="不含PK或者UK的表">
                  <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="cluster.noPkUk" readonly/>
                </FormItem>
                <FormItem label="GTID_MODE">
                  <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="cluster.gtidMode" readonly/>
                </FormItem>
                <FormItem label="BINLOG_TRANSACTION_DEPENDENCY">
                  <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="cluster.binlogTransactionDependency" readonly/>
                </FormItem>
                <FormItem label="APPROVED TRUNCATE LIST">
                  <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="cluster.approvedTruncatelist" readonly/>
                </FormItem>
              </Form>
            </Modal>
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
      cluster: {
        noOnUpdate: '',
        noOnUpdateKey: '',
        noPkUk: '',
        gtidMode: '',
        binlogTransactionDependency: '',
        approvedTruncatelist: '',
        modal: false
      },
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
          title: '集群名称',
          key: 'cluster'
        },
        {
          title: '集群区域',
          key: 'zoneId'
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
      clusters: [],
      total: 0,
      current: 1,
      size: 10,
      keyWord: ''
    }
  },
  computed: {
  },
  methods: {
    getCluster () {
      const that = this
      that.axios.get('/api/drc/v1/mha/mhanames?keyWord=' + this.keyWord)
        .then(response => {
          console.log(response)
          that.total = response.data.data.length
          let n = 0
          let temp = []
          that.clusters = []
          response.data.data.forEach((item, index) => {
            if (n === that.size - 1) {
              temp.push({ cluster: item.cluster, zoneId: item.zoneId })
              that.clusters.push(temp)
              n = 0
              temp = []
            } else {
              temp.push({ cluster: item.cluster, zoneId: item.zoneId })
              n++
            }
          })
          that.clusters.push(temp)
        })
    },
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getCluster()
      })
    },
    doApply (row, index) {
      console.log(row.cluster)
      let zone = ''
      switch (row.zoneId) {
        case '上海欧阳IDC(电信)': zone = 'shaoy'; break
        case '上海金钟路B栋': zone = 'shajz'; break
        case '上海日阪IDC(联通)': zone = 'sharb'; break
        case '上海金桥IDC(联通)': zone = 'shajq'; break
        case '上海福泉路': zone = 'shafq'; break
        case '南通星湖大道': zone = 'ntgxh'; break
      }
      this.$router.push({ path: '/access', query: { clustername: row.cluster, zoneId: zone } })
    },
    preCheck (row, index) {
      console.log(row.cluster)
      this.$Spin.show({
        render: (h) => {
          return h('div', [
            h('Icon', {
              class: 'demo-spin-icon-load',
              props: {
                size: 18
              }
            }),
            h('div', '检测中，请稍等...')
          ])
        }
      })
      this.axios.post('/api/drc/v1/access/precheck', {
        clustername: row.cluster
      }).then(response => {
        const result = response.data.data.result
        console.log(result)
        this.cluster.noOnUpdate = result.noOnUpdate
        this.cluster.noOnUpdateKey = result.noOnUpdateKey
        this.cluster.noPkUk = result.noPkUk
        this.cluster.gtidMode = result.gtidMode
        this.cluster.binlogTransactionDependency = result.binlogTransactionDependency
        this.cluster.approvedTruncatelist = result.approvedTruncatelist
        this.$Spin.hide()
        this.cluster.modal = true
      })
    }
  },
  created () {
    this.getCluster()
  }
}
</script>

<style scoped>

</style>

<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/deletedDrcClusters">DRC软删除集群</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Table stripe :columns="columns" :data="dataWithPage" border :span-method="handleSpan" >
          <template slot-scope="{ row, index }" slot="action">
            <Button type="success" size="small" style="margin-right: 5px" @click="checkConfig(row, index)">查看</Button>
            <Button type="error" size="small" style="margin-right: 5px" @click="previewRecoverConfig(row, index)">恢复并配置drc</Button>
          </template>
        </Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :transfer="true"
            :total="total"
            :current.sync="current"
            :page-size="size"
            show-sizer
            show-elevator
            @on-page-size-change="handleChangeSize"></Page>
        </div>
      </div>
      <Modal
        v-model="cluster.modal.config"
        title="DRC历史配置"
        width="1200px">
        <Form style="width: 100%">
          <FormItem label="集群配置">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="cluster.config" readonly/>
          </FormItem>
        </Form>
      </Modal>
      <Modal
        v-model="cluster.modal.recover"
        title="DRC历史配置"
        width="1200px"
        @on-ok="recoverConfig"
        @on-cancel="clearRecoverConfig">
        <Form style="width: 100%">
          <FormItem label="确认恢复双向复制并进入Drc配置吗？" id="fontsize">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="cluster.config" readonly/>
          </FormItem>
        </Form>
      </Modal>
    </Content>
  </base-component>
</template>

<script>
export default {
  data () {
    return {
      cluster: {
        config: '',
        mhaAToBeRecovered: '',
        mhaBToBeRecovered: '',
        modal: {
          config: false,
          recover: false,
          recover2: false
        }
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
          title: '集群A',
          key: 'srcMha'
        },
        {
          title: '集群B',
          key: 'destMha'
        },
        {
          title: '删除状态',
          key: 'deleted',
          width: 150,
          align: 'center',
          render: (h, params) => {
            const color = 'volcano'
            const text = '已删除'
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center'
        }
      ],
      mhaGroups: [],
      total: 0,
      current: 1,
      size: 10,
      mergeColData: []
    }
  },
  computed: {
    dataWithPage () {
      const data = this.mhaGroups
      const mergeData = this.mergeColData
      const start = this.current * this.size - this.size
      let end = start + this.size
      if (end >= this.total) {
        end = this.total
      }
      for (let i = start; i < end; i++) {
        if (mergeData[i] + i > end) {
          data[i].mergeCol = end - i
          // console.log('i: ' + i)
          // console.log('data[i].mergeCol: ' + data[i].mergeCol)
        } else {
          data[i].mergeCol = mergeData[i]
        }
      }
      if (start >= this.size) {
        for (let preI = start - this.size; preI < start; preI++) {
          // console.log('preI:' + preI)
          // console.log('data[preI].mergeCol' + mergeData[preI])
          if (mergeData[preI] + preI > start) {
            data[start].mergeCol = mergeData[preI] + preI - start
            break
          }
        }
      }
      // console.log('start: ' + start)
      // console.log('end: ' + end)
      return [...data].slice(start, end)
    }
  },
  methods: {
    handleSpan ({ row, column, rowIndex, columnIndex }) {
      if (columnIndex === 1) {
        const x = row.mergeCol === 0 ? 0 : row.mergeCol
        const y = row.mergeCol === 0 ? 0 : 1
        // console.log(x , y)
        return [x, y]
      }
    },
    assembleData (data) {
      const names = []
      data.forEach(e => {
        if (!names.includes(e.srcMha)) {
          names.push(e.srcMha)
        }
      })
      const nameNums = []
      names.forEach(e => {
        nameNums.push({ srcMha: e, num: 0 })
      })
      data.forEach(e => {
        nameNums.forEach(n => {
          if (e.srcMha === n.srcMha) {
            n.num++
          }
        })
      })
      data.forEach(e => {
        nameNums.forEach(n => {
          if (e.srcMha === n.srcMha) {
            if (names.includes(e.srcMha)) {
              e.mergeCol = n.num
              this.mergeColData.push(e.mergeCol)
              names.splice(names.indexOf(n.srcMha), 1)
            } else {
              e.mergeCol = 0
              this.mergeColData.push(e.mergeCol)
            }
          }
        })
      })
      const tmp = data
      this.mhaGroups = tmp
      console.log('assemble')
    },
    getDeletedMhaGroups () {
      this.axios.get('/api/drc/v1/meta/orderedDeletedGroups/all')
        .then(response => {
          this.mhaGroups = response.data.data
          this.total = this.mhaGroups.length
          this.assembleData(this.mhaGroups)
        })
      this.total = this.mhaGroups.length
      this.assembleData(this.mhaGroups)
    },
    handleChangeSize (val) {
      this.size = val
    },
    checkConfig (row, index) {
      console.log(row.srcMha)
      console.log(row.destMha)
      this.$Spin.show({
        render: (h) => {
          return h('div', [
            h('Icon', {
              class: 'demo-spin-icon-load',
              props: {
                size: 18
              }
            }),
            h('div', '请稍等...')
          ])
        }
      })
      this.axios.get('/api/drc/v1/meta/config/deletedMhas/' + row.srcMha + ',' + row.destMha).then(response => {
        const data = response.data.data
        console.log(data)
        this.cluster.config = data
        this.$Spin.hide()
        this.cluster.modal.config = true
      })
    },
    previewRecoverConfig (row, index) {
      console.log(row.srcMha)
      console.log(row.destMha)
      this.cluster.mhaAToBeRecovered = row.srcMha
      this.cluster.mhaBToBeRecovered = row.destMha
      this.$Spin.show({
        render: (h) => {
          return h('div', [
            h('Icon', {
              class: 'demo-spin-icon-load',
              props: {
                size: 18
              }
            }),
            h('div', '请稍等...')
          ])
        }
      })
      this.axios.get('/api/drc/v1/meta/config/deletedMhas/' + row.srcMha + ',' + row.destMha).then(response => {
        const data = response.data.data
        console.log(data)
        this.cluster.config = data
        this.$Spin.hide()
        this.cluster.modal.recover = true
      })
    },
    recoverConfig () {
      console.log('mhaAToBeRecovered', this.cluster.mhaAToBeRecovered)
      console.log('mhaBToBeRecovered', this.cluster.mhaBToBeRecovered)
      this.$Spin.show({
        render: (h) => {
          return h('div', [
            h('Icon', {
              class: 'demo-spin-icon-load',
              props: {
                size: 18
              }
            }),
            h('div', '请稍等...进入drc配置选择页面')
          ])
        }
      })
      this.axios.post('/api/drc/v1/meta/config/recoverMhas/' + this.cluster.mhaAToBeRecovered + ',' + this.cluster.mhaBToBeRecovered).then(response => {
        const data = response.data.data
        console.log(data)
        if (data) {
          location.reload()
        }
        this.$Spin.hide()
      }).then(this.goToLink({ srcMha: this.cluster.mhaAToBeRecovered, destMha: this.cluster.mhaBToBeRecovered }))
      this.clearRecoverConfig()
    },
    goToLink (data) {
      console.log('go to change config for ' + data.srcMha + ' and ' + data.destMha)
      this.$router.push({ path: '/access', query: { step: '3', clustername: data.srcMha, newclustername: data.destMha } })
    },
    clearRecoverConfig () {
      console.log('clear mhaAToBeRecovered', this.cluster.mhaAToBeRecovered)
      console.log('clear mhaBToBeRecovered', this.cluster.mhaBToBeRecovered)
      this.cluster.mhaAToBeRecovered = ''
      this.cluster.mhaBToBeRecovered = ''
    },
    moreOperation (row) {
      this.$router.push({
        name: 'incrementDataConsistencyCheck',
        query: { clusterA: row.srcMha, clusterB: row.destMha }
      })
    }
  },
  created () {
    this.getDeletedMhaGroups()
  }
}
</script>

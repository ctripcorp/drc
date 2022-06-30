<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/drcclusters">DRC集群</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Card>
          机房: <Select v-model="searchCondition.dcIds"  style="width: 200px" placeholder="默认全部" multiple @on-change="getMhaGroups" >
                      <Option v-for="item in dcs" :value="item.id" :key="item.dcName" >{{ item.dcName }}</Option>
                  </Select>
          部门：<Select v-model="searchCondition.buId"  style="width: 200px" placeholder="默认全部" @on-change="getMhaGroups">
                  <Option v-for="item in bus" :value="item.id" :key="item.buName" >{{ item.buName }}</Option>
              </Select>
          类型：<Select v-model="searchCondition.type"  style="width: 200px" placeholder="默认全部" @on-change="getMhaGroups">
                    <Option v-for="item in searchOption.types" :value="item.value" :key="item.label" >{{ item.label }}</Option>
                </Select>
          <br/>
          <br/>
          Mhas：<Input :style="{width: '200px', marginRight: '10px'}" placeholder="默认全部，逗号分隔" v-model="searchCondition.mhas" />
          cluster：<Input :style="{width: '200px', marginRight: '10px'}" placeholder="默认全部"  v-model="searchCondition.clusterName" />
          <Button :style="{marginLeft: '50px'}" type="primary" @click="getMhaGroups">查询</Button>
          <Button :style="{marginLeft: '20px'}" type="primary" @click="reset">重置</Button>
          <br/>
          <br/>
          <Table stripe :columns="columns" :data="dataWithPage" border :span-method="handleSpan" >
            <template slot-scope="{ row, index }" slot="action">
              <Button type="success" size="small" style="margin-right: 5px" @click="checkConfig(row, index)">查看</Button>
              <Button type="primary" size="small" style="margin-right: 5px" @click="goToLink(row, index)">修改</Button>
              <Button type="error" size="small" style="margin-right: 5px" @click="previewRemoveConfig(row, index)">删除</Button>
            </template>
          </Table>
          <div style="text-align: center;margin: 16px 0">
            <Page
              :transfer="true"
              :total="total"
              :current.sync="current"
              :page-size="size"
              :page-size-opts="[10,20,40,80,100]"
              show-sizer
              show-elevator
              @on-page-size-change="handleChangeSize"></Page>
          </div>
        </Card>
      </div>
      <Modal
        v-model="cluster.modal.config"
        title="DRC配置"
        width="1200px">
        <Form style="width: 100%">
          <FormItem label="集群配置">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="cluster.config" readonly/>
          </FormItem>
        </Form>
      </Modal>
      <Modal
        v-model="cluster.modal.remove"
        title="删除DRC配置"
        width="1200px"
        @on-ok="removeConfig"
        @on-cancel="clearRemoveConfig">
        <Form style="width: 100%">
          <FormItem label="确认删除以下双向复制吗？">
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
      switchOneInfo: {},
      cluster: {
        config: '',
        mhaAToBeRemoved: '',
        mhaBToBeRemoved: '',
        modal: {
          config: false,
          remove: false
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
          title: '类型',
          key: 'type',
          width: 100,
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.type === 'duplex' ? '双向' : row.type === 'simplex' ? '单向' : 'null'
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '部门',
          key: 'buId',
          width: 100,
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = this.buIdMaps.get(row.buId)
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '状态',
          key: 'drcEstablishStatus',
          width: 100,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const color = row.drcEstablishStatus === 60 ? 'blue' : 'volcano'
            const text = row.drcEstablishStatus === 60 ? '已接入' : '未接入'
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '监控',
          key: 'monitorSwitch',
          width: 100,
          align: 'center',
          render: (h, params) => {
            const row = params.row

            return h('i-switch', {
              props: {
                size: 'large',
                value: row.monitorSwitch === 1,
                beforeChange: this.handleBeforeChange
              },
              scopedSlots: {
                open: () => h('span', '开启'),
                close: () => h('span', '关闭')
              },
              on: {
                'on-change': () => {
                  this.switchMonitor(row)
                }
              },
              nativeOn: {
                mousedown: () => { // 监听组件原生事件mousedown,此事件在click之前触发
                  this.switchOneInfo = {
                    monitorSwitch: row.monitorSwitch
                  }
                }
              }
            })
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
      size: 100,
      mergeColData: [],
      bus: [],
      buIdMaps: new Map(),
      dcs: [],
      searchCondition: {
        mhas: null,
        dcIds: [],
        clusterName: null,
        buId: null,
        type: null
      },
      searchOption: {
        types: [
          {
            label: '单向',
            value: 'simplex'
          },
          {
            label: '双向',
            value: 'duplex'
          },
          {
            label: '未配置',
            value: 'noConfig'
          }
        ]
      }
    }
  },
  computed: {
    dataWithPage () {
      console.log('dataWithPage')
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
        // console.log(x, y)
        return [x, y]
      }
    },
    assembleData (data) {
      this.mergeColData = []
      console.log('assembleData')
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
    },
    getMhaGroups () {
      const that = this
      let uri = '/api/drc/v1/meta/orderedGroups/all?deleted=0'
      if (this.searchCondition.mhas !== null && this.searchCondition.mhas !== undefined) {
        uri = uri + '&mhas=' + this.searchCondition.mhas
      }
      if (this.searchCondition.dcIds !== null && this.searchCondition.dcIds !== undefined && this.searchCondition.dcIds.length !== 0) {
        uri = uri + '&dcIds=' + this.searchCondition.dcIds
      }
      if (this.searchCondition.clusterName !== null && this.searchCondition.clusterName !== undefined) {
        uri = uri + '&clusterName=' + this.searchCondition.clusterName
      }
      if (this.searchCondition.buId !== null && this.searchCondition.buId !== undefined) {
        uri = uri + '&buId=' + this.searchCondition.buId
      }
      if (this.searchCondition.type !== null && this.searchCondition.type !== undefined) {
        uri = uri + '&type=' + this.searchCondition.type
      }
      this.axios.get(uri)
        .then(response => {
          this.mhaGroups = response.data.data
          that.total = this.mhaGroups.length
          that.assembleData(this.mhaGroups)
        })
    },
    reset () {
      this.searchCondition.mhas = null
      this.searchCondition.dcIds = []
      this.searchCondition.clusterName = null
      this.searchCondition.buId = null
      this.searchCondition.type = null
    },
    getBus () {
      this.axios.get('/api/drc/v1/meta/bus/all')
        .then(response => {
          this.bus = response.data.data
          this.buIdMaps = new Map()
          this.bus.forEach((item, index) => {
            this.buIdMaps.set(item.id, item.buName)
          })
        })
    },
    getDcs () {
      this.axios.get('/api/drc/v1/meta/dcs/all')
        .then(response => {
          this.dcs = response.data.data
          console.log(this.dcs)
        })
    },
    handleChangeSize (val) {
      this.size = val
    },
    goToLink (row, index) {
      console.log('go to change config for ' + row.srcMha + ' and ' + row.destMha)
      // this.$router.push({ path: '/access', query: { step: '3', clustername: row.srcMha, newclustername: row.destMha } })
      this.$router.push({ path: '/accessV2', query: { step: '3', clustername: row.srcMha, newclustername: row.destMha } })
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
      this.axios.get('/api/drc/v1/meta/config/mhas/' + row.srcMha + ',' + row.destMha).then(response => {
        const data = response.data.data
        console.log(data)
        this.cluster.config = data
        this.$Spin.hide()
        this.cluster.modal.config = true
      })
    },
    previewRemoveConfig (row, index) {
      console.log(row.srcMha)
      console.log(row.destMha)
      this.cluster.mhaAToBeRemoved = row.srcMha
      this.cluster.mhaBToBeRemoved = row.destMha
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
      this.axios.get('/api/drc/v1/meta/config/mhas/' + row.srcMha + ',' + row.destMha).then(response => {
        const data = response.data.data
        console.log(data)
        this.cluster.config = data
        this.$Spin.hide()
        this.cluster.modal.remove = true
      })
    },
    removeConfig () {
      console.log('mhaAToBeRemoved', this.cluster.mhaAToBeRemoved)
      console.log('mhaBToBeRemoved', this.cluster.mhaBToBeRemoved)
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
      this.axios.delete('/api/drc/v1/meta/config/remove/mhas/' + this.cluster.mhaAToBeRemoved + ',' + this.cluster.mhaBToBeRemoved).then(response => {
        const data = response.data.data
        console.log(data)
        if (data) {
          location.reload()
        }
        this.$Spin.hide()
      })
      this.clearRemoveConfig()
    },
    clearRemoveConfig () {
      console.log('clear mhaAToBeRemoved', this.cluster.mhaAToBeRemoved)
      console.log('clear mhaBToBeRemoved', this.cluster.mhaBToBeRemoved)
      this.cluster.mhaAToBeRemoved = ''
      this.cluster.mhaBToBeRemoved = ''
    },
    moreOperation (row) {
      this.$router.push({
        name: 'incrementDataConsistencyCheck',
        query: { clusterA: row.srcMha, clusterB: row.destMha }
      })
    },
    handleBeforeChange () {
      console.log('handleBeforeChange:', this.switchOneInfo)
      return new Promise((resolve) => {
        this.$Modal.confirm({
          title: '切换确认',
          content: '您确认要切换开关状态吗？',
          onOk: () => {
            resolve()
          }
        })
      })
    },
    switchMonitor (row) {
      const mhaGroupIds = []
      mhaGroupIds.push(row.mhaGroupId)
      const switchStatus = row.monitorSwitch === 0 ? 'on' : 'off'
      this.switchMonitors(mhaGroupIds, switchStatus)
    },
    switchMonitors (mhaGroupIds, status) {
      console.log(mhaGroupIds)
      this.axios.post('/api/drc/v1/monitor/switches/' + status,
        {
          mhaGroupIds: mhaGroupIds
        }
      ).then(res => {
        if (res.data.status === 0) {
          console.log(status)
          if (status === 'on') {
            this.$Message.info('监控开启成功')
          } else {
            this.$Message.info('监控关闭成功')
          }
        } else {
          this.$Message.info('监控操作失败')
        }
        this.getMhaGroups()
      })
    }
  },
  created () {
    this.getMhaGroups()
    this.getBus()
    this.getDcs()
  }
}
</script>

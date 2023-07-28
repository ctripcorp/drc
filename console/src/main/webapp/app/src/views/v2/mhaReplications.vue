<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/mhaReplications">MHA 复制链路</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px ">
        <div style="padding: 1px 1px; margin: 1px">
          源头 MHA：
          <Input prefix="ios-search" style="width: auto" v-model="srcMha.name" placeholder="集群名"
                 v-on:input="getReplications">
          </Input>
          <Select clearable v-model="srcMha.buId" style="width: 100px" placeholder="部门：" @on-change="getReplications">
            <Option v-for="item in bus" :value="item.id" :key="item.buName">{{ item.buName }}</Option>
          </Select>
          <Select clearable v-model="srcMha.regionId" style="width: 100px" placeholder="地域："
                  @on-change="getReplications">
            <Option v-for="item in regions" :value="item.id" :key="item.regionName">{{ item.regionName }}</Option>
          </Select>
        </div>
        <div style="padding: 1px 1px; margin: 1px">
          目标 MHA：
          <Input prefix="ios-search" style="width: auto" v-model="dstMha.name" placeholder="集群名"
                 v-on:input="getReplications">
            <Icon type="ios-search" slot="prefix"/>
          </Input>
          <Select clearable v-model="dstMha.buId" style="width: 100px" placeholder="部门：" @on-change="getReplications">
            <Option v-for="item in bus" :value="item.id" :key="item.buName">{{ item.buName }}</Option>
          </Select>
          <Select clearable v-model="dstMha.regionId" style="width: 100px" placeholder="地域："
                  @on-change="getReplications">
            <Option v-for="item in regions" :value="item.id" :key="item.regionName">{{ item.regionName }}</Option>
          </Select>
        </div>

        <Table stripe :columns="columns" :data="replications">
          <template slot-scope="{ row, index }" slot="action">
            <Button disabled type="success" size="small" style="margin-right: 5px" @click="checkConfig(row, index)">
              查看
            </Button>
            <Button disabled type="primary" size="small" style="margin-right: 5px" @click="goToLink(row, index)">
              修改
            </Button>
            <Button disabled type="error" size="small" style="margin-right: 5px"
                    @click="previewRemoveConfig(row, index)">
              删除
            </Button>
          </template>
        </Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :transfer="true"
            :total="total"
            :current.sync="current"
            show-total
            show-sizer
            @on-change="getReplications"
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
          key: 'replicationId',
          width: 100
        },
        {
          title: '源集群名称',
          key: 'srcMhaName'
        },
        {
          title: '目标集群名称',
          key: 'dstMhaName'
        },
        {
          title: '部门',
          key: 'buText',
          render: (h, params) => {
            const row = params.row
            let color = 'blue'
            let text = this.buIdMaps.get(row.srcBuId)
            if (row.srcBuId !== row.dstBuId) {
              color = 'red'
              text = this.buIdMaps.get(row.srcBuId) + ' -> ' + this.buIdMaps.get(row.dstBuId)
            }
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
            const color = row.status === 1 ? 'blue' : 'volcano'
            const text = row.status === 1 ? '已接入' : '未接入'
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '源集群监控',
          key: 'srcMhaMonitorSwitch',
          width: 100,
          align: 'center',
          render: (h, params) => {
            const row = params.row

            return h('i-switch', {
              props: {
                size: 'large',
                value: row.srcMhaMonitorSwitch === 1,
                beforeChange: this.handleBeforeChange
              },
              scopedSlots: {
                open: () => h('span', '开启'),
                close: () => h('span', '关闭')
              },
              on: {
                'on-change': () => {
                  this.switchMonitor(row.srcMhaName, row.srcMhaMonitorSwitch)
                }
              },
              nativeOn: {
                mousedown: () => { // 监听组件原生事件mousedown,此事件在click之前触发
                  this.switchOneInfo = {
                    srcMhaMonitorSwitch: row.srcMhaMonitorSwitch
                  }
                }
              }
            })
          }
        },
        {
          title: '目标集群监控',
          key: 'dstMhaMonitorSwitch',
          width: 100,
          align: 'center',
          render: (h, params) => {
            const row = params.row

            return h('i-switch', {
              props: {
                size: 'large',
                value: row.dstMhaMonitorSwitch === 1,
                beforeChange: this.handleBeforeChange
              },
              scopedSlots: {
                open: () => h('span', '开启'),
                close: () => h('span', '关闭')
              },
              on: {
                'on-change': () => {
                  this.switchMonitor(row.dstMhaName, row.dstMhaMonitorSwitch)
                }
              },
              nativeOn: {
                mousedown: () => { // 监听组件原生事件mousedown,此事件在click之前触发
                  this.switchOneInfo = {
                    dstMhaMonitorSwitch: row.dstMhaMonitorSwitch
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
      total: 0,
      current: 1,
      size: 10,
      replications: [],
      srcMha: {},
      dstMha: {},
      bus: [],
      regions: []
    }
  },
  computed: {},
  methods: {
    getBus () {
      this.axios.get('/api/drc/v2/meta/bus/all')
        .then(response => {
          this.bus = response.data.data
          this.buIdMaps = new Map()
          this.bus.forEach((item, index) => {
            this.buIdMaps.set(item.id, item.buName)
          })
        })
    },
    getRegions () {
      this.axios.get('/api/drc/v2/meta/regions/all')
        .then(response => {
          this.regions = response.data.data
          this.regionIdMaps = new Map()
          this.regions.forEach((item, index) => {
            this.regionIdMaps.set(item.id, item.regionName)
          })
        })
    },
    getReplications () {
      const that = this
      const params = {
        srcMha: this.srcMha,
        dstMha: this.dstMha,
        pageIndex: this.current,
        pageSize: this.size
      }
      const flattenObj = (ob) => {
        const result = {}
        for (const i in ob) {
          if ((typeof ob[i]) === 'object' && !Array.isArray(ob[i])) {
            const temp = flattenObj(ob[i])
            for (const j in temp) {
              result[i + '.' + j] = temp[j]
            }
          } else {
            result[i] = ob[i]
          }
        }
        return result
      }
      const reqParam = flattenObj(params)
      console.log(reqParam)
      that.axios.get('/api/drc/v2/replication/query', { params: reqParam })
        .then(response => {
          console.log(reqParam)
          const pageResult = response.data.data
          if (pageResult == null) {
            that.total = 0
            that.current = 1
            that.replications = []
          } else {
            that.total = pageResult.totalCount
            that.current = pageResult.pageIndex
            that.replications = pageResult.data
          }
        })
    },
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getReplications()
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
    switchMonitor (mhaName, status) {
      // 求反
      const switchStatus = status === 0 ? 'on' : 'off'
      this.doSwitchMonitor(mhaName, switchStatus)
    },
    doSwitchMonitor (mhaName, status) {
      console.log(mhaName)
      this.axios.post('/api/drc/v1/monitor/switch/' + mhaName + '/' + status).then(res => {
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
        this.getReplications()
      })
    }
  },
  created () {
    this.getReplications()
    this.getRegions()
    this.getBus()
  }
}
</script>

<style scoped>

</style>

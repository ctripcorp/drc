<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/mhaReplications">MHA 复制链路</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px ">
        <Row :gutter=10 align="middle">
          <Col span="10">
            <Card :padding=5>
              <template #title>源 MHA</template>
              <Row :gutter=10>
                <Col span="10">
                  <Input prefix="ios-search" v-model="srcMha.name" placeholder="集群名"
                         @on-enter="getReplications">
                  </Input>
                </Col>
                <Col span="7">
                  <Select filterable prefix="ios-home" clearable v-model="srcMha.buId" placeholder="部门"
                          @on-change="getReplications">
                    <Option v-for="item in bus" :value="item.id" :key="item.buName">{{ item.buName }}</Option>
                  </Select>
                </Col>
                <Col span="7">
                  <Select filterable prefix="ios-pin" clearable v-model="srcMha.regionId" placeholder="地域"
                          @on-change="getReplications">
                    <Option v-for="item in regions" :value="item.id" :key="item.regionName">
                      {{ item.regionName }}
                    </Option>
                  </Select>
                </Col>
              </Row>
            </Card>
          </Col>
          <Col span="10">
            <Card :padding=5>
              <template #title>目标 MHA</template>
              <Row :gutter=10>
                <Col span="10">
                  <Input prefix="ios-search" v-model="dstMha.name" placeholder="集群名"
                         @on-enter="getReplications">
                  </Input>
                </Col>
                <Col span="7">
                  <Select filterable prefix="ios-home" clearable v-model="dstMha.buId" placeholder="部门"
                          @on-change="getReplications">
                    <Option v-for="item in bus" :value="item.id" :key="item.buName">{{ item.buName }}</Option>
                  </Select>
                </Col>
                <Col span="7">
                  <Select filterable prefix="ios-pin" clearable v-model="dstMha.regionId"
                          placeholder="地域"
                          @on-change="getReplications">
                    <Option v-for="item in regions" :value="item.id" :key="item.regionName">{{
                        item.regionName
                      }}
                    </Option>
                  </Select>
                </Col>
              </Row>
            </Card>
          </Col>
          <Col span="3">
            <Button type="primary" icon="ios-search" @click="getReplications">Search</Button>
          </Col>
        </Row>
        <br>
        <Table :loading="dataLoading" stripe border :columns="columns" :data="replications" :span-method="handleSpan">
          <template slot-scope="{ row, index }" slot="action">
            <Button type="success" size="small" style="margin-right: 5px" @click="checkConfig(row, index)">
              查看
            </Button>
            <Button disabled type="primary" size="small" style="margin-right: 5px">
              修改
            </Button>
            <Button disabled type="error" size="small" style="margin-right: 5px">
              删除
            </Button>
          </template>
        </Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :transfer="true"
            :total="total"
            :current.sync="current"
            :page-size-opts="[10,20,50,100]"
            :page-size="10"
            show-total
            show-sizer
            show-elevator
            @on-change="getReplications"
            @on-page-size-change="handleChangeSize"></Page>
        </div>
        <Modal
          v-model="replicationDetailModal.show"
          title="DRC配置"
          width="1200px">
          <Form style="width: 100%">
            <FormItem label="集群配置">
              <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="replicationDetailModal.data"
                     readonly/>
            </FormItem>
          </Form>
        </Modal>
      </div>
    </Content>
  </base-component>
</template>

<script>
// eslint-disable-next-line no-unused-vars
import MhaGraph from '@/views/v2/mhaReplicationDetails.vue'

export default {
  name: 'Application',
  data () {
    return {
      columns: [
        {
          title: '类型',
          key: 'replicationId',
          width: 80,
          render: (h, params) => {
            const row = params.row
            let text = 'none'
            let type = 'error'
            switch (row.type) {
              case 'simplex':
                text = '单'
                type = 'info'
                break
              case 'duplex':
                text = '双'
                type = 'success'
                break
              default:
                text = 'none'
                break
            }
            return h('Button', {
              props: {
                type: type,
                size: 'small'
              },
              on: {
                click: () => {
                  this.showModal(row)
                }
              }
            }, text)
          }
        },
        {
          title: '源集群名',
          key: 'srcMhaName',
          render: (h, params) => {
            return h('p', params.row.srcMha.name)
          }
        },
        {
          title: '目标集群名',
          key: 'dstMhaName',
          render: (h, params) => {
            return h('p', params.row.dstMha.name)
          }
        },
        {
          title: '地域',
          key: 'regionText',
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.srcMha.regionName + ' -> ' + row.dstMha.regionName
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '部门',
          key: 'buText',
          render: (h, params) => {
            const row = params.row
            let color = 'blue'
            let text = row.srcMha.buName
            if (row.srcMha.buId !== row.dstMha.buId) {
              color = 'red'
              text = row.srcMha.buName + ' -> ' + row.dstMha.buName
            }
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '源监控',
          key: 'srcMhaMonitorSwitch',
          align: 'center',
          render: (h, params) => {
            const row = params.row

            return h('i-switch', {
              props: {
                size: 'large',
                value: row.srcMha.monitorSwitch === 1,
                beforeChange: this.handleBeforeChange
              },
              scopedSlots: {
                open: () => h('span', '开启'),
                close: () => h('span', '关闭')
              },
              on: {
                'on-change': () => {
                  this.switchMonitor(row.srcMha.name, row.srcMha.monitorSwitch)
                }
              },
              nativeOn: {
                mousedown: () => { // 监听组件原生事件mousedown,此事件在click之前触发
                  this.switchOneInfo = {
                    srcMhaMonitorSwitch: row.srcMha.monitorSwitch
                  }
                }
              }
            })
          }
        },
        {
          title: '目标监控',
          key: 'dstMhaMonitorSwitch',
          align: 'center',
          render: (h, params) => {
            const row = params.row

            return h('i-switch', {
              props: {
                size: 'large',
                value: row.dstMha.monitorSwitch === 1,
                beforeChange: this.handleBeforeChange
              },
              scopedSlots: {
                open: () => h('span', '开启'),
                close: () => h('span', '关闭')
              },
              on: {
                'on-change': () => {
                  this.switchMonitor(row.dstMha.mame, row.dstMha.monitorSwitch)
                }
              },
              nativeOn: {
                mousedown: () => { // 监听组件原生事件mousedown,此事件在click之前触发
                  this.switchOneInfo = {
                    dstMhaMonitorSwitch: row.dstMha.monitorSwitch
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
      // page
      total: 0,
      current: 1,
      size: 10,
      // query param
      srcMha: {},
      dstMha: {},
      // get from backend
      replications: [],
      bus: [],
      regions: [],
      // tags for frontend show
      replicationDetailModal: {
        show: false,
        data: {}
      },
      dataLoading: true
    }
  },
  computed: {},
  methods: {
    getBus () {
      this.axios.get('/api/drc/v2/meta/bus/all')
        .then(response => {
          this.bus = response.data.data
        })
    },
    getRegions () {
      this.axios.get('/api/drc/v2/meta/regions/all')
        .then(response => {
          this.regions = response.data.data
        })
    },
    flattenObj (ob) {
      const result = {}
      for (const i in ob) {
        if ((typeof ob[i]) === 'object' && !Array.isArray(ob[i])) {
          const temp = this.flattenObj(ob[i])
          for (const j in temp) {
            result[i + '.' + j] = temp[j]
          }
        } else {
          result[i] = ob[i]
        }
      }
      return result
    },
    getReplications () {
      const that = this
      const params = {
        srcMha: this.srcMha,
        dstMha: this.dstMha,
        pageIndex: this.current,
        pageSize: this.size
      }

      const reqParam = this.flattenObj(params)
      that.dataLoading = true
      that.axios.get('/api/drc/v2/replication/query', { params: reqParam })
        .then(response => {
          const pageResult = response.data.data
          if (!pageResult || pageResult.totalCount === 0) {
            that.total = 0
            that.current = 1
            that.replications = []
            that.$Message.warning('查询结果为空')
          } else {
            that.total = pageResult.totalCount
            that.current = pageResult.pageIndex
            that.calTableSpan(pageResult.data)
            that.replications = pageResult.data
            that.$Message.success('查询成功')
          }
        })
        .catch(message => {
          that.$Message.error('查询异常: ' + message)
        })
        .finally(() => {
          that.dataLoading = false
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
    },
    checkConfig (row, index) {
      console.log(row.srcMha.name)
      console.log(row.dstMha.name)
      this.dataLoading = true
      this.axios.get('/api/drc/v2/meta/queryConfig/mhaReplication', {
        params: {
          replicationId: row.replicationId
        }
      }).then(response => {
        const data = response.data.data
        console.log(data)
        this.replicationDetailModal.data = data
        this.replicationDetailModal.show = true
      }).catch(message => {
        this.$Message.error('查询异常: ' + message)
      }).finally(() => {
        this.dataLoading = false
      })
    },
    showModal (row) {
      console.log('show modal')
      this.$Modal.success({
        title: '节点拓扑信息',
        width: '1000',
        closable: true,
        render: (h) => {
          return h('div', [
            h('div', '点击可加载关联节点'),
            h(MhaGraph, {
              props: {
                mhaIdList: [row.srcMha.id, row.dstMha.id]
              }
            })
          ])
        }
      })
    },
    calTableSpan (replications) {
      console.log('calTableSpan')
      if (replications == null || replications.length === 0) {
        return
      }
      const length = replications.length

      calSrcMhaSpan()
      calDstMhaSpan()

      function calSrcMhaSpan () {
        let i = 0
        while (i < length) {
          let j = i + 1
          while (j < length && replications[i].srcMha.id === replications[j].srcMha.id) {
            replications[j].srcMha.rowSpan = 0
            replications[j].srcMha.colSpan = 0
            j++
          }
          replications[i].srcMha.rowSpan = j - i
          replications[i].srcMha.colSpan = 1
          i = j
        }
      }

      function calDstMhaSpan () {
        let i = 0
        while (i < length) {
          let j = i + 1
          while (j < length && replications[i].dstMha.id === replications[j].dstMha.id) {
            replications[j].dstMha.rowSpan = 0
            replications[j].dstMha.colSpan = 0
            j++
          }
          replications[i].dstMha.rowSpan = j - i
          replications[i].dstMha.colSpan = 1
          i = j
        }
      }
    },
    handleSpan ({ row, column, rowIndex, columnIndex }) {
      if (column.key === 'srcMhaName') {
        return [row.srcMha.rowSpan, row.srcMha.colSpan]
      } else if (column.key === 'dstMhaName') {
        return [row.dstMha.rowSpan, row.dstMha.colSpan]
      }
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

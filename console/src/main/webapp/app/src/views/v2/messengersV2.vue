<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/v2/messengersV2">MQ投递集群</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Card>
          <Table :loading="dataLoading" stripe :columns="columns" :data="dataWithPage" border>
            <template slot-scope="{ row, index }" slot="action">
              <Button type="success" size="small" style="margin-right: 5px" @click="checkConfig(row, index)">查看
              </Button>
              <Button type="primary" size="small" style="margin-right: 5px" @click="goToLink(row, index)">修改</Button>
              <Button type="error" size="small" style="margin-right: 5px" @click="previewRemoveConfig(row, index)">
                删除
              </Button>
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
      <Drawer title="DRC配置" width="80" :closable="true" v-model="cluster.drawer.show">
        <template #header>
          查看详情
          <div style="float:right;margin-right: 100px">
            自动换行
            <i-switch v-model="cluster.drawer.lineWrap"/>
            黑夜模式
            <i-switch v-model="cluster.drawer.darkMode"/>
          </div>
        </template>
        <div id="xmlCode">
          <codemirror
            v-model="cluster.drawer.data"
            class="code"
            :options="{
                  mode: 'xml',
                  theme: cluster.drawer.darkMode? 'monokai':'default',
                  autofocus: true,
                  lineWrapping: cluster.drawer.lineWrap,
                  readOnly: true,
                  lineNumbers: true,
                  foldGutter: true,
                  styleActiveLine: true,
                  gutters: ['CodeMirror-linenumbers', 'CodeMirror-foldgutter']
            }">
          </codemirror>
        </div>
      </Drawer>
      <Modal
        v-model="cluster.modal.remove"
        title="删除DRC配置"
        width="1200px"
        @on-ok="removeConfig"
        @on-cancel="clearRemoveConfig">
        <Form style="width: 100%">
          <FormItem label="确认删除改Messenger吗？">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="cluster.config" readonly/>
          </FormItem>
        </Form>
      </Modal>
    </Content>
  </base-component>
</template>

<script>
import 'codemirror/theme/monokai.css'
import 'codemirror/mode/xml/xml.js'

import 'codemirror/addon/fold/foldgutter.css'
import 'codemirror/addon/fold/foldgutter.js'

export default {
  name: 'messengers',
  data () {
    return {
      cluster: {
        modal: {
          remove: false,
          show: false,
          data: ''
        },
        drawer: {
          lineWrap: false,
          darkMode: true,
          show: false,
          data: ''
        },
        mhaToBeRemoved: ''
      },
      total: 0,
      current: 1,
      size: 100,
      tableData: [],
      switchOneInfo: {},
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
          title: '集群名',
          key: 'mhaName'
        },
        {
          title: '部门',
          key: 'bu',
          width: 100,
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.bu
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

      dataLoading: false
    }
  },
  computed: {
    dataWithPage () {
      const data = this.tableData
      const start = this.current * this.size - this.size
      const end = start + this.size
      return [...data].slice(start, end)
    }
  },
  methods: {
    getAllMessengerVos () {
      this.dataLoading = true
      this.axios.get('/api/drc/v2/messenger/all').then(response => {
        if (response.data.status === 1) {
          this.$Message.error('查询失败：' + response.message)
          return
        }
        this.tableData = response.data.data
        this.total = this.tableData.length
        this.$Message.success('查询成功')
      }).catch(message => {
        this.$Message.error('查询异常: ' + message)
      }).finally(() => {
        this.dataLoading = false
      })
    },
    handleChangeSize (val) {
      this.size = val
    },
    goToLink (row, index) {
      console.log('go to change messenger config for ' + row.mhaName)
      this.$router.push({ path: '/v2/buildMessengerV2', query: { step: '3', mhaName: row.mhaName } })
    },
    checkConfig (row, index) {
      console.log(row.mhaName)
      this.dataLoading = true
      this.axios.get('/api/drc/v2/meta/queryConfig/mhaMessenger', {
        params: {
          mhaName: row.mhaName
        }
      }).then(response => {
        if (response.data.status === 1) {
          this.$Message.error('查询失败：' + response.message)
          return
        }
        this.cluster.drawer.data = response.data.data
        this.cluster.drawer.show = true
        this.$Message.success('查询成功')
      }).catch(message => {
        this.$Message.error('查询异常: ' + message)
      }).finally(() => {
        this.dataLoading = false
      })
    },
    previewRemoveConfig (row, index) {
      this.cluster.mhaToBeRemoved = row.mhaName
      console.log(row.mhaName)
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
      this.axios.get('/api/drc/v2/meta/queryConfig/mhaMessenger', {
        params: {
          mhaName: row.mhaName
        }
      }).then(response => {
        const data = response.data.data
        console.log(data)
        this.cluster.config = data
        this.$Spin.hide()
        this.cluster.modal.remove = true
      })
    },
    removeConfig () {
      console.log('mhaToBeRemoved', this.cluster.mhaToBeRemoved)
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
      this.axios.delete('/api/drc/v2/messenger/deleteMha/?mhaName=' + this.cluster.mhaToBeRemoved).then(response => {
        if (response.data.status === 0) {
          location.reload()
        } else {
          alert('删除失败！')
        }
        this.$Spin.hide()
      })
      this.clearRemoveConfig()
    },
    clearRemoveConfig () {
      this.cluster.mhaToBeRemoved = ''
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
      // 取反
      const switchStatus = row.monitorSwitch === 0 ? 'on' : 'off'
      this.switchMonitors(row.mhaName, switchStatus)
    },
    switchMonitors (mhaName, status) {
      console.log(mhaName)
      this.axios.post('/api/drc/v2/monitor/switch/' + mhaName + '/' + status).then(res => {
        if (res.data.status === 0) {
          console.log(status)
          if (status === 'on') {
            this.$Message.success('监控开启成功')
          } else {
            this.$Message.success('监控关闭成功')
          }
        } else {
          this.$Message.warning('监控操作失败')
        }
        this.getAllMessengerVos()
      })
    }
  },
  created () {
    this.getAllMessengerVos()
  }
}
</script>

<style scoped>

</style>
<style lang="scss">
#xmlCode {
  .CodeMirror {
    overscroll-y: scroll !important;
    height: auto !important;
  }
}
</style>

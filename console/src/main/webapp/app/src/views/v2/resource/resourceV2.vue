<template xmlns:font-family="http://www.w3.org/1999/xhtml">
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px ">
        <Row :gutter=10 align="middle">
          <Col span="16">
            <Card :padding=5>
              <template #title>查询条件</template>
              <Row :gutter=10>
                <Col span="8">
                  <Input prefix="ios-search" v-model="queryParam.ip" placeholder="ip" @on-enter="getResources"></Input>
                </Col>
                <Col span="4">
                  <Select filterable clearable v-model="queryParam.type" placeholder="类型" @on-change="getResources">
                    <Option v-for="item in typeList" :value="item.val" :key="item.val">{{ item.name }}</Option>
                  </Select>
                </Col>
                <Col span="4">
                  <Select filterable clearable v-model="queryParam.region" placeholder="地域" @on-change="getResources">
                    <Option v-for="item in regions" :value="item.regionName" :key="item.regionName">
                      {{ item.regionName }}
                    </Option>
                  </Select>
                </Col>
                <Col span="4">
                  <Select filterable clearable v-model="queryParam.tag" placeholder="tag" @on-change="getResources">
                    <Option v-for="item in tagList" :value="item" :key="item">{{ item }}</Option>
                  </Select>
                </Col>
                <Col span="4">
                  <Select filterable clearable v-model="queryParam.active" placeholder="启用状态" @on-change="getResources">
                    <Option v-for="item in activeList" :value="item.val" :key="item.val">{{ item.name }}</Option>
                  </Select>
                </Col>
              </Row>
            </Card>
          </Col>
          <Col span="1">
            <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getResources">查询</Button>
            <Button icon="md-refresh" :loading="dataLoading" @click="resetParam" style="margin-top: 20px">重置</Button>
          </Col>
        </Row>
        <br>
        <Row  style="background: #fdfdff; border: 1px solid #e8eaec;">
          <Col span="2" style="display: flex;float: left;margin: 5px" >
            <Dropdown placement="bottom-start">
              <Button type="default" icon="ios-hammer">
                操作
                <Icon type="ios-arrow-down"></Icon>
              </Button>
              <template #list>
                <DropdownMenu >
                  <DropdownItem @click.native="() => {$router.push({path: '/drcResource'})}">资源录入</DropdownItem>
                </DropdownMenu>
              </template>
            </Dropdown>
          </Col>
        </Row>
        <Table stripe border :columns="columns" :data="resources">
          <template slot-scope="{ row, index }" slot="action">
            <Button type="error" size="small" style="margin-right: 5px" @click="preDeleteResource(row, index)">
              下线
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
            @on-change="getResources"
            @on-page-size-change="handleChangeSize"></Page>
        </div>
        <Modal
          v-model="deleteResourceModal"
          title="确认删除以下机器资源"
          @on-ok="deleteResource"
          @on-cancel="clearDeleteResource">
          <p>ip: "{{deleteResourceInfo.ip}}" ,tag: "{{deleteResourceInfo.tag}}" az: "{{deleteResourceInfo.az}}"</p>
          <p><span>部署实例数量: </span><span style="color: red;font-size: 20px">{{deleteResourceInfo.instanceNum}}</span></p>
        </Modal>
        <Modal
          v-model="showMha"
          title="关联mha">
          <List>
            <ListItem v-for="item in mhaInfo" :value="item" :key="item">{{ item }}</ListItem>
          </List>
        </Modal>
        <Modal
          v-model="showMhaReplication"
          title="关联mha复制链路">
          <List>
            <ListItem v-for="item in mhaReplicationInfo" :value="item" :key="item">
              {{item.srcMhaName}}({{item.srcDcName}}) ==> {{item.dstMhaName}}({{item.dstDcName}})
            </ListItem>
          </List>
        </Modal>
      </div>
    </Content>
  </base-component>
</template>

<script>
// eslint-disable-next-line no-unused-vars
import MhaGraph from '@/views/v2/meta/mhaReplicationDetails.vue'
import 'codemirror/theme/monokai.css'
import 'codemirror/mode/xml/xml.js'

import 'codemirror/addon/fold/foldgutter.css'
import 'codemirror/addon/fold/foldgutter.js'

export default {
  name: 'resourceV2',
  data () {
    return {
      columns: [
        {
          title: '类型',
          key: 'type',
          width: 80,
          render: (h, params) => {
            const row = params.row
            let text = 'none'
            let type = 'error'
            let disabled = false
            switch (row.type) {
              case 0:
                text = 'R'
                type = 'info'
                break
              case 1:
                text = 'A'
                type = 'success'
                break
              default:
                text = '无'
                disabled = true
                break
            }
            return h('Button', {
              props: {
                type: type,
                size: 'small',
                disabled: disabled
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
          title: 'ip',
          key: 'ip'
        },
        {
          title: 'tag',
          key: 'tag'
        },
        {
          title: 'AZ',
          key: 'az',
          sortable: true
        },
        {
          title: '部署实例数量',
          key: 'instanceNum',
          sortable: true
        },
        {
          title: '启用状态',
          key: 'active',
          sortable: true,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            return h('i-switch', {
              props: {
                size: 'large',
                value: row.active === 1,
                beforeChange: this.handleBeforeChange
              },
              scopedSlots: {
                open: () => h('span', '开启'),
                close: () => h('span', '关闭')
              },
              on: {
                'on-change': () => {
                  this.switchActiveStatus(row.resourceId, row.active)
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
      // get from backend
      resources: [],
      typeList: [
        {
          name: 'Replicator',
          val: 0
        },
        {
          name: 'Applier',
          val: 1
        }
      ],
      activeList: [
        {
          name: '启用',
          val: 1
        },
        {
          name: '停用',
          val: 0
        }
      ],
      regions: [],
      tagList: this.constant.tagList,
      deleteResourceModal: false,
      showMha: false,
      showMhaReplication: false,
      mhaInfo: [],
      mhaReplicationInfo: [],
      deleteResourceInfo: {
        resourceId: null,
        ip: '',
        tag: '',
        az: '',
        instanceNum: 0
      },
      queryParam: {
        ip: '',
        type: null,
        tag: '',
        active: null,
        region: '',
        pageReq: {
          pageSize: 10,
          pageIndex: 1
        }
      },
      // for detail show
      replicationDetail: {
        show: false,
        data: null,
        darkMode: true,
        lineWrap: false,
        row: {}
      },
      dataLoading: true
    }
  },
  computed: {},
  methods: {
    showModal (row) {
      if (row.type === 0) {
        this.getRelatedMha(row)
      } else if (row.type === 1) {
        this.getRelatedMhaReplication(row)
      }
    },
    getRegions () {
      this.axios.get('/api/drc/v2/meta/regions/all')
        .then(response => {
          this.regions = response.data.data
        })
    },
    resetParam () {
      this.queryParam = {
        ip: '',
        type: null,
        tag: '',
        active: null,
        region: '',
        pageReq: {
          pageSize: 10,
          pageIndex: 1
        }
      }
      this.getResources()
    },
    getRelatedMha (row) {
      this.axios.get('/api/drc/v2/resource/mha?resourceId=' + row.resourceId).then(res => {
        if (res.data.status === 0) {
          this.mhaInfo = res.data.data
        } else {
          this.$Message.warning('查询异常')
        }
      })
      this.showMha = true
    },
    getRelatedMhaReplication (row) {
      this.axios.get('/api/drc/v2/resource/mhaReplication?resourceId=' + row.resourceId).then(res => {
        if (res.data.status === 0) {
          this.mhaReplicationInfo = res.data.data
        } else {
          this.$Message.warning('查询异常')
        }
      })
      this.showMhaReplication = true
    },
    getResources () {
      const that = this
      const params = {
        ip: this.queryParam.ip,
        type: this.queryParam.type,
        tag: this.queryParam.tag,
        active: this.queryParam.active,
        region: this.queryParam.region,
        pageReq: {
          pageSize: this.size,
          pageIndex: this.current
        }
      }
      const reqParam = this.flattenObj(params)
      that.dataLoading = true
      that.axios.get('/api/drc/v2/resource/all', { params: reqParam })
        .then(response => {
          console.log('/api/drc/v2/resource/all response:' + response)
          if (response.data.status === 403) {
            window.location = '/#/nopermission?permissionCode=' + response.data.data
            return
          }
          const data = response.data
          const pageResult = response.data.pageReq
          if (data.status === 1) {
            that.$Message.error('查询异常: ' + data.message)
          } else if (data.data.length === 0 || pageResult.totalCount === 0) {
            that.total = 0
            that.current = 1
            that.resources = []
            that.$Message.warning('查询结果为空')
          } else {
            that.total = pageResult.totalCount
            that.current = pageResult.pageIndex
            // that.calTableSpan(pageResult.data)
            that.resources = data.data
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
        this.getResources()
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
    preDeleteResource (row, index) {
      this.deleteResourceInfo = {
        resourceId: row.resourceId,
        ip: row.ip,
        tag: row.tag,
        az: row.az,
        instanceNum: row.instanceNum
      }
      this.deleteResourceModal = true
    },
    clearDeleteResource () {
      this.deleteResourceInfo = {
        resourceId: null,
        ip: '',
        tag: '',
        az: '',
        instanceNum: 0
      }
    },
    deleteResource (row, index) {
      this.axios.delete('/api/drc/v2/resource/?resourceId=' + this.deleteResourceInfo.resourceId).then(res => {
        if (res.data.status === 0) {
          this.$Message.success('删除成功')
          this.getResources()
        } else {
          this.$Message.warning('下线失败 ' + res.data.message)
        }
      })
    },
    switchActiveStatus (resourceId, active) {
      if (active === 0) {
        this.activeResource(resourceId)
      } else {
        this.deactivateResource(resourceId)
      }
    },
    deactivateResource (resourceId) {
      this.axios.post('/api/drc/v2/resource/deactivate?resourceId=' + resourceId).then(res => {
        if (res.data.status === 0) {
          this.$Message.success('已停用')
        } else {
          this.$Message.warning('停用失败')
        }
        this.getResources()
      })
    },
    activeResource (resourceId) {
      this.axios.post('/api/drc/v2/resource/active?resourceId=' + resourceId).then(res => {
        if (res.data.status === 0) {
          this.$Message.success('已启用')
        } else {
          this.$Message.warning('启用失败')
        }
        this.getResources()
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
    }
  },
  created () {
    this.axios.get('/api/drc/v2/permission/resource/machine').then((response) => {
      if (response.data.status === 403) {
        this.$router.push('/nopermission')
        return
      }
      console.log('still execute')
      this.getResources()
      this.getRegions()
    })
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

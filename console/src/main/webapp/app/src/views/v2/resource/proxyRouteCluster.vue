<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/proxyRouteCluster">路由列表</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Card>
          <Row :gutter=10 align="middle">
            <Col span="16">
              <Card :padding=5>
                <template #title>查询条件</template>
                <Row :gutter=10>
                  <Col span="6">
                    <Select filterable clearable v-model="queryParam.buId" placeholder="BU" @on-change="getRoutes">
                      <Option v-for="item in bus" :value="item.id" :key="item.id">{{ item.buName }}</Option>
                    </Select>
                  </Col>
                  <Col span="6">
                    <Select filterable clearable v-model="queryParam.srcDcName" placeholder="源端（Applier）机房"
                            @on-change="getRoutes">
                      <Option v-for="item in drcZoneList" :value="item.value" :key="item.value">{{
                          item.label
                        }}
                      </Option>
                    </Select>
                  </Col>
                  <Col span="6">
                    <Select filterable clearable v-model="queryParam.dstDcName" placeholder="目标（Replicator）机房"
                            @on-change="getRoutes">
                      <Option v-for="item in drcZoneList" :value="item.value" :key="item.value">{{
                          item.label
                        }}
                      </Option>
                    </Select>
                  </Col>
                  <Col span="3">
                    <Select filterable clearable v-model="queryParam.tag" placeholder="tag" @on-change="getRoutes">
                      <Option v-for="item in tags" :value="item" :key="item">{{ item }}</Option>
                    </Select>
                  </Col>
                  <Col span="3">
                    <Select filterable v-model="queryParam.globalActive" placeholder="是否全局生效" @on-change="getRoutes">
                      <Option v-for="item in globalActiveList" :value="item.value" :key="item.value">{{
                          item.label
                        }}
                      </Option>
                    </Select>
                  </Col>
                </Row>
              </Card>
            </Col>
            <Col span="1">
              <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getRoutes">查询</Button>
              <Button icon="md-refresh" :loading="dataLoading" @click="resetParam" style="margin-top: 20px">重置
              </Button>
            </Col>
          </Row>
          <br>
          <Row style="background: #fdfdff; border: 1px solid #e8eaec;">
            <Col span="2" style="display: flex;float: left;margin: 5px">
              <Button type="default" icon="ios-hammer" @click="goToAddRoute">
                添加路由
              </Button>
            </Col>
          </Row>
          <Table stripe :columns="columns" :data="dataWithPage">
            <template slot-scope="{ row, index }" slot="action">
              <Dropdown :transfer="true" placement="bottom-start">
                <Button type="default" icon="ios-hammer">
                  操作
                  <Icon type="ios-arrow-down"></Icon>
                </Button>
                <template #list>
                  <DropdownMenu>
                    <DropdownItem>
                      <Button type="success" size="small" style="margin-right: 5px" @click="goToUpdate(row, index)">
                        修改
                      </Button>
                    </DropdownItem>
                    <DropdownItem>
                      <Button type="error" size="small" style="margin-right: 5px" @click="goToDelete(row, index)">
                        删除
                      </Button>
                    </DropdownItem>
                    <DropdownItem>
                      <Tooltip :transfer="true" content="修改路由全局生效状态,请谨慎操作!">
                        <Button type="warning" size="small" style="margin-right: 5px"
                                @click="preChangeRouteActive(row, index)">
                          修改全局生效状态
                        </Button>
                      </Tooltip>
                    </DropdownItem>
                  </DropdownMenu>
                </template>
              </Dropdown>
            </template>
          </Table>
          <div style="text-align: center;margin: 16px 0">
            <Page
              :transfer="true"
              :total="routes.length"
              :current.sync="current"
              show-sizer
              show-elevator
              @on-page-size-change="handleChangeSize"></Page>
          </div>
        </Card>
        <Modal
          v-model="dbModal"
          title="关联db"
          width="1000px" :scrollable="true" :draggable="true"
          @on-cancel="clearDbSelection"
          @on-ok="clearDbSelection">
          <Table stripe border :columns="dbColumns" :data="dbData" @on-selection-change="changeDbSelection"></Table>
          <Row style="background: #fdfdff; border: 1px solid #e8eaec;">
            <Col span="2" style="display: flex;float: left;margin: 5px">
              <Button type="primary" size="small" style="margin-right: 5px"
                      @click="deleteDbRoutes()">
                删除关联db链路
              </Button>
            </Col>
          </Row>
        </Modal>
        <Modal
          v-model="mhaModal"
          title="关联mha"
          width="800px" :scrollable="true" :draggable="true"
          @on-cancel="clearMhaSelection"
          @on-ok="clearMhaSelection">
          <Table stripe border :columns="mhaColumns" :data="dbData" @on-selection-change="changeMhaSelection"></Table>
          <Row style="background: #fdfdff; border: 1px solid #e8eaec;">
            <Col span="2" style="display: flex;float: left;margin: 5px">
              <Button type="primary" size="small" style="margin-right: 5px"
                      @click="deleteMhaRoutes()">
                删除关联mha
              </Button>
            </Col>
          </Row>
        </Modal>
        <Modal
          v-model="activeModal"
          title="修改路由规则全局状态"
          width="800px" :scrollable="true" :draggable="true"
          @on-ok="changeRouteActive">
          <Card :padding=5>
            <Row :gutter=20>
              <Col span="6">
                <label>是否全局生效：</label>
              </Col>
              <Col span="6">
                <label>{{ activeRow.activeText }}</label>
              </Col>
            </Row>
            <Row :gutter=20>
              <Col span="6">
                <label>关联db数：</label>
              </Col>
              <Col span="6">
                <label>{{ activeRow.relatedNum }}</label>
              </Col>
            </Row>
            <Row :gutter=20>
              <Col span="6">
                <label>BU：</label>
              </Col>
              <Col span="6">
                <label>{{ activeRow.routeOrgName }}</label>
              </Col>
            </Row>
            <Row :gutter=20>
              <Col span="6">
                <label>Applier源机房：</label>
              </Col>
              <Col span="14">
                <label>{{ activeRow.srcDcName }}</label>
              </Col>
            </Row>
            <Row :gutter=20>
              <Col span="6">
                <label>Replicator目标机房：</label>
              </Col>
              <Col span="14">
                <label>{{ activeRow.dstDcName }}</label>
              </Col>
            </Row>
            <Row :gutter=20>
              <Col span="6">
                <label>源端Proxy：</label>
              </Col>
              <Col span="14">
                <label>{{ activeRow.srcProxyUris }}</label>
              </Col>
            </Row>
            <Row :gutter=20>
              <Col span="6">
                <label>目标Proxy：</label>
              </Col>
              <Col span="14">
                <label>{{ activeRow.dstProxyUris }}</label>
              </Col>
            </Row>
            <Row :gutter=20>
              <Col span="6">
                <label>中继Proxy：</label>
              </Col>
              <Col span="14">
                <label>{{ activeRow.relayProxyUris }}</label>
              </Col>
            </Row>
          </Card>
        </Modal>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'proxyRouteCluster',
  data () {
    return {
      bus: [],
      drcZoneList: this.constant.dcList,
      tags: this.constant.routeTags,
      globalActiveList: [
        {
          value: 1,
          label: '是'
        },
        {
          value: 0,
          label: '否'
        }
      ],
      dataLoading: false,
      dbModal: false,
      mhaModal: false,
      activeModal: false,
      activeRow: {
        id: null,
        relatedNum: null,
        routeOrgName: '',
        srcDcName: '',
        dstDcName: '',
        srcProxyUris: '',
        dstProxyUris: '',
        relayProxyUris: '',
        globalActive: '',
        activeText: ''
      },
      dbData: [],
      multiData: [],
      multiDbData: [],
      multiMhaData: [],
      deleteMhaModal: false,
      deleteDbModal: false,
      selectedRow: null,
      mhaColumns: [
        {
          type: 'selection',
          width: 60,
          align: 'center'
        },
        {
          title: '序号',
          width: 75,
          align: 'center',
          // fixed: 'left',
          render: (h, params) => {
            return h(
              'span',
              params.index + 1
            )
          }
        },
        {
          title: 'mha',
          key: 'srcMhaName',
          render: (h, params) => {
            const row = params.row
            const text = row.srcMhaName
            return h('a', {
              props: {
                type: 'info',
                size: 'small',
                disabled: false
              },
              on: {
                click: () => {
                  this.toMhaReplication(row)
                }
              }
            }, text)
          }
        }
      ],
      dbColumns: [
        {
          type: 'selection',
          width: 60,
          align: 'center'
        },
        {
          title: '序号',
          width: 75,
          align: 'center',
          // fixed: 'left',
          render: (h, params) => {
            return h(
              'span',
              params.index + 1
            )
          }
        },
        {
          title: 'db',
          key: 'dbName',
          width: 300
        },
        {
          title: '同步链路',
          key: 'relatedId',
          render: (h, params) => {
            const row = params.row
            const text = row.srcMhaName + '(' + row.srcDcName + ')' + ' => ' + row.dstMhaName + '(' + row.dstDcName + ')'
            return h('a', {
              props: {
                type: 'info',
                size: 'small',
                disabled: false
              },
              on: {
                click: () => {
                  this.toMhaDbReplication(row)
                }
              }
            }, text)
          }
        }
      ],
      columns: [
        {
          title: '关联链路数',
          key: 'relatedNum',
          width: 150,
          sortable: true,
          render: (h, params) => {
            const row = params.row
            let text = row.relatedNum
            const type = row.tag === 'meta' ? 'success' : 'warning'
            let disabled = false
            if (row.relatedNum === null) {
              disabled = true
              text = 0
            }
            return h('Button', {
              props: {
                type: type,
                size: 'small',
                disabled: disabled
              },
              on: {
                click: () => {
                  this.showRelatedModal(row)
                }
              }
            }, text)
          }
        },
        {
          title: 'BU',
          key: 'routeOrgName',
          width: 80,
          sortable: true
        },
        {
          title: 'Applier源端机房',
          key: 'srcDcName',
          width: 200,
          sortable: true
        },
        {
          title: 'Replicator目标机房',
          key: 'dstDcName',
          width: 200,
          sortable: true
        },
        {
          title: '源端Proxy',
          key: 'srcProxyUris'
        },
        {
          title: '目标Proxy',
          key: 'dstProxyUris'
        },
        {
          title: '中继Proxy',
          key: 'relayProxyUris'
        },
        {
          title: 'tag',
          key: 'tag',
          width: 100,
          sortable: true,
          render: (h, params) => {
            const row = params.row
            const color = row.tag === 'meta' ? 'success' : 'warning'
            const text = row.tag
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '是否全局生效',
          key: 'globalActive',
          align: 'center',
          width: 140,
          sortable: true,
          resizable: true,
          render: (h, params) => {
            const row = params.row
            const color = row.globalActive === 1 ? 'blue' : 'volcano'
            const text = row.globalActive === 1 ? '是' : '否'
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
          align: 'center',
          width: 200
        }
      ],
      queryParam: {
        buId: 0,
        srcDcName: '',
        dstDcName: '',
        tag: '',
        globalActive: 1
      },
      routes: [],
      total: 0,
      current: 1,
      size: 10,
      searchOption: {
        deleteOptions: [
          {
            value: 0,
            key: 'inUse'
          },
          {
            value: 1,
            key: 'discard'
          }
        ]
      }
    }
  },
  computed: {
    dataWithPage () {
      const data = this.routes
      const start = this.current * this.size - this.size
      const end = start + this.size
      return [...data].slice(start, end)
    }
  },
  methods: {
    clearDbSelection () {
      this.selectedRow = null
      this.multiDbData = []
      this.getRoutes()
    },
    clearMhaSelection () {
      this.selectedRow = null
      this.multiMhaData = []
      this.getRoutes()
    },
    changeDbSelection (val) {
      this.multiDbData = val
      console.log(this.multiDbData)
    },
    changeMhaSelection (val) {
      this.multiMhaData = val
      console.log(this.multiMhaData)
    },
    changeSelection (val) {
      this.multiData = val
      console.log(this.multiData)
    },
    deleteMhaRoutes () {
      if (this.multiMhaData.length === 0) {
        alert('请勾选关联mha！')
        return
      }
      const mhaIds = []
      this.multiMhaData.forEach(e => mhaIds.push(e.relatedId))
      const params = {
        routeId: this.selectedRow.id,
        mhaIds: mhaIds,
        mhaDbReplicationIds: []
      }
      console.log(params)
      this.deleteMhaModal = true
      this.axios.delete('/api/drc/v2/meta/routeMappings/mha', { data: params })
        .then(res => {
          if (res.data.status === 1) {
            this.$Message.error('删除mha关联链路失败: ' + res.data.message)
          } else {
            this.$Message.success('删除mha关联链路成功')
          }
        })
        .catch(message => {
          this.$Message.error('删除mha关联链路失败: ' + message)
        })
        .finally(() => {
          this.getRelatedMha(this.selectedRow.id)
          this.deleteMhaModal = true
          this.multiMhaData = []
        })
    },
    deleteDbRoutes () {
      if (this.multiDbData.length === 0) {
        alert('请勾选关联db！')
        return
      }
      const relatedIds = []
      this.multiDbData.forEach(e => relatedIds.push(e.relatedId))
      const params = {
        routeId: this.selectedRow.id,
        mhaDbReplicationIds: relatedIds
      }
      this.deleteDbModal = true
      this.axios.delete('/api/drc/v2/meta/routeMappings', { data: params })
        .then(res => {
          if (res.data.status === 1) {
            this.$Message.error('删除db关联链路失败: ' + res.data.message)
          } else {
            this.$Message.success('删除db关联链路成功')
          }
        })
        .catch(message => {
          this.$Message.error('删除db关联链路失败: ' + message)
        })
        .finally(() => {
          this.getRelatedDb(this.selectedRow.id)
          this.deleteDbModal = true
          this.multiMhaData = []
        })
    },
    toMhaDbReplication (row) {
      const detail = this.$router.resolve({
        path: '/v2/mhaDbReplications',
        query: {
          srcMhaName: row.srcMhaName,
          dstMhaName: row.dstMhaName,
          preciseSearchMode: true,
          relatedDbName: row.dbName
        }
      })
      window.open(detail.href, '_blank')
    },
    toMhaReplication (row) {
      const detail = this.$router.resolve({
        path: '/v2/mhaDbReplications',
        query: {
          relatedMhaName: row.srcMhaName,
          preciseSearchMode: false
        }
      })
      window.open(detail.href, '_blank')
    },
    showDbModal (row) {
      this.getRelatedDb(row.id)
      this.dbModal = true
      this.selectedRow = row
    },
    showMhaModal (row) {
      this.getRelatedMha(row.id)
      this.mhaModal = true
      this.selectedRow = row
    },
    getRelatedDb (routeId) {
      this.dbData = []
      this.axios.get('/api/drc/v2/meta/routeMapping/dbs?routeId=' + routeId)
        .then(res => {
          this.dbData = res.data.data
        })
    },
    getRelatedMha (routeId) {
      this.dbData = []
      this.axios.get('/api/drc/v2/meta/routeMapping/mhas?routeId=' + routeId)
        .then(res => {
          this.dbData = res.data.data
        })
    },
    showRelatedModal (row) {
      if (row.tag === 'meta') {
        this.showDbModal(row)
      } else if (row.tag === 'console') {
        this.showMhaModal(row)
      }
    },
    resetParam () {
      this.queryParam = {
        buId: 0,
        srcDcName: '',
        dstDcName: '',
        tag: '',
        globalActive: 1
      }
    },
    getRoutes () {
      const params = {
        buId: this.queryParam.buId,
        srcDcName: this.queryParam.srcDcName,
        dstDcName: this.queryParam.dstDcName,
        tag: this.queryParam.tag,
        globalActive: this.queryParam.globalActive
      }
      const reqParam = this.flattenObj(params)
      this.dataLoading = true
      this.axios.get('/api/drc/v2/meta/routes', { params: reqParam })
        .then(response => {
          this.total = response.data.data.length
          this.routes = response.data.data
          this.dataLoading = false
        })
    },
    handleChangeSize (val) {
      this.size = val
    },
    goToUpdate (row, index) {
      console.log('go to manage route for' + row.routeOrgName + '-' + row.tag + ', from ' + row.srcDcName + ' to ' + row.dstDcName)
      this.$router.push({
        path: '/proxyRouteManagement',
        query: {
          updateStatus: 'true',
          routeId: row.id
        }
      })
    },
    preChangeRouteActive (row, index) {
      this.activeRow = row
      if (row.globalActive === 1) {
        this.activeRow.activeText = '是'
      } else {
        this.activeRow.activeText = '否'
      }
      this.activeModal = true
    },
    changeRouteActive () {
      let url
      if (this.activeRow.globalActive === 0) {
        url = '/api/drc/v2/meta/activeRoute?routeId=' + this.activeRow.id
      } else if (this.activeRow.globalActive === 1) {
        url = '/api/drc/v2/meta/deactivateRoute?routeId=' + this.activeRow.id
      }
      this.axios.post(url).then(response => {
        if (response.data.status === 0) {
          this.$Message.success('修改路由全局生效状态成功')
        } else {
          this.$Message.success('修改路由全局生效状态失败')
        }
        this.getRoutes()
      })
    },
    goToDelete (row, index) {
      this.axios.delete('/api/drc/v2/meta/route?routeId=' + row.id).then(response => {
        if (response.data.status === 0) {
          this.$Message.success('下线成功')
        } else {
          this.$Message.success('下线失败')
        }
        this.getRoutes()
      })
    },
    goToRecover (row, index) {
      if (row.deleted === 0) {
        // forbidden
        alert('正在使用中无需回滚！')
      } else {
        this.axios.post('/api/drc/v1/meta/routes', {
          id: 0,
          routeOrgName: row.routeOrgName,
          srcDcName: row.srcDcName,
          dstDcName: row.dstDcName,
          srcProxyUris: row.srcProxyUris,
          relayProxyUris: row.relayProxyUris,
          dstProxyUris: row.dstProxyUris,
          tag: row.tag,
          deleted: 0
        }).then(response => {
          console.log(response.data)
          console.log(response.data.data)
          if (response.data.status === 0) {
            alert('回滚成功！')
            this.queryParam.deleted = 0
            this.getRoutes()
          } else {
            alert('操作失败！')
          }
        })
      }
    },
    goToAddRoute () {
      console.log('go to manage route')
      this.$router.push({
        path: '/proxyRouteManagement',
        query: {
          updateStatus: 'false',
          srcProxyUris: [],
          relayProxyUris: [],
          dstProxyUris: []
        }
      })
    },
    getBus () {
      this.axios.get('/api/drc/v2/meta/bus/all')
        .then(response => {
          this.bus = response.data.data
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
    this.axios.get('/api/drc/v2/permission/resource/route').then((response) => {
      if (response.data.status === 403) {
        this.$router.push('/nopermission')
        return
      }
      this.getRoutes()
      this.getBus()
    })
  }
}
</script>

<style scoped>

</style>

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
            <Dropdown :transfer="true" placement="bottom-start">
              <Button type="default" icon="ios-hammer">
                操作
                <Icon type="ios-arrow-down"></Icon>
              </Button>
              <template #list>
                <DropdownMenu >
                  <DropdownItem>
                    <Button type="success" size="small" style="margin-right: 5px" @click="showMigrateInfo(row, index)">
                      实例迁移
                    </Button>
                  </DropdownItem>
                  <DropdownItem>
                    <Button type="error" size="small" style="margin-right: 5px" @click="preDeleteResource(row, index)">
                      下线
                    </Button>
                  </DropdownItem>
                </DropdownMenu>
              </template>
            </Dropdown>
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
          v-model="migrateApplierInfo.modal"
          title="applier实例迁移"
          footer-hide="true"
          width="900px" :scrollable="true" :draggable="true">
          <Row :gutter=10>
            <Col span="4">
              <span>当前ip: </span><span style="color: red;font-size: 15px">{{migrateApplierInfo.oldIpInfo.ip}}</span>
            </Col>
            <Col span="4">
              <span>&nbsp;&nbsp;tag: </span><span style="color: red;font-size: 15px">{{migrateApplierInfo.oldIpInfo.tag}}</span>
            </Col>
            <Col span="4">
              <span>&nbsp;&nbsp;AZ: </span><span style="color: red;font-size: 15px">{{migrateApplierInfo.oldIpInfo.az}}</span>
            </Col>
          </Row>
          <Form>
            <FormItem label="选择目标ip">
              <Select filterable clearable v-model="migrateApplierInfo.newIp" style="width: 400px" placeholder="目标ip">
                <Option v-for="item in migrateApplierInfo.ipChooseList" :value="item" :key="item">
                  {{ item }}
                </Option>
              </Select>
              <Button type="primary" style="margin-left: 100px" :loading="migrateApplierInfo.loading2" @click="migrateAllApplier">全部实例迁移</Button>
            </FormItem>
            <FormItem>
              <Table stripe border :columns="migrateApplierInfo.columns" :data="aDataWithPage" ref="multipleTable" @on-selection-change="changeSelectionA">
              </Table>
              <div>
                <Page
                  :transfer="true"
                  :total="migrateApplierInfo.tableData.length"
                  :current.sync="migrateApplierInfo.current"
                  :page-size-opts="migrateApplierInfo.pageSizeOpts"
                  :page-size="this.migrateApplierInfo.size"
                  show-total
                  show-sizer
                  show-elevator
                  @on-page-size-change="aHandleChangeSize"></Page>
              </div>
            </FormItem>
            <FormItem>
              <Button type="primary" :loading="migrateApplierInfo.loading" @click="migrateApplier">迁移</Button>
              <Button type="success" style="margin-left: 100px" @click="refreshMigrateApplier">刷新</Button>
            </FormItem>
          </Form>
        </Modal>
        <Modal
          v-model="migrateReplicatorInfo.modal"
          title="replicator实例迁移"
          footer-hide="true"
          width="900px" :scrollable="true" :draggable="true">
          <Row :gutter=10>
            <Col span="4">
              <span>当前ip: </span><span style="color: red;font-size: 15px">{{migrateReplicatorInfo.oldIpInfo.ip}}</span>
            </Col>
            <Col span="4">
              <span>&nbsp;&nbsp;tag: </span><span style="color: red;font-size: 15px">{{migrateReplicatorInfo.oldIpInfo.tag}}</span>
            </Col>
            <Col span="4">
              <span>&nbsp;&nbsp;AZ: </span><span style="color: red;font-size: 15px">{{migrateReplicatorInfo.oldIpInfo.az}}</span>
            </Col>
          </Row>
          <Form>
            <FormItem label="选择目标ip">
              <Select filterable clearable v-model="migrateReplicatorInfo.newIp" style="width: 400px" placeholder="目标ip">
                <Option v-for="item in migrateReplicatorInfo.ipChooseList" :value="item" :key="item">
                  {{ item }}
                </Option>
              </Select>
            </FormItem>
            <FormItem>
              <Table stripe border :columns="migrateReplicatorInfo.columns" :data="rDataWithPage" ref="multipleTable" @on-selection-change="changeSelectionR">
              </Table>
              <div>
                <Page
                  :transfer="true"
                  :total="migrateReplicatorInfo.tableData.length"
                  :current.sync="migrateReplicatorInfo.current"
                  :page-size-opts="migrateReplicatorInfo.pageSizeOpts"
                  :page-size="this.migrateReplicatorInfo.size"
                  show-total
                  show-sizer
                  show-elevator
                  @on-page-size-change="rHandleChangeSize"></Page>
              </div>
            </FormItem>
            <FormItem>
              <Button type="primary" :loading="migrateReplicatorInfo.loading" @click="migrateReplicator">迁移</Button>
              <Button type="success" style="margin-left: 100px" @click="refreshMigrateReplicator">刷新</Button>
            </FormItem>
          </Form>
        </Modal>
        <Modal
          v-model="relatedMhaData.modal"
          title="关联mha"
          width="900px" :scrollable="true" :draggable="true">
          <Table stripe border :columns="relatedMhaData.columns" :data="relatedMhaData.tableData"></Table>
        </Modal>
        <Modal
          v-model="relatedReplicationData.modal"
          title="关联复制链路"
          width="900px" :scrollable="true" :draggable="true">
          <Table stripe border :columns="relatedReplicationData.columns" :data="relatedReplicationData.tableData"></Table>
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
      relatedReplicationData: {
        tableData: [],
        modal: false,
        columns: [
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
            title: '类型',
            key: 'type',
            width: 150,
            render: (h, params) => {
              const row = params.row
              let text = 'none'
              let type = 'error'
              let disabled = false
              switch (row.type) {
                case 1:
                  text = 'Applier'
                  type = 'info'
                  break
                case 2:
                  text = 'DbApplier'
                  type = 'success'
                  break
                case 3:
                  text = 'Messenger'
                  type = 'warning'
                  break
                case 4:
                  text = 'DbMessenger'
                  type = 'primary'
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
            title: '同步链路',
            key: 'relatedId',
            render: (h, params) => {
              const row = params.row
              const type = row.type
              let text = ''
              if (type === 1) {
                text = row.srcMhaName + '(' + row.srcDcName + ')' + ' => ' + row.dstMhaName + '(' + row.dstDcName + ')'
              } else if (type === 2) {
                text = row.srcMhaName + '(' + row.srcDcName + ')' + ' => ' + row.dstMhaName + '(' + row.dstDcName + ')' + ' db: ' + row.dbName
              } else if (type === 3) {
                text = row.srcMhaName + '(' + row.srcDcName + ')'
              } else if (type === 4) {
                text = row.srcMhaName + '(' + row.srcDcName + ')' + ' db: ' + row.dbName
              }
              return h('a', {
                props: {
                  type: 'info',
                  size: 'small',
                  disabled: false
                },
                on: {
                  click: () => {
                    if (type === 1) {
                      this.toMhaReplication(row)
                    } else if (type === 2) {
                      this.toMhaDbReplication(row)
                    } else if (type === 3) {
                      this.toMessenger(row)
                    }
                  }
                }
              }, text)
            }
          }
        ]
      },
      relatedMhaData: {
        tableData: [],
        modal: false,
        columns: [
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
            key: 'mhaName',
            render: (h, params) => {
              const row = params.row
              const text = row.mhaName
              return h('a', {
                props: {
                  type: 'info',
                  size: 'small',
                  disabled: false
                },
                on: {
                  click: () => {
                    this.toMhaReplicationByR(row)
                  }
                }
              }, text)
            }
          }
        ]
      },
      migrateApplierInfo: {
        loading: false,
        loading2: false,
        modal: false,
        newIp: '',
        ipChooseList: [],
        oldIpInfo: {
          ip: '',
          tag: '',
          az: '',
          resourceId: null
        },
        migrateDataList: [],
        tableData: [],
        columns: [
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
            title: '类型',
            key: 'type',
            width: 150,
            render: (h, params) => {
              const row = params.row
              let text = 'none'
              let type = 'error'
              let disabled = false
              switch (row.type) {
                case 1:
                  text = 'Applier'
                  type = 'info'
                  break
                case 2:
                  text = 'DbApplier'
                  type = 'success'
                  break
                case 3:
                  text = 'Messenger'
                  type = 'warning'
                  break
                case 4:
                  text = 'DbMessenger'
                  type = 'primary'
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
            title: '同步链路',
            key: 'relatedId',
            render: (h, params) => {
              const row = params.row
              const type = row.type
              let text = ''
              if (type === 1) {
                text = row.srcMhaName + '(' + row.srcDcName + ')' + ' => ' + row.dstMhaName + '(' + row.dstDcName + ')'
              } else if (type === 2) {
                text = row.srcMhaName + '(' + row.srcDcName + ')' + ' => ' + row.dstMhaName + '(' + row.dstDcName + ')' + ' db: ' + row.dbName
              } else if (type === 3) {
                text = row.srcMhaName + '(' + row.srcDcName + ')'
              } else if (type === 4) {
                text = row.srcMhaName + '(' + row.srcDcName + ')' + ' db: ' + row.dbName
              }
              return h('a', {
                props: {
                  type: 'info',
                  size: 'small',
                  disabled: false
                },
                on: {
                  click: () => {
                    if (type === 1) {
                      this.toMhaReplication(row)
                    } else if (type === 2) {
                      this.toMhaDbReplication(row)
                    } else if (type === 3) {
                      this.toMessenger(row)
                    }
                  }
                }
              }, text)
            }
          }
        ],
        total: 0,
        current: 1,
        size: 10,
        pageSizeOpts: [10, 20, 50, 100]
      },
      migrateReplicatorInfo: {
        loading: false,
        modal: false,
        newIp: '',
        ipChooseList: [],
        oldIpInfo: {
          ip: '',
          tag: '',
          az: '',
          resourceId: null
        },
        migrateMhaList: [],
        tableData: [],
        columns: [
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
            key: 'mhaName',
            render: (h, params) => {
              const row = params.row
              const text = row.mhaName
              return h('a', {
                props: {
                  type: 'info',
                  size: 'small',
                  disabled: false
                },
                on: {
                  click: () => {
                    this.toMhaReplicationByR(row)
                  }
                }
              }, text)
            }
          }
        ],
        total: 0,
        current: 1,
        size: 10,
        pageSizeOpts: [10, 20, 50, 100]
      },
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
  computed: {
    rDataWithPage () {
      const data = this.migrateReplicatorInfo.tableData
      const start = this.migrateReplicatorInfo.current * this.migrateReplicatorInfo.size - this.migrateReplicatorInfo.size
      const end = start + this.migrateReplicatorInfo.size
      return [...data].slice(start, end)
    },
    aDataWithPage () {
      const data = this.migrateApplierInfo.tableData
      const start = this.migrateApplierInfo.current * this.migrateApplierInfo.size - this.migrateApplierInfo.size
      const end = start + this.migrateApplierInfo.size
      return [...data].slice(start, end)
    }
  },
  methods: {
    aHandleChangeSize (val) {
      this.migrateApplierInfo.size = val
    },
    changeSelectionA (val) {
      this.migrateApplierInfo.migrateDataList = val
      console.log(this.migrateApplierInfo.migrateDataList)
    },
    rHandleChangeSize (val) {
      this.migrateReplicatorInfo.size = val
    },
    changeSelectionR (val) {
      this.migrateReplicatorInfo.migrateMhaList = val
      console.log(this.migrateReplicatorInfo.migrateMhaList)
    },
    migrateAllApplier () {
      const newIp = this.migrateApplierInfo.newIp
      if (newIp === null || newIp === '') {
        this.$Message.warning('请选择目标ip')
        return
      }
      const tableData = this.migrateApplierInfo.tableData
      if (tableData.length === 0) {
        this.$Message.warning('没有相关实例')
        return
      }
      const oldIp = this.migrateApplierInfo.oldIpInfo.ip
      this.migrateApplierInfo.loading2 = true
      this.axios.post('/api/drc/v2/resource/migrate/applier?newIp=' + newIp + '&oldIp=' + oldIp).then(res => {
        if (res.data.status === 0) {
          this.migrateApplierInfo.tableData = res.data.data
          this.$Message.success('共迁移' + res.data.data + '个实例')
        } else {
          this.$Message.error('迁移失败' + res.data.message)
        }
      })
      this.migrateApplierInfo.loading2 = false
    },
    migrateApplier () {
      const multiData = this.migrateApplierInfo.migrateDataList
      if (multiData === undefined || multiData === null || multiData.length === 0) {
        this.$Message.warning('请勾选！')
        return
      }
      const newIp = this.migrateApplierInfo.newIp
      if (newIp === null || newIp === '') {
        this.$Message.warning('请选择目标ip')
        return
      }
      const migrateApplierInfos = []
      multiData.forEach(data => {
        const applierInfo = {
          relatedId: data.relatedId,
          type: data.type
        }
        migrateApplierInfos.push(applierInfo)
      })
      const params = {
        oldIp: this.migrateApplierInfo.oldIpInfo.ip,
        newIp: this.migrateApplierInfo.newIp,
        applierResourceDtos: migrateApplierInfos
      }
      this.migrateApplierInfo.loading = true
      this.axios.post('/api/drc/v2/resource/partialMigrate/applier', params).then(res => {
        if (res.data.status === 0) {
          this.migrateApplierInfo.tableData = res.data.data
          this.$Message.success('共迁移' + res.data.data + '个实例')
        } else {
          this.$Message.error('迁移失败' + res.data.message)
        }
      })
      this.migrateApplierInfo.loading = false
    },
    migrateReplicator () {
      const multiData = this.migrateReplicatorInfo.migrateMhaList
      if (multiData === undefined || multiData === null || multiData.length === 0) {
        this.$Message.warning('请勾选！')
        return
      }
      const newIp = this.migrateReplicatorInfo.newIp
      if (newIp === null || newIp === '') {
        this.$Message.warning('请选择目标ip')
        return
      }
      const mhaList = []
      multiData.forEach(data => mhaList.push(data.mhaName))
      const params = {
        oldIp: this.migrateReplicatorInfo.oldIpInfo.ip,
        newIp: this.migrateReplicatorInfo.newIp,
        mhaList: mhaList
      }
      this.migrateReplicatorInfo.loading = true
      this.axios.post('/api/drc/v2/resource/partialMigrate/replicator', params).then(res => {
        if (res.data.status === 0) {
          this.$Message.success('共迁移' + res.data.data + '个实例')
        } else {
          this.$Message.error('迁移失败' + res.data.message)
        }
      })
      this.migrateReplicatorInfo.loading = false
    },
    refreshMigrateApplier () {
      this.migrateApplierInfo.current = 1
      const resourceId = this.migrateApplierInfo.oldIpInfo.resourceId
      this.axios.get('/api/drc/v2/resource/mhaReplication?resourceId=' + resourceId).then(res => {
        if (res.data.status === 0) {
          this.migrateApplierInfo.tableData = res.data.data
          console.log(this.migrateApplierInfo.tableData)
          this.$forceUpdate()
        } else {
          this.$Message.warning('查询异常')
        }
      })
    },
    refreshMigrateReplicator () {
      this.migrateReplicatorInfo.current = 1
      const resourceId = this.migrateReplicatorInfo.oldIpInfo.resourceId
      this.axios.get('/api/drc/v2/resource/mha?resourceId=' + resourceId).then(res => {
        if (res.data.status === 0) {
          this.migrateReplicatorInfo.tableData = res.data.data
          console.log(this.migrateReplicatorInfo.tableData)
          this.$forceUpdate()
        } else {
          this.$Message.warning('查询异常')
        }
      })
    },
    showMigrateInfo (row) {
      if (row.type === 0) {
        this.showMigrateReplicator(row)
      } else if (row.type === 1) {
        this.showMigrateApplier(row)
      }
    },
    showMigrateApplier (row) {
      this.migrateApplierInfo.oldIpInfo = {
        ip: row.ip,
        tag: row.tag,
        az: row.az,
        resourceId: row.resourceId
      }
      this.axios.get('/api/drc/v2/resource/mhaReplication?resourceId=' + row.resourceId).then(res => {
        if (res.data.status === 0) {
          this.migrateApplierInfo.tableData = res.data.data
        } else {
          this.$Message.warning('查询异常')
        }
      })
      this.axios.get('/api/drc/v2/resource/list?ip=' + row.ip).then(res => {
        this.migrateApplierInfo.ipChooseList = []
        res.data.data.forEach(e => this.migrateApplierInfo.ipChooseList.push(e.ip))
        console.log(this.migrateApplierInfo.ipChooseList)
      })
      this.migrateApplierInfo.modal = true
    },
    showMigrateReplicator (row) {
      this.migrateReplicatorInfo.oldIpInfo = {
        ip: row.ip,
        tag: row.tag,
        az: row.az,
        resourceId: row.resourceId
      }
      this.axios.get('/api/drc/v2/resource/mha?resourceId=' + row.resourceId).then(res => {
        if (res.data.status === 0) {
          this.migrateReplicatorInfo.tableData = res.data.data
        } else {
          this.$Message.warning('查询异常')
        }
      })
      this.axios.get('/api/drc/v2/resource/list?ip=' + row.ip).then(res => {
        this.migrateReplicatorInfo.ipChooseList = []
        res.data.data.forEach(e => this.migrateReplicatorInfo.ipChooseList.push(e.ip))
        console.log(this.migrateReplicatorInfo.ipChooseList)
      })
      this.migrateReplicatorInfo.modal = true
    },
    toMessenger (row) {
      const detail = this.$router.resolve({
        path: '/v2/messengersV2',
        query: {
          mhaName: row.srcMhaName
        }
      })
      window.open(detail.href, '_blank')
    },
    toDbMessenger (row) {},
    toMhaReplication (row) {
      const detail = this.$router.resolve({
        path: '/v2/mhaReplications',
        query: {
          srcMhaName: row.srcMhaName,
          dstMhaName: row.dstMhaName,
          preciseSearchMode: true
        }
      })
      window.open(detail.href, '_blank')
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
    toMhaReplicationByR (row) {
      const detail = this.$router.resolve({
        path: '/v2/mhaReplications',
        query: {
          mhaName: row.mhaName,
          preciseSearchMode: false
        }
      })
      window.open(detail.href, '_blank')
    },
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
          this.relatedMhaData.tableData = res.data.data
        } else {
          this.$Message.warning('查询异常')
          this.relatedMhaData.tableData = []
        }
      })
      this.relatedMhaData.modal = true
    },
    getRelatedMhaReplication (row) {
      this.relatedReplicationData.tableData = []
      this.axios.get('/api/drc/v2/resource/mhaReplication?resourceId=' + row.resourceId).then(res => {
        if (res.data.status === 0) {
          this.relatedReplicationData.tableData = res.data.data
        } else {
          this.$Message.warning('查询异常')
          this.relatedReplicationData.tableData = []
        }
      })
      this.relatedReplicationData.modal = true
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
        // this.getResources()
      })
    },
    activeResource (resourceId) {
      this.axios.post('/api/drc/v2/resource/active?resourceId=' + resourceId).then(res => {
        if (res.data.status === 0) {
          this.$Message.success('已启用')
        } else {
          this.$Message.warning('启用失败')
        }
        // this.getResources()
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

<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/v2/mhaDbReplications">DB 复制链路</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px ">
        <Row :gutter=10 align="middle">
          <Col span="8">
            <Card :padding=5>
              <template #title>DB</template>
              <Row :gutter=10 align="middle">
                <Col span="12">
                  <Input prefix="ios-search" v-model="relatedMhaDb.dbName" placeholder="DB 名↵"
                         @on-enter="getReplications(1)">
                  </Input>
                </Col>
                <Col span="12">
                  <Select filterable prefix="ios-home" clearable v-model="relatedMhaDb.buCode" placeholder="部门"
                          @on-change="getReplications(1)">
                    <Option v-for="item in buCodes" :value="item" :key="item">{{ item }}</Option>
                  </Select>
                </Col>
              </Row>
            </Card>
          </Col>
          <Col span="10">
            <Row :gutter=10 align="middle" v-show="preciseSearchMode">
              <Col span="12">
                <Card :padding=5>
                  <template #title>源端</template>
                  <Row :gutter=10>
                    <Col span="12">
                      <Input prefix="ios-search" v-model="srcMhaDb.mhaName" placeholder="Mha 名↵"
                             @on-enter="getReplications(1)">
                      </Input>
                    </Col>
                    <Col span="12">
                      <Select filterable prefix="ios-pin" clearable v-model="srcMhaDb.regionId" placeholder="地域"
                              @on-change="getReplications(1)">
                        <Option v-for="item in regions" :value="item.id" :key="item.regionName">
                          {{ item.regionName }}
                        </Option>
                      </Select>
                    </Col>
                  </Row>
                </Card>
              </Col>
              <Col span="12">
                <Card :padding=5>
                  <template #title>目标端</template>
                  <Row :gutter=10>
                    <Col span="12">
                      <Input prefix="ios-search" v-model="dstMhaDb.mhaName" placeholder="Mha 名↵"
                             @on-enter="getReplications(1)">
                      </Input>
                    </Col>
                    <Col span="12">
                      <Select filterable prefix="ios-pin" clearable v-model="dstMhaDb.regionId"
                              placeholder="地域"
                              @on-change="getReplications(1)">
                        <Option v-for="item in regions" :value="item.id" :key="item.regionName">{{
                            item.regionName
                          }}
                        </Option>
                      </Select>
                    </Col>
                  </Row>
                </Card>
              </Col>
            </Row>
            <Row :gutter=10 align="middle" v-show="!preciseSearchMode">
              <Col span="24">
                <Card :padding=5>
                  <template #title>相关</template>
                  <Row :gutter=10>
                    <Col span="12">
                      <Input prefix="ios-search" v-model="relatedMhaDb.mhaName" placeholder="Mha 名↵"
                             @on-enter="getReplications(1)">
                      </Input>
                    </Col>
                    <Col span="12">
                      <Select filterable prefix="ios-pin" clearable v-model="relatedMhaDb.regionId"
                              placeholder="地域"
                              @on-change="getReplications(1)">
                        <Option v-for="item in regions" :value="item.id" :key="item.regionName">{{
                            item.regionName
                          }}
                        </Option>
                      </Select>
                    </Col>
                  </Row>
                </Card>
              </Col>
            </Row>
          </Col>
          <Col span="3">
            <Card :padding=5>
              <template #title>同步状态</template>
              <Select filterable prefix="ios-pin" clearable v-model="drcStatus"
                      placeholder="状态"
                      @on-change="getReplications(1)">
                <Option v-for="item in drcStatusList" :value="item.value" :key="item.status">{{
                    item.status
                  }}
                </Option>
              </Select>
            </Card>
          </Col>
          <Col span="3">
            <Row :gutter=10 align="middle">
              <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getReplications(1)">查询</Button>
              <i-switch v-model="preciseSearchMode" size="large" style="margin-left: 10px">进阶
                <template #open>
                  <span>进阶</span>
                </template>
                <template #close>
                  <span>进阶</span>
                </template>
              </i-switch>
            </Row>
            <Row :gutter=10 align="middle" style="margin-top: 20px">
              <Button icon="md-refresh" @click="resetParam" :loading="dataLoading">重置</Button>
            </Row>
          </Col>
        </Row>
        <br>
        <Row  style="background: #fdfdff; border: 1px solid #e8eaec;">
          <Col span="2" style="display: flex;float: left;margin: 5px" >
            <Dropdown placement="bottom-start">
              <Button type="default" icon="ios-hammer">
                新建配置
                <Icon type="ios-arrow-down"></Icon>
              </Button>
                <template #list>
                  <DropdownMenu >
                    <DropdownItem @click.native="() => {$router.push({path: '/drcV2'})}">新建DRC配置（MHA）</DropdownItem>
                    <DropdownItem @click.native="() => {$router.push({path: '/v2/dbDrcBuildV2'})}">新建DRC配置（DB维度）</DropdownItem>
                  </DropdownMenu>
                </template>
              </Dropdown>
          </Col>
        </Row>
        <Table :loading="dataLoading" stripe border :columns="columns" :data="replications" :span-method="handleSpan">
          <template slot-scope="{ row }" slot="action">
            <Button type="success" size="small" style="margin-right: 5px" @click="checkConfig(row.src.mhaName,row.dst.mhaName,false)">
              查看
            </Button>
            <Button type="primary" size="small" style="margin-right: 5px" @click="goToMhaDbReplicationDetails(row)">
              编辑
            </Button>
            <Button type="default" size="small" style="margin-right: 5px" @click="goToMhaReplicationDetails(row)">
              跳转MHA
            </Button>
          </template>
        </Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :transfer="true"
            :total="total"
            :current.sync="current"
            :page-size-opts="[20,50,100,200,1000]"
            :page-size="20"
            show-total
            show-sizer
            show-elevator
            @on-change="getReplications(current)"
            @on-page-size-change="handleChangeSize"></Page>
        </div>
        <Drawer title="Basic Drawer" width="80" :closable="true" v-model="replicationDetail.show">
          <template #header>
            查看详情
            <div style="float:right;margin-right: 100px">
              自动换行
              <i-switch v-model="replicationDetail.lineWrap"/>
              黑夜模式
              <i-switch v-model="replicationDetail.darkMode"
                        on-change="(status)=>{this.$Message.info('开关状态：'+status)}"/>
            </div>
          </template>
          <div id="xmlCode">
            <codemirror
              v-model="replicationDetail.data"
              class="code"
              :options="{
                  mode: 'xml',
                  theme: replicationDetail.darkMode? 'monokai':'default',
                  autofocus: true,
                  lineWrapping: replicationDetail.lineWrap,
                  readOnly: true,
                  lineNumbers: true,
                  foldGutter: true,
                  styleActiveLine: true,
                  gutters: ['CodeMirror-linenumbers', 'CodeMirror-foldgutter']
            }">
            </codemirror>
            <Divider />
            <Button v-if="replicationDetail.deleteMode" type="error"  style="float:right;margin-right:20px" @click="deleteConfig()">
              删除
            </Button>
          </div>
        </Drawer>
      </div>
    </Content>
  </base-component>
</template>

<script>
// eslint-disable-next-line no-unused-vars
import 'codemirror/theme/monokai.css'
import 'codemirror/mode/xml/xml.js'

import 'codemirror/addon/fold/foldgutter.css'
import 'codemirror/addon/fold/foldgutter.js'
import prettyMilliseconds from 'pretty-ms'
import Vue from 'vue'

export default {
  name: 'Application',
  data () {
    return {
      columns: [
        {
          title: '类型',
          width: 80,
          render: (h, params) => {
            const row = params.row
            let text = 'none'
            let type = 'error'
            let disabled = false
            switch (row.transmissionType) {
              case 'simplex':
                text = '单'
                type = 'info'
                break
              case 'duplex':
                text = '双'
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
          title: '延迟',
          key: 'drcStatus',
          align: 'center',
          renderHeader: (h, params) => {
            return h('span', [
              h('span', '延迟'),
              h('Button', {
                on: {
                  click: async () => {
                    await this.getDelay()
                  }
                },
                props: {
                  loading: this.delayDataLoading,
                  size: 'small',
                  shape: 'circle',
                  type: 'default',
                  icon: 'md-refresh'
                }
              })
            ])
          },
          render: (h, params) => {
            const row = params.row
            let color, text
            if (row.drcStatus === true) {
              if (row.delay != null) {
                text = prettyMilliseconds(row.delay, { compact: false })
                if (row.delay > 10000) {
                  color = 'warning'
                } else {
                  color = 'success'
                }
              } else {
                text = '已接入'
                color = 'blue'
              }
            } else {
              text = '未接入'
              color = 'default'
            }
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: 'DB 名',
          key: 'srcDbName',
          width: 300,
          render: (h, params) => {
            return h('p', params.row.src.dbName)
          }
        },
        {
          title: '业务部门',
          key: 'buText',
          width: 100,
          render: (h, params) => {
            return h('span', params.row.src.buName)
          }
        },
        {
          title: '源集群',
          key: 'srcMhaName',
          width: 250,
          render: (h, params) => {
            return h('div', [
              params.row.src.mhaName + ' ',
              h('Tag', {
                props: {
                  color: 'blue'
                }
              }, params.row.src.regionName)
            ])
          }
        },
        {
          title: '目标集群',
          key: 'dstMhaName',
          width: 250,
          render: (h, params) => {
            return h('div', [
              params.row.dst.mhaName + ' ',
              h('Tag', {
                props: {
                  color: 'blue'
                }
              }, params.row.dst.regionName)
            ])
          }
        },
        {
          title: '操作',
          slot: 'action',
          width: 300,
          align: 'center'
        }
      ],
      // page
      total: 0,
      current: 1,
      size: 20,
      // query param
      srcMhaDb: {
        mhaName: this.$route.query.srcMhaName,
        dbName: this.$route.query.srcDbName,
        buCode: null,
        regionId: null
      },
      dstMhaDb: {
        mhaName: this.$route.query.dstMhaName,
        dbName: this.$route.query.dstDbName,
        buCode: null,
        regionId: null
      },
      relatedMhaDb: {
        mhaName: this.$route.query.relatedMhaName,
        dbName: this.$route.query.relatedDbName,
        buCode: null,
        regionId: null
      },
      drcStatus: this.$route.query.drcStatus ? Number(this.$route.query.drcStatus) : 1,
      preciseSearchMode: this.$route.query.preciseSearchMode === true || this.$route.query.preciseSearchMode === 'true',
      timerId: null,
      // get from backend
      replications: [],
      buCodes: [],
      regions: [],
      drcStatusList: [
        {
          status: '未接入',
          value: 0
        },
        {
          status: '已接入',
          value: 1
        }
      ],
      // for detail show
      replicationDetail: {
        show: false,
        replicationId: null,
        deleteMode: false,
        data: null,
        darkMode: true,
        lineWrap: false,
        row: {}
      },
      dataLoading: true,
      delayDataLoading: true
    }
  },
  computed: {},
  methods: {
    getBus () {
      this.axios.get('/api/drc/v2/meta/dbBuCodes/all')
        .then(response => {
          this.buCodes = response.data.data
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
    resetParam () {
      this.srcMha = {
        name: null,
        buCode: null,
        regionId: null
      }
      this.dstMha = {
        name: null,
        buCode: null,
        regionId: null
      }
      this.relatedMhaDb = {
        name: null,
        buCode: null,
        regionId: null
      }
      this.drcStatus = 1
      this.getReplications(1)
    },
    async getReplications (pageIndex = 1) {
      this.resetPath()
      const that = this
      const params = {
        pageIndex: pageIndex,
        pageSize: this.size
      }
      params.relatedMhaDb = {
        dbName: this.relatedMhaDb.dbName,
        buCode: this.relatedMhaDb.buCode
      }
      if (this.preciseSearchMode) {
        params.srcMhaDb = {
          mhaName: this.srcMhaDb.mhaName,
          regionId: this.srcMhaDb.regionId
        }
        params.dstMhaDb = {
          mhaName: this.dstMhaDb.mhaName,
          regionId: this.dstMhaDb.regionId
        }
      } else {
        params.relatedMhaDb.mhaName = this.relatedMhaDb.mhaName
        params.relatedMhaDb.regionId = this.relatedMhaDb.regionId
      }
      params.drcStatus = this.drcStatus
      const reqParam = this.flattenObj(params)
      that.dataLoading = true
      await that.axios.get('/api/drc/v2/replication/db/query', { params: reqParam })
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
            this.getDelay()
          }
        })
        .catch(message => {
          that.$Message.error('查询异常: ' + message)
        })
        .finally(() => {
          that.dataLoading = false
        })
    },
    getDelay () {
      const replicationIds = this.replications
        .filter(item => item.drcStatus === true)
        .map(item => item.id)
      if (replicationIds.length === 0) {
        console.log('skip delay search')
        return
      }
      this.delayDataLoading = true
      this.axios.post('/api/drc/v2/replication/db/delay', {
        replicationIds: replicationIds
      })
        .then(response => {
          const delays = response.data.data
          const emptyResult = delays == null || !Array.isArray(delays) || delays.length === 0
          if (emptyResult) {
            return
          }
          const dataMap = new Map(delays.map(e => [e.srcMha + '->' + e.dstMha + '->' + e.dbName, e.delay]))
          this.replications.forEach(line => {
            Vue.set(line, 'delay', dataMap.get(line.src.mhaName + '->' + line.dst.mhaName + '->' + line.src.dbName))
          })

          console.log(this.replications)
        })
        .catch(message => {
          console.log(message)
          this.$Message.error('查询延迟异常: ' + message)
        })
        .finally(() => {
          this.delayDataLoading = false
        })
    },
    handleChangeSize (val) {
      this.size = val
      this.$nextTick(() => {
        this.getReplications(1)
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
        this.getReplications(1)
      })
    },
    deleteConfig () {
      this.$Message.warning('暂不支持该操作')
    },
    checkConfig (srcMha, dstMha, isDeleteMode) {
      this.dataLoading = true
      this.replicationDetail.data = null
      this.axios.get('/api/drc/v2/meta/queryConfig/mhaReplicationByName', {
        params: {
          srcMha: srcMha,
          dstMha: dstMha
        }
      }).then(response => {
        if (response.data.status === 1) {
          this.$Message.warning('查询异常: ' + response.data.message)
          return
        }
        this.replicationDetail.data = response.data.data
        this.replicationDetail.deleteMode = isDeleteMode
        this.replicationDetail.show = true
      }).catch(message => {
        this.$Message.error('查询异常: ' + message)
      }).finally(() => {
        this.dataLoading = false
      })
    },
    goToMhaDbReplicationDetails (row) {
      this.$router.push({
        path: '/v2/dbDrcBuildV2',
        query: {
          fixDb: true,
          dbName: row.src.dbName,
          srcRegionName: row.src.regionName,
          dstRegionName: row.dst.regionName
        }
      })
    },
    goToMhaReplicationDetails (row) {
      this.$router.push({
        path: '/drcV2',
        query: {
          step: 3,
          srcMhaName: row.src.mhaName,
          dstMhaName: row.dst.mhaName
        }
      })
    },
    goToDrcLink () {
      this.$router.push({
        path: '/drcV2'
      })
    },
    showModal (row) {
      console.log('show modal')
      this.$Message.info('还未上线，敬请期待')
    },
    calTableSpan (replications) {
      console.log('calTableSpan', replications)
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
          while (j < length && replications[i].src.dbName === replications[j].src.dbName) {
            replications[j].src.rowSpan = 0
            replications[j].src.colSpan = 0
            j++
          }
          replications[i].src.rowSpan = j - i
          replications[i].src.colSpan = 1
          i = j
        }
      }

      function calDstMhaSpan () {
        let i = 0
        while (i < length) {
          let j = i + 1
          while (j < length && replications[i].dst.dbName === replications[j].dst.dbName) {
            replications[j].dst.rowSpan = 0
            replications[j].dst.colSpan = 0
            j++
          }
          replications[i].dst.rowSpan = j - i
          replications[i].dst.colSpan = 1
          i = j
        }
      }
    },
    handleSpan ({ row, column, rowIndex, columnIndex }) {
      if (column.key === 'srcDbName') {
        return [row.src.rowSpan, row.src.colSpan]
      } else if (column.key === 'dstDbName') {
        return [row.dst.rowSpan, row.dst.colSpan]
      }
    },
    goToHickwallMonitorPage (srcName, dstName) {
      const url = 'http://hickwall.ctripcorp.com/grafanav2/d/vWeGmjFVk/drc-mha?from=now-1h&to=now&var-mha=' + srcName +
        '&var-auto_gen_other_mha=' + dstName
      window.open(url, '_blank')
    },
    resetPath () {
      if (this.preciseSearchMode) {
        this.$router.replace({
          query: {
            srcMhaName: this.srcMhaDb.mhaName,
            dstMhaName: this.dstMhaDb.mhaName,
            relatedDbName: this.relatedMhaDb.dbName,
            drcStatus: this.drcStatus,
            preciseSearchMode: true
          }
          // eslint-disable-next-line handle-callback-err
        }).catch(err => {})
      } else {
        this.$router.replace({
          query: {
            relatedMhaName: this.relatedMhaDb.mhaName,
            relatedDbName: this.relatedMhaDb.dbName,
            drcStatus: this.drcStatus,
            preciseSearchMode: false
          }
          // eslint-disable-next-line handle-callback-err
        }).catch(err => {})
      }
      console.log('this.$route.query.preciseSearchMode', this.$route.query.preciseSearchMode, typeof this.$route.query.preciseSearchMode)
    }
  },
  created () {
    this.axios.get('/api/drc/v2/permission/meta/dbReplication/query').then((response) => {
      if (response.data.status === 403) {
        this.$router.push('/nopermission')
        return
      }
      this.getReplications(1)
      this.getRegions()
      this.getBus()
      this.timerId = setInterval(() => this.getDelay(), 5000)
      setTimeout(() => { clearInterval(this.timerId) }, 30 * 60 * 1000)
    })
  },
  beforeDestroy () {
    if (this.timerId) {
      clearInterval(this.timerId)
      this.timerId = null
    }
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

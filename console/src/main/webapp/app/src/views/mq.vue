<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/mq">消息投递接入数据</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px ">
        <br>
        <Row :gutter=10 align="middle">
          <Col span="2">
            <Card :padding=5>
              <template #title>类型</template>
              <Select prefix="ios-send-outline" v-model="mqType"
                      placeholder="mqType"
                      @on-change="getReplications(1)">
                <Option v-for="item in mqTypeList" :value="item" :key="item">{{
                    item
                  }}
                </Option>
              </Select>
            </Card>
          </Col>
          <Col span="5">
            <Card :padding=5>
              <template #title>DB名</template>
              <Input prefix="ios-search" v-model="dbNames" placeholder="DB 名↵" @on-enter="getReplications(1)">
              </Input>
            </Card>
          </Col>
          <Col span="7">
            <Card :padding=5>
              <template #title>表名</template>
              <Input prefix="ios-search" v-model="srcTbl" placeholder="源表名↵" @on-enter="getReplications(1)">
              </Input>
            </Card>
          </Col>
          <Col span="7">
            <Card :padding=5>
              <template #title>topic</template>
              <Input prefix="ios-search" v-model="dstTopic" placeholder="Topic名↵" @on-enter="getReplications(1)">
              </Input>
            </Card>
          </Col>
          <Col span="3">
            <Row :gutter=10 align="middle">
              <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getReplications(1)">查询</Button>
              <i-switch v-model="otterSearchMode" size="large" style="margin-left: 10px">otter
                <template #open>
                  <span>otter</span>
                </template>
                <template #close>
                  <span>otter</span>
                </template>
              </i-switch>
            </Row>
            <Row :gutter=10 align="middle" style="margin-top: 20px">
              <Button icon="md-refresh" :loading="dataLoading" @click="resetParam">重置</Button>
            </Row>
          </Col>
        </Row>
        <br>
        <Table :columns="columns" :data="records" :loading="dataLoading" border>
        </Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :transfer="true"
            :total="total"
            :current.sync="current"
            :page-size-opts="[10,20,50,100]"
            :page-size="10"
            show-sizer
            show-total
            show-elevator
            @on-change="getReplications(current)"
            @on-page-size-change="handleChangeSize"></Page>
        </div>
        <br>
      </div>
    </Content>
  </base-component>
</template>

<script>
import 'codemirror/theme/monokai.css'
import 'codemirror/mode/xml/xml.js'

import 'codemirror/addon/fold/foldgutter.css'
import 'codemirror/addon/fold/foldgutter.js'

export default {
  name: 'mq',
  data () {
    return {
      totalData: [
        {
          title: 'otter 接入DB数',
          key: 'otterDbCount'
        },
        {
          title: 'otter 接入topic数',
          key: 'otterTopicCount'
        },
        {
          title: 'messenger 接入DB数',
          key: 'messengerDbCount'
        },
        {
          title: 'messenger 接入topic数',
          key: 'messengerTopicCount'
        }
      ],
      columns: [
        {
          title: 'DB名',
          key: 'dbName',
          align: 'center'
        },
        {
          title: '表名',
          key: 'srcLogicTableName',
          align: 'center'
        },
        {
          title: 'topic',
          key: 'dstLogicTableName',
          align: 'center'
        },
        {
          title: '集群名',
          key: 'regionText',
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            return h('div', [
              row.mhaName,
              ' ',
              h('Tag', {
                props: {
                  color: color
                }
              }, row.dcName)
            ])
          },
          align: 'center'
        },
        {
          title: '看板(hickwall)',
          slot: 'panel',
          render: (h, params) => {
            const row = params.row
            return h('Button', {
              on: {
                click: () => {
                  window.open(row.mqPanelUrl, '_blank')
                }
              },
              props: {
                size: 'small',
                type: 'success'
              }
            }, '延迟&TPS')
          },
          align: 'center'
        }

      ],
      // page
      total: 0,
      current: 1,
      size: 10,
      // query param
      mqType: this.$route.query.mqType ? this.$route.query.mqType : 'qmq',
      srcTbl: this.$route.query.srcTbl,
      dstTopic: this.$route.query.dstTopic,
      dbNames: this.$route.query.dbNames,
      otterSearchMode: this.$route.query.otterSearchMode === true || this.$route.query.otterSearchMode === 'true',
      // get from backend
      totalCount: [{
        otterDbCount: null,
        otterTopicCount: null,
        messengerDbCount: null,
        messengerTopicCount: null
      }],
      mqTypeList: this.constant.mqTypeList,
      records: [],

      dataLoading: false,
      totalCountLoading: false
    }
  },
  methods: {
    resetPath () {
      this.$router.replace({
        query: {
          mqType: this.mqType,
          dbNames: this.dbNames,
          topic: this.dstTopic,
          srcTblName: this.srcTbl
        }
      })
    },
    async getReplications (pageIndex = 1) {
      this.resetPath()
      const that = this
      console.log(that.dataLoading)
      if (that.dataLoading) return false
      that.dataLoading = true
      const params = {
        mqType: this.mqType,
        dbNames: this.dbNames,
        topic: this.dstTopic,
        srcTblName: this.srcTbl,
        pageIndex: pageIndex,
        pageSize: this.size
      }
      if (this.otterSearchMode) {
        params.queryOtter = true
      } else {
        params.queryOtter = false
      }
      const reqParam = this.flattenObj(params)
      console.log(reqParam)
      await that.axios.get('/api/drc/v2/dbReplication/mqReplication', { params: reqParam })
        .then(res => {
          const pageResult = res.data.data
          if (!pageResult || pageResult.totalCount === 0) {
            that.total = 0
            that.current = 1
            that.records = []
            that.$Message.warning('查询结果为空')
          } else {
            that.total = pageResult.totalCount
            that.current = pageResult.pageIndex
            that.records = pageResult.data
            console.log(that.records)
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
    handleChangeSize (val) {
      this.size = val

      this.$nextTick(() => {
        this.getReplications(1)
      })
    },
    resetParam () {
      this.srcTbl = null
      this.dstTopic = null
      this.dbNames = null
    }
  },
  created () {
    this.getReplications()
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

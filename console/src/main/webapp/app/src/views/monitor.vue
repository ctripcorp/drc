<template>
  <base-component v-on:listenToChildEvent="getLogFromChild">
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/monitor">冲突日志</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <i-input :style="{margin:'10px, 0, 0, 0', width: '200px'}" v-model="keyWord" placeholder="输入DAL Cluster" v-on:input="getLogs">
          <Icon type="ios-search" slot="prefix" />
        </i-input>
        <Table style="margin-top: 20px" :columns="columns" :data="records" stripe size="small"></Table>
        <div style="text-align: center;margin: 16px 0">
          <Page
            :total="total"
            :current.sync="current"
            show-sizer
            show-elevator
            :transfer="true"
            @on-change="getLogs"
            @on-page-size-change="handleChangeSize"></Page>
        </div>
        <Modal
          v-model="this.sqlStatementModal"
          title="查看SQL"
          width="1200px"
          @on-ok="showSqlOk"
          @on-cancel="showSqlCancel">
          <Form>
            <FormItem>
              <codemirror
                ref="mycode"
                :value="curCode"
                :options="cmOptions"
                class="code">
              </codemirror>
            </FormItem>
          </Form>
        </Modal>
      </div>
    </Content>
  </base-component>
</template>

<script>
import { formatDate } from '../common/date.js'

import 'codemirror/theme/ambiance.css'
import 'codemirror/lib/codemirror.css'
import 'codemirror/addon/hint/show-hint.css'
require('codemirror/addon/edit/matchbrackets')
require('codemirror/addon/selection/active-line')
require('codemirror/mode/sql/sql')
require('codemirror/addon/hint/show-hint')
require('codemirror/addon/hint/sql-hint')

export default {
  name: 'Monitor',
  data () {
    return {
      code: '//按Ctrl键进行代码提示',
      addDc: false,
      sqlStatementModal: false,
      handleSqlConflictModal: false,
      curCode: '',
      cmOptions: {
        value: '',
        mode: 'text/x-mysql',
        theme: 'ambiance',
        lineWrapping: true,
        height: 100,
        readOnly: true,
        lineNumbers: true
      },
      columns: [
        {
          title: 'DAL Cluster',
          key: 'clusterName',
          tooltip: true
        },
        {
          title: '源mha集群',
          key: 'srcMhaName',
          tooltip: true
        },
        {
          title: '目标mha集群',
          key: 'destMhaName',
          tooltip: true
        },
        {
          title: 'SQL执行时间',
          key: 'sqlExecuteTime',
          width: 200,
          align: 'center',
          render: (h, params) => {
            return h('div',
              formatDate(new Date(params.row.sqlExecuteTime), 'yyyy-MM-dd hh:mm:ss')
              // Date是后台时间戳参数字段
              // 'yyyy-MM-dd hh:mm' 对应的时间格式 2018-12-21 18:46
              // 格式可以自行修改，例如 'yyyy-MM-dd' -> 2018-12-21,  'yyyy-MM' -> 2018-12
            )
          }
        },
        {
          title: '执行结果',
          key: 'lastResult',
          width: 120,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const color = row.lastResult === 'commit' ? 'blue' : 'volcano'
            return h('Tag', {
              props: {
                color: color
              }
            }, row.lastResult)
          }
        },
        {
          title: '操作',
          key: 'sqlStatement',
          width: 160,
          align: 'center',
          render: (h, params) => {
            return h('div', [
              h('Button', {
                props: {
                  type: 'info',
                  size: 'small'
                },
                style: {
                  marginRight: '5px'
                },
                on: {
                  click: () => {
                    this.showSqlInfo(params.row.rawSqlList)
                  }
                }
              }, '查看'),
              h('Button', {
                props: {
                  type: 'success',
                  size: 'small'
                },
                style: {
                  marginLeft: '5px'
                },
                on: {
                  click: () => {
                    this.handleSqlConflict(params.row.id)
                  }
                }
              }, '处理')
            ])
          }
        }
      ],
      records: [],
      loading: false,
      appId: 0,
      total: 0,
      current: 1,
      size: 10,
      lastTime: 0,
      keyWord: ''
    }
  },
  computed: {
  },
  methods: {
    getLogs () {
      const that = this
      this.axios.get('/api/drc/v1/logs/conflicts/' + this.current + '/' + this.size + '?keyWord=' + this.keyWord)
        .then(res => {
          console.log('test data')
          console.log(res)
          that.records = res.data.data.list
          that.total = res.data.data.count
        })
    },
    handleChangeSize (val) {
      this.size = val

      this.$nextTick(() => {
        this.getLogs()
      })
    },
    showSqlInfo (sqlStatement) {
      this.curCode = sqlStatement
      this.sqlStatementModal = true
    },
    showSqlOk () {
      this.sqlStatementModal = false
      this.curCode = ''
    },
    showSqlCancel () {
      this.sqlStatementModal = false
      this.curCode = ''
    },
    handleSqlConflict (id) {
      this.$router.push({
        name: 'conflict',
        query: { id: id }
      })
    }
  },
  created () {
    this.getLogs()
  }
}
</script>

<style>
  .CodeMirror {
    /* Set height, width, borders, and global font properties here */
    font-family: monospace;
    height: auto;
    color: black;
    direction: ltr;
  }
</style>

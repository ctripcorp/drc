<template xmlns:font-family="http://www.w3.org/1999/xhtml">
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/conflictLog">冲突处理</BreadcrumbItem>
      <BreadcrumbItem to="/dbBlacklist">黑名单</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px ">
        <Row :gutter=10 align="middle">
          <Col span="16">
            <Card :padding=5>
              <template #title>查询条件</template>
              <Row :gutter=10>
                <Col span="16">
                  <Input prefix="ios-search" v-model="queryParam.dbFilter" placeholder="dbFilter" @on-enter="getData"></Input>
                </Col>
                <Col span="8">
                  <Select filterable clearable v-model="queryParam.type" placeholder="类型" @on-change="getData">
                    <Option v-for="item in typeList" :value="item.val" :key="item.val">{{ item.name }}</Option>
                  </Select>
                </Col>
              </Row>
            </Card>
          </Col>
          <Col span="1">
            <Button type="primary" icon="ios-search" :loading="dataLoading" @click="getData">查询</Button>
            <Button icon="md-refresh" :loading="dataLoading" @click="resetParam" style="margin-top: 20px">重置</Button>
          </Col>
        </Row>
        <br>
        <Row  style="background: #fdfdff; border: 1px solid #e8eaec;">
          <Col span="2" style="display: flex;float: left;margin: 5px" >
            <Button type="default" @click="preAdd" icon="ios-hammer">
              新增黑名单
            </Button>
          </Col>
        </Row>
        <Table stripe border :columns="columns" :data="tableData">
          <template slot-scope="{ row, index }" slot="action">
            <Button type="success" size="small" style="margin-right: 5px" @click="showDetail(row, index)">
              详情
            </Button>
            <Button type="primary" size="small" style="margin-right: 5px" @click="preUpdate(row, index)">
              修改
            </Button>
            <Button type="error" size="small" style="margin-right: 5px" @click="preDelete(row, index)">
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
            @on-change="getData"
            @on-page-size-change="handleChangeSize"></Page>
        </div>
        <Modal
          v-model="deleteModal"
          title="确认删除以下黑名单"
          @on-ok="deleteBlacklist"
          @on-cancel="clearDelete">
          <p>黑名单: "{{deleteDbFilter}}"</p>
        </Modal>
        <Modal
          v-model="detailModal"
          width="900px"
          title="黑名单">
          <div id="xmlCode">
            <codemirror
              v-model="detail"
              class="code"
              :options="{
                  mode: 'text/x-mysql',
                  theme: 'ambiance',
                  autofocus: true,
                  lineWrapping: true,
                  readOnly: true,
                  lineNumbers: true,
                  foldGutter: true,
                  styleActiveLine: true,
                  gutters: ['CodeMirror-linenumbers', 'CodeMirror-foldgutter']
            }">
            </codemirror>
          </div>
        </Modal>
        <Modal
          v-model="createModal"
          title="黑名单变更"
          width="900px"
          @on-ok="blacklistChange">
          <Form style="width: 100%" label-position="right">
            <FormItem label="黑名单">
              <Input v-model="dbFilter" placeholder="请输入黑名单, 支持正则" style="width: 500px"/>
            </FormItem>
            <FormItem label="过期时间">
              <DatePicker type="datetime" :editable="false" v-model="dbFilterExpirationTime"
                          :clearable="false" placeholder="请选择黑名单过期时间"></DatePicker>
            </FormItem>
          </Form>
        </Modal>
      </div>
    </Content>
  </base-component>
</template>

<script>
import 'codemirror/theme/ambiance.css'
import 'codemirror/mode/sql/sql.js'

export default {
  name: 'dbBlacklist',
  data () {
    return {
      columns: [
        {
          title: '黑名单(正则)',
          key: 'dbFilter',
          tooltip: true
        },
        {
          title: '类型',
          key: 'type',
          width: 150,
          align: 'center',
          render: (h, params) => {
            const type = params.row.type
            let color, text
            if (type === 0) {
              text = '用户配置'
              color = 'blue'
            } else if (type === 1) {
              text = 'DRC配置'
              color = 'green'
            } else if (type === 2) {
              text = 'DBA'
              color = 'success'
            } else if (type === 3) {
              text = '告警无处理'
              color = 'volcano'
            } else if (type === 4) {
              text = '告警无流量'
              color = 'volcano'
            } else {
              text = '未知'
              color = 'volcano'
            }
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '过期时间',
          key: 'expirationTime',
          width: 180
        },
        {
          title: '修改时间',
          key: 'createTime',
          width: 180
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center',
          width: 200
        }
      ],
      // page
      total: 0,
      current: 1,
      size: 10,
      // get from backend
      tableData: [],
      deleteModal: false,
      isUpdate: false,
      createModal: 'this.$route.query'.createModal === true || this.$route.query.createModal === 'true',
      detailModal: false,
      detail: '',
      deleteDbFilter: '',
      typeList: [
        {
          name: '用户配置',
          val: 0
        },
        {
          name: 'DRC配置',
          val: 1
        },
        {
          name: 'DBA',
          val: 2
        },
        {
          name: '告警无处理',
          val: 3
        },
        {
          name: '告警无流量',
          val: 4
        }
      ],
      queryParam: {
        dbFilter: '',
        type: null,
        pageReq: {
          pageSize: 10,
          pageIndex: 1
        }
      },
      blacklistId: 0,
      blacklistType: 0,
      dbFilter: this.$route.query.dbFilter,
      dbFilterExpirationTime: null,
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
    resetParam () {
      this.queryParam = {
        dbFilter: '',
        type: null,
        pageReq: {
          pageSize: 10,
          pageIndex: 1
        }
      }
      this.getData()
    },
    getData () {
      const that = this
      const params = {
        dbFilter: this.queryParam.dbFilter,
        type: this.queryParam.type,
        pageReq: {
          pageSize: this.size,
          pageIndex: this.current
        }
      }
      const reqParam = this.flattenObj(params)
      that.dataLoading = true
      that.axios.get('/api/drc/v2/log/conflict/db/blacklist', { params: reqParam })
        .then(response => {
          const data = response.data
          const pageResult = response.data.pageReq
          if (data.status === 1) {
            that.$Message.error('查询异常: ' + data.message)
          } else if (data.data.length === 0 || pageResult.totalCount === 0) {
            that.total = 0
            that.current = 1
            that.tableData = []
            that.$Message.warning('查询结果为空')
          } else {
            that.total = pageResult.totalCount
            that.current = pageResult.pageIndex
            // that.calTableSpan(pageResult.data)
            that.tableData = data.data
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
        this.getData()
      })
    },
    showDetail (row, index) {
      this.detail = row.dbFilter
      this.detailModal = true
    },
    preUpdate (row, index) {
      this.blacklistId = row.id
      this.blacklistType = row.type
      this.dbFilter = row.dbFilter
      this.dbFilterExpirationTime = row.expirationTime
      this.isUpdate = true
      this.createModal = true
    },
    preDelete (row, index) {
      this.deleteDbFilter = row.dbFilter
      this.deleteModal = true
    },
    clearDelete () {
      this.deleteDbFilter = ''
    },
    deleteBlacklist (row, index) {
      this.axios.delete('/api/drc/v2/log/conflict/db/blacklist/?dbFilter=' + this.deleteDbFilter).then(res => {
        if (res.data.status === 0) {
          this.$Message.success('删除成功')
          this.getData()
        } else {
          this.$Message.warning('删除失败 ' + res.data.message)
        }
      })
    },
    preAdd () {
      this.isUpdate = false
      this.dbFilter = ''
      this.dbFilterExpirationTime = null
      this.createModal = true
    },
    blacklistChange () {
      if (this.isUpdate) {
        this.updateBlacklist()
      } else {
        this.addBlacklist()
      }
    },
    updateBlacklist () {
      const expirationTime = new Date(this.dbFilterExpirationTime).getTime()
      if (isNaN(expirationTime)) {
        this.$Message.warning('过期时间为空或格式不正确')
        return
      }
      const params = {
        id: this.blacklistId,
        type: this.blacklistType,
        dbFilter: this.dbFilter,
        expirationTime: expirationTime
      }
      this.axios.put('/api/drc/v2/log/conflict/db/blacklist', params).then(res => {
        if (res.data.status === 0) {
          this.$Message.success('修改成功')
          this.createModal = false
          this.getData()
        } else {
          this.$Message.warning('修改失败 ' + res.data.message)
        }
      })
    },
    addBlacklist () {
      const expirationTime = new Date(this.dbFilterExpirationTime).getTime()
      if (isNaN(expirationTime)) {
        this.$Message.warning('过期时间为空或格式不正确')
        return
      }
      this.axios.post('/api/drc/v2/log/conflict/db/blacklist/?dbFilter=' + this.dbFilter + '&expirationTime=' + expirationTime).then(res => {
        if (res.data.status === 0) {
          this.$Message.success('新增成功')
          this.createModal = false
          this.getData()
        } else {
          this.$Message.warning('新增失败 ' + res.data.message)
        }
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
    this.getData()
  }
}
</script>

<style scoped>

</style>
<style lang="scss">
.ivu-table .table-info-cell-extra-column-add {
  background-color: #2db7f5;
  color: #fff;
}

.ivu-table .table-info-cell-extra-column-diff {
  background-color: #ff6600;
  color: #fff;
}

.CodeMirror {
  /* Set height, width, borders, and global font properties here */
  font-family: monospace;
  height: auto;
  color: black;
  direction: ltr;
}
#xmlCode {
  .CodeMirror {
    overscroll-y: scroll !important;
    height: auto !important;
  }
}
</style>

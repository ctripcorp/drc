<template>
  <base-component :isFather="isFather" :subMenuName="['1']" :fatherMenu="fatherMenu">
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/incrementDataConsistencyResult">校验结果</BreadcrumbItem>
      <BreadcrumbItem>当前结果及自助处理</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Card>
          <div class="ivu-list-item-meta-title">
            <p>DRC集群：{{mhaAName}}---{{mhaBName}}</p>
          </div>
          <div class="ivu-list-item-meta-title">
            <p>数据库名：{{$route.query.dbName}}</p>
          </div>
          <div class="ivu-list-item-meta-title">
            <p>表名：{{$route.query.tableName}}</p>
          </div>
          <div class="ivu-list-item-meta-title">一致性结果：
            <Tag :color="isInconsistency" size="medium">{{inconsistency}}</Tag>
          </div>
          <Divider />
          <div class="ivu-list-item-meta-title">机房：{{mhaADc}}</div>
          <Table style="margin-top: 20px" :columns="mhaAColumn" :data="mhaARecord" size="small"></Table>
          <Divider />
          <div class="ivu-list-item-meta-title">机房：{{mhaBDc}}</div>
          <Table style="margin-top: 20px" :columns="mhaBColumn" :data="mhaBRecord" size="small"></Table>
          <Divider />
          <Card>
            <p slot="title">
              一致性处理
            </p>
            <Form>
              <FormItem>
                <div class="ivu-list-item-meta-title">机房：{{mhaADc}}</div>
              </FormItem>
              <FormItem label="操作SQL">
                <codemirror v-model="operateCode1" :options="operateCmOptions"></codemirror>
              </FormItem>
            </Form>
            <Button :style="{marginLeft: '1000px'}" type="primary" @click="SubmitSrcSql">
              提交
            </Button>
            <Divider />
            <Form>
              <FormItem>
                <div class="ivu-list-item-meta-title">机房：{{mhaBDc}}</div>
              </FormItem>
              <FormItem label="操作SQL">
                <codemirror v-model="operateCode2" :options="operateCmOptions"></codemirror>
              </FormItem>
              <Button :style="{marginLeft: '1000px'}" type="primary" @click="SubmitDestSql">
                提交
              </Button>
              <Divider />
            </Form>
          </Card>
          <Modal
            v-model="this.submitSrcSql"
            title="执行SQL"
            width="1000px"
            @on-ok="okSubmitSrcSql"
            @on-cancel="cancelSubmitSrcSql">
            <Form>
              <FormItem label="执行机房">
                <Tag color="blue">{{mhaADc}}</Tag>
              </FormItem>
              <FormItem label="执行SQL">
                <codemirror ref="submitSrcSqlEditor" v-model="operateCode1" :options="cmOptions"></codemirror>
              </FormItem>
            </Form>
          </Modal>
          <Modal
            v-model="this.submitDestSql"
            title="执行SQL"
            width="1000px"
            @on-ok="okSubmitDestSql"
            @on-cancel="cancelSubmitDestSql">
            <Form>
              <FormItem label="执行机房">
                <Tag color="blue">{{mhaBDc}}</Tag>
              </FormItem>
              <FormItem label="执行SQL">
                <codemirror ref="submitDestSqlEditor" v-model="operateCode2" :options="cmOptions"></codemirror>
              </FormItem>
            </Form>
          </Modal>
          <Modal
            v-model="this.submitResult"
            title="执行结果"
            width="1000px"
            @on-ok="okSubmitResult"
            @on-cancel="okSubmitResult">
            <Form>
              <FormItem label="执行结果">
                <Alert :type="this.operationStatus" show-icon>执行结果：{{ this.operationMsg }}</Alert>
              </FormItem>
            </Form>
          </Modal>
        </Card>
      </div>
    </Content>
  </base-component>
</template>

<script>
import { codemirror } from 'vue-codemirror'
import 'codemirror/theme/ambiance.css'
import 'codemirror/mode/sql/sql.js'
export default {
  name: 'incrementDataConsistencyHandle',
  components: {
    codemirror
  },
  data () {
    return {
      isFather: true,
      fatherMenu: '/incrementDataConsistencyResult',
      mhaAColumn: [],
      mhaARecord: [],
      mhaBColumn: [],
      mhaBRecord: [],
      mhaADc: '',
      mhaBDc: '',
      mhaAName: '',
      mhaBName: '',
      isInconsistency: false,
      inconsistency: '',
      operateCmOptions: {
        value: '',
        mode: 'text/x-mysql',
        theme: 'ambiance',
        lineWrapping: true,
        autoRefresh: true
      },
      cmOptions: {
        value: '',
        mode: 'text/x-mysql',
        theme: 'ambiance',
        lineWrapping: true,
        height: 100,
        readOnly: true,
        lineNumbers: true,
        autoRefresh: true
      },
      operateCode1: '',
      operateCode2: '',
      operationStatus: '',
      operationMsg: '',
      submitResult: false,
      submitSrcSql: false,
      submitDestSql: false
    }
  },
  methods: {
    getCurrentRecord () {
      const that = this
      this.axios.get('api/drc/v1/monitor/consistency/increment/current?' + 'mhaGroupId=' + this.$route.query.mhaGroupId + '&dbName=' + this.$route.query.dbName + '&tableName=' + this.$route.query.tableName + '&key=' + this.$route.query.key + '&keyValue=' + this.$route.query.keyValue)
        .then(res => {
          that.mhaAColumn = res.data.data.mhaAColumnPattern
          that.mhaARecord = res.data.data.mhaACurrentResultList
          that.mhaBColumn = res.data.data.mhaBColumnPattern
          that.mhaBRecord = res.data.data.mhaBCurrentResultList
          that.mhaADc = res.data.data.mhaADc
          that.mhaBDc = res.data.data.mhaBDc
          that.mhaAName = res.data.data.mhaAName
          that.mhaBName = res.data.data.mhaBName
          that.isInconsistency = res.data.data.markDifferentRecord === true ? 'error' : 'success'
          that.inconsistency = res.data.data.markDifferentRecord === true ? '数据不一致' : '数据一致'
        })
    },
    SubmitSrcSql () {
      setTimeout(() => {
        this.$refs.submitSrcSqlEditor.codemirror.refresh()
      }, 1)
      this.submitSrcSql = true
    },
    async okSubmitSrcSql () {
      const that = this
      await this.$axios.post('/api/drc/v1/monitor/consistency/update',
        {
          userName: sessionStorage.getItem('userName'),
          mhaGroupId: this.$route.query.mhaGroupId,
          mhaName: this.mhaAName,
          sql: this.operateCode1
        })
        .then(res => {
          that.operationMsg = res.data.message
          if (res.data.status === 1) {
            that.operationStatus = 'error'
          } else {
            that.operationStatus = 'success'
          }
        })
      this.submitResult = true
    },
    cancelSubmitSrcSql () {
      this.submitSrcSql = false
    },
    okSubmitResult () {
      this.submitResult = false
      this.submitSrcSql = false
      this.submitDestSql = false
      this.operationMsg = ''
      this.operationStatus = ''
    },
    SubmitDestSql () {
      setTimeout(() => {
        this.$refs.submitDestSqlEditor.codemirror.refresh()
      }, 1)
      this.submitDestSql = true
    },
    async okSubmitDestSql () {
      const that = this
      await this.$axios.post('/api/drc/v1/monitor/consistency/update',
        {
          userName: sessionStorage.getItem('userName'),
          mhaName: this.mhaBName,
          sql: this.operateCode2
        })
        .then(res => {
          that.operationMsg = res.data.message
          if (res.data.status === 1) {
            that.operationStatus = 'error'
          } else {
            that.operationStatus = 'success'
          }
        })
      this.submitResult = true
    },
    cancelSubmitDestSql () {
      this.submitDestSql = false
    }
  },
  created () {
    this.getCurrentRecord()
  }
}
</script>

<style>
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
</style>

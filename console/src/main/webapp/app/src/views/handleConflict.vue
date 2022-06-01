<template>
  <base-component :isFather="isFather" :subMenuName="['1']" :fatherMenu="fatherMenu">
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/monitor">冲突日志</BreadcrumbItem>
      <BreadcrumbItem>当前结果及自助处理</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div :style="{padding: '1px 1px',height: '100%'}">
        <Card>
          <p slot="title">
            自动冲突处理结果
          </p>
          <div v-if="getConflictLogStatus === 1" class="ivu-list-item-meta-title">
            getCurrentConflictLog occur exception
          </div>
          <div class="ivu-list-item-meta-title">事务提交结果：
            <Tag :color="lastResult==commit?success:error" size="medium">{{lastResult}}</Tag>
          </div>
          <div class="ivu-list-item-meta-title">所有机房当前冲突事务记录：
            <Tag :color="isDiffTransaction==false?success:error" size="medium">{{diffTransaction}}</Tag>
          </div>
          <Divider/>
          <div class="ivu-list-item-meta-title">源机房当前值({{srcDc}})</div>
          <Table style="margin-top: 20px" v-for="(srcTableItem, index) in srcTableItems" :key="index"
                 :columns="srcTableItem.column" :data="srcTableItem.record" size="small"></Table>
          <Divider/>
          <div class="ivu-list-item-meta-title">目标机房当前值({{destDc}})</div>
          <Table style="margin-top: 20px" v-for="(destTableItem, index) in destTableItems" :key="index"
                 :columns="destTableItem.column" :data="destTableItem.record" size="small"></Table>
        </Card>
        <Divider/>
        <Collapse>
          <div @click="refreshEditor()">
            <Panel name="1" @click="refreshEditor">
              自动冲突处理流程
              <div slot="content">
                <div class="ivu-list-item-meta-title">原SQL</div>
                <codemirror ref="cm1" v-model="rawSqlList" :options="cmOptions"
                ></codemirror>
                <Divider/>
                <div class="ivu-list-item-meta-title">原SQL处理结果</div>
                <codemirror ref="cm2" v-model="rawSqlExecutedResultList" :options="cmOptions"></codemirror>
                <Divider/>
                <div class="ivu-list-item-meta-title">冲突时Record</div>
                <codemirror ref="cm3" v-model="destCurrentRecordList" :options="cmOptions"></codemirror>
                <Divider/>
                <div class="ivu-list-item-meta-title">冲突处理SQL</div>
                <codemirror ref="cm4" v-model="conflictHandleSqlList" :options="cmOptions"></codemirror>
                <Divider/>
                <div class="ivu-list-item-meta-title">冲突处理SQL结果</div>
                <codemirror ref="cm5" v-model="conflictHandleSqlExecutedResultList" :options="cmOptions"></codemirror>
              </div>
            </Panel>
          </div>
        </Collapse>
        <Divider/>
        <Card>
          <p slot="title">
            冲突自助处理
          </p>
          <Form>
            <FormItem>
              <div class="ivu-list-item-meta-title">源机房({{srcDc}})</div>
            </FormItem>
            <FormItem label="操作SQL">
              <codemirror v-model="operateCode1" :options="operateCmOptions"></codemirror>
            </FormItem>
          </Form>
          <Button :style="{marginLeft: '1000px'}" type="primary" @click="SubmitSrcSql">
            提交
          </Button>
          <Divider/>
          <Form>
            <FormItem>
              <div class="ivu-list-item-meta-title">目标机房({{destDc}})</div>
            </FormItem>
            <FormItem label="操作SQL">
              <codemirror v-model="operateCode2" :options="operateCmOptions"></codemirror>
            </FormItem>
            <Button :style="{marginLeft: '1000px'}" type="primary" @click="SubmitDestSql">
              提交
            </Button>
            <Divider/>
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
              <Tag color="blue">{{srcDc}}</Tag>
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
              <Tag color="blue">{{destDc}}</Tag>
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
      </div>
    </Content>
  </base-component>
</template>

<script>
import { codemirror } from 'vue-codemirror'
import 'codemirror/theme/ambiance.css'
import 'codemirror/mode/sql/sql.js'

export default {
  name: 'hadleConflict',
  components: {
    codemirror
  },
  data () {
    return {
      getConflictLogStatus: '',
      isDiffTransaction: true,
      diffTransaction: '',
      collapse: '1',
      srcTableItems: [],
      destTableItems: [],
      commit: 'commit',
      success: 'success',
      error: 'error',
      isFather: true,
      fatherMenu: '/monitor',
      operationStatus: '',
      operationMsg: '',
      submitResult: false,
      submitSrcSql: false,
      submitDestSql: false,
      rawSqlList: '',
      rawSqlExecutedResultList: '',
      destCurrentRecordList: '',
      conflictHandleSqlList: '',
      conflictHandleSqlExecutedResultList: '',
      lastResult: '',
      srcDc: '',
      destDc: '',
      srcMhaName: '',
      destMhaName: '',
      curSrcRecord: '',
      curDestRecord: '',
      operateCode1: '',
      operateCode2: '',
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
      operateCmOptions: {
        value: '',
        mode: 'text/x-mysql',
        theme: 'ambiance',
        lineWrapping: true,
        autoRefresh: true
      }
    }
  },
  methods: {
    getCurrentRecord () {
      const that = this
      this.axios.get('/api/drc/v1/logs/record/conflicts/' + this.$route.query.id)
        .then(res => {
          if (res.data.status === 1) {
            that.getConflictLogStatus = 1
          } else {
            that.getConflictLogStatus = 0
            console.log('test record data')
            console.log(res)
            that.rawSqlList = res.data.data.rawSqlList
            that.rawSqlExecutedResultList = res.data.data.rawSqlExecutedResultList
            that.destCurrentRecordList = res.data.data.destCurrentRecordList
            that.conflictHandleSqlList = res.data.data.conflictHandleSqlList
            that.conflictHandleSqlExecutedResultList = res.data.data.conflictHandleSqlExecutedResultList
            that.lastResult = res.data.data.lastResult
            that.srcDc = res.data.data.srcDc
            that.destDc = res.data.data.destDc
            that.srcMhaName = res.data.data.srcMhaName
            that.destMhaName = res.data.data.destMhaName
            that.curSrcRecord = res.data.data.srcRecord
            that.curDestRecord = res.data.data.destRecord
            that.srcTableItems = res.data.data.srcTableItems
            that.destTableItems = res.data.data.destTableItems
            that.isDiffTransaction = res.data.data.diffTransaction
            that.diffTransaction = res.data.data.diffTransaction === true ? '数据不一致' : '数据一致'
            console.log('test column')
            console.log(that.destTableItems[0].column)
          }
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
      await this.$axios.post('/api/drc/v1/logs/record/conflicts/update',
        {
          userName: sessionStorage.getItem('userName'),
          mhaName0: this.srcMhaName,
          mhaName1: this.destMhaName,
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
      await this.$axios.post('/api/drc/v1/logs/record/conflicts/update',
        {
          userName: sessionStorage.getItem('userName'),
          mhaName0: this.destMhaName,
          mhaName1: this.srcMhaName,
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
    },
    refreshEditor () {
      setTimeout(() => {
        this.$refs.cm1.codemirror.refresh()
        this.$refs.cm2.codemirror.refresh()
        this.$refs.cm3.codemirror.refresh()
        this.$refs.cm4.codemirror.refresh()
        this.$refs.cm5.codemirror.refresh()
      }, 10)
      console.log('hello code mirror')
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

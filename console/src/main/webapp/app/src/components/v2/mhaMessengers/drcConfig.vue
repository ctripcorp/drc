<template v-if="current === 0" :key="0">
  <div>
    <Row>
      <Form ref="drc1" :model="drc" :label-width="250" style="float: left; margin-top: 50px">
        <FormItem label="集群名" prop="mhaName" style="width: 600px">
          <Input v-model="drc.mhaName" readonly placeholder="请输入集群名"/>
        </FormItem>
        <FormItem label="选择Replicator" prop="replicator">
          <Select v-model="drc.replicators" multiple style="width: 250px" placeholder="选择Replicator">
            <Option v-for="item in drc.replicatorList" :value="item.ip" :key="item.ip">{{ item.ip }} —— {{ item.az}}
            </Option>
          </Select>
          &nbsp;
          <Button type="success" @click="autoConfigReplicator">自动录入</Button>
        </FormItem>
        <FormItem  v-if="showMhaApplierConfig()" label="选择Messenger" prop="messenger">
          <Select v-model="drc.messengers" multiple style="width: 250px" placeholder="选择集群Messenger">
            <Option v-for="item in drc.messengerList" :value="item.ip" :key="item.ip">{{ item.ip }}  [{{ item.type === 1 ? 'A' : (item.type === 7 ? 'M' : '') }}] —— {{ item.az }}
            </Option>
          </Select>
          &nbsp;
          <Button type="success" @click="autoConfigMessenger">自动录入</Button>
        </FormItem>
        <FormItem label="初始拉取位点R" style="width: 600px">
          <Input v-model="drc.rGtidExecuted" placeholder="变更replicator机器时，请输入binlog拉取位点"/>
          <Row>
            <Col span="12">
              <Button @click="queryMhaMachineGtid">查询mha位点</Button>
              <span v-if="hasTest1">
                  <Icon :type="testSuccess1 ? 'ios-checkmark-circle' : 'ios-close-circle'"
                        :color="testSuccess1 ? 'green' : 'red'"/>
                    {{ testSuccess1 ? '查询实时位点成功' : '连接查询失败' }}
                </span>
            </Col>
            <Col span="12">
              <Button type="success" @click="queryMhaGtidCheckRes">位点校验</Button>
            </Col>
          </Row>
        </FormItem>
        <FormItem label="mq配置" style="width: 600px">
          <Button type="primary" ghost @click="goToMqConfigs">mq配置</Button>
        </FormItem>
        <FormItem v-if="showDbMhaApplierConfig()" label="DB Applier 配置"  style="width: 600px">
          <Button type="primary" ghost @click="getMhaDbMessenger">Db Applier 管理</Button>
        </FormItem>
        <FormItem v-if="showMhaApplierConfig()"  label="初始同步位点A" style="width: 600px">
          <Input v-model="drc.aGtidExecuted" placeholder="请输入DRC同步起始位点"/>
        </FormItem>
        <!--      <FormItem label="行过滤" style="width: 600px">-->
        <!--        <Button type="primary" ghost @click="goToConfigRowsFiltersInSrcApplier">配置行过滤</Button>-->
        <!--      </FormItem>-->
      </Form>
    </Row>
    <Form :label-width="250" style="margin-top: 50px">
      <FormItem>
        <Row>
          <Col span="6">
            <Button @click="handleReset('drc1')">重置</Button>
          </Col>
          <Col span="6">
            <Button type="primary" @click="preCheckConfigure ()">提交</Button>
          </Col>
        </Row>
      </FormItem>
      <Modal
        v-model="drc.reviewModal"
        title="确认配置信息"
        width="900px"
        @on-ok="submitConfig">
        <Form style="width: 80%">
          <FormItem label="集群名">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.mhaName" readonly/>
          </FormItem>
          <FormItem label="集群Replicator">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.replicators" readonly/>
          </FormItem>
          <FormItem label="集群Messenger">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.messengers" readonly/>
          </FormItem>
          <FormItem label="新增Replicator的新增binlog拉取位点">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.rGtidExecuted" readonly/>
          </FormItem>
          <FormItem label="Messenger集群的起始位点">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.aGtidExecuted" readonly/>
          </FormItem>
        </Form>
      </Modal>
      <Modal
        v-model="drc.resultModal"
        title="配置结果"
        width="1200px">
        <Form style="width: 100%">
          <FormItem label="集群配置">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="result" readonly/>
          </FormItem>
        </Form>
      </Modal>
      <!--             v-model="drc.warnModal"-->
      <Modal
        v-model="drc.warnModal"
        title="存在一对多共用运行配置请确认"
        width="900px"
        @on-ok="reviewConfigure">
        <label style="color: black">共用replicator配置的集群: </label>
        <input v-model="drc.conflictMha"></input>
        <Divider/>
        <div>
          <p style="color: red">线上一对多replicator配置</p>
          <ul>
            <ol v-for="item in drc.replicators.share" :key="item">{{ item }}</ol>
          </ul>
        </div>
        <Divider/>
        <div>
          <p style="color: black">修改后replicator配置</p>
          <ul>
            <ol v-for="item in drc.replicators.conflictCurrent" :key="item">{{ item }}</ol>
          </ul>
        </div>
      </Modal>
      <Modal
        v-model="gtidCheck.modal"
        title="gitd位点校验结果"
        width="900px">
        <Form style="width: 80%">
          <FormItem label="校验结果">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="gtidCheck.resVo.legal" readonly/>
          </FormItem>
          <FormItem label="当前Mha">
            <Input :autosize="{minRows: 1,maxRows: 30}" v-model="gtidCheck.resVo.mha" readonly/>
          </FormItem>
          <FormItem label="配置位点">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="gtidCheck.resVo.configGtid" readonly/>
          </FormItem>
          <FormItem label="purgedGtid">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="gtidCheck.resVo.purgedGtid" readonly/>
          </FormItem>
        </Form>
      </Modal>
    </Form>
  </div>
</template>

<script>
export default {
  name: 'drcConfig',
  props: {
    mhaName: String,
    dc: String
  },
  data () {
    return {
      hasTest1: false,
      testSuccess1: false,
      result: false,
      gtidCheck: {
        modal: false,
        resVo: {
          mha: '',
          legal: '',
          configGtid: '',
          purgedGtid: ''
        }
      },
      drc: {
        reviewModal: false,
        warnModal: false,
        mhaName: this.mhaName,
        rGtidExecuted: '',
        aGtidExecuted: '',
        replicators: {},
        messengers: {},
        replicatorList: {},
        messengerList: [],
        dbMessengerSwitch: false,
        dbMessengerList: []
      }
    }
  },
  methods: {
    autoConfigReplicator () {
      this.axios.get('/api/drc/v2/resource/mha/auto?mhaName=' + this.drc.mhaName + '&type=0' + '&selectedIps=' + this.drc.replicators)
        .then(response => {
          console.log(response.data)
          this.drc.replicators = []
          response.data.data.forEach(ip => this.drc.replicators.push(ip.ip))
        })
    },
    autoConfigMessenger () {
      this.axios.get('/api/drc/v2/resource/mha/auto?mhaName=' + this.drc.mhaName + '&type=7' + '&selectedIps=' + this.drc.messengers)
        .then(response => {
          console.log(response.data)
          this.drc.messengers = []
          response.data.data.forEach(ip => this.drc.messengers.push(ip.ip))
        })
    },
    async getResources () {
      this.drc.messengerList = []
      this.axios.get('/api/drc/v2/resource/mha/all?mhaName=' + this.drc.mhaName + '&type=0')
        .then(response => {
          console.log(response.data)
          this.drc.replicatorList = response.data.data
        })
      await this.axios.get('/api/drc/v2/resource/mha/all?mhaName=' + this.drc.mhaName + '&type=1')
        .then(response => {
          const applierData = response.data.data
          applierData.forEach(a => {
            const existItem = this.drc.messengerList.find(existing => existing.ip === a.ip)
            if (!existItem) {
              this.drc.messengerList.push(a)
            }
          })
          // this.drc.messengerList = { ...this.drc.messengerList, ...response.data.data }
        })
      await this.axios.get('/api/drc/v2/resource/mha/all?mhaName=' + this.drc.mhaName + '&type=7')
        .then(response => {
          const applierData = response.data.data
          applierData.forEach(a => {
            const existItem = this.drc.messengerList.find(existing => existing.ip === a.ip)
            if (!existItem) {
              this.drc.messengerList.push(a)
            }
          })
          // this.drc.messengerList = { ...this.drc.messengerList, ...response.data.data }
        })
      this.axios.get('/api/drc/v2/config/mha/dbMessenger?mhaName=' + this.drc.mhaName + '&type=1')
        .then(response => {
          this.drc.dbMessengerList = response.data.data
        })
    },
    getResourcesInUse () {
      this.axios.get('/api/drc/v2/mha/replicator', { params: { mhaName: this.drc.mhaName } })
        .then(response => {
          console.log(this.drc.mhaName + ' replicators ' + response.data.data)
          this.drc.replicators = []
          response.data.data.forEach(ip => this.drc.replicators.push(ip))
        })
      this.axios.get('/api/drc/v2/mha/messenger', { params: { mhaName: this.drc.mhaName } })
        .then(response => {
          console.log(this.drc.mhaName + ' messengers ' + response.data.data)
          this.drc.messengers = []
          response.data.data.forEach(ip => this.drc.messengers.push(ip))
        })
      this.axios.get('/api/drc/v2/messenger/initGtid?mhaName=' + this.drc.mhaName)
        .then(response => {
          console.log('aGtidExecuted:' + response.data.data)
          this.drc.aGtidExecuted = response.data.data
        })
    },
    goToMqConfigs () {
      console.log('go to change mq config for ' + this.drc.mhaName)
      this.$router.push({ path: '/v2/mqConfigs', query: { mhaName: this.drc.mhaName } })
    },
    getMhaDbMessenger () {
      this.$router.push({
        path: '/dbMessengers',
        query: {
          mhaName: this.drc.mhaName
        }
      })
    },
    handleReset (name) {
      this.$refs[name].resetFields()
    },
    preCheckConfigure () {
      const params = {
        mhaName: this.drc.mhaName,
        replicatorIps: this.drc.replicators.join(','),
        messengerIps: this.drc.messengers.join(',')
      }
      console.log(params)
      this.axios.get('/api/drc/v2/mha/replicatorIps/check', {
        params: params
      }).then(response => {
        const preCheckRes = response.data.data
        if (preCheckRes.status === 0) {
          // 无风险继续
          this.drc.reviewModal = true
        } else if (preCheckRes.status === 1) {
          // 有风险，进入确认页面
          this.drc.replicators.share = preCheckRes.workingReplicatorIps
          this.drc.replicators.conflictCurrent = this.drc.replicators
          this.drc.conflictMha = preCheckRes.conflictMha
          this.drc.warnModal = true
        } else {
          // 响应失败
          window.alert('config preCheck fail')
        }
      })
    },
    queryMhaMachineGtid () {
      this.hasTest1 = false
      this.drc.rGtidExecuted = ''
      const that = this
      console.log('/api/drc/v2/mha/gtid/executed?mha=' + this.drc.mhaName)
      that.axios.get('/api/drc/v2/mha/gtid/executed?mha=' + this.drc.mhaName)
        .then(response => {
          this.hasTest1 = true
          if (response.data.status === 0) {
            this.drc.rGtidExecuted = response.data.data
            this.testSuccess1 = true
          } else {
            this.testSuccess1 = false
            this.$Message.warning('查询位点失败: ' + response.data.message)
          }
        })
        .catch(message => {
          this.$Message.error('查询位点异常: ' + message)
        })
    },
    queryMhaGtidCheckRes () {
      if (this.drc.rGtidExecuted == null || this.drc.rGtidExecuted === '') {
        this.$Message.warning('位点为空！')
        return
      }
      const that = this
      console.log('/api/drc/v2/mha/gtid/checkResult?mha=' + this.drc.mhaName +
        '&configGtid=' + this.drc.rGtidExecuted)
      that.axios.get('/api/drc/v2/mha/gtid/checkResult?mha=' + this.drc.mhaName +
        '&configGtid=' + this.drc.rGtidExecuted)
        .then(response => {
          if (response.data.status === 0) {
            this.gtidCheck.resVo = {
              mha: this.drc.mhaName,
              legal: response.data.data.legal === true ? '合理位点' : 'binlog已经被purge',
              configGtid: this.drc.rGtidExecuted,
              purgedGtid: response.data.data.purgedGtid
            }
            this.gtidCheck.modal = true
          } else {
            console.log('api fail:' + '/api/drc/v2/mha/gtid/checkResult?mha=' + this.drc.mhaName +
              '&configGtid=' + this.drc.rGtidExecuted)
          }
        })
        .catch(message => {
          this.$Message.error('校验位点异常: ' + message)
        })
    },
    showDbMhaApplierConfig () {
      return this.drc.dbMessengerSwitch || this.hasAppliers(this.drc.dbMessengerList)
    },
    showMhaApplierConfig () {
      return true
      // return !this.hasAppliers(this.drc.dbMessengerList)
    },
    hasAppliers (dbApplierDtos) {
      for (const x of dbApplierDtos) {
        if (x.ips && x.ips.length > 0) {
          return true
        }
      }
      return false
    },
    submitConfig () {
      const that = this
      this.axios.post('/api/drc/v2/config/messenger/submitConfig', {
        mhaName: this.drc.mhaName,
        replicatorIps: this.drc.replicators,
        messengerIps: this.drc.messengers,
        aGtidExecuted: this.drc.aGtidExecuted,
        rGtidExecuted: this.drc.rGtidExecuted
      }).then(response => {
        console.log(response.data)
        if (response.data.status === 1) {
          this.$Message.warning({
            content: '提交失败：' + response.data.message,
            duration: 10
          })
          return
        }
        that.result = response.data.data
        that.drc.reviewModal = false
        that.drc.resultModal = true
      }).catch(message => {
        this.$Message.error({
          content: '提交异常: ' + message,
          duration: 10
        })
      })
    },
    reviewConfigure () {
      this.drc.reviewModal = true
    }
  },
  created () {
    this.getResources()
    this.getResourcesInUse()
  }
}
</script>

<style scoped>

</style>

<template v-if="current === 0" :key="0">
  <div>
    <Row>
      <Form ref="drc1" :model="drc"  :label-width="250" style="float: left; margin-top: 50px">
        <FormItem label="集群名" prop="mhaName" style="width: 600px">
          <Input v-model="drc.mhaName" readonly placeholder="请输入集群名"/>
        </FormItem>
        <FormItem label="选择Replicator" prop="replicator">
          <Select v-model="drc.replicators" multiple style="width: 200px" placeholder="选择集群Replicator">
            <Option v-for="item in drc.replicatorList" :value="item" :key="item">{{ item }}</Option>
          </Select>
        </FormItem>
        <FormItem label="选择Messenger" prop="messenger">
          <Select v-model="drc.messengers" multiple style="width: 200px" placeholder="选择集群Messenger">
            <Option v-for="item in drc.messengerList" :value="item" :key="item">{{ item }}</Option>
          </Select>
        </FormItem>
        <FormItem label="mq配置" style="width: 600px">
          <Button type="primary" ghost @click="goToMqConfigs">mq配置</Button>
        </FormItem>
        <FormItem label="设置executedGtid" style="width: 600px">
          <Input v-model="drc.gtidExecuted" placeholder="请输入源集群executedGtid，不填自动获取本侧gtid"/>
<!--          <Button @click="queryExecutedGtid">查询gtid</Button>-->
          <span v-if="hasTest1">
                    <Icon :type="testSuccess1 ? 'ios-checkmark-circle' : 'ios-close-circle'"
                          :color="testSuccess1 ? 'green' : 'red'"/>
                      {{ testSuccess1 ? '连接查询成功' : '连接查询失败，请手动输入gtid' }}
              </span>
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
              <FormItem label="Messenger集群executedGtid">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.gtidExecuted" readonly/>
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
            <ol v-for="item in drc.replicators.share" :key="item">{{item}}</ol>
          </ul>
        </div>
        <Divider/>
        <div>
          <p style="color: black">修改后replicator配置</p>
          <ul>
            <ol v-for="item in drc.replicators.conflictCurrent" :key="item">{{item}}</ol>
          </ul>
        </div>
      </Modal>
    </Form>
  </div>
</template>

<script>
export default {
  name: 'drcConfig.vue',
  props: {
    mhaName: String,
    dc: String
  },
  data () {
    return {
      hasTest1: false,
      testSuccess1: false,
      result: false,
      drc: {
        reviewModal: false,
        warnModal: false,
        mhaName: this.mhaName,
        gtidExecuted: '',
        replicators: {},
        messengers: {},
        replicatorList: {},
        messengerList: {}
      }
    }
  },
  methods: {
    getResources () {
      // this.axios.get('/api/drc/v1/meta/resources?type=R')
      this.axios.get('/api/drc/v1/meta/mhas/' + this.drc.mhaName + '/resources/all/types/R')
        .then(response => {
          console.log(response.data)
          this.drc.replicatorList = []
          response.data.data.forEach(ip => this.drc.replicatorList.push(ip))
        })
      // actually , messenger is an applier
      // this.axios.get('/api/drc/v1/meta/resources?type=A')
      this.axios.get('/api/drc/v1/meta/mhas/' + this.drc.mhaName + '/resources/all/types/A')
        .then(response => {
          console.log(response.data)
          this.drc.messengerList = []
          response.data.data.forEach(ip => this.drc.messengerList.push(ip))
        })
    },
    getResourcesInUse () {
      this.axios.get('/api/drc/v1/meta/resources/using/types/R?localMha=' + this.drc.mhaName)
        .then(response => {
          console.log(this.drc.mhaName + ' replicators ' + response.data.data)
          this.drc.replicators = []
          response.data.data.forEach(ip => this.drc.replicators.push(ip))
        })
      this.axios.get('/api/drc/v1/meta/resources/using/types/A?localMha=' + this.drc.mhaName)
        .then(response => {
          console.log(this.drc.mhaName + ' messengers ' + response.data.data)
          this.drc.messengers = []
          response.data.data.forEach(ip => this.drc.messengers.push(ip))
        })
    },
    goToMqConfigs () {
      console.log('go to change mq config for ' + this.drc.mhaName)
      this.$router.push({ path: '/mqConfigs', query: { mhaName: this.drc.mhaName } })
    },
    handleReset (name) {
      this.$refs[name].resetFields()
    },
    preCheckConfigure () {
      this.axios.post('/api/drc/v1/build/replicatorIps/check', {
        mhaName: this.drc.mhaName,
        replicatorIps: this.drc.replicators,
        messengerIps: this.drc.messengers,
        gtidExecuted: this.drc.gtidExecuted
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
    submitConfig () {
      const that = this
      this.axios.post('/api/drc/v1/build/config', {
        mhaName: this.drc.mhaName,
        replicatorIps: this.drc.replicators,
        messengerIps: this.drc.messengers,
        gtidExecuted: this.drc.gtidExecuted
      }).then(response => {
        console.log(response.data)
        that.result = response.data.data
        that.drc.reviewModal = false
        that.drc.resultModal = true
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

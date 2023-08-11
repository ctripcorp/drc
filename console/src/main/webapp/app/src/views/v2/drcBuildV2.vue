<template v-if="current === 0" :key="0">
  <div>
    <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
      {{title}}
      <span slot="desc" v-html="message"></span>
    </Alert>
    <Row>
      <i-col span="12">
        <Form ref="drc1" :model="srcBuildParam" :rules="ruleDrc" :label-width="250" style="float: left; margin-top: 50px">
          <FormItem label="源集群名" prop="srcMhaName" style="width: 600px">
            <Input v-model="srcBuildParam.mhaName" @input="changeSrcMha" placeholder="请输入源集群名"/>
          </FormItem>
          <FormItem label="选择Replicator" prop="replicator">
            <Select v-model="srcBuildParam.replicatorIps" multiple style="width: 200px" placeholder="选择源集群Replicator">
              <Option v-for="item in replicatorList.src" :value="item" :key="item">{{ item }}</Option>
            </Select>
          </FormItem>
          <FormItem label="选择Applier" prop="applier">
            <Select v-model="srcBuildParam.applierIps" multiple style="width: 200px" placeholder  ="选择源集群Applier">
              <Option v-for="item in applierList.src" :value="item" :key="item">{{ item }}</Option>
            </Select>
          </FormItem>
          <FormItem label="初始拉取位点R" style="width: 600px">
            <Input v-model="srcBuildParam.replicatorInitGtid" placeholder="变更replicator机器时,请输入binlog拉取位点"/>
            <Row>
              <Col span="12">
                <Button @click="querySrcMhaMachineGtid">查询mha位点</Button>
                <span v-if="hasTest1">
                  <Icon :type="testSuccess1 ? 'ios-checkmark-circle' : 'ios-close-circle'"
                        :color="testSuccess1 ? 'green' : 'red'"/>
                    {{ testSuccess1 ? '查询实时位点成功' : '连接查询失败' }}
                </span>
              </Col>
              <Col span="12">
                <Button type="success" @click="querySrcMhaGtidCheckRes">位点校验</Button>
              </Col>
            </Row>
          </FormItem>
          <FormItem label="初始同步位点A" style="width: 600px">
            <Input v-model="srcBuildParam.applierInitGtid" placeholder="请输入DRC同步起始位点"/>
          </FormItem>
          <FormItem label="同步配置" style="width: 600px">
            <Button type="primary" ghost @click="getSrcDbReplications">同步表管理</Button>
          </FormItem>
        </Form>
      </i-col>
      <i-col span="12">
        <Form ref="drc1" :model="dstBuildParam" :rules="ruleDrc" :label-width="250" style="float: left; margin-top: 50px">
          <FormItem label="目标集群名" prop="dstMhaName" style="width: 600px">
            <Input v-model="dstBuildParam.mhaName" @input="changeDstMha" placeholder="请输入目标集群名"/>
          </FormItem>
          <FormItem label="选择Replicator" prop="replicator">
            <Select v-model="dstBuildParam.replicatorIps" multiple style="width: 200px" placeholder="选择目标集群Replicator">
              <Option v-for="item in replicatorList.dst" :value="item" :key="item">{{ item }}</Option>
            </Select>
          </FormItem>
          <FormItem label="选择Applier" prop="applier">
            <Select v-model="dstBuildParam.applierIps" multiple style="width: 200px" placeholder  ="选择目标集群Applier">
              <Option v-for="item in applierList.dst" :value="item" :key="item">{{ item }}</Option>
            </Select>
          </FormItem>
          <FormItem label="初始拉取位点R" style="width: 600px">
            <Input v-model="dstBuildParam.replicatorInitGtid" placeholder="变更replicator机器时,请输入binlog拉取位点"/>
            <Row>
              <Col span="12">
                <Button @click="queryDstMhaMachineGtid">查询mha位点</Button>
                <span v-if="hasTest1">
                  <Icon :type="testSuccess1 ? 'ios-checkmark-circle' : 'ios-close-circle'"
                        :color="testSuccess1 ? 'green' : 'red'"/>
                    {{ testSuccess1 ? '查询实时位点成功' : '连接查询失败' }}
                </span>
              </Col>
              <Col span="12">
                <Button type="success" @click="queryDstMhaGtidCheckRes">位点校验</Button>
              </Col>
            </Row>
          </FormItem>
          <FormItem label="初始同步位点A" style="width: 600px">
            <Input v-model="dstBuildParam.applierInitGtid" placeholder="请输入DRC同步起始位点"/>
          </FormItem>
          <FormItem label="同步配置" style="width: 600px">
            <Button type="primary" ghost @click="getDstDbReplications">同步表管理</Button>
          </FormItem>
        </Form>
      </i-col>
    </Row>
    <Form :label-width="250" style="margin-top: 50px">
      <FormItem>
        <Button @click="getSrcMhaAppliersInUse">重置</Button>
        <br><br>
        <Button type="primary" @click="preCheckConfigure ()">提交</Button>
      </FormItem>
      <Modal
        v-model="reviewModal"
        title="确认配置信息"
        width="900px"
        @on-ok="submitConfig">
        <Row :gutter="5">
          <i-col span="12">
            <Form style="width: 80%">
              <FormItem label="源集群名">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="srcBuildParam.mhaName" readonly/>
              </FormItem>
              <FormItem label="源集群端Replicator">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="srcBuildParam.replicatorIps" readonly/>
              </FormItem>
              <FormItem label="源集群端Applier">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="srcBuildParam.applierIps" readonly/>
              </FormItem>
              <FormItem label="源集群端R位点">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="srcBuildParam.replicatorInitGtid" readonly/>
              </FormItem>
              <FormItem label="源集群端A位点">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="srcBuildParam.applierInitGtid" readonly/>
              </FormItem>
            </Form>
          </i-col>
          <i-col span="12">
            <Form style="width: 80%">
              <FormItem label="目标集群名">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="dstBuildParam.mhaName" readonly/>
              </FormItem>
              <FormItem label="目标集群端Replicator">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="dstBuildParam.replicatorIps" readonly/>
              </FormItem>
              <FormItem label="目标集群端Applier">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="dstBuildParam.applierIps" readonly/>
              </FormItem>
              <FormItem label="目标集群端R位点">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="dstBuildParam.replicatorInitGtid" readonly/>
              </FormItem>
              <FormItem label="目标集群端A位点">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="dstBuildParam.applierInitGtid" readonly/>
              </FormItem>
            </Form>
          </i-col>
        </Row>
      </Modal>
      <Modal
        v-model="resultModal"
        title="配置结果"
        width="1200px">
        <Form style="width: 100%">
          <FormItem label="集群配置">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="result" readonly/>
          </FormItem>
        </Form>
      </Modal>
      <Modal
        v-model="gtidCheck.modal"
        title="gitd位点校验结果"
        width="900px">
        <Form style="width: 80%">
          <FormItem  label="校验结果">
            <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="gtidCheck.resVo.legal" readonly/>
          </FormItem>
          <FormItem label="当前Mha">
            <Input  :autosize="{minRows: 1,maxRows: 30}" v-model="gtidCheck.resVo.mha" readonly/>
          </FormItem>
          <FormItem  label="配置位点">
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
  name: 'drcBuildV2',
  props: {
    srcMhaName: String,
    dstMhaName: String,
    srcDc: String,
    dstDc: String,
    env: String
  },
  data () {
    return {
      result: '',
      status: '',
      title: '',
      message: '',
      hasResp: false,
      hasTest1: false,
      testSuccess1: false,
      hasTest2: false,
      testSuccess2: false,
      hasTest3: false,
      testSuccess3: false,
      hasTest4: false,
      testSuccess4: false,
      reviewModal: false,
      resultModal: false,
      replicatorList: {
        src: [],
        dst: []
      },
      applierList: {
        src: [],
        dst: []
      },
      srcBuildParam: {
        mhaName: this.srcMhaName,
        replicatorIps: [],
        applierIps: [],
        replicatorInitGtid: '',
        applierInitGtid: ''
      },
      dstBuildParam: {
        mhaName: this.dstMhaName,
        replicatorIps: [],
        applierIps: [],
        replicatorInitGtid: '',
        applierInitGtid: ''
      },
      gtidCheck: {
        modal: false,
        resVo: {
          mha: '',
          legal: '',
          configGtid: '',
          purgedGtid: ''
        }
      },
      ruleDrc: {
        oldClusterName: [
          { required: true, message: '源集群名不能为空', trigger: 'blur' }
        ],
        newClusterName: [
          { required: true, message: '新集群名不能为空', trigger: 'blur' }
        ],
        env: [
          { required: true, message: '环境不能为空', trigger: 'blur' }
        ],
        replicator: [
          { required: true, message: 'replicator不能为空', trigger: 'blur' }
        ],
        applier: [
          { required: true, message: 'applier不能为空', trigger: 'blur' }
        ]
      }
    }
  },
  computed: {
    dataWithPage () {
      const data = this.nameFilterCheck.tableData
      const start = this.nameFilterCheck.current * this.nameFilterCheck.size - this.nameFilterCheck.size
      const end = start + this.nameFilterCheck.size
      return [...data].slice(start, end)
    }
  },
  methods: {
    handleReset (name) {
      this.$refs[name].resetFields()
    },
    getSrcMhaResources () {
      this.axios.get('/api/drc/v2//mha/resources?mhaName=' + this.srcBuildParam.mhaName + '&type=0')
        .then(response => {
          console.log(response.data)
          this.replicatorList.src = []
          response.data.data.forEach(ip => this.replicatorList.src.push(ip))
        })
      this.axios.get('/api/drc/v2//mha/resources?mhaName=' + this.srcBuildParam.mhaName + '&type=1')
        .then(response => {
          console.log(response.data)
          this.applierList.src = []
          response.data.data.forEach(ip => this.applierList.src.push(ip))
        })
    },
    getDstMhaResources () {
      this.axios.get('/api/drc/v2//mha/resources?mhaName=' + this.dstBuildParam.mhaName + '&type=0')
        .then(response => {
          console.log(response.data)
          this.replicatorList.dst = response.data.data
          // response.data.data.forEach(ip => this.replicatorList.src.push(ip))
        })
      this.axios.get('/api/drc/v2//mha/resources?mhaName=' + this.dstBuildParam.mhaName + '&type=1')
        .then(response => {
          console.log(response.data)
          this.applierList.dst = response.data.data
          // response.data.data.forEach(ip => this.applierList.src.push(ip))
        })
    },
    getSrcMhaReplicatorsInUse () {
      this.axios.get('/api/drc/v2/mha/replicator?mhaName=' + this.srcBuildParam.mhaName)
        .then(response => {
          this.srcBuildParam.replicatorIps = response.data.data
        })
    },
    getDstMhaReplicatorsInUse () {
      this.axios.get('/api/drc/v2/mha/replicator?mhaName=' + this.dstBuildParam.mhaName)
        .then(response => {
          console.log(response.data)
          this.dstBuildParam.replicatorIps = response.data.data
        })
    },
    getSrcMhaAppliersInUse () {
      this.axios.get('/api/drc/v2/config/mha/applier?srcMhaName=' + this.dstBuildParam.mhaName + '&dstMhaName=' + this.srcBuildParam.mhaName)
        .then(response => {
          console.log(response.data)
          this.srcBuildParam.applierIps = response.data.data
        })
      this.axios.get('/api/drc/v2/config/mha/applierGtid?srcMhaName=' + this.dstBuildParam.mhaName + '&dstMhaName=' + this.srcBuildParam.mhaName)
        .then(response => {
          console.log(response.data)
          this.srcBuildParam.applierInitGtid = response.data.data
        })
    },
    getDstMhaAppliersInUse () {
      this.axios.get('/api/drc/v2/config/mha/applier?srcMhaName=' + this.srcBuildParam.mhaName + '&dstMhaName=' + this.dstBuildParam.mhaName)
        .then(response => {
          this.dstBuildParam.applierIps = response.data.data
        })
      this.axios.get('/api/drc/v2/config/mha/applierGtid?srcMhaName=' + this.srcBuildParam.mhaName + '&dstMhaName=' + this.dstBuildParam.mhaName)
        .then(response => {
          console.log(response.data)
          this.dstBuildParam.applierInitGtid = response.data.data
        })
    },
    querySrcMhaMachineGtid () {
      const that = this
      that.axios.get('/api/drc/v2/mha/gtid/executed?mha=' + this.srcBuildParam.mhaName)
        .then(response => {
          this.hasTest1 = true
          if (response.data.status === 0) {
            this.srcBuildParam.replicatorInitGtid = response.data.data
            this.testSuccess1 = true
          } else {
            this.testSuccess1 = false
          }
        })
    },
    querySrcMhaGtidCheckRes () {
      if (this.srcBuildParam.replicatorInitGtid == null || this.srcBuildParam.replicatorInitGtid === '') {
        alert('位点为空！')
        return
      }
      const that = this
      that.axios.get('/api/drc/v2/mha/gtid/checkResult?mha=' + this.srcBuildParam.mhaName +
        '&configGtid=' + this.srcBuildParam.replicatorInitGtid)
        .then(response => {
          if (response.data.status === 0) {
            this.gtidCheck.resVo = {
              mha: this.srcBuildParam.mhaName,
              legal: response.data.data.legal === true ? '合理位点' : 'binlog已经被purge',
              configGtid: this.srcBuildParam.replicatorInitGtid,
              purgedGtid: response.data.data.purgedGtid
            }
            this.gtidCheck.modal = true
          } else {
            alert('位点校验失败!')
          }
        })
    },
    queryDstMhaMachineGtid () {
      const that = this
      that.axios.get('/api/drc/v2/mha/gtid/executed?mha=' + this.dstBuildParam.mhaName)
        .then(response => {
          this.hasTest2 = true
          if (response.data.status === 0) {
            this.dstBuildParam.replicatorInitGtid = response.data.data
            this.testSuccess2 = true
          } else {
            this.testSuccess2 = false
          }
        })
    },
    queryDstMhaGtidCheckRes () {
      if (this.dstBuildParam.replicatorInitGtid == null || this.dstBuildParam.replicatorInitGtid === '') {
        alert('位点为空！')
        return
      }
      const that = this
      that.axios.get('/api/drc/v2/mha/gtid/checkResult?mha=' + this.dstBuildParam.mhaName +
        '&configGtid=' + this.dstBuildParam.replicatorInitGtid)
        .then(response => {
          if (response.data.status === 0) {
            this.gtidCheck.resVo = {
              mha: this.dstBuildParam.mhaName,
              legal: response.data.data.legal === true ? '合理位点' : 'binlog已经被purge',
              configGtid: this.dstBuildParam.replicatorInitGtid,
              purgedGtid: response.data.data.purgedGtid
            }
            this.gtidCheck.modal = true
          } else {
            alert('位点校验失败!')
          }
        })
    },
    changeSrcMha () {
      this.$emit('srcMhaNameChanged', this.srcBuildParam.mhaName)
      this.getSrcMhaResources()
      this.getSrcMhaReplicatorsInUse()
      this.getSrcMhaAppliersInUse()
      this.getDstMhaAppliersInUse()
    },
    changeDstMha () {
      this.$emit('dstMhaNameChanged', this.dstBuildParam.mhaName)
      this.getDstMhaResources()
      this.getDstMhaReplicatorsInUse()
      this.getDstMhaAppliersInUse()
      this.getSrcMhaAppliersInUse()
    },
    start () {
      this.$Loading.start()
    },
    finish () {
      this.$Loading.finish()
    },
    error () {
      this.$Loading.error()
    },
    changeModal (name) {
      this.$refs[name].validate((valid) => {
        if (!valid) {
          this.$Message.error('仍有必填项未填!')
        } else {
          this.drc.reviewModal = true
        }
      })
    },
    preCheckConfigure () {
      this.reviewModal = true
    },
    reviewConfigure () {
      this.drc.reviewModal = true
    },
    submitConfig () {
      const that = this
      this.axios.post('/api/drc/v2/config/', {
        srcBuildParam: this.srcBuildParam,
        dstBuildParam: this.dstBuildParam
      }).then(response => {
        console.log(response.data)
        that.result = response.data.data
        that.reviewModal = false
        that.resultModal = true
      })
    },
    getSrcDbReplications () {
      this.$router.push({
        path: '/dbTables',
        query: {
          srcMhaName: this.dstBuildParam.mhaName,
          dstMhaName: this.srcBuildParam.mhaName,
          srcDc: this.dstDc,
          dstDc: this.srcDc,
          order: true
        }
      })
    },
    getDstDbReplications () {
      this.$router.push({
        path: '/dbTables',
        query: {
          srcMhaName: this.srcBuildParam.mhaName,
          dstMhaName: this.dstBuildParam.mhaName,
          srcDc: this.srcDc,
          dstDc: this.dstDc,
          order: true
        }
      })
    },
    handleChangeSize (val) {
      this.size = val
    }
  },
  created () {
    this.getSrcMhaResources()
    this.getDstMhaResources()
    this.getSrcMhaReplicatorsInUse()
    this.getDstMhaReplicatorsInUse()
    this.getSrcMhaAppliersInUse()
    this.getDstMhaAppliersInUse()
  }
}
</script>
<style scoped>
.demo-split {
  height: 200px;
  border: 1px solid #dcdee2;
}

.demo-split-pane {
  padding: 10px;
}
</style>

<template v-if="current === 0" :key="0">
  <div>
    <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
      {{title}}
      <span slot="desc" v-html="message"></span>
    </Alert>
    <Row>
      <i-col span="12">
        <Form ref="drc1" :model="drc" :rules="ruleDrc" :label-width="250" style="float: left; margin-top: 50px">
          <FormItem label="源集群名" prop="oldClusterName" style="width: 600px">
            <Input v-model="drc.oldClusterName" @input="changeOld" placeholder="请输入源集群名"/>
          </FormItem>
          <FormItem label="选择Replicator" prop="replicator">
            <Select v-model="drc.replicators.old" multiple style="width: 200px" placeholder="选择源集群Replicator">
              <Option v-for="item in drc.replicatorlist.old" :value="item" :key="item">{{ item }}</Option>
            </Select>
          </FormItem>
          <FormItem label="选择Applier" prop="applier">
            <Select v-model="drc.appliers.old" multiple style="width: 200px" placeholder="选择源集群Applier">
              <Option v-for="item in drc.applierlist.old" :value="item" :key="item">{{ item }}</Option>
            </Select>
          </FormItem>
          <FormItem label="设置includedDbs" prop="oldIncludedDbs" style="width: 600px">
            <Input v-model="drc.oldIncludedDbs" placeholder="请输入DB列表，以逗号分隔，不填默认为全部DB"/>
          </FormItem>
          <FormItem label="配置同步对象" prop="oldNameFilter" style="width: 600px">
            <Input v-model="drc.oldNameFilter" type="textarea" :autosize="true" placeholder="请输入表名，支持正则表达式，以逗号分隔，不填默认为全部表"/>
          </FormItem>
          <FormItem label="配置表名映射" prop="oldNameMapping" style="width: 600px">
            <Input v-model="drc.oldNameMapping" type="textarea" :autosize="true" placeholder="请输入映射关系，如：srcDb1.srcTable1,destDb1.destTable1;srcDb2.srcTable2,destDb2.destTable2"/>
          </FormItem>
          <FormItem label="设置executedGtid" style="width: 600px">
            <Input v-model="drc.oldExecutedGtid" placeholder="请输入源集群executedGtid，不填默认自动获取"/>
          </FormItem>
          <FormItem label="设置applyMode" style="width: 600px">
            <Select v-model="drc.oldApplyMode" style="width:200px">
              <Option v-for="item in applyModeList" :value="item.value" :key="item.value">{{ item.label }}</Option>
            </Select>
          </FormItem>
        </Form>
      </i-col>
      <i-col span="12">
        <Form ref="drc2" :model="drc" :rules="ruleDrc" :label-width="250" style="float: left; margin-top: 50px">
          <FormItem label="新集群名" prop="newClusterName" style="width: 600px">
            <Input v-model="drc.newClusterName" @input="changeNew" placeholder="请输入新集群名"/>
          </FormItem>
          <FormItem label="选择Replicator" prop="replicator">
            <Select v-model="drc.replicators.new" multiple style="width: 200px" placeholder="选择新集群Replicator">
              <Option v-for="item in drc.replicatorlist.new" :value="item" :key="item">{{ item }}</Option>
            </Select>
          </FormItem>
          <FormItem label="选择Applier" prop="applier">
            <Select v-model="drc.appliers.new" multiple style="width: 200px" placeholder="选择新集群Applier">
              <Option v-for="item in drc.applierlist.new" :value="item" :key="item">{{ item }}</Option>
            </Select>
          </FormItem>
          <FormItem label="设置includedDbs" prop="newIncludedDbs" style="width: 600px">
            <Input v-model="drc.newIncludedDbs" placeholder="请输入DB列表，以逗号分隔，不填默认全部DB"/>
          </FormItem>
          <FormItem label="配置同步对象" prop="newNameFilter" style="width: 600px">
            <Input v-model="drc.newNameFilter" type="textarea" :autosize="true" placeholder="请输入表名，支持正则表达式，以逗号分隔，不填默认为全部表"/>
          </FormItem>
          <FormItem label="配置表名映射" prop="newNameMapping" style="width: 600px">
            <Input v-model="drc.newNameMapping" type="textarea" :autosize="true" placeholder="请输入映射关系，如：srcDb1.srcTable1,destDb1.destTable1;srcDb2.srcTable2,destDb2.destTable2"/>
          </FormItem>
          <FormItem label="设置executedGtid" style="width: 600px">
            <Input v-model="drc.newExecutedGtid" placeholder="请输入新集群executedGtid，不填默认自动获取"/>
          </FormItem>
          <FormItem label="设置applyMode" style="width: 600px">
            <Select v-model="drc.newApplyMode" style="width:200px">
              <Option v-for="item in applyModeList" :value="item.value" :key="item.value">{{ item.label }}</Option>
            </Select>

          </FormItem>
        </Form>
      </i-col>
    </Row>
    <Form :label-width="250" style="margin-top: 50px">
      <FormItem>
        <Button @click="handleReset('drc1');handleReset('drc2')">重置</Button><br><br>
        <Button type="primary" @click="preCheckConfigure ()">提交</Button>
      </FormItem>
      <Modal
        v-model="drc.reviewModal"
        title="确认配置信息"
        width="900px"
        @on-ok="submitConfig">
        <Row gutter="5px">
          <i-col span="12">
            <Form style="width: 80%">
              <FormItem label="源集群名">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.oldClusterName" readonly/>
              </FormItem>
              <FormItem label="源集群端Replicator">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.replicators.old" readonly/>
              </FormItem>
              <FormItem label="源集群端Applier">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.appliers.old" readonly/>
              </FormItem>
              <FormItem label="源集群端includedDbs">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.oldIncludedDbs" readonly/>
              </FormItem>
              <FormItem label="源集群端同步对象">
                <Input v-model="drc.oldNameFilter" type="textarea" :autosize="true" readonly/>
              </FormItem>
              <FormItem label="源集群端表名映射">
                <Input v-model="drc.oldNameMapping" type="textarea" :autosize="true" readonly/>
              </FormItem>
              <FormItem label="源集群端executedGtid">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.oldExecutedGtid" readonly/>
              </FormItem>
              <FormItem label="源集群端applyMode">
                <Select v-model="drc.oldApplyMode" disabled>
                  <Option v-for="item in applyModeList" :value="item.value" :key="item.value">{{ item.label }}</Option>
                </Select>
              </FormItem>
            </Form>
          </i-col>
          <i-col span="12">
            <Form style="width: 80%">
              <FormItem label="新集群名">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.newClusterName" readonly/>
              </FormItem>
              <FormItem label="新集群端Replicator">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.replicators.new" readonly/>
              </FormItem>
              <FormItem label="新集群端Applier">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.appliers.new" readonly/>
              </FormItem>
              <FormItem label="新集群端includedDbs">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.newIncludedDbs" readonly/>
              </FormItem>
              <FormItem label="新集群端同步对象">
                <Input v-model="drc.newNameFilter" type="textarea" :autosize="true" readonly/>
              </FormItem>
              <FormItem label="新集群端表名映射">
                <Input v-model="drc.newNameMapping" type="textarea" :autosize="true" readonly/>
              </FormItem>
              <FormItem label="新集群端executedGtid">
                <Input type="textarea" :autosize="{minRows: 1,maxRows: 30}" v-model="drc.newExecutedGtid" readonly/>
              </FormItem>
              <FormItem label="新集群端applyMode">
                <Select v-model="drc.newApplyMode" disabled>
                  <Option v-for="item in applyModeList" :value="item.value" :key="item.value">{{ item.label }}</Option>
                </Select>
              </FormItem>
            </Form>
          </i-col>
        </Row>
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
            <label style="color: black">共用replicator配置的集群:  </label>
            <input v-model="drc.conflictMha"></input>
            <Divider/>
              <div>
                <p style="color: red">线上一对多replicator配置</p>
                <ul>
                  <ol v-for="item in drc.replicators.share" :key="item">{{item}}</ol>
                </ul>
              </div>
              <Divider />
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
  name: 'drc',
  props: {
    oldClusterName: String,
    newClusterName: String,
    env: String
  },
  data () {
    return {
      result: '',
      status: '',
      title: '',
      message: '',
      hasResp: false,
      applyModeList: [
        {
          value: 0,
          label: 'SET_GTID (default)'
        },
        {
          value: 1,
          label: 'TRANSACTION_TABLE'
        }
      ],
      drc: {
        reviewModal: false,
        warnModal: false,
        resultModal: false,
        oldClusterName: this.oldClusterName,
        newClusterName: this.newClusterName,
        oldIncludedDbs: '',
        newIncludedDbs: '',
        oldNameFilter: '',
        oldNameMapping: '',
        newNameFilter: '',
        newNameMapping: '',
        oldExecutedGtid: '',
        newExecutedGtid: '',
        oldApplyMode: 0,
        newApplyMode: 0,
        env: this.env,
        needread: false,
        columns: [
          {
            type: 'selection',
            width: 60,
            align: 'center'
          },
          {
            title: 'DB名',
            key: 'name'
          }
        ],
        dbNames: [],
        selectedDbs: [],
        envList: [
          {
            value: 'product',
            label: 'PRODUCT'
          },
          {
            value: 'fat',
            label: 'FAT'
          },
          {
            value: 'lpt',
            label: 'LPT'
          },
          {
            value: 'uat',
            label: 'UAT'
          }
        ],
        replicatorlist: {
          old: [],
          new: []
        },
        conflictMha: '',
        replicators: {
          old: [],
          new: [],
          share: [],
          conflictCurrent: []
        },
        replicator: {
        },
        searchReplicatorIp: '',
        usedReplicatorPorts: '',
        applierlist: {
          old: [],
          new: []
        },
        appliers: {
          old: [],
          new: []
        },
        previewMeta: ''
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
  methods: {
    handleReset (name) {
      this.$refs[name].resetFields()
    },
    getResourcesInOld () {
      this.axios.get('/api/drc/v1/meta/mhas/' + this.drc.oldClusterName + '/resources/all/types/R')
      // this.axios.get('/api/drc/v1/meta/resources?type=R')
        .then(response => {
          console.log(response.data)
          this.drc.replicatorlist.old = []
          response.data.data.forEach(ip => this.drc.replicatorlist.old.push(ip))
        })
      this.axios.get('/api/drc/v1/meta/mhas/' + this.drc.oldClusterName + '/resources/all/types/A')
      // this.axios.get('/api/drc/v1/meta/resources?type=A')
        .then(response => {
          console.log(response.data)
          this.drc.applierlist.old = []
          response.data.data.forEach(ip => this.drc.applierlist.old.push(ip))
        })
    },
    getResourcesInUseInOld () {
      this.axios.get('/api/drc/v1/meta/resources/using/types/R?localMha=' + this.drc.oldClusterName)
        .then(response => {
          console.log(this.drc.oldClusterName + ' replicators ' + response.data.data)
          this.drc.replicators.old = []
          response.data.data.forEach(ip => this.drc.replicators.old.push(ip))
        })
      this.axios.get('/api/drc/v1/meta/resources/using/types/A?localMha=' + this.drc.oldClusterName + '&remoteMha=' + this.drc.newClusterName)
        .then(response => {
          console.log(this.drc.oldClusterName + ' request ' + this.drc.newClusterName + ' appliers ' + response.data.data)
          this.drc.appliers.old = []
          response.data.data.forEach(ip => this.drc.appliers.old.push(ip))
        })
      this.axios.get('/api/drc/v1/meta/includeddbs?localMha=' + this.drc.oldClusterName + '&remoteMha=' + this.drc.newClusterName)
        .then(response => {
          console.log(this.drc.oldClusterName + ' request ' + this.drc.newClusterName + ' includedbs ' + response.data.data)
          this.drc.oldIncludedDbs = response.data.data
        })
      this.axios.get('/api/drc/v1/meta/namefilter?localMha=' + this.drc.oldClusterName + '&remoteMha=' + this.drc.newClusterName)
        .then(response => {
          console.log(this.drc.oldClusterName + ' request ' + this.drc.newClusterName + ' namefilter ' + response.data.data)
          this.drc.oldNameFilter = response.data.data
        })
      this.axios.get('/api/drc/v1/meta/namemapping?localMha=' + this.drc.oldClusterName + '&remoteMha=' + this.drc.newClusterName)
        .then(response => {
          console.log(this.drc.oldClusterName + ' request ' + this.drc.newClusterName + ' namemapping ' + response.data.data)
          this.drc.oldNameMapping = response.data.data
        })
      this.axios.get('/api/drc/v1/meta/applymode?localMha=' + this.drc.oldClusterName + '&remoteMha=' + this.drc.newClusterName)
        .then(response => {
          console.log(this.drc.oldClusterName + ' request ' + this.drc.newClusterName + ' applymode ' + response.data.data)
          this.drc.oldApplyMode = response.data.data
        })
    },
    getResourcesInNew () {
      this.axios.get('/api/drc/v1/meta/mhas/' + this.drc.newClusterName + '/resources/all/types/R')
      // this.axios.get('/api/drc/v1/meta/resources?type=R')
        .then(response => {
          console.log(response.data)
          this.drc.replicatorlist.new = []
          response.data.data.forEach(ip => this.drc.replicatorlist.new.push(ip))
        })
      this.axios.get('/api/drc/v1/meta/mhas/' + this.drc.newClusterName + '/resources/all/types/A')
      // this.axios.get('/api/drc/v1/meta/resources?type=A')
        .then(response => {
          console.log(response.data)
          this.drc.applierlist.new = []
          response.data.data.forEach(ip => this.drc.applierlist.new.push(ip))
        })
    },
    getResourcesInUseInNew () {
      this.axios.get('/api/drc/v1/meta/resources/using/types/R?localMha=' + this.drc.newClusterName)
        .then(response => {
          console.log(this.drc.newClusterName + ' replicators ' + response.data.data)
          this.drc.replicators.new = []
          response.data.data.forEach(ip => this.drc.replicators.new.push(ip))
        })
      this.axios.get('/api/drc/v1/meta/resources/using/types/A?localMha=' + this.drc.newClusterName + '&remoteMha=' + this.drc.oldClusterName)
        .then(response => {
          console.log(this.drc.newClusterName + ' request ' + this.drc.oldClusterName + ' appliers ' + response.data.data)
          this.drc.appliers.new = []
          response.data.data.forEach(ip => this.drc.appliers.new.push(ip))
        })
      this.axios.get('/api/drc/v1/meta/includeddbs?localMha=' + this.drc.newClusterName + '&remoteMha=' + this.drc.oldClusterName)
        .then(response => {
          console.log(this.drc.newClusterName + ' request ' + this.drc.oldClusterName + ' includedbs ' + response.data.data)
          this.drc.newIncludedDbs = response.data.data
        })
      this.axios.get('/api/drc/v1/meta/namefilter?localMha=' + this.drc.newClusterName + '&remoteMha=' + this.drc.oldClusterName)
        .then(response => {
          console.log(this.drc.newClusterName + ' request ' + this.drc.oldClusterName + ' namefilter ' + response.data.data)
          this.drc.newNameFilter = response.data.data
        })
      this.axios.get('/api/drc/v1/meta/namemapping?localMha=' + this.drc.newClusterName + '&remoteMha=' + this.drc.oldClusterName)
        .then(response => {
          console.log(this.drc.newClusterName + ' request ' + this.drc.oldClusterName + ' namemapping ' + response.data.data)
          this.drc.newNameMapping = response.data.data
        })
      this.axios.get('/api/drc/v1/meta/applymode?localMha=' + this.drc.newClusterName + '&remoteMha=' + this.drc.oldClusterName)
        .then(response => {
          console.log(this.drc.newClusterName + ' request ' + this.drc.oldClusterName + ' applymode ' + response.data.data)
          this.drc.newApplyMode = response.data.data
        })
    },
    debug () {
      console.log('replicators: ' + this.drc.replicatorlist)
      console.log('appliers: ' + this.drc.applierlist)
    },
    searchUsedReplicatorPorts () {
      this.axios.get('/api/drc/v1/meta/resources/ip/' + this.drc.searchReplicatorIp)
        .then(response => {
          console.log(response.data)
          this.drc.usedReplicatorPorts = []
          response.data.data.forEach(port => this.drc.usedReplicatorPorts.push(port))
        })
    },
    changeSelection () {
      this.drc.selectedDbs = this.$refs.selection.getSelection()
    },
    changeOld () {
      this.$emit('oldClusterChanged', this.drc.oldClusterName)
      this.getResourcesInOld()
      this.getResourcesInUseInOld()
    },
    changeNew () {
      this.$emit('newClusterChanged', this.drc.newClusterName)
      this.getResourcesInNew()
      this.getResourcesInUseInNew()
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
      console.log('preCheck')
      this.axios.post('/api/drc/v1/meta/config/preCheck', {
        srcMha: this.drc.oldClusterName,
        destMha: this.drc.newClusterName,
        srcReplicatorIps: this.drc.replicators.old,
        srcApplierIps: this.drc.appliers.old,
        srcApplierIncludedDbs: this.drc.oldIncludedDbs,
        srcApplierApplyMode: this.drc.oldApplyMode,
        srcGtidExecuted: this.drc.oldExecutedGtid,
        destReplicatorIps: this.drc.replicators.new,
        destApplierIps: this.drc.appliers.new,
        destApplierIncludedDbs: this.drc.newIncludedDbs,
        destApplierApplyMode: this.drc.newApplyMode,
        destGtidExecuted: this.drc.newExecutedGtid
      }).then(response => {
        const preCheckRes = response.data.data
        if (preCheckRes.status === 0) {
          // 无风险继续
          this.drc.reviewModal = true
        } else if (preCheckRes.status === 1) {
          // 有风险，进入确认页面
          this.drc.replicators.share = preCheckRes.workingReplicatorIps
          this.drc.replicators.conflictCurrent = preCheckRes.conflictMha === this.oldClusterName ? this.drc.replicators.old : this.drc.replicators.new
          this.drc.conflictMha = preCheckRes.conflictMha
          this.drc.warnModal = true
        } else {
          // 响应失败
          window.alert('config preCheck fail')
        }
      })
    },
    reviewConfigure () {
      this.drc.reviewModal = true
    },
    submitConfig () {
      const that = this
      console.log(this.drc.oldClusterName, this.drc.newClusterName)
      console.log(this.drc.replicators.old)
      console.log(this.drc.appliers.old)
      console.log(this.drc.oldIncludedDbs)
      console.log(this.drc.oldNameFilter)
      console.log(this.drc.oldNameMapping)
      console.log(this.drc.oldApplyMode)
      console.log(this.drc.oldExecutedGtid)
      console.log(this.drc.replicators.new)
      console.log(this.drc.appliers.new)
      console.log(this.drc.newIncludedDbs)
      console.log(this.drc.newNameFilter)
      console.log(this.drc.newNameMapping)
      console.log(this.drc.newApplyMode)
      console.log(this.drc.newExecutedGtid)
      this.axios.post('/api/drc/v1/meta/config', {
        srcMha: this.drc.oldClusterName,
        destMha: this.drc.newClusterName,
        srcReplicatorIps: this.drc.replicators.old,
        srcApplierIps: this.drc.appliers.old,
        srcApplierIncludedDbs: this.drc.oldIncludedDbs,
        srcApplierNameFilter: this.drc.oldNameFilter,
        srcApplierNameMapping: this.drc.oldNameMapping,
        srcApplierApplyMode: this.drc.oldApplyMode,
        srcGtidExecuted: this.drc.oldExecutedGtid,
        destReplicatorIps: this.drc.replicators.new,
        destApplierIps: this.drc.appliers.new,
        destApplierIncludedDbs: this.drc.newIncludedDbs,
        destApplierNameFilter: this.drc.newNameFilter,
        destApplierNameMapping: this.drc.newNameMapping,
        destApplierApplyMode: this.drc.newApplyMode,
        destGtidExecuted: this.drc.newExecutedGtid
      }).then(response => {
        console.log(response.data)
        that.result = response.data.data
        that.drc.reviewModal = false
        that.drc.resultModal = true
      })
    }
  },
  created () {
    this.getResourcesInOld()
    this.getResourcesInUseInOld()
    this.getResourcesInNew()
    this.getResourcesInUseInNew()
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

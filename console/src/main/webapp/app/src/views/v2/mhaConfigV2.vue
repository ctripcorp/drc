<template v-if="current === 0" :key="0">
  <div>
    <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
      {{title}}
      <span slot="desc" v-html="message"></span>
    </Alert>
    <Row>
      <i-col span="12">
        <Form ref="srcMha" :model="srcMha" :rules="ruleMhaMachine" :label-width="250" style="float: left; margin-top: 50px">
          <FormItem label="源集群名" prop="mhaName" style="width: 600px">
            <Input v-model="srcMha.mhaName" @input="changeSrcMha" placeholder="请输入源集群名"/>
          </FormItem>
          <FormItem label="添加Ip" prop="ip">
            <Input v-model="srcMha.ip"  number placeholder="请输入录入DB的IP"/>
          </FormItem>
          <FormItem label="Port" prop="port">
            <Input v-model="srcMha.port"  placeholder="请输入录入DB端口"/>
          </FormItem >
          <FormItem label="DB机房" prop="idc">
            <Select v-model="srcMha.idc" style="width: 200px"  placeholder="选择db机房区域" >
              <Option v-for="item in selectOption.drcZoneList" :value="item.value" :key="item.label">{{ item.label }}</Option>
            </Select>
          </FormItem>
          <FormItem label="UUID" prop="uuid">
            <Input v-model="srcMha.uuid"  placeholder="请输入录入DB的uuid"/>
            <Button @click="querySrcMhaUuid">连接查询</Button>
            <span v-if="hasTest1">
            <Icon :type="testSuccess1 ? 'ios-checkmark-circle' : 'ios-close-circle'"
                  :color="testSuccess1 ? 'green' : 'red'"/>
            {{ testSuccess1 ? '连接查询成功' : '连接查询失败，请手动输入uuid' }}
            </span>
          </FormItem>
          <FormItem label="Master" prop="master">
            <Select  v-model="srcMha.master" style="width: 200px"  placeholder="是否为Master" >
              <Option v-for="item in selectOption.isMaster" :value="item.value" :key="item.key">{{ item.key }}</Option>
            </Select>
          </FormItem>
          <FormItem>
            <Button type="primary"  @click="preChecksrcMha ('srcMha')">录入DB</Button><br><br>
          </FormItem>
        </Form>
      </i-col>
      <i-col span="12">
        <Form ref="dstMha" :model="dstMha" :rules="ruleMhaMachine" :label-width="250" style="float: left; margin-top: 50px">
          <FormItem label="新集群名" prop="mhaName" style="width: 600px">
            <Input v-model="dstMha.mhaName" @input="changeDstMha" placeholder="请输入源集群名"/>
          </FormItem>
          <FormItem label="添加Ip" prop="ip">
            <Input v-model="dstMha.ip"  placeholder="请输入录入DB的IP"/>
          </FormItem>
          <FormItem label="Port" prop="port">
            <Input v-model="dstMha.port"  number placeholder="请输入录入DB端口"/>
          </FormItem >
          <FormItem label="DB机房" prop="idc">
            <Select v-model="dstMha.idc" style="width: 200px"  placeholder="选择db机房区域" >
              <Option boolean v-for="item in selectOption.drcZoneList" :value="item.value" :key="item.label">{{ item.label }}</Option>
            </Select>
          </FormItem>
          <FormItem label="UUID" prop="uuid">
            <Input v-model="dstMha.uuid"  placeholder="请输入录入DB的uuid"/>
            <Button @click="queryDstMhaUuid">连接查询</Button>
            <span v-if="hasTest2">
            <Icon :type="testSuccess2 ? 'ios-checkmark-circle' : 'ios-close-circle'"
                  :color="testSuccess2 ? 'green' : 'red'"/>
            {{ testSuccess2 ? '连接查询成功' : '连接查询失败，请手动输入uuid' }}
            </span>
          </FormItem>
          <FormItem label="Master" prop="master">
            <Select  v-model="dstMha.master" style="width: 200px"  placeholder="是否为Master" >
              <Option v-for="item in selectOption.isMaster" :value="item.value" :key="item.key">{{ item.key }}</Option>
            </Select>
          </FormItem>
          <FormItem>
            <Button type="primary"  @click="preCheckdstMha ('dstMha')">录入DB</Button><br><br>
          </FormItem>
        </Form>
      </i-col>
    </Row>
    <Modal
      v-model="srcMha.modal"
      title="录入左侧Mha Db信息"
      @on-ok="submitsrcMha">
      <p>Mha: {{srcMha.mhaName}} </p>
      <p> db信息 [host: {{srcMha.ip}}:{{srcMha.port}}]</p>
      <p> db信息 [isMaster:{{srcMha.master}}]</p>
      <p> db信息 [idc:{{srcMha.idc}}]</p>
      <p> db信息 [uuid:{{srcMha.uuid}}]</p>
    </Modal>
    <Modal
      v-model="dstMha.modal"
      title="录入右侧Mha Db信息"
      @on-ok="submitdstMha">
      <p>Mha: {{dstMha.mhaName}} </p>
      <p> db信息 [host: {{dstMha.ip}}:{{dstMha.port}}]</p>
      <p> db信息 [isMaster:{{dstMha.master}}]</p>
      <p> db信息 [idc:{{dstMha.idc}}]</p>
      <p> db信息 [uuid:{{dstMha.uuid}}]</p>
    </Modal>

  </div>
</template>
<script>
export default {
  name: 'mhaConfig',
  props: {
    srcMhaName: String,
    dstMhaName: String,
    srcDc: String,
    dstDc: String
  },
  data () {
    const validateInteger = (rule, value, callback) => {
      if (/^[0-9]+$/.test(value)) {
        callback()
      } else {
        return callback(new Error('请填写整数port'))
      }
    }
    const validateBoolean = (rule, value, callback) => {
      if (/0|1/.test(value)) {
        callback()
      } else {
        return callback(new Error('请填写整数port'))
      }
    }
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
      srcMha: {
        modal: false,
        mhaName: this.srcMhaName,
        zoneId: this.srcDc,
        ip: '',
        port: '',
        idc: this.srcDc,
        uuid: '',
        master: 1
      },
      dstMha: {
        modal: false,
        mhaName: this.dstMhaName,
        zoneId: this.dstDc,
        ip: '',
        port: '',
        idc: this.dstDc,
        uuid: '',
        master: 1
      },
      ruleMhaMachine: {
        mhaName: [
          { required: true, message: 'mha集群名不能为空', trigger: 'blur' }
        ],
        ip: [
          { required: true, message: 'ip不能为空', trigger: 'blur' }
        ],
        port: [
          { required: true, validator: validateInteger, trigger: 'blur' }
        ],
        idc: [
          { required: true, message: '选择db区域', trigger: 'blur' }
        ],
        uuid: [
          { required: true, message: 'uuid不能为空', trigger: 'blur' }
        ],
        master: [
          { required: true, validator: validateBoolean, trigger: 'blur' }
        ]
      },
      selectOption: {
        isMaster: [
          {
            key: 'Master',
            value: 1
          },
          {
            key: 'Slave',
            value: 0
          }
        ],
        drcZoneList: this.constant.dcList
      }
    }
  },
  methods: {
    changeSrcMha () {
      this.$emit('srcMhaNameChanged', this.srcMha.mhaName)
    },
    changeDstMha () {
      this.$emit('dstMhaNameChanged', this.dstMha.mhaName)
    },
    querySrcMhaUuid () {
      const that = this
      that.axios.get('/api/drc/v2/mha/uuid?mhaName=' + this.srcMha.mhaName + '&ip=' + this.srcMha.ip + '&port=' + this.srcMha.port + '&master=' + this.srcMha.master)
        .then(response => {
          this.hasTest1 = true
          if (response.data.status === 0) {
            this.srcMha.uuid = response.data.data
            this.testSuccess1 = true
          } else {
            this.testSuccess1 = false
          }
        })
    },
    queryDstMhaUuid () {
      const that = this
      that.axios.get('/api/drc/v2/mha/uuid?mhaName=' + this.dstMha.mhaName + '&ip=' + this.dstMha.ip + '&port=' + this.dstMha.port + '&master=' + this.dstMha.master)
        .then(response => {
          this.hasTest2 = true
          if (response.data.status === 0) {
            this.dstMha.uuid = response.data.data
            this.testSuccess2 = true
          } else {
            this.testSuccess2 = false
          }
        })
    },
    preChecksrcMha (name) {
      this.$refs[name].validate((valid) => {
        if (!valid) {
          this.$Message.error('仍有必填项未填!')
        } else {
          this.srcMha.modal = true
        }
      })
    },
    submitsrcMha () {
      const that = this
      that.axios.post('/api/drc/v2/mha/machineInfo', {
        mhaName: this.srcMha.mhaName,
        master: this.srcMha.master,
        mySQLInstance: {
          ip: this.srcMha.ip,
          port: this.srcMha.port,
          idc: this.srcMha.idc,
          uuid: this.srcMha.uuid
        }
      }).then(response => {
        that.hasResp = true
        if (response.data.status === 0) {
          that.status = 'success'
          that.title = 'mha:' + this.srcMha.mhaName + '录入db成功!'
          that.message = response.data.message
        } else {
          that.status = 'error'
          that.title = 'mha:' + this.srcMha.mhaName + '录入db失败!'
          that.message = response.data.message
        }
      })
    },
    preCheckdstMha (name) {
      this.$refs[name].validate((valid) => {
        if (!valid) {
          this.$Message.error('仍有必填项未填!')
        } else {
          this.dstMha.modal = true
        }
      })
    },
    submitdstMha () {
      const that = this
      that.axios.post('/api/drc/v2/mha/machineInfo', {
        mhaName: this.dstMha.mhaName,
        master: this.dstMha.master,
        mySQLInstance: {
          ip: this.dstMha.ip,
          port: this.dstMha.port,
          idc: this.dstMha.idc,
          uuid: this.dstMha.uuid
        }
      }).then(response => {
        that.hasResp = true
        if (response.data.status === 0) {
          that.status = 'success'
          that.title = 'mha:' + this.dstMha.mhaName + '录入db成功!'
          that.message = response.data.message
        } else {
          that.status = 'error'
          that.title = 'mha:' + this.dstMha.mhaName + '录入db失败!'
          that.message = response.data.message
        }
      })
    }
  }
}
</script>

<style scoped>

</style>

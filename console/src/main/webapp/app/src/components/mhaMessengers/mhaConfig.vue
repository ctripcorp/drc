<template v-if="current === 0" :key="0">
  <div>
    <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
      {{title}}
      <span slot="desc" v-html="message"></span>
    </Alert>
    <Form ref="dbInfo" :model="dbInfo" :rules="ruleMhaMachine" :label-width="250" style="float: left; margin-top: 50px">
      <FormItem label="源集群名" prop="mhaName" style="width: 600px">
        <Input v-model="dbInfo.mhaName"  @input="changeMhaName"  placeholder="请输入源集群名"/>
      </FormItem>
      <FormItem label="添加Ip" prop="ip">
        <Input v-model="dbInfo.ip"  number placeholder="请输入录入DB的IP"/>
      </FormItem>
      <FormItem label="Port" prop="port">
        <Input v-model="dbInfo.port"  placeholder="请输入录入DB端口"/>
      </FormItem >
      <FormItem label="DB机房" prop="idc" >
        <Select v-model="dbInfo.idc" style="width: 200px"  placeholder="选择机房区域" @input="changeDc">
          <Option v-for="item in selectOption.drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
        </Select>
      </FormItem>
      <FormItem label="UUID" prop="uuid">
        <Input v-model="dbInfo.uuid"  placeholder="请输入录入DB的uuid"/>
        <Button @click="queryUuid">连接查询</Button>
        <span v-if="hasTest1">
        <Icon :type="testSuccess1 ? 'ios-checkmark-circle' : 'ios-close-circle'"
              :color="testSuccess1 ? 'green' : 'red'"/>
        {{ testSuccess1 ? '连接查询成功' : '连接查询失败，请手动输入uuid' }}
        </span>
      </FormItem>
      <FormItem label="Master" prop="master">
        <Select  v-model="dbInfo.master" style="width: 200px"  placeholder="是否为Master" >
          <Option v-for="item in selectOption.isMaster" :value="item.value" :key="item.key">{{ item.key }}</Option>
        </Select>
      </FormItem>
      <FormItem>
        <Button type="primary"  @click="preCheckDbInfo ('dbInfo')">录入DB</Button><br><br>
      </FormItem>
    </Form>
    <Modal
      v-model="dbInfo.modal"
      title="录入左侧Mha Db信息"
      @on-ok="submitDbInfo">
      <p>Mha: {{dbInfo.mhaName}} </p>
      <p> db信息 [host: {{dbInfo.ip}}:{{dbInfo.port}}]</p>
      <p> db信息 [isMaster:{{dbInfo.master}}]</p>
      <p> db信息 [idc:{{dbInfo.idc}}]</p>
      <p> db信息 [uuid:{{dbInfo.uuid}}]</p>
    </Modal>
  </div>
</template>
<script>
export default {
  name: 'mhaConfig',
  props: {
    mhaName: String,
    dc: String
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
      dbInfo: {
        modal: false,
        mhaName: this.mhaName,
        ip: '',
        port: '',
        idc: this.dc,
        uuid: '',
        master: 1
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
      }
    }
  },
  methods: {
    queryUuid () {
      // todo 重写接口
      const that = this
      console.log('/api/drc/v1/mha/uuid/' + this.dbInfo.mhaName + '/' + this.dbInfo.ip + '/' + this.dbInfo.port + '/' + this.dbInfo.master)
      that.axios.get('/api/drc/v1/mha/uuid/' + this.dbInfo.mhaName + '/' + this.dbInfo.ip + '/' + this.dbInfo.port + '/' + this.dbInfo.master)
        .then(response => {
          this.hasTest1 = true
          if (response.data.status === 0) {
            this.dbInfo.uuid = response.data.data
            this.testSuccess1 = true
          } else {
            this.testSuccess1 = false
          }
        })
    },
    preCheckDbInfo (name) {
      this.$refs[name].validate((valid) => {
        if (!valid) {
          this.$Message.error('仍有必填项未填!')
        } else {
          this.dbInfo.modal = true
        }
      })
    },
    submitDbInfo () {
      const that = this
      that.axios.post('/api/drc/v1/access/mha/machineInfo', {
        mhaName: this.dbInfo.mhaName,
        master: this.dbInfo.master,
        mySQLInstance: {
          ip: this.dbInfo.ip,
          port: this.dbInfo.port,
          idc: this.dbInfo.idc,
          uuid: this.dbInfo.uuid
        }
      }).then(response => {
        that.hasResp = true
        if (response.data.status === 0) {
          that.status = 'success'
          that.title = 'mha:' + this.dbInfo.mhaName + '录入db成功!'
          that.message = response.data.message
        } else {
          that.status = 'error'
          that.title = 'mha:' + this.dbInfo.mhaName + '录入db失败!'
          that.message = response.data.message
        }
      })
    },
    changeMhaName () {
      this.$emit('mhaNameChanged', this.dbInfo.mhaName)
    },
    changeDc () {
      this.$emit('dcChanged', this.dbInfo.idc)
    }
  }
}
</script>

<style scoped>

</style>

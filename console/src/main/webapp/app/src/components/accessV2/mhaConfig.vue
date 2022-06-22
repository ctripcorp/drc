<template v-if="current === 0" :key="0">
  <div>
    <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
      {{title}}
      <span slot="desc" v-html="message"></span>
    </Alert>
    <Row>
      <i-col span="12">
        <Form ref="oldMhaMachine" :model="oldMhaMachine" :rules="ruleMhaMachine" :label-width="250" style="float: left; margin-top: 50px">
          <FormItem label="源集群名" prop="mhaName" style="width: 600px">
            <Input v-model="oldMhaMachine.mhaName" @input="changeOldMha" placeholder="请输入源集群名"/>
          </FormItem>
          <FormItem label="添加Ip" prop="ip">
           <Input v-model="oldMhaMachine.ip"  number placeholder="请输入录入DB的IP"/>
          </FormItem>
          <FormItem label="Port" prop="port">
            <Input v-model="oldMhaMachine.port"  placeholder="请输入录入DB端口"/>
          </FormItem >
          <FormItem label="DB机房" prop="idc">
            <Select v-model="oldMhaMachine.idc" style="width: 200px"  placeholder="选择db机房区域" >
              <Option v-for="item in selectOption.drcZoneList" :value="item.value" :key="item.key">{{ item.key }}</Option>
            </Select>
          </FormItem>
          <FormItem label="UUID" prop="uuid">
            <Input v-model="oldMhaMachine.uuid"  placeholder="请输入录入DB的uuid"/>
            <Button @click="queryOldMhaMachineUuid">连接查询</Button>
            <span v-if="hasTest1">
            <Icon :type="testSuccess1 ? 'ios-checkmark-circle' : 'ios-close-circle'"
                :color="testSuccess1 ? 'green' : 'red'"/>
            {{ testSuccess1 ? '连接查询成功' : '连接查询失败，请手动输入uuid' }}
            </span>
          </FormItem>
          <FormItem label="Master" prop="master">
            <Select  v-model="oldMhaMachine.master" style="width: 200px"  placeholder="是否为Master" >
              <Option v-for="item in selectOption.isMaster" :value="item.value" :key="item.key">{{ item.key }}</Option>
            </Select>
          </FormItem>
          <FormItem>
            <Button type="primary"  @click="preCheckOldMhaMachine ('oldMhaMachine')">录入DB</Button><br><br>
          </FormItem>
        </Form>
      </i-col>
      <i-col span="12">
        <Form ref="newMhaMachine" :model="newMhaMachine" :rules="ruleMhaMachine" :label-width="250" style="float: left; margin-top: 50px">
          <FormItem label="新集群名" prop="mhaName" style="width: 600px">
            <Input v-model="newMhaMachine.mhaName" @input="changeNewMha" placeholder="请输入源集群名"/>
          </FormItem>
          <FormItem label="添加Ip" prop="ip">
            <Input v-model="newMhaMachine.ip"  placeholder="请输入录入DB的IP"/>
          </FormItem>
          <FormItem label="Port" prop="port">
            <Input v-model="newMhaMachine.port"  number placeholder="请输入录入DB端口"/>
          </FormItem >
          <FormItem label="DB机房" prop="idc">
            <Select v-model="newMhaMachine.idc" style="width: 200px"  placeholder="选择db机房区域" >
              <Option boolean v-for="item in selectOption.drcZoneList" :value="item.value" :key="item.key">{{ item.key }}</Option>
            </Select>
          </FormItem>
          <FormItem label="UUID" prop="uuid">
            <Input v-model="newMhaMachine.uuid"  placeholder="请输入录入DB的uuid"/>
            <Button @click="queryNewMhaMachineUuid">连接查询</Button>
            <span v-if="hasTest2">
            <Icon :type="testSuccess2 ? 'ios-checkmark-circle' : 'ios-close-circle'"
                  :color="testSuccess2 ? 'green' : 'red'"/>
            {{ testSuccess2 ? '连接查询成功' : '连接查询失败，请手动输入uuid' }}
            </span>
          </FormItem>
          <FormItem label="Master" prop="master">
            <Select  v-model="newMhaMachine.master" style="width: 200px"  placeholder="是否为Master" >
              <Option v-for="item in selectOption.isMaster" :value="item.value" :key="item.key">{{ item.key }}</Option>
            </Select>
          </FormItem>
          <FormItem>
            <Button type="primary"  @click="preCheckNewMhaMachine ('newMhaMachine')">录入DB</Button><br><br>
          </FormItem>
        </Form>
      </i-col>
    </Row>
    <Modal
      v-model="oldMhaMachine.modal"
      title="录入左侧Mha Db信息"
      @on-ok="submitOldMhaMachine">
      <p>Mha: {{oldMhaMachine.mhaName}} </p>
      <p> db信息 [host: {{oldMhaMachine.ip}}:{{oldMhaMachine.port}}]</p>
      <p> db信息 [isMaster:{{oldMhaMachine.master}}]</p>
      <p> db信息 [idc:{{oldMhaMachine.idc}}]</p>
      <p> db信息 [uuid:{{oldMhaMachine.uuid}}]</p>
    </Modal>
    <Modal
      v-model="newMhaMachine.modal"
      title="录入右侧Mha Db信息"
      @on-ok="submitNewMhaMachine">
      <p>Mha: {{newMhaMachine.mhaName}} </p>
      <p> db信息 [host: {{newMhaMachine.ip}}:{{newMhaMachine.port}}]</p>
      <p> db信息 [isMaster:{{newMhaMachine.master}}]</p>
      <p> db信息 [idc:{{newMhaMachine.idc}}]</p>
      <p> db信息 [uuid:{{newMhaMachine.uuid}}]</p>
    </Modal>

  </div>
</template>
<script>
export default {
  name: 'mhaConfig',
  props: {
    oldClusterName: String,
    newClusterName: String,
    oldDrcZone: String,
    newDrcZone: String
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
      oldMhaMachine: {
        modal: false,
        mhaName: this.oldClusterName,
        zoneId: this.oldDrcZone,
        ip: '',
        port: '',
        idc: this.oldDrcZone,
        uuid: '',
        master: 1
      },
      newMhaMachine: {
        modal: false,
        mhaName: this.newClusterName,
        zoneId: this.newDrcZone,
        ip: '',
        port: '',
        idc: this.newDrcZone,
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
        drcZoneList: [
          {
            value: 'shaoy',
            key: '上海欧阳'
          },
          {
            value: 'shaxy',
            key: '上海新源'
          },
          {
            value: 'sharb',
            key: '上海日版'
          },
          {
            value: 'shajq',
            key: '上海金桥'
          },
          {
            value: 'shafq',
            key: '上海福泉'
          },
          {
            value: 'shajz',
            key: '上海金钟'
          },
          {
            value: 'ntgxh',
            key: '南通星湖大道'
          },
          {
            value: 'ntgxy',
            label: 'ntgxy'
          },
          {
            value: 'fraaws',
            key: '法兰克福AWS'
          },
          {
            value: 'shali',
            key: '上海阿里'
          },
          {
            value: 'sinibuaws',
            key: 'IBU-VPC'
          },
          {
            value: 'sinibualiyun',
            label: 'IBU-VPC(aliyun)'
          },
          {
            value: 'sinaws',
            key: '新加坡AWS'
          }
        ]
      }
    }
  },
  methods: {
    changeOldMha () {
      this.$emit('oldClusterChanged', this.oldMhaMachine.mhaName)
    },
    changeNewMha () {
      this.$emit('newClusterChanged', this.newMhaMachine.mhaName)
    },
    queryOldMhaMachineUuid () {
      const that = this
      console.log('/api/drc/v1/mha/uuid/' + this.oldMhaMachine.mhaName +
        ',' + this.newMhaMachine.mhaName + '/' + this.oldMhaMachine.ip + '/' + this.oldMhaMachine.port + '/' + this.oldMhaMachine.master)
      that.axios.get('/api/drc/v1/mha/uuid/' + this.oldMhaMachine.mhaName +
        ',' + this.newMhaMachine.mhaName + '/' + this.oldMhaMachine.ip + '/' + this.oldMhaMachine.port + '/' + this.oldMhaMachine.master)
        .then(response => {
          this.hasTest1 = true
          if (response.data.status === 0) {
            this.oldMhaMachine.uuid = response.data.data
            this.testSuccess1 = true
          } else {
            this.testSuccess1 = false
          }
        })
    },
    queryNewMhaMachineUuid () {
      const that = this
      that.axios.get('/api/drc/v1/mha/uuid/' + this.oldMhaMachine.mhaName +
        ',' + this.newMhaMachine.mhaName + '/' + this.newMhaMachine.ip + '/' + this.newMhaMachine.port + '/' + this.oldMhaMachine.master)
        .then(response => {
          this.hasTest2 = true
          if (response.data.status === 0) {
            this.newMhaMachine.uuid = response.data.data
            this.testSuccess2 = true
          } else {
            this.testSuccess2 = false
          }
        })
    },
    preCheckOldMhaMachine (name) {
      this.$refs[name].validate((valid) => {
        if (!valid) {
          this.$Message.error('仍有必填项未填!')
        } else {
          this.oldMhaMachine.modal = true
        }
      })
    },
    submitOldMhaMachine () {
      const that = this
      that.axios.post('/api/drc/v1/access/mha/machineInfo', {
        mhaName: this.oldMhaMachine.mhaName,
        master: this.oldMhaMachine.master,
        mySQLInstance: {
          ip: this.oldMhaMachine.ip,
          port: this.oldMhaMachine.port,
          idc: this.oldMhaMachine.idc,
          uuid: this.oldMhaMachine.uuid
        }
      }).then(response => {
        that.hasResp = true
        if (response.data.status === 0) {
          that.status = 'success'
          that.title = 'mha:' + this.oldMhaMachine.mhaName + '录入db成功!'
          that.message = response.data.message
        } else {
          that.status = 'error'
          that.title = 'mha:' + this.oldMhaMachine.mhaName + '录入db失败!'
          that.message = response.data.message
        }
      })
    },
    preCheckNewMhaMachine (name) {
      this.$refs[name].validate((valid) => {
        if (!valid) {
          this.$Message.error('仍有必填项未填!')
        } else {
          this.newMhaMachine.modal = true
        }
      })
    },
    submitNewMhaMachine () {
      const that = this
      that.axios.post('/api/drc/v1/access/mha/machineInfo', {
        mhaName: this.newMhaMachine.mhaName,
        master: this.newMhaMachine.master,
        mySQLInstance: {
          ip: this.newMhaMachine.ip,
          port: this.newMhaMachine.port,
          idc: this.newMhaMachine.idc,
          uuid: this.newMhaMachine.uuid
        }
      }).then(response => {
        that.hasResp = true
        if (response.data.status === 0) {
          that.status = 'success'
          that.title = 'mha:' + this.newMhaMachine.mhaName + '录入db成功!'
          that.message = response.data.message
        } else {
          that.status = 'error'
          that.title = 'mha:' + this.newMhaMachine.mhaName + '录入db失败!'
          that.message = response.data.message
        }
      })
    }
  }
}
</script>

<style scoped>

</style>

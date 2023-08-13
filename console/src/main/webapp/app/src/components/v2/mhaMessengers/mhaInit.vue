<template v-if="current === 0" :key="0">
  <div>
    <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
      {{title}}
      <span slot="desc" v-html="message"></span>
    </Alert>
    <Form ref="mhaInfo" :model="mhaInfo" :rules="ruleOfMhaInfo" :label-width="250" style="margin-top: 50px">
      <FormItem label="Mha名称" prop="mhaName" style="width: 600px">
        <Input v-model="mhaInfo.mhaName" @input="changeMhaName" placeholder="请输入集群名" />
      </FormItem>
      <FormItem label="源集群机房区域" prop="dc">
        <Select v-model="mhaInfo.dc" style="width: 200px"  placeholder="选择机房区域" @input="changeDc">
          <Option v-for="item in mhaInfo.drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
        </Select>
      </FormItem>
      <FormItem label="BU名" style="width: 600px" prop="bu">
        <Input v-model="mhaInfo.bu"  placeholder="请输入BU名，自动绑定route策略" />
      </FormItem>
      <FormItem label="DAL Cluster名" style="width: 600px" prop="dalclustername">
        <Input v-model="mhaInfo.dalclustername"  placeholder="请输入DAL Cluster名" />
      </FormItem>
      <FormItem label="appid" style="width: 600px" prop="appid">
        <Input v-model="mhaInfo.appid"  placeholder="请输入appid" />
      </FormItem>
      <FormItem>
        <Button @click="handleReset('mhaInfo')">重置</Button>
        <Button type="primary" @click="changeModal('mhaInfo')" style="margin-left: 150px">录入</Button>
        <Modal
          v-model="mhaInfo.modal"
          title="录入mha相关信息"
          @on-ok="postMhaInfo('mhaInfo')">
          <p>
            确定录入Mha: "{{mhaInfo.mhaName}}" 吗？
            并且设置DC:"{{mhaInfo.dc}}"
            BU:"{{mhaInfo.bu}}"
            DAL Cluster:"{{mhaInfo.dalclustername}}"
            appid"{{mhaInfo.appid}}"
          </p>
        </Modal>
      </FormItem>
    </Form>
  </div>
</template>

<script>
export default {
  name: 'mhaInit.vue',
  props: {
    mhaName: String,
    dc: String
  },
  data () {
    return {
      mhaInfo: {
        modal: false,
        mhaName: this.mhaName,
        dc: this.dc,
        bu: '',
        dalclustername: '',
        appid: '',
        drcZoneList: this.constant.dcList
      },
      status: '',
      title: '',
      message: '',
      hasResp: false,
      ruleOfMhaInfo: {
        mhaName: [
          { required: true, message: '源集群名不能为空', trigger: 'blur' }
        ],
        dc: [
          { required: true, message: '新集群名不能为空', trigger: 'blur' }
        ],
        bu: [
          { required: true, message: 'BU名不能为空', trigger: 'blur' }
        ],
        dalclustername: [
          { required: true, message: 'DAL Cluster名不能为空', trigger: 'blur' }
        ],
        appid: [
          { required: true, message: 'cluster appid不能为空', trigger: 'blur' }
        ]
      }
    }
  },
  methods: {
    postMhaInfo (name) {
      const that = this
      that.$refs[name].validate((valid) => {
        if (!valid) {
          that.$Message.error('仍有必填项未填!')
        } else {
          this.hasResp = false
          // todo by yongnian: replace
          console.log('/api/drc/v1/mha/')
          that.axios.post('/api/drc/v1/mha/', {
            buName: this.mhaInfo.bu,
            dalClusterName: this.mhaInfo.dalclustername,
            appid: this.mhaInfo.appid,
            mhaName: this.mhaInfo.mhaName,
            dc: this.mhaInfo.dc
          }).then(response => {
            that.hasResp = true
            if (response.data.status === 0) {
              that.status = 'success'
              that.title = 'Mha录入完成!'
              that.message = response.data.message
            } else {
              that.status = 'error'
              that.title = 'Mha录入失败!'
              that.message = response.data.message
            }
          })
        }
      })
    },
    handleReset (name) {
      this.$refs[name].resetFields()
    },
    changeMhaName () {
      this.$emit('mhaNameChanged', this.mhaInfo.mhaName)
    },
    changeDc () {
      this.$emit('dcChanged', this.mhaInfo.dc)
    },
    changeModal (name) {
      this.$refs[name].validate((valid) => {
        if (!valid) {
          this.$Message.error('仍有必填项未填!')
        } else {
          this.mhaInfo.modal = true
        }
      })
    }
  }

}
</script>

<style scoped>

</style>

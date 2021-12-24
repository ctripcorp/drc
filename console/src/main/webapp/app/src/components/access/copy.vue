<template v-if="current === 0" :key="0">
  <div>
    <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
      {{title}}
      <span slot="desc" v-html="message"></span>
    </Alert>
    <Form ref="copy" :model="copy" :rules="ruleCopy" :label-width="250" style="margin-top: 50px">
      <FormItem label="源集群名" prop="oldClusterName" style="width: 600px">
        <Input v-model="copy.oldClusterName" @input="changeOld" placeholder="请输入源集群名"/>
      </FormItem>
      <FormItem label="新集群名" prop="newClusterName" style="width: 600px">
        <Input v-model="copy.newClusterName" placeholder="请输入新集群名"/>
      </FormItem>
      <FormItem>
        <Button @click="handleReset('copy')">重置</Button><br><br>
        <Button type="primary" @click="checkCopy('copy')">复制状态检测</Button>
        <Button type="error" :disabled="disabledDisconnect" @click="changeModal('copy')" style="margin-left: 50px">断开复制</Button>
        <Modal
          v-model="copy.modal"
          title="断开复制"
          @on-ok="disconnect('copy')">
          <p>确定断开复制连接吗？</p>
        </Modal>
      </FormItem>
    </Form>
  </div>
</template>
<script>
export default {
  name: 'copy',
  props: {
    oldClusterName: String,
    newClusterName: String
  },
  data () {
    return {
      status: '',
      title: '',
      message: '',
      hasResp: false,
      disabledDisconnect: true,
      copy: {
        modal: false,
        oldClusterName: this.oldClusterName,
        newClusterName: this.newClusterName
      },
      ruleCopy: {
        oldClusterName: [
          { required: true, message: '源集群名不能为空', trigger: 'blur' }
        ],
        newClusterName: [
          { required: true, message: '新集群名不能为空', trigger: 'blur' }
        ]
      }
    }
  },
  methods: {
    checkCopy (name) {
      const that = this
      that.$refs[name].validate((valid) => {
        if (!valid) {
          that.$Message.error('仍有必填项未填!')
        } else {
          // loading start
          this.start()
          that.hasResp = false
          that.axios.post('/api/drc/v1/access/copystatus', {
            OriginalCluster: this.copy.oldClusterName,
            DrcCluster: this.copy.newClusterName,
            rplswitch: 0
          }).then(response => {
            that.hasResp = true
            if (response.data.data.status === 'T') {
              that.disabledDisconnect = false
              that.status = 'success'
              that.title = '复制状态正常!'
            } else {
              that.disabledDisconnect = true
              that.status = 'error'
              that.title = '复制状态异常!'
            }
            that.message = response.data.data.message
            // loading finish
            this.finish()
          })
        }
      })
    },
    disconnect (name) {
      const that = this
      that.$refs[name].validate((valid) => {
        if (!valid) {
          that.$Message.error('仍有必填项未填!')
        } else {
          // loading start
          this.start()
          that.hasResp = false
          that.axios.post('/api/drc/v1/access/copystatus', {
            OriginalCluster: this.copy.oldClusterName,
            DrcCluster: this.copy.newClusterName,
            rplswitch: 1
          }).then(response => {
            that.hasResp = true
            if (response.data.data.status === 'T') {
              that.status = 'success'
              that.title = '断开复制成功!'
            } else {
              that.status = 'error'
              that.title = '断开复制失败!'
            }
            that.message = response.data.data.message
            // loading finish
            this.finish()
          })
        }
      })
    },
    handleReset (name) {
      this.$refs[name].resetFields()
    },
    changeOld () {
      this.$emit('oldClusterChanged', this.copy.oldClusterName)
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
          this.copy.modal = true
        }
      })
    }
  }
}
</script>
<style scoped>
</style>

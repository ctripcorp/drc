<template v-if="current === 0" :key="0">
  <div>
    <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
      {{title}}
      <span slot="desc" v-html="message"></span>
    </Alert>
    <Form ref="build" :model="build" :rules="ruleBuild" :label-width="250" style="margin-top: 50px">
      <FormItem label="源集群名" prop="oldClusterName" style="width: 600px">
        <Input v-model="build.oldClusterName" @input="changeOld" placeholder="请输入源集群名" />
      </FormItem>
      <FormItem label="生成新集群机房区域" prop="drcZone">
        <Select v-model="build.drcZone" style="width: 200px"  placeholder="选择机房区域" @input="changeNewZone">
          <Option v-for="item in build.drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
        </Select>
      </FormItem>
      <FormItem label="BU名" style="width: 600px" prop="bu">
        <Input v-model="build.bu" @input="changeBu" placeholder="请输入BU名" />
      </FormItem>
      <FormItem label="DAL Cluster名" style="width: 600px" prop="dalclustername">
        <Input v-model="build.dalclustername" @input="changeDalClusterName" placeholder="请输入DAL Cluster名" />
      </FormItem>
      <FormItem label="appid" style="width: 600px" prop="appid">
        <Input v-model="build.appid" @input="changeAppId" placeholder="请输入appid" />
      </FormItem>
      <FormItem>
        <Button @click="handleReset('build')">重置</Button>
        <Button type="primary" @click="changeModal('build')" style="margin-left: 150px">新建集群</Button>
        <Modal
          v-model="build.modal"
          title="创建新集群"
          @on-ok="postBuild('build')">
          <p>确定创建新集群 "{{build.oldClusterName + build.drcZone}}" 吗？并且设置BU/DAL Cluster/appid为 "{{build.bu}}"/"{{build.dalclustername}}"/"{{build.appid}}"</p>
        </Modal>
      </FormItem>
    </Form>
  </div>
</template>
<script>
export default {
  name: 'build',
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
      build: {
        modal: false,
        oldClusterName: this.oldClusterName,
        newClusterName: this.newClusterName,
        bu: '',
        dalclustername: '',
        appid: '',
        drcZone: '',
        drcZoneList: this.constant.dcList
      },
      ruleBuild: {
        oldClusterName: [
          { required: true, message: '源集群名不能为空', trigger: 'blur' }
        ],
        drcZone: [
          { required: true, message: '选择新建集群区域', trigger: 'change' }
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
    postBuild (name) {
      const that = this
      that.$refs[name].validate((valid) => {
        if (!valid) {
          that.$Message.error('仍有必填项未填!')
        } else {
          // loading start
          this.start()
          this.hasResp = false
          that.axios.post('/api/drc/v1/access/mhaV2', {
            clustername: this.build.oldClusterName,
            drczone: this.build.drcZone,
            bu: this.build.bu,
            dalclustername: this.build.dalclustername,
            appid: this.build.appid
          }).then(response => {
            that.hasResp = true
            if (response.data.data.status === 'T') {
              that.status = 'success'
              that.title = '集群创建完成!'
              that.message = '新集群名为：' + response.data.data.drcCluster
              that.$emit('newClusterChanged', response.data.data.drcCluster)
            } else if (response.data.data.status === 'W') {
              that.status = 'establishing'
              this.title = '集群创建中...'
            } else {
              that.status = 'error'
              that.title = '集群创建失败!'
            }
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
      this.$emit('oldClusterChanged', this.build.oldClusterName)
    },
    changeNewZone () {
      this.$emit('newDrcZoneChanged', this.build.drcZone)
    },
    changeBu () {
      this.$emit('buChanged', this.build.bu)
    },
    changeDalClusterName () {
      this.$emit('dalclusternameChanged', this.build.dalclustername)
    },
    changeAppId () {
      this.$emit('appidChanged', this.build.appid)
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
          this.build.modal = true
        }
      })
    }
  }
}
</script>
<style scoped>
</style>

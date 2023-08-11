<template v-if="current === 0" :key="0">
  <div>
    <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
      {{title}}
      <span slot="desc" v-html="message"></span>
    </Alert>
    <Form ref="build" :model="build" :rules="ruleBuild" :label-width="250" style="margin-top: 50px">
      <FormItem label="源Mha集群名" prop="srcMhaName" style="width: 600px">
        <Input v-model="build.srcMhaName" @input="changeSrcMha" placeholder="请输入源集群名" />
      </FormItem>
      <FormItem label="源集群机房区域" prop="srcDc">
        <Select v-model="build.srcDc" style="width: 200px"  placeholder="选择机房区域" @input="changeSrcDc">
          <Option v-for="item in build.drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
        </Select>
      </FormItem>
      <FormItem label="新Mha集群名" prop="dstMhaName" style="width: 600px">
        <Input v-model="build.dstMhaName" @input="changeDstMha" placeholder="请输入新集群名" />
      </FormItem>
      <FormItem label="新集群机房区域" prop="dstDc">
        <Select v-model="build.dstDc" style="width: 200px"  placeholder="选择机房区域" @input="changeDstDc">
          <Option v-for="item in build.drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
        </Select>
      </FormItem>
      <FormItem label="BU名" style="width: 600px" prop="buName">
        <Input v-model="build.buName" @input="changeBu" placeholder="请输入BU名，自动绑定route策略" />
      </FormItem>
      <FormItem>
        <Button @click="handleReset('build')">重置</Button>
        <Button type="primary" @click="changeModal('build')" style="margin-left: 150px">新建DRC同步集群</Button>
        <Modal
          v-model="build.modal"
          title="创建DRC"
          @on-ok="postBuild('build')">
          <p>确定创建新DRC "{{build.srcMhaName + build.dstMhaName}}" 吗？</p>
        </Modal>
      </FormItem>
    </Form>
  </div>
</template>
<script>
export default {
  name: 'mhaBuild',
  props: {
    srcMhaName: String,
    dstMhaName: String,
    srcDc: String,
    dstDc: String
  },
  data () {
    return {
      status: '',
      title: '',
      message: '',
      hasResp: false,
      build: {
        modal: false,
        srcMhaName: this.srcMhaName,
        dstMhaName: this.dstMhaName,
        buName: '',
        srcDc: this.srcDc,
        dstDc: this.dstDc,
        drcZoneList: this.constant.dcList
      },
      ruleBuild: {
        srcMhaName: [
          { required: true, message: '源集群名不能为空', trigger: 'blur' }
        ],
        dstMhaName: [
          { required: true, message: '目标集群名不能为空', trigger: 'blur' }
        ],
        srcDc: [
          { required: true, message: '选择源集群区域', trigger: 'change' }
        ],
        dstDc: [
          { required: true, message: '选择目标集群区域', trigger: 'change' }
        ],
        buName: [
          { required: true, message: 'BU名不能为空', trigger: 'blur' }
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
          this.hasResp = false
          that.axios.post('/api/drc/v2/drcBuild/mha', {
            buName: this.build.buName,
            srcMhaName: this.build.srcMhaName,
            dstMhaName: this.build.dstMhaName,
            srcDc: this.build.srcDc,
            dstDc: this.build.dstDc
          }).then(response => {
            that.hasResp = true
            if (response.data.status === 0) {
              that.status = 'success'
              that.title = '集群创建完成!'
              that.message = response.data.message
            } else {
              that.status = 'error'
              that.title = '集群创建失败!'
              that.message = response.data.message
            }
          })
        }
      })
    },
    handleReset (name) {
      this.$refs[name].resetFields()
    },
    changeSrcMha () {
      this.$emit('srcMhaNameChanged', this.build.srcMhaName)
    },
    changeDstMha () {
      this.$emit('dstMhaNameChanged', this.build.dstMhaName)
    },
    changeSrcDc () {
      this.$emit('srcDcChanged', this.build.srcDc)
    },
    changeDstDc () {
      this.$emit('dstDcChanged', this.build.dstDc)
    },
    changeBu () {
      this.$emit('buNameChanged', this.build.buName)
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

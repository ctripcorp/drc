<template v-if="current === 0" :key="0">
  <div>
    <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
      {{title}}
      <span slot="desc" v-html="message"></span>
    </Alert>
    <Form ref="build" :model="build" :rules="ruleBuild" :label-width="250" style="margin-top: 50px">
      <FormItem label="源Mha集群名" prop="oldClusterName" style="width: 600px">
        <Input v-model="build.oldClusterName" @input="changeOldMha" placeholder="请输入源集群名" />
      </FormItem>
      <FormItem label="源集群机房区域" prop="oldDrcZone">
        <Select v-model="build.oldDrcZone" style="width: 200px"  placeholder="选择机房区域" @input="changeOldZone">
          <Option v-for="item in build.drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
        </Select>
      </FormItem>
      <FormItem label="新Mha集群名" prop="newClusterName" style="width: 600px">
        <Input v-model="build.newClusterName" @input="changeNewMha" placeholder="请输入新集群名" />
      </FormItem>
      <FormItem label="新集群机房区域" prop="newDrcZone">
        <Select v-model="build.newDrcZone" style="width: 200px"  placeholder="选择机房区域" @input="changeNewZone">
          <Option v-for="item in build.drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
        </Select>
      </FormItem>
      <FormItem label="BU名" style="width: 600px" prop="bu">
        <Input v-model="build.bu" @input="changeBu" placeholder="请输入BU名，自动绑定route策略" />
      </FormItem>
      <FormItem label="DAL Cluster名" style="width: 600px" prop="dalclustername">
        <Input v-model="build.dalclustername" @input="changeDalClusterName" placeholder="请输入DAL Cluster名" />
      </FormItem>
      <FormItem label="appid" style="width: 600px" prop="appid">
        <Input v-model="build.appid" @input="changeAppId" placeholder="请输入appid" />
      </FormItem>
      <FormItem>
        <Button @click="handleReset('build')">重置</Button>
        <Button type="primary" @click="changeModal('build')" style="margin-left: 150px">新建DRC同步集群</Button>
        <Modal
          v-model="build.modal"
          title="创建DRC"
          @on-ok="postBuild('build')">
          <p>确定创建新DRC "{{build.oldClusterName + build.newClusterName}}" 吗？并且设置BU/DAL Cluster/appid为 "{{build.bu}}"/"{{build.dalclustername}}"/"{{build.appid}}"</p>
        </Modal>
      </FormItem>
    </Form>
  </div>
</template>
<script>
export default {
  name: 'buildV2',
  props: {
    oldClusterName: String,
    newClusterName: String,
    oldDrcZone: String,
    newDrcZone: String
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
        oldDrcZone: this.oldDrcZone,
        newDrcZone: this.newDrcZone,
        drcZoneList: [
          {
            value: 'shaoy',
            label: '上海欧阳'
          },
          {
            value: 'shaxy',
            label: '上海新源'
          },
          {
            value: 'sharb',
            label: '上海日版'
          },
          {
            value: 'shajq',
            label: '上海金桥'
          },
          {
            value: 'shafq',
            label: '上海福泉'
          },
          {
            value: 'shajz',
            label: '上海金钟'
          },
          {
            value: 'ntgxh',
            label: '南通星湖大道'
          },
          {
            value: 'fraaws',
            label: '法兰克福AWS'
          },
          {
            value: 'shali',
            label: '上海阿里'
          },
          {
            value: 'sinibuaws',
            label: 'IBU-VPC'
          },
          {
            value: 'sinibualiyun',
            label: 'IBU-VPC(aliyun)'
          },
          {
            value: 'sinaws',
            label: '新加坡AWS'
          }
        ]
      },
      ruleBuild: {
        oldClusterName: [
          { required: true, message: '源集群名不能为空', trigger: 'blur' }
        ],
        newClusterName: [
          { required: true, message: '新集群名不能为空', trigger: 'blur' }
        ],
        oldDrcZone: [
          { required: true, message: '选择源集群区域', trigger: 'change' }
        ],
        newDrcZone: [
          { required: true, message: '选择新集群区域', trigger: 'change' }
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
          this.hasResp = false
          that.axios.post('/api/drc/v1/access/mha/standalone', {
            buName: this.build.bu,
            dalClusterName: this.build.dalclustername,
            appid: this.build.appid,
            originalMha: this.build.oldClusterName,
            originalMhaDc: this.build.oldDrcZone,
            newBuiltMha: this.build.newClusterName,
            newBuiltMhaDc: this.build.newDrcZone
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
    changeOldMha () {
      this.$emit('oldClusterChanged', this.build.oldClusterName)
    },
    changeNewMha () {
      this.$emit('newClusterChanged', this.build.newClusterName)
    },
    changeOldZone () {
      this.$emit('oldDrcZoneChanged', this.build.oldDrcZone)
    },
    changeNewZone () {
      this.$emit('newDrcZoneChanged', this.build.newDrcZone)
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

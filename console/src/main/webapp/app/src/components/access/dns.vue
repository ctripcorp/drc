<template v-if="current === 0" :key="0">
  <div>
    <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
      {{title}}
      <span slot="desc" v-html="message"></span>
    </Alert>
    <Form ref="dns" :model="dns" :rules="ruleDns" :label-width="250" style="margin-top: 50px">
      <FormItem label="源集群名" prop="oldClusterName" style="width: 600px">
        <Input v-model="dns.oldClusterName" @input="changeOld" placeholder="请输入源集群名"/>
      </FormItem>
      <FormItem label="新集群名" prop="newClusterName" style="width: 600px">
        <Input v-model="dns.newClusterName" placeholder="请输入新集群名"/>
      </FormItem>
      <FormItem label="选择环境" prop="env">
        <Select v-model="dns.env" style="width: 200px" @input="changeEnv();getAllDbs()" placeholder="选择环境">
          <Option v-for="item in dns.envList" :value="item.value" :key="item.value">{{ item.label }}</Option>
        </Select>
      </FormItem>
      <FormItem label="选择DB" style="width: 700px">
        <div>
          <Table highlight-row border ref="selection" :columns="dns.columns" :data="dns.dbNames" v-model="dns.selectedDbs" @on-selection-change="changeSelection"></Table>
        </div>
      </FormItem>
      <FormItem label="需要读域名" prop="needread">
        <i-switch v-model="dns.needread"/>
      </FormItem>
      <FormItem>
        <Button @click="handleReset('dns')" >重置</Button>
        <Button type="primary" @click="changeModal('dns')" style="margin-left: 150px">确定提交</Button>
        <Modal
          v-model="dns.modal"
          title="生成分机房域名"
          @on-ok="postDns('dns')">
          <p>确定在 "{{dns.oldClusterName}}" 和 "{{dns.newClusterName}}" 集群中生成带区域信息的域名吗？</p>
        </Modal>
      </FormItem>
    </Form>
  </div>
</template>
<script>
export default {
  name: 'dns',
  props: {
    oldClusterName: String,
    newClusterName: String,
    env: String
  },
  data () {
    return {
      status: '',
      title: '',
      message: '',
      hasResp: false,
      dns: {
        modal: false,
        oldClusterName: this.oldClusterName,
        newClusterName: this.newClusterName,
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
        dbNames: [
        ],
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
        ]
      },
      ruleDns: {
        oldClusterName: [
          { required: true, message: '源集群名不能为空', trigger: 'blur' }
        ],
        newClusterName: [
          { required: true, message: '新集群名不能为空', trigger: 'blur' }
        ],
        env: [
          { required: true, message: '环境不能为空', trigger: 'blur' }
        ]
      }
    }
  },
  methods: {
    postDns (name) {
      const that = this
      that.$refs[name].validate((valid) => {
        if (!valid) {
          that.$Message.error('仍有必填项未填!')
        } else {
          // loading start
          this.start()
          const dbs = this.dns.selectedDbs
            .map(db => db.name)
          that.hasResp = false
          that.title = '部署DNS成功!'
          that.status = 'success'
          that.message = ''
          // old cluster
          that.axios.post('/api/drc/v1/access/dns', {
            cluster: this.dns.oldClusterName,
            dbnames: String(dbs),
            env: this.dns.env,
            needread: this.dns.needread ? 1 : 0
          }).then(response => {
            that.hasResp = true
            if (!response.data.data.success) {
              that.status = 'error'
              that.title = '部署DNS失败!'
              that.message += 'DNS部署失败！<br/>集群名：' + this.dns.oldClusterName + '<br/>提示信息：' + response.data.data.content + '<hr/>'
            } else {
              that.message += 'DNS部署成功！<br/>集群名：' + this.dns.oldClusterName + '<br/>提示信息：' + response.data.data.content + '<hr/>'
            }
            // loading finish
            this.finish()
          })
          // new cluster
          that.axios.post('/api/drc/v1/access/dns', {
            cluster: this.dns.newClusterName,
            dbnames: String(dbs),
            env: this.dns.env,
            needread: this.dns.needread ? 1 : 0
          }).then(response => {
            that.hasResp = true
            if (!response.data.data.success) {
              that.status = 'error'
              that.title = '部署DNS失败!'
              that.message += 'DNS部署失败！<br/>集群名：' + this.dns.newClusterName + '<br/>提示信息：' + response.data.data.content + '<hr/>'
            } else {
              that.message += 'DNS部署成功！<br/>集群名：' + this.dns.newClusterName + '<br/>提示信息：' + response.data.data.content + '<hr/>'
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
    changeEnv () {
      this.$emit('envChanged', this.dns.env)
      this.getAllDbs()
    },
    getAllDbs () {
      this.axios.get('/api/drc/v1/mha/dbnames/cluster/' + this.dns.oldClusterName + '/env/' + this.dns.env)
        .then(response => {
          console.log(response.data)
          this.dns.dbNames = []
          response.data.data.forEach(db => this.dns.dbNames.push({ name: db }))
        })
    },
    changeSelection () {
      this.dns.selectedDbs = this.$refs.selection.getSelection()
    },
    changeOld () {
      this.getAllDbs()
      this.$emit('oldClusterChanged', this.dns.oldClusterName)
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
          this.dns.modal = true
        }
      })
    }
  },
  created () {
    this.getAllDbs()
  }
}
</script>
<style scoped>
</style>

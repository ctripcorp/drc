<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/buildMetaMessage">行过滤唯一标识配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Form ref="metaMessage" :rules="ruleForm" :model="metaMessage" :label-width="250" style="margin-top: 50px">
          <FormItem label="行过滤唯一标识" prop="metaFilterName">
            <Input v-model="metaMessage.metaFilterName" placeholder="请输入行过滤唯一标识" style="width: 600px"></Input>
          </FormItem>
          <FormItem label="请选择目标子环境" prop="targetSubEnv">
            <Select v-model="metaMessage.targetSubEnv" placeholder="目标子环境" style="width: 600px" multiple>
              <Option v-for="(item, index) in subEnvs" :value="item" :key="index"></Option>
            </Select>
          </FormItem>
          <FormItem label="bu" prop="bu">
            <Input v-model="metaMessage.bu" placeholder="请输入bu" style="width: 600px"></Input>
          </FormItem>
          <FormItem label="负责人" prop="owner">
            <Input v-model="metaMessage.owner" placeholder="请输入负责人" style="width: 600px"></Input>
          </FormItem>
          <FormItem label="行过滤类型" prop="filterType">
            <Select v-model="metaMessage.filterType" placeholder="请选择行过滤类型" style="width: 600px">
              <Option value=1>黑名单</Option>
              <Option value=2>白名单</Option>
            </Select>
          </FormItem>
          <FormItem>
            <Button @click="resetForm">重置</Button>
            <Button type="primary" @click="putMetaMessage" style="margin-left: 150px">新建配置</Button>
            <Button type="primary" to="metaMessage" style="margin-left: 150px">返回</Button>
          </FormItem>
        </Form>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'buildMetaMessage',
  props: {
  },
  data () {
    return {
      status: '',
      msg: '',
      title: '',
      hasResp: '',
      subEnvs: [],
      metaMessage: {
        metaFilterName: '',
        targetSubEnv: '',
        bu: '',
        owner: '',
        filterType: ''
      },
      ruleForm: {
        metaFilterName: [
          { required: true, message: '行过滤唯一标识不能为空', trigger: 'blur' }
        ],
        targetSubEnv: [
          {
            validator: (rule, value, callback) => {
              if (value.length === 0) {
                callback(new Error('请至少选择一个子环境'))
              } else {
                callback()
              }
            },
            trigger: 'blur'
          }
        ],
        bu: [
          { required: true, message: 'BU名不能为空', trigger: 'blur' }
        ],
        owner: [
          { required: true, message: '请填写负责人', trigger: 'blur' }
        ],
        filterType: [
          { required: true, message: '请选择行过滤类型', trigger: 'change' }
        ]
      }
    }
  },
  methods: {
    getSubEnvs () {
      this.axios.get('/api/drc/v1/filter/row/subEnv').then(response => {
        this.subEnvs = response.data.data
        console.log(this.subEnvs)
      })
    },
    putMetaMessage () {
      this.$refs.metaMessage.validate((valid) => {
        if (!valid) {
          this.$Message.error('仍有必填项未填!')
        } else {
          console.log(this.metaMessage)
          this.axios.put('/api/drc/v1/filter/row/meta', {
            metaFilterName: this.metaMessage.metaFilterName,
            targetSubEnv: this.metaMessage.targetSubEnv,
            bu: this.metaMessage.bu,
            owner: this.metaMessage.owner,
            filterType: this.metaMessage.filterType
          }).then(response => {
            this.hasResp = true
            if (response.data.status === 0) {
              this.$router.push('/metaMessage')
            } else {
              this.$Message.error('新建配置失败')
            }
          }).catch(message => {
            this.status = 'error'
            this.title = '创建失败！'
            console.log(message)
            // throw error
          })
        }
      })
    },
    resetForm () {
      this.$refs.metaMessage.resetFields()
    }
  },
  created () {
    this.axios.get('/api/drc/v2/permission/meta/rowsFilterMark').then((response) => {
      if (response.data.status === 403) {
        this.$router.push('/nopermission')
        return
      }
      this.getSubEnvs()
    })
  }
}
</script>

<style scoped>

</style>

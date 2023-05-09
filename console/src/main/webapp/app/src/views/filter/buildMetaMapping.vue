<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/metaMapping">行过滤映射配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <p style="font-size: 16px; font-weight: bold">行过滤标识: {{metaFilterName}}</p>
        <Form style="margin-top: 30px">
          <div v-for="(item, index) in formData" :key="index">
            <FormItem>
              <Input type="text" v-model="formData[index]" placeholder="请输入行过滤key" required="true" style="width: 500px"></Input>
              <Button :style="{marginLeft: '50px'}" type="primary" @click="delRow(item, index)">删除</Button>
            </FormItem>
          </div>
        </Form>
        <Button type="primary" @click="addRow">新增</Button>
        <Button type="primary" @click="updateMetaMapping" style="margin-left: 100px">提交</Button>
        <Button type="primary" to="/metaMessage" style="margin-left: 100px">返回</Button>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'buildMetaMapping',
  props: {},
  data () {
    return {
      metaFilterId: Number,
      metaFilterName: String,
      formData: ['']
    }
  },
  methods: {
    addRow () {
      this.formData.push('')
    },
    delRow (item, index) {
      if (this.formData.length > 1) {
        this.formData.splice(index, 1)
      } else {
        this.$Message.warning('至少一项')
      }
    },
    getMetaMappings () {
      console.log('metaFilterName:' + this.metaFilterName)
      const url = '/api/drc/v1/filter/row/mapping?metaFilterId=' + this.metaFilterId
      this.axios.get(url).then(response => {
        if (response.data.data.filterKeys.length > 0) {
          this.formData = response.data.data.filterKeys
        }
        console.log('formData: ' + this.formData)
      })
    },
    updateMetaMapping () {
      if (this.formData.length < 1) {
        this.$Message.warning('至少一项')
      }
      console.log('filterKeys: ' + this.formData)
      let valid = true
      this.formData.forEach((item, index) => {
        if (item === '') {
          valid = false
        }
      })
      if (!valid) {
        this.$Message.warning('不能为空!')
      } else {
        this.axios.post('/api/drc/v1/filter/row/mapping', {
          metaFilterId: this.metaFilterId,
          filterKeys: this.formData
        }).then(response => {
          if (response.data.status === 0) {
            this.$Message.success('提交成功!')
          } else {
            this.$Message.error('提交失败!')
          }
        })
      }
    }
  },
  created () {
    this.metaFilterId = this.$route.query.metaFilterId
    this.metaFilterName = this.$route.query.metaFilterName
    this.getMetaMappings()
  }
}
</script>

<style scoped>

</style>

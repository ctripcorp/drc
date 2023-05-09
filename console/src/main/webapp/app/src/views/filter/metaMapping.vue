<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/metaMapping">行过滤映射配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <p style="font-size: 16px; font-weight: bold">行过滤标识: {{metaFilterName}}</p>
        <br>
        <List>
          <ListItem v-for="(item , index) in formData" :key="index">{{ item }}</ListItem>
        </List>
        <Button type="primary" to="/metaMessage" style="margin-left: 100px">返回</Button>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'MetaMapping',
  props: {},
  data () {
    return {
      metaFilterId: Number,
      metaFilterName: String,
      formData: ['']
    }
  },
  methods: {
    getMetaMappings () {
      console.log('metaFilterName:' + this.metaFilterName)
      const url = '/api/drc/v1/filter/row/mapping?metaFilterId=' + this.metaFilterId
      this.axios.get(url).then(response => {
        if (response.data.data.filterKeys.length > 0) {
          this.formData = response.data.data.filterKeys
        }
        console.log('formData: ' + this.formData)
      })
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

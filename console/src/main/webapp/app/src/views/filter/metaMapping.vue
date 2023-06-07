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
        <p style="font-weight: bold">token: </p>
        <p>{{formData.token}}</p>
        <br>
        <p v-if="formData.filterType !== null" style="font-weight: bold">{{filterLabel}}: </p>
        <textarea disabled v-model="formData.filterValue" style="width: 100%"></textarea>
        <br><br>
        <List>
          <ListItem style="font-weight: bold">行过滤映射key:</ListItem>
          <ListItem v-for="(item , index) in formData.filterKeys" :key="index">{{ item }}</ListItem>
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
      filterLabel: '',
      filterKeys: [''],
      formData: {
        etaFilterId: Number,
        filterType: null,
        filterValue: '',
        token: '',
        filterKeys: ['']
      }
    }
  },
  methods: {
    getMetaMappings () {
      const url = '/api/drc/v1/filter/row/mapping?metaFilterId=' + this.metaFilterId
      this.axios.get(url).then(response => {
        if (response.data.status === 0) {
          this.formData = response.data.data
          const filterType = response.data.data.filterType
          this.filterLabel = filterType != null ? filterType === 0 ? '黑名单' : '白名单' : null
        } else {
          this.$Message.error(response.data.message)
        }
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

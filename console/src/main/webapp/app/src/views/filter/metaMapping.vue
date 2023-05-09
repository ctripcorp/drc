<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/metaMapping">行过滤映射配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
          <div v-for="(item, index) in metaMapping.filterKeyList" :key="index" >
            <Form ref="metaMapping" :model="metaMapping" :label-width="250" style="margin-top: 50px">
              <FormItem label="行过滤唯一标识" prop="metaFilterName">
                <Input v-model="item.filterKey" placeholder="请输入行过滤key" style="width: 400px"></Input>
                <Button :style="{marginLeft: '50px'}" type="primary" @click="delRow(item, index)" >删除</Button>
              </FormItem>
            </Form>
          </div>
        <Button type="primary" @click="addRow" style="margin-left: 150px">新增</Button>
        <Button type="primary" @click="addRow" style="margin-left: 150px">提交</Button>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'metaMapping',
  data () {
    return {
      metaMapping: {
        metaFilterId: '',
        filterKeyList: [
          { filterKey: '' }
        ]
      }
    }
  },
  methods: {
    addRow () {
      this.metaMapping.filterKeyList.push({ filterKey: '' })
    },
    delRow (item, index) {
      if (this.metaMapping.filterKeyList.length > 1) {
        this.metaMapping.filterKeyList.splice(index, 1)
      } else {
        this.$Message.warning('至少一项')
      }
    }
  }
}
</script>

<style scoped>

</style>

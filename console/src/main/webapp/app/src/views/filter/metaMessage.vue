<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/metaMessage">行过滤唯一标识配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Card>
          行过滤标识:<Input :style="{width: '200px', marginRight: '10px'}" placeholder="默认全部" v-model="metaFilterName"/>
          <Button :style="{marginLeft: '50px'}" type="primary" @click="getMetaMappings">查询</Button>
          <Button :style="{marginLeft: '50px'}" type="primary" to="/buildMetaMessage">新增</Button>
          <br/>
          <br/>
          <Table stripe :columns="columns" :data="metaMappings" border :span-method="handleSpan">
            <template slot-scope="{ row, index }" slot="action">
              <Button type="success" size="small" style="margin-right: 5px" @click="checkConfig(row, index)">查看</Button>
              <Button type="primary" size="small" style="margin-right: 5px" @click="addMapping(row, index)">新增映射</Button>
              <Button type="error" size="small" style="margin-right: 5px" @click="previewRemoveConfig(row, index)">删除
              </Button>
            </template>
          </Table>
        </Card>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'metaMessage',
  data () {
    return {
      metaMappings: [],
      metaFilterName: null,
      columns: [
        {
          title: '序号',
          width: 75,
          align: 'center',
          render: (h, params) => {
            return h(
              'span',
              params.index + 1
            )
          }
        },
        {
          title: '行过滤标识',
          key: 'metaFilterName'
        },
        {
          title: 'BU',
          key: 'bu',
          width: 100,
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.bu
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '负责人',
          key: 'owner',
          width: 100,
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.owner
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '行过滤类型',
          key: 'filterType',
          width: 100,
          render: (h, params) => {
            const row = params.row
            const color = row.filterType === 1 ? 'blue' : row.filterType === 2 ? 'green' : 'red'
            const text = row.filterType === 1 ? '黑名单' : row.filterType === 2 ? '白名单' : ''
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: 'token',
          key: 'token'
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center'
        }
      ]
    }
  },
  methods: {
    getMetaMappings () {
      let url = '/api/drc/v1/filter/row/meta/all'
      if (this.metaFilterName !== null) {
        url = url + '?metaFilterName=' + this.metaFilterName
      }
      this.axios.get(url).then(response => {
        this.metaMappings = response.data.data
        console.log(this.metaMappings)
      })
    },
    addMapping (row, index) {
      console.log('add mapping metaFilterId: ' + row.metaFilterId)
      this.$router.push({ path: '/metaMapping', query: { metaFilterId: row.metaFilterId } })
    }
  },
  created () {
    this.getMetaMappings()
  }
}
</script>

<style scoped>

</style>

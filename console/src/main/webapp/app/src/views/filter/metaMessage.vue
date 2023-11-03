<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/metaMessage">行过滤标识配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Card>
          行过滤标识:<Input :style="{width: '200px', marginRight: '10px'}" placeholder="默认全部" v-model="metaFilterName"/>
          mha:<Input :style="{width: '200px', marginRight: '10px'}" placeholder="默认全部" v-model="mhaName"/>
          <Button :style="{marginLeft: '50px'}" type="primary" @click="getMetaMappings">查询</Button>
          <Button :style="{marginLeft: '50px'}" type="primary" @click="reset">重置</Button>
          <Button :style="{marginLeft: '50px'}" type="primary" to="/buildMetaMessage">新增</Button>
          <br/>
          <br/>
          <Table stripe :columns="columns" :data="metaMappings" border>
            <template slot-scope="{ row, index }" slot="action">
              <Button type="success" size="small" style="margin-right: 5px" @click="showMapping(row, index)">查看</Button>
              <Button type="primary" size="small" style="margin-right: 5px" @click="addMapping(row, index)">新增映射</Button>
              <Button type="error" size="small" style="margin-right: 5px" @click="changeModal(row, index)">删除</Button>
              <Modal
                v-model="modal"
                title="删除行过滤标识"
                @on-ok="deleteMapping">
                <p>确定删除行过滤标识: {{deletedItem.metaFilterName}} 吗?</p>
              </Modal>
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
      mhaName: null,
      modal: false,
      deletedItem: {
        metaFilterId: Number,
        metaFilterName: String
      },
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
          title: '子环境',
          key: 'targetSubEnv',
          render: (h, params) => {
            const dataList = params.row.targetSubEnv
            return h('span', dataList.join(','))
          }
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
      let flag = false
      if (this.metaFilterName !== null && this.metaFilterName !== '') {
        flag = true
        url = url + '?metaFilterName=' + this.metaFilterName
      }
      if (this.mhaName !== null && this.mhaName !== '') {
        if (flag) {
          url = url + '&'
        } else {
          url = url + '?'
        }
        url = url + 'mhaName=' + this.mhaName
      }
      this.axios.get(url).then(response => {
        this.metaMappings = response.data.data
        console.log(this.metaMappings)
      })
    },
    addMapping (row, index) {
      console.log('add mapping metaFilterId: ' + row.metaFilterId)
      this.$router.push({
        path: '/buildMetaMapping',
        query: { metaFilterId: row.metaFilterId, metaFilterName: row.metaFilterName }
      })
    },
    showMapping (row, index) {
      this.$router.push({
        path: '/metaMapping',
        query: { metaFilterId: row.metaFilterId, metaFilterName: row.metaFilterName }
      })
    },
    deleteMapping () {
      console.log('deleted metaFilterId: ' + this.deletedItem.metaFilterId)
      console.log('deleted metaFilterName: ' + this.deletedItem.metaFilterName)
      this.axios.delete('/api/drc/v1/filter/row/meta?metaFilterId=' + this.deletedItem.metaFilterId).then(response => {
        if (response.data.status === 0) {
          this.$Message.success('删除成功')
          this.getMetaMappings()
        } else {
          this.$Message.error('删除失败')
        }
      })
    },
    changeModal (row, index) {
      console.log('change modal')
      this.modal = true
      this.deletedItem.metaFilterId = row.metaFilterId
      this.deletedItem.metaFilterName = row.metaFilterName
    },
    reset () {
      this.metaFilterName = null
      this.mhaName = null
    }
  },
  created () {
    this.axios.get('/api/drc/v2/permission/meta/rowsFilterMark').then((response) => {
      if (response.data.status === 403) {
        this.$router.push('/nopermission')
        return
      }
      this.getMetaMappings()
    })
  }
}
</script>

<style scoped>

</style>

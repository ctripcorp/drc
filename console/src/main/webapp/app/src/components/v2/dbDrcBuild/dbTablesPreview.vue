<template>
  <div :style="{padding: '1px 1px',height: '100%'}">
    <Divider style="margin-top: 50px">预览：同步表</Divider>
    <Button type="primary" :loading="dbTableLoading" @click="getTableInfo" style="margin-bottom: 5px">
      检查同步表
    </Button>
    <Table size="small" :loading="dbTableLoading" stripe :columns="table.dbTableColumn"
           :data="preCheckTablePage" border></Table>
    <div>
      <Page
        :transfer="true"
        :total="checkTableDataList.length"
        :current.sync="table.dbTablePage.current"
        :page-size-opts="table.dbTablePage.pageSizeOpts"
        :page-size="table.dbTablePage.size"
        show-total
        show-sizer
        show-elevator
        @on-change="(val) => {table.dbTablePage.current = val}"
        @on-page-size-change="(val) => {table.dbTablePage.size = val}"></Page>
    </div>
  </div>
</template>

<script>

export default {
  name: 'dbTablesPreview',
  props: {
    mode: Number,
    dbName: String,
    srcRegionName: String,
    dstRegionName: String,
    tableNames: String
  },
  emits: ['updated'],
  data () {
    return {
      checkTableDataList: [],
      dbTableLoading: false,
      table: {
        dbTableColumn: [
          {
            title: '序',
            width: 50,
            key: 'indexForShow'
            // resizable: true
          },
          {
            title: '库名',
            key: 'schema',
            resizable: true
          },
          {
            title: '表名',
            key: 'table',
            resizable: true
          },
          {
            title: '结果',
            align: 'center',
            render: (h, params) => {
              const row = params.row
              const color = row.res !== 'ok' ? 'volcano' : 'green'
              const text = row.res
              return h('Tag', {
                props: {
                  color: color
                }
              }, text)
            }
          }
        ],
        dbTablePage: {
          total: 0,
          current: 1,
          size: 10,
          pageSizeOpts: [5, 10, 20, 100]
        }
      }
    }
  },
  methods: {
    async getTableInfo () {
      this.checkTableDataList = []
      this.table.dbTablePage.current = 1
      if (this.mode === 1) {
        if (!this.dalClusterName) {
          this.$Message.warning('请先填写 dalcluster')
          return
        }
      } else {
        if (!this.dbName) {
          this.$Message.warning('请先填写数据库')
          return
        }
      }
      if (!this.srcRegionName || !this.dstRegionName) {
        this.$Message.warning('请先填写同步方向')
        return
      }
      if (!this.tableNames) {
        this.$Message.warning('请先填写表名')
        return
      }
      this.dbTableLoading = true
      await this.axios.get('/api/drc/v2/autoconfig/preCheckTable', {
        params: this.flattenObj({
          mode: this.mode,
          dbName: this.dbName,
          srcRegionName: this.srcRegionName,
          dstRegionName: this.dstRegionName,
          tblsFilterDetail: {
            tableNames: this.tableNames
          }
        })
      })
        .then(response => {
          const data = response.data.data
          if (data) {
            for (const [index, val] of data.entries()) {
              val.indexForShow = index
            }
            this.checkTableDataList = data
            this.$Message.success('同步表检测成功, 共找到 ' + data.length + ' 个表')
          } else {
            this.$Message.warning('同步表检测失败：' + response.data.message)
          }
        })
        .catch(message => {
          this.$Message.error('查询dalcluster异常: ' + message)
        })
        .finally(() => {
          this.dbTableLoading = false
        })
    },
    flattenObj (ob) {
      const result = {}
      for (const i in ob) {
        if ((typeof ob[i]) === 'object' && !Array.isArray(ob[i])) {
          const temp = this.flattenObj(ob[i])
          for (const j in temp) {
            result[i + '.' + j] = temp[j]
          }
        } else {
          result[i] = ob[i]
        }
      }
      return result
    }
  },
  computed: {
    preCheckTablePage () {
      const data = this.checkTableDataList
      const start = (this.table.dbTablePage.current - 1) * this.table.dbTablePage.size
      const end = start + this.table.dbTablePage.size
      return [...data].slice(start, end)
    }
  },
  created () {
    if (this.tableNames && this.tableNames.trim().length > 0) {
      this.getTableInfo()
    }
  }
}
</script>

<style scoped>

</style>

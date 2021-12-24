<template>
  <dev-article>
    <div style="padding: 32px 64px">
      <h1 align="center">冲突日志</h1>
      <Table :columns="columns" :data="dataWithPage" :loading="loading" border size="small"></Table>

      <div style="text-align: center;margin: 16px 0">
        <Page
          :total="total"
          :current.sync="current"
          show-sizer
          @on-change="getData"
          @on-page-size-change="handleChangeSize"></Page>
      </div>
    </div>
  </dev-article>
</template>

<script>
export default {
  name: 'Monitor',
  data () {
    return {
      columns: [
        {
          type: 'index',
          width: 60,
          align: 'center',
          indexMethod: (row) => {
            return (row._index + 1) + (this.size * this.current) - this.size
          }
        },
        {
          title: '日志号',
          key: 'logId'
        },
        {
          title: '源数据中心',
          key: 'srcDcName'
        },
        {
          title: '目的数据中心',
          key: 'destDcName'
        },
        {
          title: 'uid',
          key: 'clusterId'
        },
        {
          title: '日志类型',
          key: 'logType'
        },
        {
          title: 'SQL语句',
          key: 'sqlStatement'
        },
        {
          title: 'SQL执行时间',
          key: 'sqlTime'
        },
        {
          title: '入库时间',
          key: 'datachangeLasttime'
        }
      ],
      records: [],
      loading: false,
      total: 0,
      current: 1,
      size: 10
    }
  },
  computed: {
    dataWithPage () {
      const data = this.records
      const start = this.current * this.size - this.size
      const end = start + this.size

      return [...data].slice(start, end)
    }
  },
  methods: {
    getData () {
      const that = this
      if (that.loading) return false

      that.loading = true

      this.axios.get('/api/log/v1/all')
        .then(res => {
          setTimeout(() => {
            console.log(res.data)
            that.records = res.data.data
            that.total = that.records.length
            console.log(that.total)
            that.loading = false
          }, 500)
        })
    },
    handleChangeSize (val) {
      this.size = val

      this.$nextTick(() => {
        this.getData()
      })
    }
  },
  mounted () {
    this.getData()
  }
}
</script>

<style scoped>

</style>

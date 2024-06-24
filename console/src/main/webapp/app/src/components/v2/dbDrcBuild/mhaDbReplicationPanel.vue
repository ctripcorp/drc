<template>

  <div>
    <Table stripe border :columns="columns" :data="mhaDbReplications" v-if="!mhaReplication.mhaApplierDto">
    </Table>
    <Table stripe border :columns="mhaColumns" :data="[mhaReplication]" v-if="mhaReplication.mhaApplierDto">
    </Table>
  </div>

</template>

<script>
import prettyMilliseconds from 'pretty-ms'

export default {
  name: 'mhaDbReplicationPanel',
  props: {
    mhaReplication: {}
  },
  data () {
    return {
      mhaDbReplications: this.mhaReplication.mhaDbReplications,
      value: ['1'],
      delayDataLoading: false,
      columns: [
        {
          title: '类型',
          width: 80,
          render: (h, params) => {
            const row = params.row
            let text = 'none'
            let type = 'error'
            let disabled = false
            switch (row.transmissionType) {
              case 'simplex':
                text = '单'
                type = 'info'
                break
              case 'duplex':
                text = '双'
                type = 'success'
                break
              default:
                text = '无'
                disabled = true
                break
            }
            return h('Button', {
              props: {
                type: type,
                size: 'small',
                disabled: disabled
              },
              on: {
                click: () => {
                  this.showModal(row)
                }
              }
            }, text)
          }
        },
        {
          title: '延迟',
          key: 'drcStatus',
          width: 100,
          align: 'center',
          renderHeader: (h, params) => {
            return h('span', [
              h('span', '延迟'),
              this.replicationIds && this.replicationIds.length !== 0 && h('Button', {
                on: {
                  click: async () => {
                    await this.getDelay()
                  }
                },
                props: {
                  loading: this.delayDataLoading,
                  size: 'small',
                  shape: 'circle',
                  type: 'default',
                  icon: 'md-refresh'
                }
              })
            ])
          },
          render: (h, params) => {
            const row = params.row
            let color, text
            if (row.drcStatus === true) {
              if (row.delay != null) {
                text = prettyMilliseconds(row.delay, { compact: true })
                if (row.delay > 10000) {
                  color = 'warning'
                } else {
                  color = 'success'
                }
              } else {
                text = '已接入'
                color = 'blue'
              }
            } else {
              text = '未接入'
              color = 'default'
            }
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: 'DB 名',
          key: 'srcDbName',
          render: (h, params) => {
            return h('p', params.row.src.dbName)
          }
        },
        {
          title: 'applier',
          key: 'srcDbName',
          render: (h, params) => {
            return h('p', params.row.dbApplierDto ? params.row.dbApplierDto.ips.join(', ') : null)
          }
        }
      ],
      mhaColumns: [
        {
          title: '延迟',
          key: 'drcStatus',
          width: 100,
          align: 'center',
          renderHeader: (h, params) => {
            return h('span', [
              h('span', '延迟'),
              this.mhaReplication.mhaApplierDto.ips && h('Button', {
                on: {
                  click: async () => {
                    await this.getMhaDelay()
                  }
                },
                props: {
                  loading: this.delayDataLoading,
                  size: 'small',
                  shape: 'circle',
                  type: 'default',
                  icon: 'md-refresh'
                }
              })
            ])
          },
          render: (h, params) => {
            const row = params.row
            let color, text
            if (row.mhaApplierDto.ips) {
              if (row.delay != null) {
                text = prettyMilliseconds(row.delay, { compact: true })
                if (row.delay > 10000) {
                  color = 'warning'
                } else {
                  color = 'success'
                }
              } else {
                text = '已接入'
                color = 'blue'
              }
            } else {
              text = '未接入'
              color = 'default'
            }
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '类型',
          width: 100,
          key: '',
          render: (h, params) => {
            return h('p', 'mha 同步')
          }
        },
        {
          title: 'applier',
          key: '',
          render: (h, params) => {
            return h('p', params.row.mhaApplierDto.ips.join(','))
          }
        },
        {
          title: 'initGtid',
          key: '',
          render: (h, params) => {
            return h('p', params.row.mhaApplierDto.gtidInit)
          }
        }
      ]
    }
  },
  methods: {
    showModal (row) {
      this.$Message.info('还未上线，敬请期待')
    },
    async getDelay () {
      const param = {
        replicationIds: this.replicationIds
      }
      if (param.replicationIds.length === 0) {
        console.log('skip delay search')
        return
      }
      this.delayDataLoading = true
      this.axios.get('/api/drc/v2/replication/db/delay', { params: param })
        .then(response => {
          const delays = response.data.data
          const emptyResult = delays == null || !Array.isArray(delays) || delays.length === 0
          if (emptyResult) {
            return
          }
          const dataMap = new Map(delays.map(e => [e.srcMha + '->' + e.dstMha + '->' + e.dbName, e.delay]))
          this.mhaDbReplications.forEach(line => {
            this.$set(line, 'delay', dataMap.get(line.src.mhaName + '->' + line.dst.mhaName + '->' + line.src.dbName))
          })
        })
        .catch(message => {
          console.log(message)
          this.$Message.error('查询延迟异常: ' + message)
        })
        .finally(() => {
          this.delayDataLoading = false
        })
    },
    async getMhaDelay () {
      const param = {
        srcMha: this.mhaReplication.srcMha.name,
        dstMha: this.mhaReplication.dstMha.name
      }
      this.delayDataLoading = true
      this.axios.get('/api/drc/v2/replication/delayByName', { params: param })
        .then(response => {
          const delays = response.data.data.delay
          this.$set(this.mhaReplication, 'delay', delays)
        })
        .catch(message => {
          console.log(message)
          this.$Message.error('查询 MHA 延迟异常: ' + message)
        })
        .finally(() => {
          this.delayDataLoading = false
        })
    }
  },
  computed: {
    replicationIds () {
      return this.mhaDbReplications
        .filter(item => item.drcStatus === true)
        .map(item => item.id)
        .join(',')
    }
  },
  created () {
    this.getDelay()
    this.getMhaDelay()
  }
}
</script>

<style scoped>

</style>

<template>

  <div>
    <Table stripe border :columns="columns" :data="mhaDbReplications" v-if="!mhaMqDtos.mhaMessengerDto">
    </Table>
    <Table stripe border :columns="mhaColumns" :data="[mhaMqDtos]" v-if="mhaMqDtos.mhaMessengerDto">
    </Table>
  </div>

</template>

<script>
import prettyMilliseconds from 'pretty-ms'

export default {
  name: 'mhaDbMqPanel',
  props: {
    mhaMqDtos: {}
  },
  data () {
    return {
      mhaDbReplications: this.mhaMqDtos.mhaDbReplications,
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
                    await this.getMhaMessengerDelay()
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
          title: 'messenger',
          key: 'srcDbName',
          render: (h, params) => {
            return h('p', params.row.dbMessengerDto ? params.row.dbMessengerDto.ips.join(', ') : null)
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
              this.mhaMqDtos.mhaMessengerDto.ips && h('Button', {
                on: {
                  click: async () => {
                    await this.getMhaMessengerDelay()
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
            if (row.mhaMessengerDto.ips) {
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
          title: 'DB 名',
          key: 'srcDbName',
          render: (h, params) => {
            return h('p', params.row.mhaDbReplications.map(item => item.src.dbName).join(','))
          }
        },
        {
          title: 'messenger',
          key: '',
          render: (h, params) => {
            return h('p', params.row.mhaMessengerDto.ips.join(','))
          }
        },
        {
          title: 'initGtid',
          key: '',
          render: (h, params) => {
            return h('p', params.row.mhaMessengerDto.gtidInit)
          }
        }
      ]
    }
  },
  methods: {
    showModal (row) {
      this.$Message.info('还未上线，敬请期待')
    },
    async getMhaMessengerDelay () {
      const param = {
        mhas: this.mhaMqDtos.srcMha.name,
        dbs: this.mhaMqDtos.mhaDbReplications.map(item => item.src.dbName).join(',')
      }
      this.delayDataLoading = true
      this.axios.get('/api/drc/v2/messenger/delay', { params: param })
        .then(response => {
          const delays = response.data.data[0].delayInfoDto.delay
          this.$set(this.mhaMqDtos, 'delay', delays)
        })
        .catch(message => {
          console.log(message)
          // this.$Message.error('查询 MHA 延迟异常: ' + message)
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
    this.getMhaMessengerDelay()
  }
}
</script>

<style scoped>

</style>

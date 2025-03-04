<template>
  <div :style="{padding: '1px 1px',height: '100%'}">
    <Table style="margin-top: 20px" stripe :columns="columns" :data="tableData" :loading="dataLoading" border
           ref="multipleTable">
      <template slot="action" slot-scope="{ row, index }">
        <Button type="primary" size="small" style="margin-right: 5px" @click="goToUpdateConfig(row, index)">修改
        </Button>
        <Button type="error" size="small" style="margin-right: 5px" @click="goToDeleteConfig(row, index)">删除
        </Button>
      </template>
    </Table>
    <br/>
    <Button type="success" @click="goToInsertConfig" icon="md-add">
      新增
    </Button>
    <Modal v-model="editModal.open" width="1200px" :footer-hide="true" :title="actionTitle">
      <db-mq-config ref="dbReplicationConfigComponent" @finished="finishConfig"
                             v-if="editModal.open"
                             :dalcluster-name="dalclusterName"
                             :mq-type="mqType"
                             :config-data="selectedRow"
                             :src-region="srcRegion"
                             :dst-region="srcRegion"
                             :db-names="dbNames"
                             :form-action="action"
                             :filter-read-only="filterReadOnly"
      />
    </Modal>
  </div>
</template>

<script>
import DbMqConfig from '@/components/v2/dbDrcBuild/dbMqConfig.vue'

export default {
  name: 'mqTables',
  components: { DbMqConfig },
  props: {
    dalclusterName: String,
    mqType: String,
    dbName: String,
    dbNames: Array,
    srcRegion: String,
    dstRegion: String,
    tableData: Array,
    dataLoading: Boolean
  },
  emits: ['updated'],
  data () {
    return {
      selectedRow: {},
      action: null,
      actionTitleMap: new Map([
        ['create', '新增 MQ投递配置'],
        ['edit', '更新 MQ投递配置'],
        ['delete', '⚠️删除 MQ投递配置']
      ]),
      editModal: {
        open: false
      },
      filterReadOnly: false,
      columns: [
        {
          title: '序号',
          width: 75,
          align: 'center',
          fixed: 'left',
          render: (h, params) => {
            return h(
              'span',
              params.index + 1
            )
          }
        },
        {
          title: '表名',
          key: 'config.logicTable',
          render: (h, params) => {
            const config = params.row.config
            return h(
              'span',
              config.logicTable
            )
          }
        },
        {
          title: '主题',
          key: 'topic',
          render: (h, params) => {
            const config = params.row.config
            return h(
              'span',
              config.dstLogicTable
            )
          }
        },
        {
          title: '有序',
          key: 'order',
          width: 100,
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.order ? '有序' : '无序'
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '有序相关字段',
          key: 'orderKey',
          width: 100,
          render: (h, params) => {
            const row = params.row
            const text = row.order ? (row.orderKey ? row.orderKey : '主键') : ''
            return h('span', text)
          }
        },
        {
          title: '过滤类型',
          key: 'excludeFilterTypes'
        },
        {
          title: '延迟投递(s)',
          width: 50,
          key: 'delayTime',
          render: (h, params) => {
            const time = params.row.delayTime === 0 ? '' : params.row.delayTime
            return h('span', time)
          }
        },
        {
          title: '仅更新时投递',
          key: 'sendOnlyUpdated',
          width: 50,
          render: (h, params) => {
            const row = params.row
            const text = row.sendOnlyUpdated ? '✓' : ''
            return h('span', text)
          }
        },
        {
          title: '投递字段',
          key: 'filterFields',
          render: (h, params) => {
            const value = params.row.filterFields !== null ? params.row.filterFields.join(',') : '全部'
            return h('span', value)
          }
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center',
          fixed: 'right'
        }
      ]
    }
  },
  methods: {
    async goToInsertConfig () {
      this.selectedRow = null
      this.action = DbMqConfig.FORM_ACTION_OPTION.CREATE
      this.editModal.open = true
      this.filterReadOnly = false
    },
    async goToUpdateConfig (row, index) {
      this.selectedRow = row
      this.action = DbMqConfig.FORM_ACTION_OPTION.EDIT
      this.editModal.open = true
      this.filterReadOnly = true
    },
    async goToDeleteConfig (row, index) {
      this.selectedRow = row
      this.action = DbMqConfig.FORM_ACTION_OPTION.DELETE
      this.editModal.open = true
    },
    finishConfig () {
      this.selectedRow = null
      this.editModal.open = false
      this.$emit('updated')
    }
  },
  computed: {
    actionTitle () {
      return this.actionTitleMap.get(this.action) + ': ' + this.mqType
    }
  },
  created () {
  }
}
</script>

<style scoped>

</style>

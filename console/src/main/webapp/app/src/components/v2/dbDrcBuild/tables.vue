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
      <db-replication-config ref="dbReplicationConfigComponent" @finished="finishConfig"
                             v-if="editModal.open"
                             :config-data="selectedRow"
                             :src-region="srcRegion"
                             :dst-region="dstRegion"
                             :db-names="dbNames"
                             :form-action="action"
      />
    </Modal>
  </div>
</template>

<script>
import DbReplicationConfig from '@/components/v2/dbDrcBuild/dbReplicationConfig.vue'

export default {
  name: 'tables',
  components: { DbReplicationConfig },
  props: {
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
        ['create', '新增 表同步'],
        ['edit', '更新 表同步'],
        ['delete', '⚠️删除 表同步']
      ]),
      editModal: {
        open: false
      },
      columns: [
        {
          title: '表名',
          key: 'config',
          minWidth: 300,
          render: (h, params) => {
            const row = params.row
            return h('div', row.config.logicTable)
          }
        },
        {
          title: '过滤规则',
          key: 'filterTypes',
          minWidth: 100,
          render: (h, params) => {
            const row = params.row
            const rowsFilterId = row.config.rowsFilterId
            const colsFilterId = row.config.colsFilterId
            if (!rowsFilterId && !colsFilterId) {
              return h('Tag', {
                props: {
                  color: 'red'
                }
              }, '无')
            } else {
              return h('div', [
                rowsFilterId && h('Tag', {
                  props: {
                    color: 'blue'
                  }
                }, '行过滤'),
                colsFilterId && h('Tag', {
                  props: {
                    color: 'green'
                  }
                }, '字段过滤')
              ])
            }
          }
        },
        {
          title: '操作',
          minWidth: 100,
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
      this.action = DbReplicationConfig.FORM_ACTION_OPTION.CREATE
      this.editModal.open = true
    },
    async goToUpdateConfig (row, index) {
      this.selectedRow = row
      this.action = DbReplicationConfig.FORM_ACTION_OPTION.EDIT
      this.editModal.open = true
    },
    async goToDeleteConfig (row, index) {
      this.selectedRow = row
      this.action = DbReplicationConfig.FORM_ACTION_OPTION.DELETE
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
      return this.actionTitleMap.get(this.action)
    }
  },
  created () {
  }
}
</script>

<style scoped>

</style>

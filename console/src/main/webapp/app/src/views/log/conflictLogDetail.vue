<template>
  <base-component :isFather="isFather" :subMenuName="['1']" :fatherMenu="fatherMenu">
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/conflictLog">冲突事务</BreadcrumbItem>
      <BreadcrumbItem>冲突详情</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div :style="{padding: '1px 1px',height: '100%'}">
        <Card>
          <p slot="title">
            自动冲突处理结果
          </p>
          <div class="ivu-list-item-meta-title">事务提交结果：
            <Tag :loading="logTableLoading" :color="trxLog.trxResult==0?'success':'error'" size="medium">{{trxLog.trxResultStr}}</Tag>
          </div>
          <div class="ivu-list-item-meta-title">所有机房当前冲突事务记录：
            <Tag :color="trxLog.hasDiff==false?'success':'error'" size="medium">{{trxLog.diffStr}}</Tag>
          </div>
          <Divider/>
          <div class="ivu-list-item-meta-title">源机房({{trxLog.srcDc}})</div>
          <Table size="small" :loading="tableLoading" stripe :columns="srcTable.columns" :data="srcTable.data"
                 border></Table>
          <Divider/>
          <div class="ivu-list-item-meta-title">目标机房({{trxLog.dstDc}})</div>
          <Table size="small" :loading="tableLoading" stripe :columns="dstTable.columns" :data="dstTable.data"
                 border></Table>
          <Divider/>
        </Card>
        <Divider/>
        <Card>
          <p slot="title">
            自动冲突处理流程
          </p>
          <Table :loading="logTableLoading" stripe :columns="trxLog.columns" :data="trxLog.tableData"
                 border></Table>
        </Card>
      </div>
    </Content>
  </base-component>
</template>

<script>
import conflictRowsLogDetail from './conflictRowsLogDetail.vue'

export default {
  name: 'conflictLogDetail',
  props: {
    rowData: String
  },
  data () {
    return {
      conflictTrxLogId: 0,
      tableLoading: true,
      logTableLoading: false,
      srcTable: {
        data: [],
        columns: []
      },
      dstTable: {
        data: [],
        columns: {}
      },
      trxLog: {
        srcDc: 'shaxy',
        dstDc: 'sinaws',
        trxResult: 0,
        trxResultStr: 'commit',
        hasDiff: true,
        diffStr: '数据不一致',
        tableData: [
          {
            rawSql: '/*DRC INSERT 0*/ INSERT INTO `migrationdb`.`benchmark` (`id`,`drc_id_int`,`datachange_lasttime`) VALUES (1,1,\'2023-09-28 16:06:15.019\')',
            rawSqlResult: 'BATCHED',
            dstRowRecord: '|1|1|2023-09-28 16:01:15.537|',
            handlerSql: '/*DRC INSERT 1*/ UPDATE `migrationdb`.`benchmark` SET `id`=1,`drc_id_int`=1,`datachange_lasttime`=\'2023-09-28 16:06:15.019\' WHERE `id`=1 AND `datachange_lasttime`<=\'2023-09-28 16:06:15.019\'',
            handlerSqlResult: 'UPDATE_COUNT_EQUALS_ONE',
            rowResult: 0
          },
          {
            rawSql: '/*DRC INSERT 0*/ INSERT INTO `migrationdb`.`benchmark` (`id`,`drc_id_int`,`datachange_lasttime`) VALUES (5,5,\'2023-09-28 16:06:15.019\')',
            rawSqlResult: 'DUPLICATE_ENTRY',
            dstRowRecord: '|5|5|2023-09-28 16:01:15.537|',
            handlerSql: '/*DRC INSERT 1*/ UPDATE `migrationdb`.`benchmark` SET `id`=5,`drc_id_int`=5,`datachange_lasttime`=\'2023-09-28 16:06:15.019\' WHERE `id`=5 AND `datachange_lasttime`<=\'2023-09-28 16:06:15.019\'',
            handlerSqlResult: 'UPDATE_COUNT_EQUALS_ONE',
            rowResult: 1
          }],
        columns: [
          {
            type: 'expand',
            width: 50,
            render: (h, params) => {
              const row = params.row
              return h(conflictRowsLogDetail, {
                props: {
                  rowData: '/*原始SQL*/\n' + row.rawSql + '\n/*原始SQL处理结果: ' + row.rawSqlResult + '*/\n\n' + '/*冲突时行记录*/\n' +
                    row.dstRowRecord + '\n\n' + '/*冲突时行记录*/\n' + row.handlerSql + '\n/*冲突时行记录处理结果: ' + row.handlerSqlResult + '*/\n'
                }
              })
            }
          },
          {
            title: '序号',
            width: 50,
            align: 'center',
            render: (h, params) => {
              return h(
                'span',
                params.index + 1
              )
            }
          },
          {
            title: '原始sql',
            key: 'rawSql'
          },
          {
            title: '处理结果',
            key: 'rowResult',
            width: 120,
            align: 'center',
            render: (h, params) => {
              const row = params.row
              const color = row.rowResult === 0 ? 'blue' : 'volcano'
              const text = row.rowResult === 0 ? 'commit' : 'rollback'
              return h('Tag', {
                props: {
                  color: color
                }
              }, text)
            }
          }
        ]
      }
    }
  },
  methods: {
    getLogDetailView () {
      this.logTableLoading = true
      this.axios.get('/api/drc/v2/conflict/log/detail?conflictTrxLogId=' + this.conflictTrxLogId)
        .then(response => {
          if (response.data.status === 1) {
            this.$Message.error('查询冲突详情失败!')
          } else {
            const data = response.data.data
            this.trxLog = {
              srcDc: data.srcDc,
              dstDc: data.dstDc,
              trxResult: data.trxResult,
              trxResultStr: data.trxResult === 0 ? 'commit' : 'rollback',
              tableData: data.rowsLogDetailViews
            }
          }
        })
      this.logTableLoading = true
    }
  },
  created () {
    this.conflictTrxLogId = this.$route.query.conflictTrxLogId
  }
}
</script>

<style scoped>

</style>

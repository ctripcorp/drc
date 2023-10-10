<template>
  <Collapse>
    <div >
      <div slot="content">
        <div class="ivu-list-item-meta-title">原SQL</div>
        <codemirror ref="cm1" v-model="rawSqlList" :options="cmOptions"
        ></codemirror>
        <Divider/>
        <div class="ivu-list-item-meta-title">原SQL处理结果</div>
        <codemirror ref="cm2" v-model="rawSqlExecutedResultList" :options="cmOptions"></codemirror>
        <Divider/>
        <div class="ivu-list-item-meta-title">冲突时Record</div>
        <codemirror ref="cm3" v-model="destCurrentRecordList" :options="cmOptions"></codemirror>
        <Divider/>
        <div class="ivu-list-item-meta-title">冲突处理SQL</div>
        <codemirror ref="cm4" v-model="conflictHandleSqlList" :options="cmOptions"></codemirror>
        <Divider/>
        <div class="ivu-list-item-meta-title">冲突处理SQL结果</div>
        <codemirror ref="cm5" v-model="conflictHandleSqlExecutedResultList" :options="cmOptions"></codemirror>
      </div>
    </div>
  </Collapse>
</template>

<script>
import { codemirror } from 'vue-codemirror'
import 'codemirror/theme/ambiance.css'
import 'codemirror/mode/sql/sql.js'

export default {
  name: 'test',
  components: {
    codemirror
  },
  data () {
    return {
      rawSqlList: '/*DRC INSERT 0*/ INSERT INTO `migrationdb`.`benchmark` (`id`,`drc_id_int`,`datachange_lasttime`) VALUES (1,1,\'2023-09-28 16:06:15.019\')',
      cmOptions: {
        value: '',
        mode: 'text/x-mysql',
        theme: 'ambiance',
        lineWrapping: true,
        height: 100,
        readOnly: true,
        lineNumbers: true,
        autoRefresh: true
      }
    }
  }
}
</script>

<style>
.ivu-table .table-info-cell-extra-column-add {
  background-color: #2db7f5;
  color: #fff;
}

.ivu-table .table-info-cell-extra-column-diff {
  background-color: #ff6600;
  color: #fff;
}

.CodeMirror {
  /* Set height, width, borders, and global font properties here */
  font-family: monospace;
  height: auto;
  color: black;
  direction: ltr;
}
</style>

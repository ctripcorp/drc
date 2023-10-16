<template>
  <div>
    <codemirror v-model="rowData" :options="options"></codemirror>
  </div>
</template>

<script>
import { codemirror } from 'vue-codemirror'
import 'codemirror/theme/ambiance.css'
import 'codemirror/mode/sql/sql.js'

export default {
  name: 'conflictRowsLogDetail',
  props: {
    rowData: String
  },
  components: {
    codemirror
  },
  data () {
    return {
      options: {
        value: '',
        mode: 'text/x-mysql',
        theme: 'ambiance',
        lineWrapping: true,
        height: 100,
        readOnly: true,
        lineNumbers: true
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

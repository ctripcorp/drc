<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <Tabs v-model="tabValue">
        <TabPane label="冲突事务" name="trxLog">
          <conflict-trx-log :gtid="gtid" v-on:tabValueChanged="updateTabValue"
                            v-on:gtidChanged="updateGtid"></conflict-trx-log>
        </TabPane>
        <TabPane label="冲突行" name="rowsLog">
          <conflict-rows-log v-if="refresh" :gtid="gtid"></conflict-rows-log>
        </TabPane>
      </Tabs>
    </Content>
  </base-component>
</template>

<script>
import conflictRowsLog from '../log/conflictRowsLog'
import conflictTrxLog from '../log/conflictTrxLog'

export default {
  components: {
    conflictRowsLog,
    conflictTrxLog
  },
  name: 'conflictLog',
  data () {
    return {
      tabValue: 'trxLog',
      gtid: null,
      refresh: false
    }
  },
  watch: {
    tabValue (val, newVal) {
      // alert(newVal + ':' + val)
      if (newVal === 'trxLog') {
        this.refresh = true
        // this.refresh = false
      } else {
        this.refresh = false
      }
    }
  },
  methods: {
    jump (val) {
      this.tabValue = val
    },
    updateTabValue (e) {
      this.tabValue = e
    },
    updateGtid (e) {
      this.gtid = e
    }
  }
}
</script>

<style scoped>

</style>

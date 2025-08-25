<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/conflictLog">冲突处理</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <Tabs v-model="tabValue"  @on-click="handleTabClick">
        <TabPane label="冲突行" name="rowsLog">
          <conflict-rows-log v-if="refresh" :gtid="gtid" :begin-handle-time="beginHandleTime"
                             :end-handle-time="endHandleTime"
                             :searchMode="searchMode" v-on:tabValueChanged="updateTabValue"
                             v-on:gtidChanged="updateGtid" v-on:beginHandleTimeChanged="updateBeginHandleTime"
                             v-on:endHandleTimeChanged="updateEndHandleTime"></conflict-rows-log>
        </TabPane>
        <TabPane label="冲突事务" name="trxLog">
          <conflict-trx-log v-if="!refresh" :gtid="gtid" :begin-handle-time="beginHandleTime"
                            :end-handle-time="endHandleTime" v-on:tabValueChanged="updateTabValue"
                            v-on:gtidChanged="updateGtid" v-on:searchModeChanged="updateSearchMode"
                            v-on:beginHandleTimeChanged="updateBeginHandleTime"
                            v-on:endHandleTimeChanged="updateEndHandleTime"></conflict-trx-log>
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
      tabValue: this.$route.query.tab || 'rowsLog',
      gtid: null,
      beginHandleTime: new Date(new Date().setSeconds(0, 0) - 10 * 60 * 1000),
      endHandleTime: new Date(new Date().setSeconds(0, 0) + 60 * 1000),
      refresh: true,
      searchMode: this.$route.query.searchMode || false
    }
  },
  watch: {
    tabValue (val, newVal) {
      // alert(newVal + ':' + val)
      if (newVal === 'trxLog') {
        this.refresh = true
        // this.refresh = false
      } else if (newVal === 'rowsLog') {
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
    },
    updateBeginHandleTime (e) {
      this.beginHandleTime = e
    },
    updateEndHandleTime (e) {
      this.endHandleTime = e
    },
    updateSearchMode (e) {
      this.searchMode = e
    },
    handleTabClick (tabName) {
      this.tabValue = tabName
      this.updateUrl()
    },
    updateUrl () {
      const query = { ...this.$route.query }
      if (this.tabValue !== 'rowsLog') {
        query.tab = this.tabValue
      } else {
        delete query.tab
      }
      this.$router.replace({ path: '/conflictLog', query: query }).catch(err => {
        if (err.name !== 'NavigationDuplicated') {
          console.error(err)
        }
      })
      console.log(this.$route.query)
    }
  },
  created () {
    console.log('主页面data')
    console.log(this.searchMode)
    const tabParam = this.$route.query.tab
    if (tabParam === 'trxLog') {
      this.tabValue = 'trxLog'
      this.refresh = false
    } else {
      this.tabValue = 'rowsLog'
      this.refresh = true
    }
    const searchModeParam = this.$route.query.searchMode
    if (searchModeParam === 'true') {
      this.searchMode = true
    } else {
      this.searchMode = false
    }
  }
}
</script>

<style scoped>

</style>

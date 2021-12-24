<template>
  <base-component>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Card>
          <div class="ivu-list-item-meta-title">
            <p>数据库名：{{this.schema}}</p>
          </div>
          <div class="ivu-list-item-meta-title">
            <p>表名：{{this.table}}</p>
          </div>
          <div class="ivu-list-item-meta-title">一致性结果：
            <Tag :color="isInconsistency" size="medium">{{inconsistency}}</Tag>
          </div>
          <div class="ivu-list-item-meta-title">
            <p>当前不一致条数：{{this.differentCount}}</p>
          </div>
          <Divider/>
          <div class="ivu-list-item-meta-title">机房：{{ipA}} : {{portA}}</div>
          <Table style="margin-top: 20px" :columns="columnA" :data="recordA" size="small"></Table>
          <Divider/>
          <div class="ivu-list-item-meta-title">机房：{{ipB}} : {{portB}}</div>
          <Table style="margin-top: 20px" :columns="recordA" :data="recordB" size="small"></Table>
          <Divider/>
        </Card>
      </div>
    </Content>
  </base-component>
</template>

<script>

export default {
  name: 'fullDataConsistencyResultForTest',
  data () {
    return {
      ipA: '',
      portA: '',
      columnA: [],
      recordA: [],
      ipB: '',
      portB: '',
      columnB: [],
      recordB: [],
      schema: '',
      table: '',
      isInconsistency: false,
      inconsistency: '',
      differentCount: 0
    }
  },
  methods: {
    getCurrentRecordForTest: function () {
      const that = this
      const testConfig = {
        ipA: this.$route.query.ipA,
        portA: this.$route.query.portA,
        userA: this.$route.query.userA,
        passwordA: this.$route.query.passwordA,
        ipB: this.$route.query.ipB,
        portB: this.$route.query.portB,
        userB: this.$route.query.userB,
        passwordB: this.$route.query.passwordB,
        schema: this.$route.query.schema,
        table: this.$route.query.table,
        key: this.$route.query.key
      }
      console.log(typeof testConfig)
      console.log(typeof this.$route.query.checkTime)
      that.ipA = testConfig.ipA
      that.portA = testConfig.portA
      that.ipB = testConfig.ipB
      that.portB = testConfig.portB
      that.schema = testConfig.schema
      that.table = testConfig.table
      this.axios.post('api/drc/v1/monitor/consistency/full/historyForTest/' + this.$route.query.checkTime, testConfig)
        .then(res => {
          console.log(res)
          that.columnA = res.data.data.mhaAColumnPattern
          that.recordA = res.data.data.mhaACurrentResultList
          that.columnB = res.data.data.mhaBColumnPattern
          that.recordB = res.data.data.mhaBCurrentResultList
          that.differentCount = res.data.data.differentCount
          that.isInconsistency = res.data.data.markDifferentRecord === true ? 'error' : 'success'
          that.inconsistency = res.data.data.markDifferentRecord === true ? '数据不一致' : '数据一致'
        })
    }
  },
  created () {
    this.getCurrentRecordForTest()
  }
}
</script>

<style scoped>

</style>

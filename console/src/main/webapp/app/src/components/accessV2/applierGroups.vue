<template  v-if="current === 0" :key="0">
  <div>
    <template v-if="display.applierGroupTables">
      <span style="margin-right: 5px;color:black;font-weight:600">同步链路</span>
      <Button v-if="applierGroupsData.length < 2" type="primary" @click="goToCreateApplierGroup">添加</Button>
      <br/>
      <br/>
      <Table stripe :columns="columns" :data="applierGroupsData" border >
<!--        <template slot-scope="{ row, index }" slot="action">-->
<!--          <Button type="primary" size="small" style="margin-right: 5px" @click="goToUpdateApplierGroup(row, index)">修改</Button>-->
<!--&lt;!&ndash;          <Button type="error" size="small" style="margin-right: 5px" @click="removeApplierGroup(row, index)">删除</Button>&ndash;&gt;-->
<!--        </template>-->
        <template slot-scope="{ row, index }" slot="rowsFilterMappings">
          <Button type="success" size="small" style="margin-right: 5px" @click="goToShowRowsFilterMappings(row, index)">详情</Button>
        </template>
      </Table>
    </template>
    <Form v-if="display.applierGroupForm" ref="applierGroupConfig" :model="applierGroupConfig" :label-width="250" style="margin-top: 50px">
      <FormItem label="源mha"  style="width: 600px">
        <Select v-model="applierGroupConfig.srcMha"  style="width: 200px" placeholder="请选择源集群名">
          <Option  :value="oldClusterName" :key="oldClusterName">{{oldClusterName}}</Option>
          <Option  :value="newClusterName" :key="newClusterName">{{newClusterName}}</Option>
        </Select>
      </FormItem>
      <FormItem label="目标mha"  style="width: 600px">
        <Select v-model="applierGroupConfig.destMha"  style="width: 200px" placeholder="请选择源集群名">
          <Option  :value="newClusterName" :key="newClusterName">{{newClusterName}}</Option>
          <Option  :value="oldClusterName" :key="oldClusterName">{{oldClusterName}}</Option>
        </Select>
      </FormItem>
      <FormItem>
        <Button type="primary" @click="submitApplierGroupConfig">提交</Button>
        <Button  @click="goToApplierGroups" style="margin-left: 50px">返回</Button>
      </FormItem>
    </Form>
    <rowsFilterMap v-if="display.rowsFilterMap" v-bind="applierGroupConfig" v-on:closeRowsFilterMap="closeRowsFilterMap"></rowsFilterMap>
  </div>
</template>

<script>
import rowsFilterMap from '../accessV2/rowsFilterMap'

export default {
  components: {
    rowsFilterMap
  },
  name: 'applierGroups',
  props: {
    // oldClusterName means srcMhaName
    // newClusterName means destMhaName
    oldClusterName: String,
    newClusterName: String,
    oldDrcZone: String,
    newDrcZone: String
  },
  data () {
    return {
      display: {
        applierGroupTables: true,
        applierGroupForm: false,
        rowsFilterMap: false
      },
      columns: [
        // {
        //   title: '序号',
        //   key: 'applierGroupId',
        //   width: 60
        // },
        {
          title: '序号',
          width: 75,
          align: 'center',
          render: (h, params) => {
            return h(
              'span',
              params.index + 1
            )
          }
        },
        {
          title: '源集群',
          key: 'srcMha'
        },
        {
          title: '目标集群',
          key: 'destMha'
        },
        {
          title: '方向',
          key: 'direction',
          width: 260,
          align: 'center',
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.srcDc + ' ==> ' + row.destDc
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '行过滤',
          slot: 'rowsFilterMappings',
          align: 'center'
        }
        // {
        //   title: '操作',
        //   slot: 'action',
        //   align: 'center'
        // }
      ],
      applierGroupsData: [],
      applierGroupConfig: {
        srcMha: null,
        destMha: null,
        srcDc: null,
        destDc: null,
        applierGroupId: null
      },
      hasResp: false
    }
  },
  methods: {
    subCurrentChange () {
      this.$emit('subCurrentChange', this.subCurrent)
    },
    getApplierGroupsData () {
      this.axios.get('/api/drc/v1/build/applierGroups/' +
        this.oldClusterName + '/' +
        this.newClusterName)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('查询同步链路信息失败！')
          } else {
            this.applierGroupsData = response.data.data
          }
        })
    },
    goToCreateApplierGroup () {
      this.display = {
        applierGroupTables: false,
        applierGroupForm: true,
        rowsFilterMap: false
      }
    },
    goToApplierGroups () {
      this.display.applierGroupTables = true
      this.display.applierGroupForm = false
    },
    goToShowRowsFilterMappings (row, index) {
      this.applierGroupConfig.srcMha = row.srcMha
      this.applierGroupConfig.destMha = row.destMha
      this.applierGroupConfig.srcDc = row.srcDc
      this.applierGroupConfig.destDc = row.destDc
      this.applierGroupConfig.applierGroupId = row.applierGroupId.toString()
      this.display.applierGroupTables = false
      this.display.applierGroupForm = false
      this.display.rowsFilterMap = true
    },
    submitApplierGroupConfig () {
      if (this.applierGroupConfig.srcMha !== null &&
        this.applierGroupConfig.destMha !== null &&
        this.applierGroupConfig.srcMha !== this.applierGroupConfig.destMha &&
        this.applierGroupsData.size !== 2) {
        console.log('post build')
        this.axios.post('/api/drc/v1/build/simplexDrc', {
          srcMha: this.applierGroupConfig.srcMha,
          destMha: this.applierGroupConfig.destMha
        }).then(response => {
          this.hasResp = true
          if (response.data.status === 0) {
            this.status = 'success'
            this.title = '创建单向同步通道成功!'
            this.applierGroupConfig.applierGroupId = response.data.data.applierGroupId
            this.getApplierGroupsData()
          } else {
            this.status = 'error'
            this.title = '创建单向同步通道失败!'
            this.message = response.data.message
          }
        })
      } else {
        console.log('forbid post build')
        this.hasResp = true
        this.status = 'error'
        this.title = '无效配置，禁止创建!'
      }
    },
    handleReset (name) {
      this.$refs[name].resetFields()
    },
    goToUpdateApplierGroup (row, index) {
      this.applierGroupConfig.srcMha = row.srcMha
      this.applierGroupConfig.destMha = row.destMha
      this.applierGroupConfig.srcDc = row.srcDc
      this.applierGroupConfig.destDc = row.destDc
      this.applierGroupConfig.applierGroupId = row.applierGroupId
      console.log(row.applierGroupId)
      this.goToCreateApplierGroup()
    },
    removeApplierGroup (row, index) {
    },
    closeRowsFilterMap () {
      this.display = {
        applierGroupTables: true,
        applierGroupForm: false,
        rowsFilterMap: false
      }
    }
  },
  created () {
    // this.oldClusterName = 'commonordershardnt'
    // this.newClusterName = 'commonordershardst'
    // this.oldDrcZone = 'ntgxh'
    // this.newDrcZone = 'ntgxy'
    this.getApplierGroupsData()
  }

}
</script>

<style scoped>

</style>

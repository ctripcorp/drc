<template>
    <base-component>
      <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
        <BreadcrumbItem to="/home">首页</BreadcrumbItem>
<!--        <BreadcrumbItem to="/">DRC配置</BreadcrumbItem>-->
        <BreadcrumbItem to="/rowsFilterConfigs">行过滤配置</BreadcrumbItem>
      </Breadcrumb>
      <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
        <span style="margin-right: 5px;color:#464c5b;font-weight:600">{{drc.srcMha}}({{drc.srcDc}})==>{{drc.destMha}}({{drc.destDc}})</span>
        <div :style="{padding: '1px 1px',height: '100%'}">
          <template v-if="display.rowsFilterConfigs">
            <Table style="margin-top: 20px" stripe :columns="columns" :data="rowsFilterConfigsData" border>
              <template slot-scope="{ row, index }" slot="action">
                <Button type="success" size="small" style="margin-right: 5px" @click="goToShowConfig(row, index)">查看</Button>
                <Button type="primary" size="small" style="margin-right: 5px" @click="goToUpdateConfig(row, index)">修改</Button>
                <Button type="error" size="small" style="margin-right: 10px" @click="deleteConfig(row, index)">删除</Button>
              </template>
            </Table>
            <Button style="margin-top: 10px" type="primary" ghost @click="goToAddRowsFilter">添加</Button>
          </template>
        </div>
      </Content>
      <Modal
        v-model="display.rowsFilterModal"
        title="行过滤配置"
        width="1000px"
        >
        <Row :gutter="10">
          <Col span="12">
            <Card>
              <div slot="title">
                  <span>配置</span>
              </div>
              <Form ref="rowsFilterConfig" :model="rowsFilterConfig" :label-width="100"
                    style="margin-top: 10px">
                <FormItem label="数据源" >
                  <Input v-model="rowsFilterConfig.dataMediaSourceName" readonly style="width: 200px"/>
                </FormItem>
                <FormItem label="库名">
                  <Input v-model="rowsFilterConfig.namespace" style="width:200px" placeholder="支持正则"/>
                </FormItem>
                <FormItem label="表名">
                  <Row>
                    <Col span="16">
                      <Input v-model="rowsFilterConfig.name" style="width:200px" placeholder="请输入逻辑表名，支持正则"/>
                    </Col>
                    <Col span="4">
                      <Button  type="primary" @click="checkTable" ghost style="margin-left: 10px">匹配</Button>
                    </Col>
                  </Row>
                </FormItem>
                <FormItem label="规则名">
                  <Input v-model="rowsFilterConfig.rowsFilterName" style="width: 200px" placeholder="行过滤规则名"/>
                </FormItem>
                <FormItem label="模式">
                  <Select v-model="rowsFilterConfig.mode" style="width: 200px" placeholder="选择行过滤模式">
                    <Option v-for="item in modesForChose" :value="item" :key="item">{{ item }}</Option>
                  </Select>
                </FormItem>
                <FormItem label="选择列名">
                  <Select v-model="rowsFilterConfig.columns" multiple style="width: 200px" placeholder="选择相关列">
                    <Option v-for="item in columnsForChose" :value="item" :key="item">{{ item }}</Option>
                  </Select>
                </FormItem>
                <FormItem label="添加列名">
                  <Row>
                    <Col span="16">
                      <Input v-model="columnForAdd" style="width:200px" placeholder="手动添加列名"/>
                    </Col>
                    <Col span="4">
                      <Button  type="primary" @click="addColumn" ghost style="margin-left: 10px">添加</Button>
                    </Col>
                  </Row>
                </FormItem>
                <FormItem label="默认过滤" >
                  <Checkbox v-model="rowsFilterConfig.illegalArgument">过滤【字段为空时】</Checkbox>
                </FormItem>
                <FormItem label="规则内容" >
                  <Input v-if="rowsFilterConfig.mode !== 'trip_uid'" type="textarea" v-model="rowsFilterConfig.context" style="width: 250px" placeholder="请输入行过滤内容"/>
                  <Select v-if="rowsFilterConfig.mode === 'trip_uid'"  v-model="rowsFilterConfig.context" style="width: 200px" placeholder="Region 选择">
                    <Option v-for="item in regionsForChose" :value="item" :key="item">{{ item }}</Option>
                  </Select>
                </FormItem>
              </Form>
            </Card>
          </Col>
          <Col span="12">
            <Card>
              <div slot="title">
                <span>相关表</span>
              </div>
              <Table stripe :columns="columnsForTableCheck" :data="tableData" border ></Table>
            </Card>
          </Col>
        </Row>
        <template #footer>
          <Button type="text" size="large"  @click="cancelSubmit">取消</Button>
          <Button type="primary"  @click="submitConfig">提交</Button>
        </template>
      </Modal>
    </base-component>
</template>

<script>
export default {
  name: 'rowsFilterConfigs',
  data () {
    return {
      display: {
        rowsFilterConfigs: true,
        rowsFilterModal: false,
        showOnly: false
      },
      drc: {
        srcMha: this.$route.query.srcMha,
        destMha: this.$route.query.destMha,
        srcDc: '',
        destDc: '',
        applierGroupId: 0
      },
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
          title: '库名',
          key: 'namespace',
          width: 300
        },
        {
          title: '表名',
          key: 'name',
          width: 300
        },
        {
          title: '规则名',
          key: 'rowsFilterName',
          width: 200
        },
        {
          title: '模式',
          key: 'mode',
          width: 120
        },
        {
          title: '默认过滤',
          key: 'illegalArgument',
          width: 100
        },
        {
          title: '相关列',
          key: 'columns',
          width: 200
        },
        {
          title: '内容',
          key: 'context',
          width: 300
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center',
          width: 200,
          fixed: 'right'
        }
      ],
      rowsFilterConfigsData: [],
      tableData: [],
      columnsForTableCheck: [
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
          title: '库名',
          key: 'schema'
        },
        {
          title: '表名',
          key: 'name'
        }
      ],
      rowsFilterConfig: {
        mappingId: 0,
        dataMediaId: 0,
        namespace: '',
        name: '',
        dataMediaSourceId: 0,
        dataMediaSourceName: '',
        rowsFilterId: 0,
        rowsFilterName: '',
        mode: 'trip_uid',
        columns: [],
        context: '',
        illegalArgument: false
      },
      modesForChose: [
        'aviator_regex',
        'java_regex',
        'trip_uid',
        'custom'
      ],
      columnsForChose: [
        'UID'
      ],
      regionsForChose: [
        'SIN',
        'SH'
      ],
      columnForAdd: '',
      forceCommit: false
    }
  },
  methods: {
    getOrInitSimplexDrc () {
      console.log(this.drc.srcMha + '/' + this.drc.destMha)
      this.axios.post('/api/drc/v1/build/simplexDrc/' + this.drc.srcMha + '/' + this.drc.destMha)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('获取或创建该方向同步!')
          } else {
            const vo = response.data.data
            this.drc.srcDc = vo.srcDc
            this.drc.destDc = vo.destDc
            this.drc.applierGroupId = vo.destApplierGroupId
            this.getRowsFilterConfigs()
          }
        })
    },
    getRowsFilterConfigs () {
      console.log(this.drc.applierGroupId)
      this.axios.get('/api/drc/v1/build/rowsFilterMappings/' + this.drc.applierGroupId)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('查询行过滤配置失败!')
          } else {
            this.rowsFilterConfigsData = response.data.data
          }
        })
    },
    goToShowConfig (row, index) {
      this.rowsFilterConfigInitFormRow(row, index)
      this.checkTable()
      this.columnsForChose = row.columns
      this.display.rowsFilterModal = true
      this.display.showOnly = true
    },
    goToUpdateConfig  (row, index) {
      this.rowsFilterConfigInitFormRow(row, index)
      this.columnsForChose = row.columns
      this.forceCommit = false
      this.display.showOnly = false
      this.display.rowsFilterModal = true
    },
    deleteConfig (row, index) {
      this.axios.delete('/api/drc/v1/build/rowsFilterConfig/' + row.id).then(response => {
        console.log(response.data)
        console.log(response.data.data)
        if (response.data.status === 0) {
          alert('删除成功！')
          this.getRowsFilterConfigs()
        } else {
          alert('操作失败！')
        }
      })
    },
    goToAddRowsFilter () {
      this.rowsFilterConfigInit()
      this.checkTable()
      this.columnsForChose = ['UID']
      this.forceCommit = false
      this.display.showOnly = false
      this.display.rowsFilterModal = true
    },
    rowsFilterConfigInitFormRow (row, index) {
      this.rowsFilterConfig = {
        mappingId: row.id,
        dataMediaId: row.dataMediaId,
        namespace: row.namespace,
        name: row.name,
        dataMediaSourceId: row.dataMediaSourceId,
        dataMediaSourceName: this.drc.srcMha,
        rowsFilterId: row.rowsFilterId,
        rowsFilterName: row.rowsFilterName,
        mode: row.mode,
        columns: row.columns,
        context: row.context,
        illegalArgument: row.illegalArgument
      }
      this.tableData = []
    },
    rowsFilterConfigInit () {
      this.rowsFilterConfig = {
        mappingId: 0,
        dataMediaId: 0,
        namespace: '.*',
        name: '.*',
        dataMediaSourceId: 0,
        dataMediaSourceName: this.drc.srcMha,
        rowsFilterId: 0,
        rowsFilterName: '',
        mode: 'trip_uid',
        columns: [],
        context: '',
        illegalArgument: false
      }
      this.tableData = []
    },
    submitConfig () {
      if (this.display.showOnly) {
        window.alert('查看时 禁止提交！')
      } else if (this.rowsFilterConfig.namespace === '' ||
      this.rowsFilterConfig.name === '' ||
      this.rowsFilterConfig.mode === '' ||
      this.rowsFilterConfig.rowsFilterName === '' ||
      this.rowsFilterConfig.columns.length === 0 ||
      this.rowsFilterConfig.context === '') {
        alert('缺少配置 禁止提交！')
      } else if (this.tableData.length === 0 && !this.forceCommit) {
        this.forceCommit = true
        alert('无匹配表 下一次提交将强制执行！！')
      } else {
        this.axios.post('/api/drc/v1/build/rowsFilterConfig', {
          id: this.rowsFilterConfig.mappingId === 0 ? null : this.rowsFilterConfig.mappingId,
          applierGroupId: this.drc.applierGroupId,
          namespace: this.rowsFilterConfig.namespace === '' ? null : this.rowsFilterConfig.namespace,
          name: this.rowsFilterConfig.name === '' ? null : this.rowsFilterConfig.name,
          type: 0,
          dataMediaSourceId: this.rowsFilterConfig.dataMediaSourceId === 0 ? null : this.rowsFilterConfig.dataMediaSourceId,
          dataMediaSourceName: this.drc.srcMha,
          rowsFilterId: this.rowsFilterConfig.rowsFilterId === 0 ? null : this.rowsFilterConfig.rowsFilterId,
          rowsFilterName: this.rowsFilterConfig.rowsFilterName === '' ? null : this.rowsFilterConfig.rowsFilterName,
          mode: this.rowsFilterConfig.mode,
          columns: this.rowsFilterConfig.columns === [] ? null : this.rowsFilterConfig.columns,
          illegalArgument: this.rowsFilterConfig.illegalArgument,
          context: this.rowsFilterConfig.context === '' ? null : this.rowsFilterConfig.context
        }).then(response => {
          if (response.data.status === 1) {
            window.alert('提交失败!' + response.data.data)
          } else {
            window.alert('提交成功!' + response.data.data)
            this.display.rowsFilterModal = false
            this.getRowsFilterConfigs()
          }
        })
      }
    },
    cancelSubmit () {
      this.display.rowsFilterModal = false
    },
    checkTable () {
      if (this.rowsFilterConfig.namespace === '' || this.rowsFilterConfig.name === '') {
        window.alert('库名表名不能为空！')
        return
      }
      this.axios.get('/api/drc/v1/build/dataMedia/check/' +
        this.rowsFilterConfig.namespace + '/' +
        this.rowsFilterConfig.name + '/' +
        this.drc.srcDc + '/' +
        this.drc.srcMha + '/0')
        .then(response => {
          if (response.data.status === 1) {
            window.alert('查询匹配表失败')
          } else {
            console.log(response.data.data)
            this.tableData = response.data.data
            if (this.tableData.length === 0) {
              window.alert('无匹配表 或 查询匹配表失败')
            }
          }
        })
      this.axios.get('/api/drc/v1/build/rowsFilter/commonColumns/' +
        this.drc.srcDc + '/' +
        this.drc.srcMha + '/' +
        this.rowsFilterConfig.namespace + '/' +
        this.rowsFilterConfig.name)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('查询公共列名失败，请手动添加！')
          } else {
            console.log(response.data.data)
            this.columnsForChose = response.data.data
          }
        })
    },
    addColumn () {
      this.columnsForChose.push(this.columnForAdd)
      this.rowsFilterConfig.columns.push(this.columnForAdd)
      this.columnForAdd = ''
    }
  },
  created () {
    console.log(this.$route.query.srcMha)
    console.log(this.$route.query.destMha)
    this.drc.srcMha = this.$route.query.srcMha
    this.drc.destMha = this.$route.query.destMha
    this.getOrInitSimplexDrc()
  }

}
</script>

<style scoped>

</style>

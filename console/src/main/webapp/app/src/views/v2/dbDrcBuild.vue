<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/v2/mhaReplications">建立 DRC 同步</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#ffffff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <Form :model="formItem" :label-width="100">
        <FormItem label="数据库" :required=true>
          <Select
            v-model="formItem.dbName"
            filterable
            allow-create
            placeholder="请搜索数据库信息"
            @on-change="selectDb"
            :remote-method="getExistDb"
            :loading="dataLoading">
            <Option v-for="(option, index) in meta.dbOptions" :value="option.dbName" :key="index" >{{option.dbName}}</Option>
          </Select>
        </FormItem>
        <FormItem label="业务部门" :required=true>
          <Select filterable prefix="ios-home" clearable v-model="formItem.buName" placeholder="部门">
            <Option v-for="item in meta.bus" :value="item.buName" :key="item.buName">{{ item.buName }}</Option>
          </Select>
        </FormItem>
        <FormItem label="同步方向" :required=true>
          <Row>
            <Col span="8">
              <Card :bordered="true">
                <template #title>                  <Icon type="ios-pin"/>
                  源集群</template>
                <FormItem>
                  <Select filterable clearable v-model="formItem.srcRegionName" placeholder="地域">
                    <Option v-for="mha in dbClusterInfoList" :value="mha.name" :key="mha.name" :label="mha.name">
                      {{ mha.name }}
                      <span style="float:right;color:#ccc"> {{ mha.regionName }} ({{ mha.dcName }})</span>
                    </Option>
                  </Select>
                </FormItem>
              </Card>
            </Col>
            <Col span="1" style="text-align: center">-></Col>
            <Col span="8">
              <Card :bordered="true">
                <template #title>
                  <Icon type="ios-pin"/>
                  目标集群
                </template>
                <FormItem>
                  <Select filterable clearable v-model="formItem.dstRegionName" placeholder="地域">
                    <Option v-for="mha in dbClusterInfoList" :value="mha.name" :key="mha.name" :label="mha.name">
                      {{ mha.name }}
                      <span style="float:right;color:#ccc"> {{ mha.regionName }} ({{ mha.dcName }})</span>
                    </Option>
                  </Select>
                </FormItem>
              </Card>
            </Col>
          </Row>
        </FormItem>
        <!--        todo by yongnian: 同步表过滤使用选择       -->
        <FormItem label="同步表" :required=true>
          <Input v-model="formItem.tableName" placeholder="请输入正则表达式"></Input>
        </FormItem>
        <FormItem label="行过滤">
          <i-switch v-model="formItem.switch.rowsFilter" size="large">
            <template #open>
              <span>On</span>
            </template>
            <template #close>
              <span>Off</span>
            </template>
          </i-switch>
        </FormItem>
        <Card v-if="formItem.switch.rowsFilter" style="margin-left: 100px">
          <template #title>
            <Icon type="md-settings"/>
            行过滤配置
          </template>
          <FormItem label="行过滤模式">
            <Select v-model="formItem.rowsFilterDetail.mode">
              <Option value="trip-uld">UDL</Option>
            </Select>
          </FormItem>
          <FormItem label="UDL字段">
            <Select v-model="formItem.rowsFilterDetail.row">
              <Option value="1">row1</Option>
              <Option value="2">row2</Option>
            </Select>
          </FormItem>
          <FormItem label="空处理">
            <CheckboxGroup v-model="formItem.rowsFilterDetail.empty">
              <Checkbox label="empty">【字段为空时】同步</Checkbox>
            </CheckboxGroup>
          </FormItem>
        </Card>

        <FormItem label="列过滤">
          <i-switch v-model="formItem.switch.colsFilter" size="large">
            <template #open>
              <span>On</span>
            </template>
            <template #close>
              <span>Off</span>
            </template>
          </i-switch>
        </FormItem>
        <Card v-if="formItem.switch.colsFilter" style="margin-left: 100px">
          <template #title>
            <Icon type="md-settings"/>
            行过滤配置
          </template>
          <FormItem label="模式">
            <Select v-model="formItem.colsFilterDetail.mode">
              <Option value="exclude">exclude</Option>
              <Option value="include">include</Option>
            </Select>
          </FormItem>
          <FormItem label="字段">
            <Select v-model="formItem.colsFilterDetail.col">
              <Option value="1">col1</Option>
              <Option value="2">col2</Option>
            </Select>
          </FormItem>
        </Card>
        <FormItem>
          <Button type="primary" :loading="dataLoading" @click="submitAll">Submit</Button>
          <Button style="margin-left: 8px">Cancel</Button>
          <Button style="margin-left: 8px" type="primary" :loading="dataLoading" @click="getDbInfo">getDbInfo</Button>
        </FormItem>
      </Form>
    </Content>
  </base-component>
</template>
<script>

export default {
  data () {
    return {
      formItem: {
        dbName: null,
        buName: null,
        tableName: null,
        srcRegionName: null,
        dstRegionName: null,
        rowsFilterDetail: {
          mode: 'trip-uld',
          row: null
        },
        colsFilterDetail: {
          mode: null,
          row: null
        },
        textarea: '',
        switch: {
          rowsFilter: false,
          colsFilter: false
        }
      },
      meta: {
        bus: [],
        regions: [],
        dbOptions: [],
        selectedDb: {}
      },
      dbClusterInfoList: [],
      dataLoading: false
    }
  },
  methods: {
    getBus () {
      this.axios.get('/api/drc/v2/meta/bus/all')
        .then(response => {
          this.meta.bus = response.data.data
        })
    },
    getRegions () {
      this.axios.get('/api/drc/v2/meta/regions/all')
        .then(response => {
          this.meta.regions = response.data.data
        })
    },
    getParams: function () {
      const param = {
        dbName: this.formItem.dbName,
        buName: this.formItem.buName,
        srcRegionName: this.formItem.srcRegionName,
        dstRegionName: this.formItem.dstRegionName,
        tblsFilterDetail: {
          tableNames: this.formItem.tableName
        }
      }
      if (this.formItem.switch.rowsFilter) {
        param.rowsFilterDetail = this.formItem.rowsFilterDetail
      }
      if (this.formItem.switch.colsFilter) {
        param.colsFilterDetail = this.formItem.colsFilterDetail
      }
      return param
    },
    async submitAll () {
      const that = this
      that.dataLoading = true
      const params = this.getParams()
      console.log(params)
      await that.axios.post('/api/drc/v2/autoconfig/submit', params)
        .then(response => {
          const pageResult = response.data.data
          const success = !pageResult || pageResult.totalCount === 0
          if (success) {
            that.$Message.success('提交成功')
          } else {
            that.$Message.warning('提交失败')
          }
        })
        .catch(message => {
          that.$Message.error('提交异常: ' + message)
        })
        .finally(() => {
          that.dataLoading = false
        })
    },
    async selectDb () {
      const dbName = this.formItem.dbName
      this.meta.selectedDb = this.meta.dbOptions.filter((item) => item.dbName === dbName)[0]
      console.log('selected', this.meta.selectedDb)
      this.formItem.buName = this.meta.selectedDb.buCode
      console.log('selected', this.formItem.tableName)
      await this.getDbInfo()
    },
    async getDbInfo () {
      const that = this
      that.dataLoading = true
      that.dbClusterInfoList = []
      that.formItem.srcRegionName = null
      that.formItem.dstRegionName = null
      const params = this.getParams()
      await that.axios.get('/api/drc/v2/autoconfig/getDbClusterInfo', {
        params: {
          dbName: params.dbName
        }
      })
        .then(response => {
          const data = response.data.data
          if (data) {
            console.log(JSON.stringify(data))
            that.dbClusterInfoList = data
            that.$Message.success('查询DB成功, 共 ' + data.length + ' 个集群')
          } else {
            that.$Message.warning('查询DB失败')
          }
        })
        .catch(message => {
          that.$Message.error('查询DB异常: ' + message)
        })
        .finally(() => {
          that.dataLoading = false
        })
    },
    async getExistDb (dbName) {
      if (dbName === null || dbName.length === null || dbName.length <= 4) {
        return []
      }
      const that = this
      that.dataLoading = true
      that.dbClusterInfoList = []
      await that.axios.get('/api/drc/v2/autoconfig/getExistDb', {
        params: {
          dbName: dbName
        }
      })
        .then(response => {
          const data = response.data.data
          if (data) {
            that.meta.dbOptions = data
          } else {
            that.$Message.warning('查询DB失败')
          }
        })
        .catch(message => {
          that.$Message.error('查询DB异常: ' + message)
        })
        .finally(() => {
          that.dataLoading = false
        })
    }
  },
  created () {
    this.getRegions()
    this.getBus()
  }
}
</script>

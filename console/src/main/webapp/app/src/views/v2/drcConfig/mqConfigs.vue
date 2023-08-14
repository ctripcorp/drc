<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/v2/mqConfigs">Mq投递配置 v2</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <Row>
        <Col span="22">
          <span style="margin-top: 10px;color:#464c5b;font-weight:600">{{ drc.mhaName }}-Messenger</span>
        </Col>
        <Col span="2">
          <Button style="margin-top: 10px;text-align: right" type="primary" ghost @click="goToAddMqConfig">添加</Button>
        </Col>
      </Row>
      <div :style="{padding: '1px 1px',height: '100%'}">
        <template>
          <Table style="margin-top: 20px" stripe :columns="columns" :loading="dataLoading" :data="mqConfigsData" border>
            <template slot-scope="{ row, index }" slot="action">
              <Button type="success" size="small" style="margin-right: 5px" @click="goToShowConfig(row, index)">查看
              </Button>
              <Button type="primary" size="small" style="margin-right: 5px" @click="goToUpdateConfig(row, index)">修改
              </Button>
              <Button type="error" size="small" style="margin-right: 10px" @click="deleteConfig(row, index)">删除
              </Button>
            </template>
          </Table>
        </template>
      </div>
    </Content>
    <Modal
      v-model="display.mqConfigModal"
      title="MQ 投递配置"
      width="1200px"
    >
      <Modal
        v-model="tagInfo.modal"
        title="冲突配置"
        width="800px">
        <Table :columns="tagInfo.columns" :data="tagInfo.conflictVos"></Table>
      </Modal>
      <!--      <Row>-->
      <!--        <Col span="2">-->
      <!--          <Button type="dashed" size="large" @click="goToNormalTopicApplication">标准</Button>-->
      <!--        </Col>-->
      <!--        <Col span="2">-->
      <!--          <Button type="dashed" size="large" @click="goToCustomTopicApplication">自定义</Button>-->
      <!--        </Col>-->
      <!--      </Row>-->
      <!--      <Divider/>-->
      <Row :gutter="10">
        <Col span="14">
          <Card>
            <div slot="title">
              <span>配置</span>
            </div>
            <Form v-if="display.normalTopicForm" :model="mqConfig" :label-width="100"
                  style="margin-top: 10px">
              <!--             <FormItem label="MQ类型">-->
              <!--               <Select v-model="mqConfig.mqType" style="width: 200px" placeholder="选择MQ类型">-->
              <!--                 <Option value="qmq">qmq</Option>-->
              <!--                 <Option value="kafka">hermes-kafka</Option>-->
              <!--               </Select>-->
              <!--             </FormItem>-->
              <FormItem label="MQ主题">
                <Row>
                  <Col span="5">
                    <Select v-model="topic.bu" filterable placeholder="选择部门">
                      <Option v-for="item in buForChosen" :value="item" :key="item">{{ item }}</Option>
                    </Select>
                  </Col>
                  <Col span="6">
                    <Input v-model="topic.db" placeholder="库名">
                      <template #prepend><span>.</span></template>
                      <template #append><span>.</span></template>
                    </Input>
                  </Col>
                  <Col span="6">
                    <Input v-model="topic.table" placeholder="表名">
                      <template #prepend><span>.</span></template>
                      <template #append><span>.drc</span></template>
                    </Input>
                  </Col>
                  <Col span="4">
                    <Button type="info" @click="check" :loading="checkDbTableLoading" style="margin-left: 10px">表校验
                    </Button>
                  </Col>
                </Row>
              </FormItem>
              <!--             <FormItem v-if="mqConfig.mqType === 'kafka'" label="序列化">-->
              <!--               <Select v-model="mqConfig.serialization" style="width: 200px" placeholder="序列化类型">-->
              <!--                 <Option value="json">json</Option>-->
              <!--                 <Option value="arvo">arvo</Option>-->
              <!--               </Select>-->
              <!--             </FormItem>-->
              <Row>
                <Col span="4">
                  <FormItem label="刷缓存Binlog">
                    <Checkbox v-model="mqConfig.refreshCache" @on-change="refreshCacheChange"></Checkbox>
                  </FormItem>
                </Col>
                <Col span="4">
                  <FormItem label="有序消息">
                    <Checkbox v-model="mqConfig.order" @on-change="mqConfigOrderChange"
                              :disabled="mqConfig.refreshCache"></Checkbox>
                  </FormItem>
                </Col>
                <Col span="16">
                  <FormItem v-if="mqConfig.order" label="字段">
                    <Select v-model="mqConfig.orderKey" filterable allow-create @on-create="handleCreateColumn"
                            style="width: 200px" placeholder="选择有序相关字段">
                      <Option v-for="item in columnsForChose" :value="item" :key="item">{{ item }}</Option>
                    </Select>
                  </FormItem>
                </Col>
              </Row>
              <!--             <FormItem v-if="mqConfig.mqType === 'qmq'" label="延迟投递">-->
              <!--               <Input v-model="mqConfig.delayTime" style="width:200px" placeholder="qmq延迟投递时间,单位:秒"/>-->
              <!--             </FormItem>-->
              <!--              <FormItem label="tag" v-if="tagInfo.inputDisplay">-->
              <!--                <Input v-model="mqConfig.tag" style="width:350px"-->
              <!--                       placeholder="存在冲突配置，需要输入业务名（小写英文）区分"/>-->
              <!--              </FormItem>-->
            </Form>
            <Form v-if="!display.normalTopicForm" :model="mqConfig" :label-width="100"
                  style="margin-top: 10px">
              <!--             <FormItem label="MQ类型">-->
              <!--               <Select v-model="mqConfig.mqType" style="width: 200px" placeholder="选择MQ类型">-->
              <!--                 <Option value="qmq">qmq</Option>-->
              <!--                 <Option value="kafka">hermes-kafka</Option>-->
              <!--               </Select>-->
              <!--             </FormItem>-->
              <FormItem label="部门">
                <Select v-model="topic.bu" filterable style="width:150px" placeholder="选择部门">
                  <Option v-for="item in buForChosen" :value="item" :key="item">{{ item }}</Option>
                </Select>
              </FormItem>
              <FormItem label="库表名">
                <Row>
                  <Col span="10">
                    <Input v-model="topic.db" style="width:200px" placeholder="库名（支持正则）">
                      <template #append><span>\.</span></template>
                    </Input>
                  </Col>
                  <Col span="10">
                    <Input v-model="topic.table" style="width:200px" placeholder="表名（支持正则）"/>
                  </Col>
                  <Col span="4">
                    <Button type="info" @click="check" :loading="checkDbTableLoading" style="margin-left: 10px">表校验
                    </Button>
                  </Col>
                </Row>
              </FormItem>
              <FormItem label="MQ主题">
                <Input v-model="mqConfig.topic" style="width:350px" placeholder="请输入自定义Topic"/>
              </FormItem>
              <!--             <FormItem v-if="mqConfig.mqType === 'kafka'" label="序列化">-->
              <!--               <Select v-model="mqConfig.serialization" style="width: 200px" placeholder="序列化类型">-->
              <!--                 <Option value="json">json</Option>-->
              <!--                 <Option value="arvo">arvo</Option>-->
              <!--               </Select>-->
              <!--             </FormItem>-->
              <Row>
                <Col span="4">
                  <FormItem label="刷缓存Binlog">
                    <Checkbox v-model="mqConfig.refreshCache" @on-change="refreshCacheChange"></Checkbox>
                  </FormItem>
                </Col>
                <Col span="4">
                  <FormItem label="有序消息">
                    <Checkbox v-model="mqConfig.order" @on-change="mqConfigOrderChange"
                              :disabled="mqConfig.refreshCache"></Checkbox>
                  </FormItem>
                </Col>
                <Col span="20">
                  <FormItem v-if="mqConfig.order" label="字段">
                    <Select v-model="mqConfig.orderKey" filterable allow-create @on-create="handleCreateColumn"
                            style="width: 200px" placeholder="选择有序相关字段">
                      <Option v-for="item in columnsForChose" :value="item" :key="item">{{ item }}</Option>
                    </Select>
                  </FormItem>
                </Col>
              </Row>
              <!--             <FormItem v-if="mqConfig.mqType === 'qmq'" label="延迟投递">-->
              <!--               <Input v-model="mqConfig.delayTime" style="width:200px" placeholder="qmq延迟投递时间,单位:秒"/>-->
              <!--             </FormItem>-->
              <!--              <FormItem label="tag" v-if="tagInfo.inputDisplay">-->
              <!--                <Input v-model="mqConfig.tag" style="width:350px"-->
              <!--                       placeholder="存在冲突，请输入业务名作为唯一标识（小写英文）"/>-->
              <!--              </FormItem>-->
            </Form>
          </Card>
        </Col>
        <Col span="10">
          <Card>
            <div slot="title">
              <span>相关表</span>
            </div>
            <Table stripe :loading="checkDbTableLoading" :columns="columnsForTableCheck" :data="dataWithPage"
                   border></Table>
            <div style="text-align: center;margin: 16px 0">
              <Page
                :transfer="true"
                :total="tableData.length"
                :current.sync="current"
                :page-size-opts="pageSizeOpts"
                :page-size="this.size"
                show-total
                show-sizer
                show-elevator
                @on-page-size-change="handleChangeSize"></Page>
            </div>
          </Card>
        </Col>
      </Row>
      <template #footer>
        <Button type="text" size="large" @click="cancelSubmit">取消</Button>
        <Button type="primary" @click="submitConfig" :disabled="display.showOnly" :loading="checkDbTableLoading || submitConfigDataLoading">提交
        </Button>
      </template>
    </Modal>
  </base-component>
</template>

<script>
export default {
  name: 'mqConfigs.vue',
  data () {
    return {
      customSize: 10,
      drc: {
        mhaName: this.$route.query.mhaName,
        messengerGroupId: 0
      },
      display: {
        mqConfigModal: false,
        normalTopicForm: false,
        showOnly: false
      },
      mqConfig: {
        id: 0,
        mqType: 'qmq',
        table: '',
        topic: '',
        serialization: 'json',
        persistent: false,
        persistentDb: '',
        order: false,
        orderKey: '',
        delayTime: 0,
        processor: '',
        refreshCache: false,
        tag: ''
      },
      checkDbTableLoading: false,
      submitConfigDataLoading: false,
      tagInfo: {
        inputDisplay: false,
        modal: false,
        conflictVos: [],
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
          // {
          //   title: '标识',
          //   key: 'tag',
          //   width: 100
          // },
          {
            title: '库表名',
            key: 'table',
            width: 400
          },
          {
            title: '主题',
            key: 'topic',
            width: 400
          }
        ]
      },
      topic: {
        bu: '',
        db: '',
        table: ''
      },
      mqConfigsData: [],
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
          title: '类型',
          key: 'mqType',
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.mqType === 'qmq' ? 'QMQ' : 'Kafka'
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '库表名',
          key: 'table',
          width: 200
        },
        {
          title: '主题',
          key: 'topic',
          width: 200
        },
        {
          title: '有序',
          key: 'order',
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.order ? '有序' : '无序'
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '有序相关字段',
          key: 'orderKey'
        },
        {
          title: '序列化',
          key: 'serialization'
        },
        {
          title: '持久化消息',
          key: 'persistent',
          render: (h, params) => {
            const row = params.row
            const color = 'blue'
            const text = row.persistent ? '持久化' : '非持久化'
            return h('Tag', {
              props: {
                color: color
              }
            }, text)
          }
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center',
          width: 200,
          fixed: 'right'
        }
      ],
      total: 0,
      current: 1,
      size: 5,
      pageSizeOpts: [5, 10, 20, 100],
      columnsForTableCheck: [
        {
          title: '序号',
          width: 75,
          align: 'center',
          render: (h, params) => {
            return h(
              'span',
              params.index + 1 + (this.current - 1) * this.size
            )
          }
        },
        {
          title: '库表名',
          key: 'directSchemaTableName'
        }
      ],
      tableData: [],
      dataLoading: false,
      // qmqURL/api/producer/getBusFromQmq
      buForChosen: [],
      columnsForChose: []
    }
  },
  methods: {
    goToShowConfig (row, index) {
      this.mqInitConfigInitFormRow(row, index)
      this.check()
      this.columnsForChose = []
      this.columnsForChose.push(row.orderKey)
      this.display.showOnly = true
      this.display.mqConfigModal = true
    },
    goToUpdateConfig (row, index) {
      this.mqInitConfigInitFormRow(row, index)
      this.check()
      this.columnsForChose = []
      this.columnsForChose.push(row.orderKey)
      this.display.showOnly = false
      this.display.mqConfigModal = true
    },
    deleteConfig (row, index) {
      this.dataLoading = true
      const reqBody = {
        mhaName: this.drc.mhaName,
        dbReplicationIdList: [row.dbReplicationId]
      }
      console.log(reqBody)
      this.axios.delete('/api/drc/v2/messenger/deleteMqConfig', {
        data: reqBody
      }).then(response => {
        if (response.data.status === 0) {
          this.$Message.success('删除成功！')
        } else {
          this.$Message.error('删除失败: ' + response.data.message)
        }
      }).catch(message => {
        this.$Message.error('删除: ' + message)
      }).finally(() => {
        this.getMqConfigs()
        this.dataLoading = false
      })
    },
    goToAddMqConfig () {
      this.mqConfigInit()
      this.columnsForChose = []
      this.display = {
        showOnly: false,
        normalTopicForm: false,
        mqConfigModal: true
      }
    },
    mqConfigInit () {
      this.mqConfig = {
        id: 0,
        mqType: 'qmq',
        table: '',
        topic: '',
        serialization: 'json',
        persistent: false,
        persistentDb: '',
        order: false,
        orderKey: '',
        delayTime: 0,
        processor: '',
        refreshCache: false,
        tag: ''
      }
      this.topic = {
        bu: '',
        db: '',
        table: ''
      }
      this.tableData = []
      this.tagInfo.inputDisplay = false
    },
    mqInitConfigInitFormRow: function (row, index) {
      this.mqConfig = {
        dbReplicationId: row.dbReplicationId,
        mqType: row.mqType,
        table: row.table, // full name:schema\.table
        topic: row.topic,
        serialization: row.serialization,
        persistent: row.persistent,
        persistentDb: row.persistentDb,
        order: row.order,
        orderKey: row.orderKey,
        delayTime: row.delayTime,
        processor: row.processor,
        refreshCache: false,
        tag: row.tag == null ? '' : row.tag
      }
      const topicInfo = row.topic.split('.')
      const tableInfo = row.table.split('\\.')
      this.topic = {
        bu: topicInfo[0],
        db: tableInfo[0],
        table: tableInfo[1]
      }
      this.tagInfo.inputDisplay = row.tag != null
      this.display.normalTopicForm = row.topic.endsWith('.drc') // 判断是否为规范topic
    },
    goToNormalTopicApplication () {
      this.display.normalTopicForm = true
    },
    goToCustomTopicApplication () {
      this.display.normalTopicForm = false
    },
    cancelSubmit () {
      this.display.mqConfigModal = false
    },
    getCommonColumns () {
      this.columnsForChose = []
      if (this.topic.db == null || this.topic.db === '' || this.topic.table == null || this.topic.table === '') {
        this.$Message.warning('查询公共字段失败：库名、表名不能为空')
        return
      }
      console.log('/api/drc/v2/mysql/commonColumns?' +
        '&mhaName=' + this.drc.mhaName +
        '&namespace=' + this.topic.db +
        '&name=' + this.topic.table)
      this.axios.get('/api/drc/v2/mysql/commonColumns?' +
        '&mhaName=' + this.drc.mhaName +
        '&namespace=' + this.topic.db +
        '&name=' + this.topic.table)
        .then(response => {
          if (response.data.status === 1) {
            alert('查询公共列名失败，请手动添加！' + response.data.data)
            this.columnsForChose = []
          } else {
            console.log(response.data.data)
            this.columnsForChose = response.data.data
            if (this.columnsForChose.length === 0) {
              alert('查询无公共字段！')
            }
          }
        })
    },
    doSubmitAfterCheck: async function (dto) {
      this.$Message.loading('步骤二：提交中')
      await this.axios.post('/api/drc/v2/messenger/submitConfig', dto).then(response => {
        if (response.data.status === 1) {
          this.$Message.error('提交失败: ' + response.data.message)
          return
        }
        this.$Message.success('提交成功: ' + response.data.message)
        this.display.mqConfigModal = false
        this.getMqConfigs()
      })
    },
    checkAndSubmit: function (dto) {
      // check mqConfig before submit
      this.$Message.loading('步骤一：检查中...')
      this.submitConfigDataLoading = true
      this.axios.get('/api/drc/v2/messenger/checkMqConfig', {
        params: dto
      }).then(async response => {
        if (response.data.status === 1) {
          this.$Message.error('配置校验失败: ' + response.data.message)
          return
        }
        const res = response.data.data
        if (!res.allowSubmit) {
          if (res.tag != null) {
            this.mqConfig.tag = res.tag
          }
          this.tagInfo.conflictVos = res.conflictTables
          this.tagInfo.inputDisplay = true
          this.tagInfo.modal = true
          this.$Message.warning('校验未通过，请检查冲突')
          return
        }
        this.$Message.success('校验成功！')
        // submit
        await this.doSubmitAfterCheck(dto)
      }).catch(message => {
        this.$Message.error('配置校验异常: ' + message)
      }).finally(() => {
        this.submitConfigDataLoading = false
      })
    },
    submitConfig () {
      // preCheckInput
      if (this.display.showOnly) {
        alert('查看状态，禁止提交！')
        return
      }
      if (this.topic.db == null || this.topic.db === '') {
        alert('db 不能为空')
        return
      }
      if (this.topic.table == null || this.topic.table === '') {
        alert('table 不能为空！')
        return
      }
      if (this.mqConfig.order && (
        this.mqConfig.orderKey == null || this.mqConfig.orderKey === '')) {
        alert('有序topic 相关字段不能为空')
        return
      }
      if (this.display.normalTopicForm) {
        if (this.tableData.length !== 1) {
          alert('标准topic 只能对应一张表,目前匹配表数量：' + this.tableData.length)
          return
        }
      } else {
        if (this.tableData.length === 0) {
          alert('未匹配到表')
          return
        }
      }
      // config
      if (this.display.normalTopicForm) {
        this.mqConfig.table = this.topic.db + '\\.' + this.topic.table
        this.mqConfig.topic = this.topic.bu + '.' + this.topic.db + '.' + this.topic.table + '.drc'
      } else {
        this.mqConfig.table = this.topic.db + '\\.' + this.topic.table
      }
      const dto = {
        dbReplicationId: this.mqConfig.dbReplicationId,
        bu: this.topic.bu,
        mqType: this.mqConfig.mqType,
        table: this.mqConfig.table,
        topic: this.mqConfig.topic,
        serialization: this.mqConfig.serialization,
        persistent: this.mqConfig.persistent,
        persistentDb: this.mqConfig.persistentDb === '' ? null : this.mqConfig.persistentDb,
        order: this.mqConfig.order,
        orderKey: this.mqConfig.orderKey === '' ? null : this.mqConfig.orderKey,
        delayTime: this.mqConfig.delayTime === null ? 0 : this.mqConfig.delayTime,
        processor: this.mqConfig.processor === '' ? null : this.processor,
        messengerGroupId: this.drc.messengerGroupId,
        mhaName: this.drc.mhaName,
        tag: this.mqConfig.tag === '' ? null : this.mqConfig.tag
      }
      this.tagInfo.conflictVos = []
      this.checkAndSubmit(dto)
    },
    getMqConfigs () {
      this.dataLoading = true
      this.axios.get('/api/drc/v2/messenger/queryConfigs', {
        params: { mhaName: this.drc.mhaName }
      }).then(response => {
        if (response.data.status === 1) {
          this.$Message.error('查询 messenger 配置失败: ' + response.data.message)
          return
        }
        this.$Message.success('查询成功')
        this.mqConfigsData = response.data.data
      }).catch(message => {
        this.$Message.error('查询异常: ' + message)
      }).finally(() => {
        this.dataLoading = false
      })
    },
    getBuListFromQmq () {
      this.axios.get('/api/drc/v2/messenger/qmq/bus')
        .then(response => {
          if (response.data.status === 1) {
            window.alert('从查询qmq 部门信息失败!')
          } else {
            this.buForChosen = response.data.data
          }
        })
    },
    refreshCacheChange () {
      if (this.mqConfig.refreshCache) {
        this.mqConfig.order = true
        alert('刷缓存Binlog 需要有序topic 请联系QMQ团队配置!!!')
        this.getCommonColumns()
      }
    },
    mqConfigOrderChange () {
      if (this.mqConfig.order) {
        alert('有序topic 需要联系QMQ团队配置!!!')
        this.getCommonColumns()
      }
    },
    check () {
      this.showMatchTables()
    },
    getTableData: function (response) {
      return response.map(item => {
        return {
          directSchemaTableName: item.schema + '.' + item.table,
          name: item.table,
          schema: item.schema
        }
      })
    },
    showMatchTables () {
      this.current = 1
      this.tableData = []
      if (this.topic.db === '' || this.topic.table === '') {
        this.$Message.warning('查询失败：库表名不能为空')
        return
      }
      this.checkDbTableLoading = true
      this.axios.get('/api/drc/v2/mysql/preCheckMySqlTables', {
        params: {
          mha: this.drc.mhaName,
          nameFilter:
            this.topic.db + '.' + this.topic.table
        }
      }).then(response => {
        if (response.data.status === 1) {
          this.$Message.error('查询匹配表失败' + response.data.data)
        } else {
          console.log(response.data.data)
          this.tableData = this.getTableData(response.data.data)
          if (this.display.normalTopicForm && this.tableData.length !== 1) {
            this.display.normalTopicForm = false
          }
          if (this.tableData.length === 0) {
            this.$Message.warning('无匹配表')
            return
          }
          this.$Message.success('查询表结构成功')
        }
      }).catch(message => {
        this.$Message.error('表校验查询异常: ' + message)
      }).finally(() => {
        this.checkDbTableLoading = false
      })
    },
    handleCreateColumn (val) {
      if (this.contains(this.columnsForChose, val)) {
        alert('已有项禁止创建')
        return
      }
      if (val === '' || val === undefined || val === null) {
        alert('字段不能为空')
        return
      }
      console.log('/api/drc/v2/mysql/columnCheck?' +
        'mhaName=' + this.drc.mhaName +
        '&namespace=' + this.topic.db +
        '&name=' + this.topic.table +
        '&column=' + val)
      this.axios.get(
        '/api/drc/v2/mysql/columnCheck?' +
        'mhaName=' + this.drc.mhaName +
        '&namespace=' + this.topic.db +
        '&name=' + this.topic.table +
        '&column=' + val)
        .then(response => {
          if (response.data.status === 1) {
            alert('查询字段:' + val + '失败！' + response.data.data)
            this.columnsForChose.push(val)
          } else {
            const tablesWithoutColumn = response.data.data
            if (tablesWithoutColumn.length !== 0) {
              alert('以下表无字段' + val + '如下:' +
                tablesWithoutColumn)
            }
            this.columnsForChose.push(val)
          }
        })
    },
    handleChangeSize (val) {
      this.size = val
    },
    contains (a, obj) {
      var i = a.length
      while (i--) {
        if (a[i] === obj) {
          return true
        }
      }
      return false
    }
  },
  computed: {
    dataWithPage () {
      const data = this.tableData
      const start = this.current * this.size - this.size
      const end = start + this.size
      return [...data].slice(start, end)
    }
  },
  created () {
    console.log(this.$route.query.mhaName)
    this.drc.mhaName = this.$route.query.mhaName
    this.getBuListFromQmq()
    this.getMqConfigs()
  }
}
</script>

<style scoped>

</style>

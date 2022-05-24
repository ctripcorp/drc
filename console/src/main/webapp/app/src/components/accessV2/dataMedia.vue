<template v-if="current === 0" :key="0">
  <div>
    <template v-if="display.dataMediasTable">
      <span style="margin-right: 5px;color:black;font-weight:600">同步链路>行过滤>datMedia</span>
      <Button  type="primary" @click="goToCreateDataMedia" style="margin-right: 5px">创建</Button>
      <Button  @click="returnToMapping" style="margin-right: 5px">返回</Button>
      <br/>
      <br/>
      <Table stripe :columns="columns" :data="dataMediasData" border >
<!--        <template slot-scope="{ row, index }" slot="action">-->
<!--          <Button type="primary" size="small" style="margin-right: 5px" @click="goToUpdateDataMedia(row, index)">修改</Button>-->
<!--          <Button type="error" size="small" style="margin-right: 5px" @click="removeDataMediaConfig(row, index)">删除</Button>-->
<!--        </template>-->
        <template slot-scope="{ row, index }" slot="action">
          <Button type="primary" size="small" style="margin-right: 5px" @click="choseDataMedia(row, index)">选择</Button>
          <Button type="success" size="small" style="margin-right: 5px" @click="showDataMedia(row, index)">查看</Button>
        </template>
      </Table>
    </template>
    <Form v-if="display.dataMediaForm" ref="dataMediaForSubmit" :model="dataMediaForSubmit" :label-width="250" style="margin-top: 50px">
      <FormItem label="逻辑库" style="width: 600px">
        <Input v-model="dataMediaForSubmit.namespace" placeholder="请输入逻辑库名，支持正则"/>
      </FormItem>
      <FormItem label="逻辑表" style="width: 720px" >
        <Row>
          <Col span="18">
          <Input v-model="dataMediaForSubmit.name" placeholder="请输入逻辑表名，支持正则"/>
          </Col>
          <Col span="4">
          <Button  type="primary" @click="checkTable" style="margin-left: 50px" >匹配相关表</Button>
          </Col>
        </Row>
      </FormItem>
      <FormItem label="数据源"  style="width: 600px">
        <Input v-model="dataMediaForSubmit.dataMediaSourceName" readonly/>
      </FormItem>
      <FormItem>
        <Button v-if="display.checkBeforeSubmit" type="primary" @click="submitDataMediaConfig" >提交</Button>
        <Button  @click="cancelSubmit" style="margin-left: 50px">返回</Button>
      </FormItem>
    </Form>
    <Table v-if="display.checkBeforeSubmit" stripe :columns="columnsForTableCheck" :data="tableData" border >
    </Table>
  </div>

</template>

<script>
export default {
  name: 'dataMedia',
  props: {
    srcMha: String,
    destMha: String,
    srcDc: String,
    destDc: String,
    applierGroupId: String
  },
  data () {
    return {
      display: {
        dataMediasTable: true,
        dataMediaForm: false,
        checkBeforeSubmit: false
      },
      dataMediasData: [],
      columns: [
        // {
        //   title: '序号',
        //   key: 'id',
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
          title: '数据源',
          key: 'dataMediaSourceName'
        },
        {
          title: '库名',
          key: 'namespace'
        },
        {
          title: '表名',
          key: 'name'
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center'
        }],
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
      dataMediaForSubmit: {
        namespace: '.*',
        name: '.*',
        type: null,
        dataMediaSourceId: null,
        dataMediaSourceName: null
      },
      applierGroupConfig: {
        srcMha: null,
        destMha: null,
        srcDc: null,
        destDc: null,
        applierGroupId: null
      }
    }
  },
  methods: {
    getDataMediasData () {
      this.axios.get('/api/drc/v1/build/dataMedias/' +
        this.applierGroupId + '/' +
        this.srcMha)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('查询dataMedias 失败')
          } else {
            this.dataMediasData = response.data.data
          }
        })
    },
    goToCreateDataMedia () {
      console.log('创建')
      this.dataMediaForSubmit = {
        namespace: '.*',
        name: '.*',
        type: null,
        dataMediaSourceId: null,
        dataMediaSourceName: this.srcMha
      }
      this.display = {
        dataMediasTable: false,
        dataMediaForm: true,
        checkBeforeSubmit: false
      }
    },
    checkTable () {
      this.axios.get('/api/drc/v1/build/dataMedia/check/' +
        this.dataMediaForSubmit.namespace + '/' +
        this.dataMediaForSubmit.name + '/' +
        this.srcDc + '/' +
        this.dataMediaForSubmit.dataMediaSourceName + '/0')
        .then(response => {
          this.display.checkBeforeSubmit = true
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
    },
    goToUpdateDataMedia (row, index) {
    },
    submitDataMediaConfig () {
      if (this.tableData.length === 0) {
        window.alert('没有匹配的表,禁止提交！')
        return
      }
      this.axios.post('/api/drc/v1/meta/dataMedia', {
        namespace: this.dataMediaForSubmit.namespace,
        name: this.dataMediaForSubmit.name,
        type: 0,
        dataMediaSourceName: this.dataMediaForSubmit.dataMediaSourceName
      }).then(response => {
        if (response.data.status === 1) {
          window.alert('提交失败!')
        } else {
          this.getDataMediasData()
          window.alert('提交成功！')
        }
      })
    },
    cancelSubmit () {
      this.display = {
        dataMediasTable: true,
        dataMediaForm: false,
        checkBeforeSubmit: false
      }
    },
    removeDataMediaConfig (row, index) {
    },
    showDataMedia (row, index) {
      this.dataMediaForSubmit = {
        namespace: row.namespace,
        name: row.name,
        type: 0,
        dataMediaSourceName: this.srcMha
      }
      this.display = {
        dataMediasTable: false,
        dataMediaForm: true,
        checkBeforeSubmit: false
      }
    },
    choseDataMedia (row, index) {
      this.resetDisplay()
      this.$emit('dataMediaChose', [row.id, row.namespace + '\\.' + row.name])
    },
    returnToMapping () {
      this.resetDisplay()
      this.$emit('closeDataMedia')
    },
    resetDisplay () {
      this.display = {
        dataMediasTable: true,
        dataMediaForm: false,
        checkBeforeSubmit: false
      }
    }
  },
  created () {
    this.getDataMediasData()
  }
}
</script>

<style scoped>

</style>

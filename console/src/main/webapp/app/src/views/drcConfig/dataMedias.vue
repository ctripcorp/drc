<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/dataMedias">数据表</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div :style="{padding: '1px 1px',height: '100%'}">
        <template v-if="display.dataMediasTable">
          <!--      <Button type="primary" @click="goToCreateDataMedia" style="margin-right: 5px">创建</Button>-->
          <!--      <br/>-->
          <!--      <br/>-->
          <Table stripe :columns="columns" :data="dataMediasData" border>
            <template slot-scope="{ row, index }" slot="action">
              <Button type="success" size="small" style="margin-right: 5px" @click="goToShowDataMedia(row, index)">查看
              </Button>
              <Button type="primary" size="small" style="margin-right: 5px" @click="goToUpdateDataMedia(row, index)">
                修改
              </Button>
              <Button type="error" size="small" style="margin-right: 5px" @click="deleteDataMedia(row, index)">删除
              </Button>
            </template>
          </Table>
        </template>
        <Form v-if="display.dataMediaForm" ref="dataMediaForSubmit" :model="dataMediaForSubmit" :label-width="250"
              style="margin-top: 50px">
          <FormItem label="逻辑库" style="width: 600px">
            <Input v-model="dataMediaForSubmit.namespace" placeholder="请输入逻辑库名，支持正则"/>
          </FormItem>
          <FormItem label="逻辑表" style="width: 720px">
            <Row>
              <Col span="18">
                <Input v-model="dataMediaForSubmit.name" placeholder="请输入逻辑表名，支持正则"/>
              </Col>
              <Col span="4">
                <Button type="primary" @click="checkTable" style="margin-left: 50px">匹配相关表</Button>
              </Col>
            </Row>
          </FormItem>
          <FormItem label="数据源" style="width: 600px">
            <Input v-model="dataMediaForSubmit.dataMediaSourceName" readonly/>
          </FormItem>
          <FormItem>
            <Button v-if="display.submitButton && !display.showAction" type="primary" @click="submitDataMediaConfig">
              提交
            </Button>
            <Button @click="cancelSubmit" style="margin-left: 50px">返回</Button>
          </FormItem>
        </Form>
        <Table v-if="display.checkBeforeSubmit" stripe :columns="columnsForTableCheck" :data="tableData" border>
        </Table>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'dataMedias',
  data () {
    return {
      display: {
        dataMediasTable: true,
        dataMediaForm: false,
        checkBeforeSubmit: false,
        submitButton: false,
        showAction: false
      },
      dataMediasData: [],
      columns: [
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
        id: null,
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
      this.axios.get('/api/drc/v1/build/allDataMedias')
        .then(response => {
          if (response.data.status === 1) {
            window.alert('查询dataMedias 失败')
          } else {
            this.dataMediasData = response.data.data
          }
        })
    },
    goToShowDataMedia (row, index) {
      this.dataMediaForSubmit = {
        namespace: row.namespace,
        name: row.name,
        type: 0,
        dataMediaSourceName: row.dataMediaSourceName
      }
      this.showDataMediasForm()
    },
    goToCreateDataMedia () {
      console.log('创建')
      this.dataMediaForSubmit = {
        id: null,
        namespace: '.*',
        name: '.*',
        type: null,
        dataMediaSourceId: null,
        dataMediaSourceName: null
      }
      this.showUpdateOrCreateDataMediasForm()
    },
    goToUpdateDataMedia (row, index) {
      this.dataMediaForSubmit = {
        id: row.id,
        namespace: row.namespace,
        name: row.name,
        type: 0,
        dataMediaSourceName: row.dataMediaSourceName
      }
      this.showUpdateOrCreateDataMediasForm()
    },
    deleteDataMedia (row, index) {
      this.axios.delete('/api/drc/v1/meta/dataMedia/' + row.id)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('删除失败!')
          } else {
            window.alert('删除成功')
            this.getDataMediasData()
          }
        })
    },
    checkTable () {
      this.axios.get('/api/drc/v1/build/dataMedia/check/' +
        this.dataMediaForSubmit.namespace + '/' +
        this.dataMediaForSubmit.name + '/' +
        null + '/' +
        this.dataMediaForSubmit.dataMediaSourceName + '/0')
        .then(response => {
          this.showChangeAfterCheckTable()
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
    submitDataMediaConfig () {
      if (this.tableData.length === 0) {
        window.alert('没有匹配的表,禁止提交！')
        return
      }
      this.axios.post('/api/drc/v1/meta/dataMedia', {
        id: this.dataMediaForSubmit.id,
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
      this.getDataMediasData()
      this.showDataMediasTable()
    },
    showDataMediasTable () {
      this.display = {
        dataMediasTable: true,
        dataMediaForm: false,
        checkBeforeSubmit: false,
        submitButton: false,
        showAction: false
      }
    },
    showUpdateOrCreateDataMediasForm () {
      this.display = {
        dataMediasTable: false,
        dataMediaForm: true,
        checkBeforeSubmit: false,
        submitButton: false,
        showAction: false
      }
    },
    showDataMediasForm () {
      this.display = {
        dataMediasTable: false,
        dataMediaForm: true,
        checkBeforeSubmit: false,
        submitButton: false,
        showAction: true
      }
    },
    showChangeAfterCheckTable () {
      this.display.checkBeforeSubmit = true
      this.display.submitButton = true
    }
  },
  created () {
    this.getDataMediasData()
  }
}
</script>

<style scoped>

</style>

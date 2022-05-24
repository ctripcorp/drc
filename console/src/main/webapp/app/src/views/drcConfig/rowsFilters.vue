<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/rowsFilters">行过滤规则</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div :style="{padding: '1px 1px',height: '100%'}">
        <template v-if="display.rowsFiltersTable">
          <Button type="primary" @click="goToCreateRowsFilter" style="margin-right: 5px">创建</Button>
          <br/>
          <br/>
          <Table stripe :columns="columns" :data="rowsFiltersData" border>
            <template slot-scope="{ row, index }" slot="action">
              <Button type="success" size="small" style="margin-right: 5px" @click="goToShowRowsFilter(row, index)">查看
              </Button>
              <Button type="primary" size="small" style="margin-right: 5px" @click="goToUpdateRowsFilter(row, index)">
                修改
              </Button>
              <Button type="error" size="small" style="margin-right: 5px" @click="deleteRowsFilter(row, index)">删除
              </Button>
            </template>
          </Table>
        </template>
        <Form v-if="display.rowsFilterForm" ref="rowsFilterForSubmit" :model="rowsFilterForSubmit" :label-width="250"
              style="margin-top: 50px">
          <FormItem label="规则名" style="width: 600px">
            <Input v-model="rowsFilterForSubmit.name" style="width: 300px" placeholder="请为该规则命名"/>
          </FormItem>
          <FormItem label="模式" style="width: 600px">
            <Select v-model="rowsFilterForSubmit.mode" style="width: 300px" placeholder="选择行过滤模式">
              <Option v-for="item in modesForChose" :value="item" :key="item">{{ item }}</Option>
            </Select>
          </FormItem>
          <FormItem label="选择列名" style="width: 600px">
            <Select v-model="rowsFilterForSubmit.columns" multiple style="width: 300px" placeholder="选择相关列">
              <Option v-for="item in columnsForChose" :value="item" :key="item">{{ item }}</Option>
            </Select>
          </FormItem>
          <FormItem label="手动添加列名" style="width: 720px">
            <Row>
              <Col span="18">
                <Input v-model="columnForAdd" placeholder="手动添加列"/>
              </Col>
              <Col span="4">
                <Button type="primary" @click="addColumn" style="margin-left: 50px">添加</Button>
              </Col>
            </Row>
          </FormItem>
          <FormItem label="规则内容" style="width: 600px">
            <Input v-if="rowsFilterForSubmit.mode !== 'trip_uid'" type="textarea" v-model="rowsFilterForSubmit.content"
                   style="width: 300px" placeholder="请输入行过滤内容"/>
            <Select v-if="rowsFilterForSubmit.mode === 'trip_uid'" v-model="rowsFilterForSubmit.content"
                    style="width: 300px" placeholder="Region 选择">
              <Option v-for="item in regionsForChose" :value="item" :key="item">{{ item }}</Option>
            </Select>
          </FormItem>
          <FormItem>
            <Button v-if="display.submitButton && !display.showAction" type="primary" @click="submitRowsFilter">提交</Button>
            <Button @click="cancelSubmit" style="margin-left: 50px">返回</Button>
          </FormItem>
        </Form>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'rowsFilters',
  data () {
    return {
      display: {
        rowsFiltersTable: true,
        rowsFilterForm: false,
        submitButton: true
      },
      rowsFiltersData: [],
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
          title: '规则名',
          key: 'name'
        },
        {
          title: '模式',
          key: 'mode'
        },
        {
          title: '列名',
          key: 'columns'
        },
        {
          title: '内容',
          key: 'content'
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center'
        }

      ],
      rowsFilterForSubmit: {
        id: null,
        name: null,
        mode: 'trip_uid',
        columns: [],
        content: null
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
      columnForAdd: null
    }
  },
  methods: {
    getAllRowsFilters () {
      this.axios.get('/api/drc/v1/build/rowsFilters')
        .then(response => {
          if (response.data.status === 1) {
            window.alert('查询RowsFilters 失败')
          } else {
            console.log(response.data.data)
            this.rowsFiltersData = response.data.data
          }
        })
    },
    goToUpdateRowsFilter (row, index) {
      this.columnsForChose = row.columns
      this.rowsFilterForSubmit = {
        id: row.id,
        name: row.name,
        mode: row.mode,
        columns: row.columns,
        content: row.content
      }
      this.showUpdateOrCreateRowsFiltersForm()
    },
    goToCreateRowsFilter () {
      this.columnsForChose = ['UID']
      this.rowsFilterForSubmit = {
        id: null,
        name: null,
        mode: 'trip_uid',
        columns: ['UID'],
        content: 'SIN'
      }
      this.showUpdateOrCreateRowsFiltersForm()
    },
    goToShowRowsFilter (row, index) {
      this.columnsForChose = row.columns
      this.rowsFilterForSubmit = {
        id: row.id,
        name: row.name,
        mode: row.mode,
        columns: row.columns,
        content: row.content
      }
      this.showRowsFiltersForm()
    },
    deleteRowsFilter (row, index) {
      this.axios.delete('/api/drc/v1/meta/rowsFilter/' + row.id)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('删除失败!')
          } else {
            window.alert('删除成功')
            this.getAllRowsFilters()
          }
        })
    },
    addColumn () {
      this.columnsForChose.push(this.columnForAdd)
      this.rowsFilterForSubmit.columns.push(this.columnForAdd)
    },
    submitRowsFilter () {
      const data = this.rowsFilterForSubmit
      if (data.name === null || data.mode === null) {
        window.alert('参数缺失，禁止提交！')
        return
      }
      this.axios.post('/api/drc/v1/meta/rowsFilter', data).then(response => {
        if (response.data.status === 1) {
          window.alert('提交失败!')
        } else {
          window.alert('提交成功！')
          this.rowsFiltersData = response.data.data
          this.getAllRowsFilters()
        }
      })
    },
    cancelSubmit () {
      this.getAllRowsFilters()
      this.showRowsFiltersTable()
    },
    showRowsFiltersTable () {
      this.display = {
        rowsFiltersTable: true,
        rowsFilterForm: false,
        submitButton: false
      }
    },
    showUpdateOrCreateRowsFiltersForm () {
      this.display = {
        rowsFiltersTable: false,
        rowsFilterForm: true,
        submitButton: true
      }
    },
    showRowsFiltersForm () {
      this.display = {
        rowsFiltersTable: false,
        rowsFilterForm: true,
        submitButton: false
      }
    }
  },
  created () {
    this.getAllRowsFilters()
  }
}
</script>

<style scoped>

</style>

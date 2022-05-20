<template>
  <div>
    <template v-if="display.rowsFiltersTable">
      <span style="margin-right: 5px">同步链路>行过滤>RowsFilter</span>
      <Button type="primary" @click="goToCreateRowsFiltersTable" style="margin-right: 5px">创建</Button>
      <Button @click="returnToMapping" style="margin-right: 5px">返回</Button>
      <br/>
      <br/>
      <Table stripe :columns="columns" :data="rowsFiltersData" border>
        <template slot-scope="{ row, index }" slot="action">
          <Button type="primary" size="small" style="margin-right: 5px" @click="choseRowsFilter(row, index)">选择</Button>
          <Button type="success" size="small" style="margin-right: 5px" @click="showRowsFilter(row, index)">查看</Button>
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
      <FormItem label="手动添加列名" style="width: 720px" >
        <Row>
          <Col span="18">
            <Input v-model="columnForAdd" placeholder="手动添加列"/>
          </Col>
          <Col span="4">
            <Button  type="primary" @click="addColumn" style="margin-left: 50px" >添加</Button>
          </Col>
        </Row>
      </FormItem>
      <FormItem label="规则内容" style="width: 600px">
        <Input type="textarea" v-model="rowsFilterForSubmit.content" style="width: 300px" placeholder="请输入行过滤内容"/>
      </FormItem>
      <FormItem>
        <Button type="primary" @click="submitRowsFilter">提交</Button>
        <Button @click="cancelSubmit" style="margin-left: 50px">返回</Button>
      </FormItem>
    </Form>
  </div>
</template>

<script>
export default {
  name: 'rowsFilter',
  data () {
    return {
      display: {
        rowsFiltersTable: true,
        rowsFilterForm: false
      },
      rowsFiltersData: [],
      columns: [
        {
          title: '序号',
          key: 'id',
          width: 60
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
        mode: null,
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
        'columnA',
        'columnB'
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
    goToCreateRowsFiltersTable () {
      console.log('创建')
      this.rowsFilterForSubmit = {
        id: null,
        name: null,
        mode: null,
        columns: [],
        content: null
      }
      this.display = {
        rowsFiltersTable: false,
        rowsFilterForm: true
      }
    },
    showRowsFilter (row, index) {
      this.columnsForChose = row.columns
      this.rowsFilterForSubmit = {
        id: row.id,
        name: row.name,
        mode: row.mode,
        columns: row.columns,
        content: row.content
      }
      this.display = {
        rowsFiltersTable: false,
        rowsFilterForm: true
      }
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
          this.dataMediasData = response.data.data
          this.getAllRowsFilters()
        }
      })
    },
    cancelSubmit () {
      this.display = {
        rowsFiltersTable: true,
        rowsFilterForm: false
      }
    },
    choseRowsFilter (row, index) {
      this.resetDisplay()
      console.log(row.id)
      console.log(row.name)
      this.$emit('rowsFilterChose', [row.id, row.name])
    },
    returnToMapping () {
      this.resetDisplay()
      this.$emit('closeRowsFilter')
    },
    resetDisplay () {
      this.display = {
        rowsFiltersTable: true,
        rowsFilterForm: false
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

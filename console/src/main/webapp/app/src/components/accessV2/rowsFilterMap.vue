<template>
  <div>
    <template v-if="display.rowsFilterMapsTable">
      <span style="margin-right: 5px">同步链路>行过滤</span>
      <Button  type="primary" @click="goToAddMap" style="margin-right: 5px">添加</Button>
      <Button  @click="returnToApplierGroups" style="margin-right: 5px">返回</Button>
      <br/>
      <br/>
      <Table  stripe :columns="columns" :data="rowsFilterMappingsData" border >
        <template slot-scope="{ row, index }" slot="action">
          <Button type="primary" size="small" style="margin-right: 5px" @click="goToUpdateMapping(row, index)">修改</Button>
          <Button type="error" size="small" style="margin-right: 10px" @click="goToDeleteRule(row, index)">删除</Button>
        </template>
      </Table>
    </template>
    <Form v-if="display.rowsFilterMapForm" ref="mappingForSubmit" :model="mappingForSubmit" :label-width="250" style="margin-top: 50px">
      <FormItem label="逻辑表" style="width: 600px">
        <Row>
          <Col span="18">
            <Input v-model="mappingForSubmit.dataMediaId" readonly placeholder="请选择逻辑表"/>
          </Col>
          <Col span="4">
            <Button type="primary" @click="goToChoseDataMedia" style="margin-left: 50px">选择</Button>
          </Col>
        </Row>
      </FormItem>
      <FormItem label="行过滤规则"  style="width: 600px">
        <Row>
          <Col span="18">
            <Input v-model="mappingForSubmit.rowsFilter.name"  readonly placeholder="请选择行过滤规则,不选默认无行过滤"/>
          </Col>
          <Col span="4">
            <Button type="primary" @click="goToChoseRowsFilter" style="margin-left: 50px">选择</Button>
          </Col>
        </Row>
      </FormItem>
      <FormItem>
        <Button type="primary" @click="submitRowsFilterMappingConfig">提交</Button>
        <Button  @click="cancel" style="margin-left: 50px">返回</Button>
      </FormItem>
    </Form>
    <dataMedia v-if="display.dataMedia" v-bind="propsForDataMedia" v-on:dataMediaChose="dataMediaIdChose" v-on:closeDataMedia="closeDataMedia"></dataMedia>
    <rowsFilter v-if="display.rowsFilter"  v-on:rowsFilterChose="rowsFilterChose" v-on:closeRowsFilter="closeRowsFilter"></rowsFilter>
  </div>
</template>

<script>
import dataMedia from '../accessV2/dataMedia.vue'
import rowsFilter from '../accessV2/rowsFilter.vue'
export default {
  components: {
    dataMedia,
    rowsFilter
  },
  name: 'rowsFilterMap',
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
        rowsFilterMapsTable: true,
        rowsFilterMapForm: false,
        dataMedia: false,
        rowsFilter: false
      },
      rowsFilterMappingsData: [],
      columns: [
        {
          title: '序号',
          key: 'id',
          width: '50'
        },
        {
          title: '数据源',
          width: 200,
          align: 'center',
          render: (h, params) => {
            return h(
              'span',
              this.srcMha
            )
          }
        },
        {
          title: '逻辑库',
          key: 'namespace'
        },
        {
          title: '逻辑表',
          key: 'name'
        },
        {
          title: '行过滤规则',
          key: 'rowsFilterName'
        },
        {
          title: '操作',
          slot: 'action',
          align: 'center'
        }
      ],
      mappingForSubmit: {
        id: null,
        dataMediaId: null,
        rowsFilter: {
          id: null,
          name: null
        }
      },
      propsForDataMedia: {
        srcMha: this.srcMha,
        destMha: this.destMha,
        srcDc: this.srcDc,
        destDc: this.destDc,
        applierGroupId: this.applierGroupId
      }
    }
  },
  methods: {
    getRowsFilterMappings () {
      console.log('getRowsFilterMappings' + '/api/drc/v1/build/RowsFilterMappings/' +
        this.applierGroupId)
      this.axios.get('/api/drc/v1/build/RowsFilterMappings/' +
        this.applierGroupId)
        .then(response => {
          if (response.data.status === 1) {
            window.alert('查询dataMedias 失败')
          } else {
            console.log(response.data.data)
            this.rowsFilterMappingsData = response.data.data
          }
        })
    },
    goToAddMap () {
      this.mappingForSubmit = {
        id: null,
        dataMediaId: null,
        rowsFilter: {
          id: null,
          name: null
        }
      }
      this.display = {
        rowsFilterMapsTable: false,
        rowsFilterMapForm: true,
        dataMedia: false,
        rowsFilter: false
      }
    },
    goToUpdateMapping (row, index) {
      console.log(row)
      this.mappingForSubmit = {
        id: row.id,
        dataMediaId: row.dataMediaId,
        rowsFilter: {
          id: row.rowsFilterId,
          name: row.rowsFilterName
        }
      }
      this.display = {
        rowsFilterMapsTable: false,
        rowsFilterMapForm: true,
        dataMedia: false,
        rowsFilter: false
      }
    },
    goToDeleteRule (row, index) {
      this.axios.post('/api/drc/v1/meta/rowsFilterMapping/delete', {
        id: row.id,
        applierGroupId: this.applierGroupId,
        dataMediaId: row.dataMediaId,
        rowsFilterId: row.rowsFilterId
      }).then(response => {
        console.log(response.data)
        console.log(response.data.data)
        if (response.data.status === 0) {
          alert('删除成功！')
          this.getRowsFilterMappings()
        } else {
          alert('操作失败！')
        }
      })
    },
    submitRowsFilterMappingConfig () {
      this.axios.post('/api/drc/v1/meta/rowsFilterMapping', {
        id: this.mappingForSubmit.id,
        applierGroupId: this.applierGroupId,
        dataMediaId: this.mappingForSubmit.dataMediaId,
        rowsFilterId: this.mappingForSubmit.rowsFilter.id
      }).then(response => {
        if (response.data.status === 1) {
          window.alert('失败！' + response.data.data)
        } else {
          this.getRowsFilterMappings()
          window.alert('成功！' + response.data.data)
        }
      })
    },
    cancel () {
      this.display = {
        rowsFilterMapsTable: true,
        rowsFilterMapForm: false,
        dataMedia: false,
        rowsFilter: false
      }
    },
    goToChoseDataMedia () {
      this.display = {
        rowsFilterMapsTable: false,
        rowsFilterMapForm: false,
        dataMedia: true,
        rowsFilter: false
      }
    },
    dataMediaIdChose (e) {
      this.display = {
        rowsFilterMapsTable: false,
        rowsFilterMapForm: true,
        dataMedia: false,
        rowsFilter: false
      }
      this.mappingForSubmit.dataMediaId = e
    },
    closeDataMedia () {
      this.display = {
        rowsFilterMapsTable: false,
        rowsFilterMapForm: true,
        dataMedia: false,
        rowsFilter: false
      }
    },
    goToChoseRowsFilter () {
      if (this.mappingForSubmit.dataMediaId === null) {
        window.alert('请先选择逻辑表！')
        return
      }
      this.display = {
        rowsFilterMapsTable: false,
        rowsFilterMapForm: false,
        dataMedia: false,
        rowsFilter: true
      }
    },
    rowsFilterChose (params) {
      this.display = {
        rowsFilterMapsTable: false,
        rowsFilterMapForm: true,
        dataMedia: false,
        rowsFilter: false
      }
      console.log('map' + params)
      this.mappingForSubmit.rowsFilter.id = params[0]
      this.mappingForSubmit.rowsFilter.name = params[1]
    },
    closeRowsFilter () {
      this.display = {
        rowsFilterMapsTable: false,
        rowsFilterMapForm: true,
        dataMedia: false,
        rowsFilter: false
      }
    },
    returnToApplierGroups () {
      this.$emit('closeRowsFilterMap')
    }
  },
  created () {
    this.getRowsFilterMappings()
  }
}
</script>

<style scoped>

</style>

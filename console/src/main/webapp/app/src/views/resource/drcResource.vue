<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/drcResource">DRC资源管理</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div :style="{padding: '1px 1px',height: '100%'}">
        <Row>
            <i-col span="12">
                <Form ref="drcResource" :model="drcResource" :rules="ruleDrcResource" :label-width="250" style="float: left; margin-top: 50px">
                    <FormItem label="类型" prop="type">
                        <Select v-model="drcResource.type" style="width: 200px" placeholder="请选择资源类型">
                        <Option v-for="item in drcResource.typeList" :value="item.value" :key="item.value">{{ item.label }}</Option>
                        </Select>
                    </FormItem>
                    <FormItem label="dc"  prop="dc">
                        <Select v-model="drcResource.dc" filterable allow-create style="width: 200px" placeholder="请选择资源所在机房" @on-create="handleCreateDc">
                        <Option v-for="item in drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
                        </Select>
                    </FormItem>
                    <FormItem label="ip"  prop="ip" style="width: 450px">
                        <Input v-model="drcResource.ip" placeholder="请输入资源ip"/>
                    </FormItem>
                    <FormItem>
                      <Button @click="handleReset()">重置</Button><br><br>
                      <Button type="primary" @click="reviewInputResource ()">录入</Button>
                    </FormItem>
                    <Modal
                        v-model="drcResource.reviewModal"
                        title="录入确认"
                        @on-ok="inputResource">
                        确认录入资源{{ this.drcResource.ip }}吗？所在机房：{{ this.drcResource.dc }}，类型：{{ this.drcResource.type }}
                    </Modal>
                    <Modal
                        v-model="drcResource.resultModal"
                        title="录入结果">
                        {{ this.result }}
                    </Modal>
                </Form>
            </i-col>
        </Row>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'drcResource',
  data () {
    return {
      status: '',
      title: '',
      message: '',
      hasResp: false,
      drcResource: {
        reviewModal: false,
        ip: '',
        type: '',
        dc: '',
        typeList: [
          {
            value: 'R',
            label: 'Replicator'
          },
          {
            value: 'A',
            label: 'Applier'
          }
        ]
      },
      ruleDrcResource: {
        ip: [
          { required: true, message: 'ip不能为空', trigger: 'blur' }
        ],
        type: [
          { required: true, message: 'ip不能为空', trigger: 'blur' }
        ],
        dc: [
          { required: true, message: 'dc不能为空', trigger: 'blur' }
        ]
      },
      drcZoneList: [
        {
          value: 'shaoy',
          label: '上海欧阳'
        },
        {
          value: 'shaxy',
          label: '上海新源'
        },
        {
          value: 'sharb',
          label: '上海日版'
        },
        {
          value: 'shajq',
          label: '上海金桥'
        },
        {
          value: 'shafq',
          label: '上海福泉'
        },
        {
          value: 'shajz',
          label: '上海金钟'
        },
        {
          value: 'ntgxh',
          label: '南通星湖大道'
        },
        {
          value: 'shali',
          label: '上海阿里'
        },
        {
          value: 'fraaws',
          label: '法兰克福AWS'
        },
        {
          value: 'sinibuaws',
          label: 'IBU-VPC'
        },
        {
          value: 'sinibualiyun',
          label: 'IBU-VPC(aliyun)'
        },
        {
          value: 'sinaws',
          label: '新加坡AWS'
        }
      ],
      result: ''
    }
  },
  methods: {
    reviewInputResource () {
      console.log('review input ' + this.drcResource.type + '(' + this.drcResource.ip + ') in ' + this.drcResource.dc)
      this.drcResource.reviewModal = true
    },
    inputResource () {
      const that = this
      const uri = '/api/drc/v1/meta/resource/ips/' + this.drcResource.ip + '/dcs/' + this.drcResource.dc + '/descriptions/' + this.drcResource.type
      console.log('do input ' + this.drcResource.type + '(' + this.drcResource.ip + ') in ' + this.drcResource.dc)
      console.log(uri)
      this.axios.post(uri)
        .then(res => {
          console.log('show result')
          console.log(res.data)
          that.result = res.data.data
          that.drcResource.reviewModal = false
          that.drcResource.resultModal = true
        })
    },
    handleCreateDc (val) {
      console.log('customize add dc: ' + val)
      this.drcZoneList.push({
        value: val,
        label: val
      })
      console.log(this.drcZoneList)
    },
    handleReset () {
      console.log('reset input request type: ' + this.drcResource.type + ', ip: ' + this.drcResource.ip + ', dc: ' + this.drcResource.dc + '.')
      this.drcResource.ip = ''
      this.drcResource.type = ''
      this.drcResource.dc = ''
      this.result = ''
      console.log('reset input request type result: ' + this.drcResource.type + ', ip: ' + this.drcResource.ip + ', dc: ' + this.drcResource.dc + '.')
    }
  },
  created () {
  }
}
</script>

<style scoped>

</style>

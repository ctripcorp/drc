<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/proxyResource">Proxy资源管理</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div :style="{padding: '1px 1px',height: '100%'}">
        <Row>
          <i-col span="12">
            <Form ref="proxyResource" :model="proxyResource" :rules="ruleproxyResource" :label-width="250" style="float: left; margin-top: 50px">
              <FormItem label="机房"  prop="dc">
                <Select v-model="proxyResource.dc" filterable allow-create style="width: 200px" placeholder="请选择资源所在机房" @on-create="handleCreateDc">
                  <Option v-for="item in drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
                </Select>
              </FormItem>
              <FormItem label="协议" prop="protocol" style="width: 600px">
                <Select v-model="proxyResource.protocol" filterable allow-create style="width: 200px" placeholder="请选择或输入Proxy协议" @on-create="handleCreateProtocol">
                  <Option v-for="item in protocolList" :value="item" :key="item">{{ item }}</Option>
                </Select>
              </FormItem>
              <FormItem label="IP"  prop="ip" style="width: 450px">
                <Input v-model="proxyResource.ip" placeholder="请输入Proxy IP"/>
              </FormItem>
              <FormItem label="端口" prop="port" style="width: 600px">
                <Select v-model="proxyResource.port" filterable allow-create style="width: 200px" placeholder="请选择或输入Proxy端口" @on-create="handleCreatePort">
                  <Option v-for="item in portList" :value="item" :key="item">{{ item }}</Option>
                </Select>
              </FormItem>
              <FormItem>
                <Button @click="handleReset()">重置</Button><br><br>
                <Button type="primary" @click="reviewInputResource ()">录入</Button>
              </FormItem>
              <Modal
                v-model="proxyResource.reviewModal"
                title="录入确认"
                @on-ok="inputResource">
                确认将资源{{ this.proxyResource.protocol }}://{{ this.proxyResource.ip }}:{{ this.proxyResource.port }}录入{{ this.proxyResource.dc }}吗？
              </Modal>
              <Modal
                v-model="proxyResource.resultModal"
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
  name: 'proxyResource',
  data () {
    return {
      proxyResource: {
        dc: '',
        protocol: '',
        ip: '',
        port: '',
        reviewModal: false
      },
      ruleproxyResource: {
        dc: [
          { required: true, message: '机房不能为空', trigger: 'blur' }
        ],
        protocol: [
          { required: true, message: '协议不能为空', trigger: 'blur' }
        ],
        ip: [
          { required: true, message: 'IP不能为空', trigger: 'blur' }
        ],
        port: [
          { required: true, message: '端口不能为空', trigger: 'blur' }
        ]

      },
      protocolList: ['PROXY', 'PROXYTLS'],
      portList: ['80', '443'],
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
          value: 'fraaws',
          label: '法兰克福AWS'
        },
        {
          value: 'shali',
          label: '上海阿里'
        },
        {
          value: 'sinibuaws',
          label: 'IBU-VPC'
        }
      ],
      result: ''
    }
  },
  methods: {
    reviewInputResource () {
      console.log('review input: add Proxy %s://%s:%s in %s', this.proxyResource.protocol, this.proxyResource.ip, this.proxyResource.port, this.proxyResource.dc)
      this.proxyResource.reviewModal = true
    },
    inputResource () {
      const that = this
      console.log('do input: dc: %s, protocol: %s, ip: %s, port: %s', this.proxyResource.dc, this.proxyResource.protocol, this.proxyResource.ip, this.proxyResource.port)
      this.axios.post('/api/drc/v1/meta/proxy', {
        dc: this.proxyResource.dc,
        protocol: this.proxyResource.protocol,
        ip: this.proxyResource.ip,
        port: this.proxyResource.port
      }).then(response => {
        console.log('result: %s', response.data)
        that.result = response.data.data.data
        that.proxyResource.reviewModal = false
        that.proxyResource.resultModal = true
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
    handleCreateProtocol (val) {
      console.log('customize add protocol: ' + val)
      this.protocolList.push(val)
      console.log(this.protocolList)
    },
    handleCreatePort (val) {
      console.log('customize add port: ' + val)
      this.portList.push(val)
      console.log(this.portList)
    },
    handleReset () {
      console.log('reset input: add Proxy %s://%s:%s in %s', this.proxyResource.protocol, this.proxyResource.ip, this.proxyResource.port, this.proxyResource.dc)
      this.proxyResource.dc = ''
      this.proxyResource.protocol = ''
      this.proxyResource.ip = ''
      this.proxyResource.port = ''
      this.result = ''
      console.log('reset input result: add Proxy (%s)://(%s):(%s) in (%s)', this.proxyResource.protocol, this.proxyResource.ip, this.proxyResource.port, this.proxyResource.dc)
    }
  },
  created () {
  }
}
</script>

<style scoped>

</style>

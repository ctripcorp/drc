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
              <FormItem label="IP"  prop="ip" style="width: 450px">
                <Input v-model="proxyResource.ip" placeholder="请输入Proxy IP"/>
              </FormItem>
              <FormItem>
                <Button type="primary" @click="inputResource ()">录入</Button>
              </FormItem>
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
        ip: ''
      },
      ruleproxyResource: {
        dc: [
          { required: true, message: '机房不能为空', trigger: 'blur' }
        ],
        ip: [
          { required: true, message: 'IP不能为空', trigger: 'blur' }
        ]

      },
      drcZoneList: this.constant.dcList,
      result: ''
    }
  },
  methods: {
    inputResource () {
      const that = this
      console.log('do input: dc: %s, protocol: %s, ip: %s, port: %s', this.proxyResource.dc, this.proxyResource.protocol, this.proxyResource.ip, this.proxyResource.port)
      this.axios.post('/api/drc/v2/meta/proxy?dc=' + this.proxyResource.dc + '&ip=' + this.proxyResource.ip).then(response => {
        console.log('result: %s', response.data)
        that.result = response.data.data
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
    getBus () {
      this.axios.get('/api/drc/v2/meta/bus/all')
        .then(response => {
          this.bus = response.data.data
        })
    }
  },
  created () {
    this.axios.get('/api/drc/v2/permission/resource/proxy').then((response) => {
      if (response.data.status === 403) {
        this.$router.push('/nopermission')
      }
    })
  }
}
</script>

<style scoped>

</style>

<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/proxyRouteManagement">录入路由规则</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Row>
          <i-col span="12">
            <Form ref="route" :model="route" :rules="ruleRoute" :label-width="250"
                  style="float: left; margin-top: 50px">
              <FormItem label="业务部门">
                <Select filterable :disabled="updateStatus==='true'" clearable v-model="route.routeOrgName" placeholder="请输入路由BU,不填默认为公用路由">
                  <Option v-for="item in bus" :value="item.buName" :key="item.buName">{{ item.buName }}</Option>
                </Select>
              </FormItem>
              <FormItem label="源端（Applier）机房" prop="srcDcName">
                <Select :disabled="updateStatus==='true'" v-model="route.srcDcName" filterable allow-create
                        style="width: 250px" @on-select="getProxyUrisInSrc" placeholder="请选择源端（Applier）机房"
                        @on-create="handleCreateDc">
                  <Option v-for="item in drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
                </Select>
              </FormItem>
              <FormItem label="源端（Applier）Proxy" prop="srcProxyUris">
                <Select v-model="route.proxyUris.src" filterable multiple style="width: 250px"
                        placeholder="请选择源端Proxy">
                  <Option v-for="item in route.proxyUriList.src" :value="item" :key="item">{{ item }}</Option>
                </Select>
              </FormItem>
              <FormItem label="中继Proxy" prop="relayProxyUris">
                <Select v-model="route.proxyUris.relay" filterable multiple style="width: 250px"
                        placeholder="请选择中继Proxy">
                  <Option v-for="item in route.proxyUriList.all" :value="item" :key="item">{{ item }}</Option>
                </Select>
              </FormItem>
              <FormItem label="目标（Replicator）机房" prop="dstDcName">
                <Select :disabled="updateStatus==='true'" v-model="route.dstDcName" filterable allow-create
                        style="width: 250px" @on-select="getProxyUrisInDst" placeholder="请选择目标（Replicator）机房"
                        @on-create="handleCreateDc">
                  <Option v-for="item in drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
                </Select>
              </FormItem>
              <FormItem label="目标（Replicator）Proxy" prop="dstProxyUris">
                <Select v-model="route.proxyUris.dst" filterable multiple style="width: 250px"
                        placeholder="请选择目标Proxy">
                  <Option v-for="item in route.proxyUriList.dst" :value="item" :key="item">{{ item }}</Option>
                </Select>
              </FormItem>
              <FormItem label="Tag" prop="tag" style="width: 500px">
                <Select filterable clearable :disabled="updateStatus==='true'" v-model="route.tag" placeholder="请输入路由tag">
                  <Option v-for="item in tags" :value="item" :key="item">{{ item }}</Option>
                </Select>
              </FormItem>
              <FormItem>
                <Button @click="handleReset()">重置</Button>
                <br><br>
                <Button type="primary" @click="reviewInputResource ()">录入</Button>
              </FormItem>
              <Modal
                v-model="route.reviewModal"
                title="录入确认"
                @on-ok="inputResource()">
                对BU[{{ this.route.routeOrgName }}]、tag[{{ this.route.tag }}]、方向[{{
                  this.route.srcDcName
                }}->{{ this.route.dstDcName }}]，确认新增/修改路由为源端Proxies：{{
                  this.route.proxyUris.src
                }}，中继Proxies：{{ this.route.proxyUris.relay }}，目标Proxies：{{ this.route.proxyUris.dst }}吗？
              </Modal>
              <Modal
                v-model="route.resultModal"
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
  name: 'proxyRouteManagement',
  data () {
    return {
      tags: this.constant.routeTags,
      updateStatus: this.$route.query.updateStatus,
      bus: [],
      route: {
        reviewModal: false,
        resultModal: false,
        routeId: this.$route.query.routeId,
        routeOrgName: '',
        srcDcName: '',
        dstDcName: '',
        tag: '',
        proxyUris: {
          src: [],
          relay: [],
          dst: []
        },
        proxyUriList: {
          src: [],
          all: [],
          dst: []
        }
      },
      ruleRoute: {
        srcDcName: [
          { required: true, message: '源端（Applier）机房不能为空', trigger: 'blur' }
        ],
        dstDcName: [
          { required: true, message: '目标（Replicator）机房不能为空', trigger: 'blur' }
        ],
        srcProxyUris: [
          { required: true, message: '源端Proxy不能为空', trigger: 'blur' }
        ],
        dstProxyUris: [
          { required: true, message: '目标Proxy不能为空', trigger: 'blur' }
        ],
        tag: [
          { required: true, message: 'tag不能为空', trigger: 'blur' }
        ]
      },
      drcZoneList: this.constant.dcList,
      result: ''
    }
  },
  methods: {
    reviewInputResource () {
      this.route.reviewModal = true
    },
    inputResource () {
      const that = this
      this.axios.post('/api/drc/v2/meta/route', {
        id: this.route.routeId,
        routeOrgName: this.route.routeOrgName,
        srcDcName: this.route.srcDcName,
        dstDcName: this.route.dstDcName,
        srcProxyUris: this.route.proxyUris.src,
        relayProxyUris: this.route.proxyUris.relay,
        dstProxyUris: this.route.proxyUris.dst,
        tag: this.route.tag
      }).then(response => {
        console.log(response.data)
        that.result = response.data.data
        that.route.reviewModal = false
        that.route.resultModal = true
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
      alert(this.updateStatus)
      this.route.routeOrgName = ''
      this.route.srcDcName = ''
      this.route.dstDcName = ''
      this.route.tag = ''
      this.route.proxyUris.src = []
      this.route.proxyUris.relay = []
      this.route.proxyUris.dst = []
    },
    getProxyUrisInSrc (event) {
      const that = this
      const uri = '/api/drc/v2/meta/proxy/uris?dc=' + event.value + '&src=true'
      console.log('uri: ' + uri)
      this.axios.get(uri)
        .then(response => {
          console.log(response.data)
          that.route.proxyUriList.src = []
          response.data.data.forEach(proxyUri => that.route.proxyUriList.src.push(proxyUri))
        })
    },
    getProxyUrisInDst (event) {
      const that = this
      const uri = '/api/drc/v2/meta/proxy/uris?dc=' + event.value + '&src=false'
      console.log('uri: ' + uri)
      this.axios.get(uri)
        .then(response => {
          console.log(response.data)
          that.route.proxyUriList.dst = []
          response.data.data.forEach(proxyUri => that.route.proxyUriList.dst.push(proxyUri))
        })
    },
    getProxyUrisInSrc2 () {
      const that = this
      const uri = '/api/drc/v2/meta/proxy/uris?dc=' + this.route.srcDcName + '&src=true'
      console.log('uri: ' + uri)
      this.axios.get(uri)
        .then(response => {
          console.log(response.data)
          that.route.proxyUriList.src = []
          response.data.data.forEach(proxyUri => that.route.proxyUriList.src.push(proxyUri))
        })
    },
    getProxyUrisInDst2 () {
      const that = this
      const uri = '/api/drc/v2/meta/proxy/uris?dc=' + this.route.dstDcName + '&src=false'
      console.log('uri: ' + uri)
      this.axios.get(uri)
        .then(response => {
          console.log(response.data)
          that.route.proxyUriList.dst = []
          response.data.data.forEach(proxyUri => that.route.proxyUriList.dst.push(proxyUri))
        })
    },
    getAllProxyUris () {
      const that = this
      const uri = '/api/drc/v2/meta/proxy/uris/relay'
      console.log('uri: ' + uri)
      this.axios.get(uri)
        .then(response => {
          console.log(response.data)
          that.route.proxyUriList.all = []
          response.data.data.forEach(proxyUri => that.route.proxyUriList.all.push(proxyUri))
        })
    },
    getProxyUrisInUse () {
      const uri = '/api/drc/v2/meta/route?routeId=' + this.route.routeId
      console.log('uri: ' + uri)
      this.axios.get(uri)
        .then(response => {
          console.log(response)
          this.route.proxyUris.src = []
          this.route.proxyUris.relay = []
          this.route.proxyUris.dst = []
          const data = response.data.data
          if (data !== null) {
            data.srcProxyUris.forEach(proxyUri => this.route.proxyUris.src.push(proxyUri))
            data.relayProxyUris.forEach(proxyUri => this.route.proxyUris.relay.push(proxyUri))
            data.dstProxyUris.forEach(proxyUri => this.route.proxyUris.dst.push(proxyUri))
            this.route.routeOrgName = data.routeOrgName
            this.route.srcDcName = data.srcDcName
            this.route.dstDcName = data.dstDcName
            this.route.tag = data.tag
            this.getProxyUrisInSrc2()
            this.getProxyUrisInDst2()
          }
        })
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
        return
      }
      this.getProxyUrisInUse()
      this.getAllProxyUris()
      this.getBus()
    })
  }
}
</script>

<style scoped>

</style>

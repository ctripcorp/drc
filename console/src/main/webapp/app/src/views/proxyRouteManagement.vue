<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/proxyRouteManagement">路由管理</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Row>
            <i-col span="12">
                <Form ref="route" :model="route" :rules="ruleRoute" :label-width="250" style="float: left; margin-top: 50px">
                    <FormItem label="BU"  prop="routeOrgName" style="width: 500px">
                        <Input :disabled="updateStatus" v-model="route.routeOrgName" placeholder="请输入路由BU,不填默认为公用路由"/>
                    </FormItem>
                    <FormItem label="源端机房"  prop="srcDcName">
                        <Select :disabled="updateStatus" v-model="route.srcDcName" filterable allow-create style="width: 250px" @on-select="getProxyUrisInSrc" placeholder="请选择源端机房" @on-create="handleCreateDc">
                        <Option v-for="item in drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
                        </Select>
                    </FormItem>
                    <FormItem label="目标机房"  prop="dstDcName">
                        <Select :disabled="updateStatus" v-model="route.dstDcName" filterable allow-create style="width: 250px" @on-select="getProxyUrisInDst" placeholder="请选择目标机房" @on-create="handleCreateDc">
                        <Option v-for="item in drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
                        </Select>
                    </FormItem>
                    <FormItem label="源端Proxy" prop="srcProxyUris">
                        <Select v-model="route.proxyUris.src" filterable multiple style="width: 250px" placeholder="请选择源端Proxy">
                        <Option v-for="item in route.proxyUriList.src" :value="item" :key="item">{{ item }}</Option>
                        </Select>
                    </FormItem>
                    <FormItem label="中继Proxy" prop="relayProxyUris">
                      <Select v-model="route.proxyUris.relay" filterable multiple style="width: 250px" placeholder="请选择中继Proxy">
                        <Option v-for="item in route.proxyUriList.all" :value="item" :key="item">{{ item }}</Option>
                      </Select>
                    </FormItem>
                    <FormItem label="目标Proxy" prop="dstProxyUris">
                        <Select  v-model="route.proxyUris.dst" filterable multiple style="width: 250px" placeholder="请选择目标Proxy">
                        <Option v-for="item in route.proxyUriList.dst" :value="item" :key="item">{{ item }}</Option>
                        </Select>
                    </FormItem>
                    <FormItem label="Tag"  prop="tag" style="width: 500px">
                        <Input :disabled="updateStatus" v-model="route.tag" placeholder="请输入路由tag"/>
                    </FormItem>
                    <FormItem>
                      <Button @click="handleReset()">重置</Button><br><br>
                      <Button type="primary" @click="reviewInputResource ()">录入</Button>
                    </FormItem>
                    <Modal
                        v-model="route.reviewModal"
                        title="录入确认"
                        @on-ok="inputResource()">
                        对BU[{{ this.route.routeOrgName }}]、tag[{{ this.route.tag }}]、方向[{{ this.route.srcDcName }}->{{ this.route.dstDcName }}]，确认新增/修改路由为源端Proxies：{{ this.route.proxyUris.src }}，中继Proxies：{{ this.route.proxyUris.relay }}，目标Proxies：{{ this.route.proxyUris.dst }}吗？
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
      updateStatus: this.$route.query.updateStatus,
      route: {
        reviewModal: false,
        resultModal: false,
        routeOrgName: this.$route.query.routeOrgName,
        srcDcName: this.$route.query.srcDcName,
        dstDcName: this.$route.query.dstDcName,
        tag: this.$route.query.tag,
        proxyUris: {
          src: this.$route.query.srcProxyUris,
          relay: this.$route.query.relayProxyUris,
          dst: this.$route.query.dstProxyUris
        },
        proxyUriList: {
          src: [],
          all: [],
          dst: []
        }
      },
      ruleRoute: {
        srcDcName: [
          { required: true, message: '源端机房不能为空', trigger: 'blur' }
        ],
        dstDcName: [
          { required: true, message: '目标机房不能为空', trigger: 'blur' }
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
      this.axios.post('/api/drc/v1/meta/routes', {
        id: 0,
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
      const uri = '/api/drc/v1/meta/proxy/uris/dcs/' + event.value
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
      const uri = '/api/drc/v1/meta/proxy/uris/dcs/' + event.value
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
      const uri = '/api/drc/v1/meta/proxy/uris/dcs/' + this.route.srcDcName
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
      const uri = '/api/drc/v1/meta/proxy/uris/dcs/' + this.route.dstDcName
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
      const uri = '/api/drc/v1/meta/proxy/uris'
      console.log('uri: ' + uri)
      this.axios.get(uri)
        .then(response => {
          console.log(response.data)
          that.route.proxyUriList.all = []
          response.data.data.forEach(proxyUri => that.route.proxyUriList.all.push(proxyUri))
        })
    },
    getProxyUrisInUse () {
      const that = this
      const uri = '/api/drc/v1/meta/routes' +
        '?routeOrgName=' + this.route.routeOrgName +
        '&srcDcName=' + this.route.srcDcName +
        '&dstDcName=' + this.route.dstDcName +
        '&tag=' + this.route.tag +
        '&deleted=' + 0
      console.log('uri: ' + uri)
      this.axios.get(uri)
        .then(response => {
          console.log(response)
          that.route.proxyUris.src = []
          that.route.proxyUris.relay = []
          that.route.proxyUris.dst = []
          if (response.data.data.length !== 0) {
            // there will be at most one route for routeOrgName, srcDcName, dstDcName, tag given
            response.data.data[0].srcProxyUris.forEach(proxyUri => that.route.proxyUris.src.push(proxyUri))
            response.data.data[0].relayProxyUris.forEach(proxyUri => that.route.proxyUris.relay.push(proxyUri))
            response.data.data[0].dstProxyUris.forEach(proxyUri => that.route.proxyUris.dst.push(proxyUri))
          }
        })
    }
  },
  created () {
    this.getProxyUrisInSrc2()
    this.getAllProxyUris()
    this.getProxyUrisInDst2()
    this.getProxyUrisInUse()
  }
}
</script>

<style scoped>

</style>

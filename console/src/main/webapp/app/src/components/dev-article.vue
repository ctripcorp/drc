<template>
  <Layout>
    <Header class="header">
      <Row>
        <i-col span="4" offset="1">
          <img src="../assets/ctrip.png" alt="logo" class="logo">
        </i-col>
        <i-col span="14">
          <Menu mode="horizontal" :active-name="activeName">
            <MenuItem name="/home" to="/home">DRC</MenuItem>
            <MenuItem name="/apply" to="/apply">用户接入</MenuItem>
            <MenuItem name="/monitor" to="/monitor">监控告警</MenuItem>
            <MenuItem name="/manage" to="/manage">审批管理</MenuItem>
          </Menu>
        </i-col>
        <i-col span="4">
          <Row>
            <i-col span="12">
              <Dropdown>
                <a href="javascript:void(0)">
                  帮助
                  <Icon type="ios-arrow-down"></Icon>
                </a>
                <DropdownMenu slot="list">
                  <DropdownItem>帮助1</DropdownItem>
                  <DropdownItem>帮助2</DropdownItem>
                </DropdownMenu>
              </Dropdown>
            </i-col>
            <i-col span="12">
              <Dropdown>
                <a href="javascript:void(0)">
                  {{ userName }}
                  <Icon type="ios-arrow-down"></Icon>
                </a>
                <DropdownMenu slot="list">
                  <DropdownItem>注销</DropdownItem>
                </DropdownMenu>
              </Dropdown>
            </i-col>
          </Row>
        </i-col>
      </Row>
    </Header>
    <Layout>
      <Sider class="sider" width="240" collapsible v-model="isCollapsed" :class="{ 'sider-hide': isCollapsed }">
        <Menu class="sider-menu" theme="dark" active-name="option2">
          <MenuItem name="option1">
            <Icon type="ios-search"></Icon>
            <span>Option 1</span>
          </MenuItem>
          <MenuItem name="option2">
            <Icon type="ios-apps"></Icon>
            <span>Option 2</span>
          </MenuItem>
          <MenuItem name="option3">
            <Icon type="ios-bookmark"></Icon>
            <span>Option 3</span>
          </MenuItem>
        </Menu>
      </Sider>
      <Content class="content" :class="{ 'content-expand': isCollapsed }">
        <Card style="margin-top: 16px">
          <slot></slot>
        </Card>
      </Content>
    </Layout>
  </Layout>
</template>

<script>

export default {
  data () {
    return {
      activeName: this.$route.path,
      isCollapsed: false,
      userName: ''
    }
  },
  created () {
    this.activeName = this.$route.path
  }
}
</script>

<style>
  .header{
    width: 100%;
    height: 60px;
    background: #fff;
    box-shadow: 0 1px 1px rgba(0, 0, 0,.05);
    position: fixed;
    top: 0;
    left: 0;
    z-index: 2;
  }
  .logo{
    height: 50px;
    margin-top: 5px;
  }
  .ivu-menu-horizontal.ivu-menu-light:after{
    display: none;
  }
  .sider{
    position: fixed;
    height: 100%;
    left: 0;
    overflow: auto;
    z-index: 1;
  }
  .sider-menu{
    margin-top: 60px;
  }
  .sider-hide .ivu-menu-item span{
    display: none;
  }
  .content{
    margin-left: 240px;
    margin-top: 60px;
    padding: 16px;
    transition: all .2s ease-in-out;
  }
  .content-expand{
    margin-left: 64px;
  }
</style>

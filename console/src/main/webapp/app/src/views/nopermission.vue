<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/nopermission">权限申请</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <Alert type="error" show-icon>
        无操作权限，请申请！
        <template #desc>
          <a href="https://iam.basebiz.ctripcorp.com/user/myPermission">点击前往IAM系统申请DRC权限！</a>
        </template>
      </Alert>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'nopermission'
}
</script>

<style scoped>

</style>

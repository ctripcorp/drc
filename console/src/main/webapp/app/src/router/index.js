import Vue from 'vue'
import VueRouter from 'vue-router'

Vue.use(VueRouter)

const routes = [
  {
    path: '/home',
    component: () => import('../views/home.vue')
  },
  {
    path: '/monitor',
    component: () => import('../views/monitor.vue')
  },
  {
    path: '/proxyRouteCluster',
    component: () => import('../views/v2/resource/proxyRouteCluster.vue')
  },
  {
    path: '/proxyRouteManagement',
    component: () => import('../views/v2/resource/proxyRouteManagement.vue')
  },
  {
    path: '/manage',
    component: () => import('../views/manage.vue')
  },
  {
    path: '/drcResource',
    name: 'drcResource',
    component: () => import('../views/resource/drcResource.vue')
  },
  {
    path: '/proxyResource',
    name: 'proxyResource',
    component: () => import('../views/v2/resource/proxyResource.vue')
  },
  {
    path: '/conflict',
    name: 'conflict',
    component: () => import('../views/handleConflict.vue')
  },
  {
    path: '/incrementDataConsistencyCheck',
    name: 'incrementDataConsistencyCheck',
    component: () => import('../views/monitor/incrementDataConsistencyCheck.vue')
  },
  {
    path: '/fullDataConsistencyCheckTest',
    name: 'fullDataConsistencyCheckTest',
    component: () => import('../views/monitor/fullDataConsistencyCheckForTest.vue')
  },
  {
    path: '/fullDataConsistencyResultTest',
    name: 'fullDataConsistencyResultTest',
    component: () => import('../views/monitor/fullDataConsistencyResultForTest.vue')
  },
  {
    path: '/fullDataConsistencyCheck',
    name: 'fullDataConsistencyCheck',
    component: () => import('../views/monitor/fullDataConsistencyCheck.vue')
  },
  {
    path: '/fullDataConsistencyCluster',
    name: 'fullDataConsistencyCluster',
    component: () => import('../views/monitor/fullDataConsistencyCluster.vue')
  },
  {
    path: '/fullDataConsistencyResult',
    name: 'fullDataConsistencyResult',
    component: () => import('../views/monitor/fullDataConsistencyResult.vue')
  },
  {
    path: '/incrementDataConsistencyCluster',
    name: 'incrementDataConsistencyCluster',
    component: () => import('../views/monitor/incrementDataConsistencyCluster.vue')
  },
  {
    path: '/incrementDataConsistencyResult',
    name: 'incrementDataConsistencyResult',
    component: () => import('../views/monitor/incrementDataConsistencyResult.vue')
  },
  {
    path: '/incrementDataConsistencyHandle',
    name: 'incrementDataConsistencyHandle',
    component: () => import('../views/monitor/incrementDataConsistencyHandle.vue')
  },
  {
    path: '/unitRouteVerificationCluster',
    name: 'unitRouteVerificationCluster',
    component: () => import('../views/monitor/unitRouteVerificationCluster.vue')
  },
  {
    path: '/unitRouteVerificationResult',
    name: 'unitRouteVerificationResult',
    component: () => import('../views/monitor/unitRouteVerificationResult.vue')
  },
  {
    path: '/rowsFilterConfigs',
    name: 'rowsFilterConfigs',
    component: () => import('../views/drcConfig/rowsFilterConfigs.vue')
  },
  {
    path: '/buildMetaMessage',
    name: 'buildMetaMessage',
    component: () => import('../views/filter/buildMetaMessage.vue')
  },
  {
    path: '/metaMessage',
    name: 'metaMessage',
    component: () => import('../views/filter/metaMessage.vue')
  },
  {
    path: '/buildMetaMapping',
    name: 'buildMetaMapping',
    component: () => import('../views/filter/buildMetaMapping.vue')
  },
  {
    path: '/metaMapping',
    name: 'metaMapping',
    component: () => import('../views/filter/metaMapping.vue')
  },
  {
    path: '/mqConfigs',
    name: 'mqConfigs',
    component: () => import('../views/drcConfig/mqConfigs.vue')
  },
  // v2 new model
  {
    path: '/v2/mhaReplications',
    component: () => import('../views/v2/meta/mhaReplications.vue')
  },
  {
    path: '/v2/mhaDbReplications',
    component: () => import('../views/v2/meta/mhaDbReplications.vue')
  },
  {
    path: '/v2/mhaReplicationDetails',
    component: () => import('../views/v2/meta/mhaReplicationDetails.vue')
  },
  {
    path: '/v2/messengersV2',
    component: () => import('../views/v2/meta/messengersV2.vue')
  },
  {
    path: '/v2/buildMessengerV2',
    component: () => import('../views/v2/meta/buildMessengerV2.vue')
  },
  {
    path: '/v2/mhaBuild',
    name: 'mhaBuild',
    component: () => import('../views/v2/meta/buildStep/mhaBuild.vue')
  },
  {
    path: '/v2/migration',
    component: () => import('../views/v2/ops/migration.vue')
  },
  {
    path: '/v2/operationlog',
    component: () => import('../views/v2/ops/operationlog.vue')
  },
  {
    path: '/drcV2',
    name: 'drcV2',
    component: () => import('../views/v2/meta/drcV2.vue')
  },
  {
    path: '/dbTables',
    name: 'dbTables',
    component: () => import('../views/v2/meta/buildStep/dbTables.vue')
  },
  {
    path: '/dbAppliers',
    name: 'dbAppliers',
    component: () => import('../views/v2/meta/buildStep/dbAppliers.vue')
  },
  {
    path: '/dbMessengers',
    name: 'dbMessengers',
    component: () => import('../components/v2/mhaMessengers/dbMessengers.vue')
  },
  {
    path: '/dbReplicationConfigV2',
    name: 'dbReplicationConfigV2',
    component: () => import('../views/v2/meta/buildStep/dbReplicationConfigV2.vue')
  },
  {
    path: '/v2/mqConfigs',
    name: 'v2/mqConfigs',
    component: () => import('../views/v2/drcConfig/mqConfigs.vue')
  },
  {
    path: '/v2/resourceV2',
    name: 'v2/resourceV2',
    component: () => import('../views/v2/resource/resourceV2.vue')
  },
  {
    path: '/v2/dbDrcBuild',
    name: 'v2/dbDrcBuild',
    component: () => import('../views/v2/meta/dbDrcBuild.vue')
  },
  {
    path: '/v2/dbDrcBuildV2',
    name: 'v2/dbDrcBuildV2',
    component: () => import('../views/v2/meta/dbDrcBuildV2.vue')
  },
  {
    path: '/v2/dbMqBuildV2',
    name: 'v2/dbMqBuildV2',
    component: () => import('../views/v2/meta/dbMqBuildV2.vue')
  },
  {
    path: '/conflictLog',
    name: 'conflictLog',
    component: () => import('../views/log/conflictLog.vue')
  },
  {
    path: '/conflictRowsLog',
    name: 'conflictRowsLog',
    component: () => import('../views/log/conflictRowsLog.vue')
  },
  {
    path: '/conflictLogDetail',
    name: 'conflictLogDetail',
    component: () => import('../views/log/conflictLogDetail.vue')
  },
  {
    path: '/conflictApproval',
    name: 'conflictApproval',
    component: () => import('../views/log/conflictApproval.vue')
  },
  {
    path: '/dbBlacklist',
    name: 'dbBlacklist',
    component: () => import('../views/log/dbBlacklist.vue')
  },
  {
    path: '/conflictRowsLogDetail',
    name: 'conflictRowsLogDetail',
    component: () => import('../views/log/conflictRowsLogDetail.vue')
  },
  {
    path: '/nopermission',
    name: 'nopermission',
    component: () => import('../views/nopermission.vue')
  },
  {
    path: '/applicationForm',
    name: 'applicationForm',
    component: () => import('../views/v2/application/applicationForm.vue')
  },
  {
    path: '/applicationBuild',
    name: 'applicationBuild',
    component: () => import('../views/v2/application/applicationBuild.vue')
  },
  {
    path: '/drcBuild',
    name: 'applicationBuild',
    component: () => import('../views/v2/application/drcBuild.vue')
  },
  {
    path: '/mha',
    name: 'applicationBuild',
    component: () => import('../views/v2/meta/mha.vue')
  },
  {
    path: '/mha/replicatorConfig',
    name: 'replicatorConfig',
    component: () => import('../views/v2/meta/replicatorConfig.vue')
  },
  {
    path: '/',
    redirect: '/home'
  }
]

const router = new VueRouter({
  routes
})

export default router

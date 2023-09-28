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
    component: () => import('../views/proxyRouteCluster.vue')
  },
  {
    path: '/proxyRouteManagement',
    component: () => import('../views/proxyRouteManagement.vue')
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
    component: () => import('../views/resource/proxyResource.vue')
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
    component: () => import('../views/v2/mhaReplications.vue')
  },
  {
    path: '/v2/mhaReplicationDetails',
    component: () => import('../views/v2/mhaReplicationDetails.vue')
  },
  {
    path: '/v2/messengersV2',
    component: () => import('../views/v2/messengersV2.vue')
  },
  {
    path: '/v2/buildMessengerV2',
    component: () => import('../views/v2/buildMessengerV2.vue')
  },
  {
    path: '/v2/mhaBuild',
    name: 'mhaBuild',
    component: () => import('../views/v2/mhaBuild.vue')
  },
  {
    path: '/v2/migration',
    component: () => import('../views/v2/migration.vue')
  },
  {
    path: '/drcV2',
    name: 'drcV2',
    component: () => import('../views/v2/drcV2.vue')
  },
  {
    path: '/dbTables',
    name: 'dbTables',
    component: () => import('../views/v2/dbTables.vue')
  },
  {
    path: '/dbReplicationConfigV2',
    name: 'dbReplicationConfigV2',
    component: () => import('../views/v2/dbReplicationConfigV2.vue')
  },
  {
    path: '/v2/mqConfigs',
    name: 'v2/mqConfigs',
    component: () => import('../views/v2/drcConfig/mqConfigs.vue')
  },
  {
    path: '/v2/resourceV2',
    name: 'v2/resourceV2',
    component: () => import('../views/v2/resourceV2.vue')
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
    path: '/',
    redirect: '/home'
  }
]

const router = new VueRouter({
  routes
})

export default router

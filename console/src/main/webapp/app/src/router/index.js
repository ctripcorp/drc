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
    path: '/apply',
    component: () => import('../views/apply.vue')
  },
  {
    path: '/clusters',
    component: () => import('../views/clusters.vue')
  },
  {
    path: '/drcclusters',
    component: () => import('../views/drcclusters.vue')
  },
  {
    path: '/messengers',
    component: () => import('../views/messengers.vue')
  },
  {
    path: '/deletedDrcClusters',
    component: () => import('../views/deletedDrcClusters.vue')
  },
  {
    path: '/access',
    component: () => import('../views/access.vue')
  },
  {
    path: '/accessV2',
    component: () => import('../views/accessV2.vue')
  },
  {
    path: '/buildMhaMessenger',
    component: () => import('../views/buildMessenger.vue')
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
    path: '/mqConfigs',
    name: 'mqConfigs',
    component: () => import('../views/drcConfig/mqConfigs.vue')
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

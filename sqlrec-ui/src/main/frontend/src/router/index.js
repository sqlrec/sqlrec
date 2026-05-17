import { createRouter, createWebHistory } from 'vue-router'
import FunctionView from '../views/FunctionView.vue'
import ApiView from '../views/ApiView.vue'
import ModelView from '../views/ModelView.vue'
import ServiceView from '../views/ServiceView.vue'

const routes = [
  {
    path: '/',
    redirect: '/function'
  },
  {
    path: '/function',
    name: 'Function',
    component: FunctionView
  },
  {
    path: '/function/:id',
    name: 'FunctionDetail',
    component: FunctionView
  },
  {
    path: '/api',
    name: 'Api',
    component: ApiView
  },
  {
    path: '/api/:id',
    name: 'ApiDetail',
    component: ApiView
  },
  {
    path: '/model',
    name: 'Model',
    component: ModelView
  },
  {
    path: '/model/:id',
    name: 'ModelDetail',
    component: ModelView
  },
  {
    path: '/service',
    name: 'Service',
    component: ServiceView
  },
  {
    path: '/service/:id',
    name: 'ServiceDetail',
    component: ServiceView
  }
]

const router = createRouter({
  history: createWebHistory('/ui/static/'),
  routes
})

export default router

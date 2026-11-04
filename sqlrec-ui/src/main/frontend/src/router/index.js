import { createRouter, createWebHistory } from 'vue-router'
import TableView from '../views/TableView.vue'
import FunctionView from '../views/FunctionView.vue'
import ApiView from '../views/ApiView.vue'
import ModelView from '../views/ModelView.vue'
import ServiceView from '../views/ServiceView.vue'

const catalogRoutes = [
  { path: '/table', name: 'Table', component: TableView },
  { path: '/function', name: 'Function', component: FunctionView },
  { path: '/api', name: 'Api', component: ApiView },
  { path: '/model', name: 'Model', component: ModelView },
  { path: '/service', name: 'Service', component: ServiceView }
].flatMap(route => [
  route,
  { ...route, path: `${route.path}/:id`, name: `${route.name}Detail` }
])

const routes = [
  {
    path: '/',
    redirect: '/table'
  },
  ...catalogRoutes,
  {
    path: '/:pathMatch(.*)*',
    redirect: '/table'
  }
]

const router = createRouter({
  history: createWebHistory('/ui/static/'),
  routes
})

export default router

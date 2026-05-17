<template>
  <header class="header">
    <div class="header-left">
      <div class="logo">SQLRec</div>
      <nav class="nav-tabs">
        <router-link 
          v-for="tab in leftTabs" 
          :key="tab.name"
          :to="tab.path"
          class="nav-tab"
          :class="{ active: isActiveTab(tab) }"
        >
          {{ tab.label }}
        </router-link>
      </nav>
    </div>
    <div class="header-right">
      <a 
        v-for="link in rightLinks" 
        :key="link.name"
        :href="link.url"
        target="_blank"
        class="nav-link"
      >
        {{ link.label }}
      </a>
    </div>
  </header>
</template>

<script setup>
import { computed } from 'vue'
import { useRoute } from 'vue-router'

const route = useRoute()

const leftTabs = [
  { name: 'function', label: 'Function', path: '/function' },
  { name: 'api', label: 'Api', path: '/api' },
  { name: 'model', label: 'Model', path: '/model' },
  { name: 'service', label: 'Service', path: '/service' }
]

const rightLinks = [
  { name: 'doc', label: 'Doc', url: 'https://sqlrec.github.io/sqlrec/' },
  { name: 'github', label: 'GitHub', url: 'https://github.com/sqlrec/sqlrec' }
]

const isActiveTab = (tab) => {
  return route.path.startsWith(tab.path)
}
</script>

<style scoped>
.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  height: 60px;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  padding: 0 24px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
}

.header-left {
  display: flex;
  align-items: center;
  gap: 32px;
}

.logo {
  font-size: 24px;
  font-weight: 700;
  letter-spacing: -0.5px;
}

.nav-tabs {
  display: flex;
  gap: 8px;
}

.nav-tab {
  padding: 8px 16px;
  border-radius: 6px;
  text-decoration: none;
  color: rgba(255, 255, 255, 0.85);
  font-weight: 500;
  transition: all 0.2s ease;
}

.nav-tab:hover {
  background: rgba(255, 255, 255, 0.15);
  color: white;
}

.nav-tab.active {
  background: rgba(255, 255, 255, 0.2);
  color: white;
}

.header-right {
  display: flex;
  gap: 16px;
}

.nav-link {
  padding: 8px 16px;
  border-radius: 6px;
  text-decoration: none;
  color: rgba(255, 255, 255, 0.85);
  font-weight: 500;
  transition: all 0.2s ease;
}

.nav-link:hover {
  background: rgba(255, 255, 255, 0.15);
  color: white;
}
</style>

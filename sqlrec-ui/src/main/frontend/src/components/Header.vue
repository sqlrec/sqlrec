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
  { name: 'table', label: 'Table', path: '/table' },
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
  height: var(--header-height);
  flex: 0 0 var(--header-height);
  background: var(--brand);
  color: white;
  padding: 0 20px;
  border-bottom: 1px solid rgba(31, 41, 55, 0.1);
  box-shadow: none;
}

.header-left {
  display: flex;
  align-items: center;
  min-width: 0;
  gap: 24px;
}

.logo {
  flex: 0 0 auto;
  font-size: 20px;
  line-height: 1;
  font-weight: 700;
  letter-spacing: -0.4px;
}

.nav-tabs {
  display: flex;
  align-items: center;
  gap: 4px;
}

.nav-tab {
  padding: 6px 12px;
  border-radius: var(--radius-control);
  text-decoration: none;
  color: rgba(255, 255, 255, 0.85);
  font-size: 14px;
  line-height: 20px;
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
  align-items: center;
  gap: 4px;
}

.nav-link {
  padding: 6px 10px;
  border-radius: var(--radius-control);
  text-decoration: none;
  color: rgba(255, 255, 255, 0.85);
  font-size: 14px;
  line-height: 20px;
  font-weight: 500;
  transition: all 0.2s ease;
}

.nav-link:hover {
  background: rgba(255, 255, 255, 0.15);
  color: white;
}

@media (max-width: 720px) {
  .header {
    padding: 0 12px;
  }

  .header-left {
    flex: 1;
    gap: 12px;
  }

  .nav-tabs {
    overflow-x: auto;
    scrollbar-width: none;
  }

  .nav-tabs::-webkit-scrollbar {
    display: none;
  }

  .nav-tab {
    flex: 0 0 auto;
    padding-inline: 9px;
  }

  .header-right {
    display: none;
  }
}

@media (max-width: 480px) {
  .logo {
    font-size: 18px;
  }
}
</style>

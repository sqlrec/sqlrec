import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  base: '/ui/static',
  build: {
    outDir: 'dist',
    assetsDir: 'assets'
  },
  server: {
    port: 5173,
    proxy: {
      '/ui/api': {
        target: 'http://127.0.0.1:30301',
        changeOrigin: true
      }
    }
  }
})

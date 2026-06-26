import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { fileURLToPath } from 'node:url'
import { dirname, resolve } from 'node:path'

const __dirname = dirname(fileURLToPath(import.meta.url))

// The Flask bridge (user_client.py) serves `dag_offload/static` at the web root
// (static_url_path=''), so we build the SPA straight into that folder. Relative
// asset paths (base: './') keep the bundle servable from any mount point — and
// fully offline: every dependency is bundled, nothing is fetched at runtime.
const FLASK_STATIC = resolve(__dirname, '../Offloading-System/dag_offload/static')

export default defineConfig({
  plugins: [react()],
  base: './',
  build: {
    outDir: FLASK_STATIC,
    emptyOutDir: false, // keep img.png and other non-bundle assets already in static/
    assetsDir: 'assets',
    sourcemap: false,
    chunkSizeWarningLimit: 1500,
  },
  server: {
    port: 5173,
    proxy: {
      // During `npm run dev`, forward the bridge API to the Flask process.
      '/api': {
        target: 'http://127.0.0.1:5000',
        changeOrigin: true,
      },
    },
  },
})

<template>
  <div class="dag-container">
    <div v-if="loading" class="loading">Loading DAG...</div>
    <div v-else-if="error" class="error">{{ error }}</div>
    <div v-else class="dag-flow">
      <VueFlow
        v-model:nodes="nodes"
        v-model:edges="edges"
        :fit-view-on-init="true"
        :default-viewport="{ zoom: 1 }"
        :min-zoom="0.2"
        :max-zoom="2"
        :default-edge-options="defaultEdgeOptions"
        @node-click="onNodeClick"
        @nodes-initialized="onNodesInitialized"
      >
        <Background />
        <Controls />
        
        <template #node-cache="nodeProps">
          <DagNode :data="nodeProps.data" :selected="nodeProps.selected" :z-score="nodeProps.data.zScore" />
        </template>
        <template #node-sql="nodeProps">
          <DagNode :data="nodeProps.data" :selected="nodeProps.selected" :z-score="nodeProps.data.zScore" />
        </template>
        <template #node-function="nodeProps">
          <DagNode :data="nodeProps.data" :selected="nodeProps.selected" :z-score="nodeProps.data.zScore" />
        </template>
        <template #node-condition="nodeProps">
          <DagNode :data="nodeProps.data" :selected="nodeProps.selected" :z-score="nodeProps.data.zScore" />
        </template>
        <template #node-set="nodeProps">
          <DagNode :data="nodeProps.data" :selected="nodeProps.selected" :z-score="nodeProps.data.zScore" />
        </template>
      </VueFlow>
    </div>
  </div>
</template>

<script setup>
import { ref, watch, nextTick } from 'vue'
import { VueFlow, useVueFlow, Position } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import { Controls } from '@vue-flow/controls'
import dagre from '@dagrejs/dagre'
import '@vue-flow/core/dist/style.css'
import '@vue-flow/core/dist/theme-default.css'
import '@vue-flow/controls/dist/style.css'
import DagNode from './DagNode.vue'

const props = defineProps({
  functionName: {
    type: String,
    required: true
  }
})

const emit = defineEmits(['node-click'])

const nodes = ref([])
const edges = ref([])
const loading = ref(false)
const error = ref(null)

const { fitView } = useVueFlow()

const defaultEdgeOptions = {
  animated: true,
  type: 'default',
  style: { stroke: '#999', strokeWidth: 1.5 }
}

const calculateZScores = (nodesData) => {
  const validTimes = nodesData
    .map(n => n.avgExecTimeMs)
    .filter(t => t !== null && t !== undefined && t >= 0)
  
  if (validTimes.length < 2) {
    return nodesData.map(n => ({ ...n, zScore: 0, dataCountZScore: 0 }))
  }
  
  const mean = validTimes.reduce((a, b) => a + b, 0) / validTimes.length
  const variance = validTimes.reduce((sum, t) => sum + Math.pow(t - mean, 2), 0) / validTimes.length
  const stdDev = Math.sqrt(variance)
  
  if (stdDev === 0) {
    return nodesData.map(n => ({ ...n, zScore: 0, dataCountZScore: 0 }))
  }
  
  return nodesData.map(n => {
    const time = n.avgExecTimeMs
    if (time === null || time === undefined || time < 0) {
      return { ...n, zScore: 0, dataCountZScore: 0 }
    }
    const zScore = (time - mean) / stdDev
    return { ...n, zScore }
  })
}

const calculateDataCountZScores = (nodesData) => {
  const validCounts = nodesData
    .map(n => n.avgDataCount)
    .filter(c => c !== null && c !== undefined && c >= 0)
  
  if (validCounts.length < 2) {
    return nodesData.map(n => ({ ...n, dataCountZScore: 0 }))
  }
  
  const mean = validCounts.reduce((a, b) => a + b, 0) / validCounts.length
  const variance = validCounts.reduce((sum, c) => sum + Math.pow(c - mean, 2), 0) / validCounts.length
  const stdDev = Math.sqrt(variance)
  
  if (stdDev === 0) {
    return nodesData.map(n => ({ ...n, dataCountZScore: 0 }))
  }
  
  return nodesData.map(n => {
    const count = n.avgDataCount
    if (count === null || count === undefined || count < 0) {
      return { ...n, dataCountZScore: 0 }
    }
    const dataCountZScore = (count - mean) / stdDev
    return { ...n, dataCountZScore }
  })
}

const getEdgeWidth = (dataCountZScore) => {
  const clampedZ = Math.max(-2, Math.min(2, dataCountZScore))
  // Z = -2 → 0.5px, Z = 0 → 4.25px, Z = +2 → 8px
  return 4.25 + clampedZ * 1.875
}

const layoutGraph = (nodesData, edgesData, direction = 'LR') => {
  const dagreGraph = new dagre.graphlib.Graph()
  
  dagreGraph.setDefaultEdgeLabel(() => ({}))
  
  dagreGraph.setGraph({
    rankdir: direction,
    nodesep: 35,
    ranksep: 80,
    marginx: 30,
    marginy: 30
  })
  
  nodesData.forEach(node => {
    dagreGraph.setNode(node.id, { width: 180, height: 50 })
  })
  
  edgesData.forEach(edge => {
    dagreGraph.setEdge(edge.source, edge.target)
  })
  
  dagre.layout(dagreGraph)
  
  const isHorizontal = direction === 'LR'
  
  return nodesData.map(node => {
    const nodeWithPosition = dagreGraph.node(node.id)
    
    return {
      id: node.id,
      type: node.type,
      position: {
        x: nodeWithPosition.x - 90,
        y: nodeWithPosition.y - 25
      },
      sourcePosition: isHorizontal ? Position.Right : Position.Bottom,
      targetPosition: isHorizontal ? Position.Left : Position.Top,
      data: {
        label: node.label,
        type: node.type,
        sql: node.sql,
        dependencyFunction: node.dependencyFunction,
        avgExecTimeMs: node.avgExecTimeMs,
        avgDataCount: node.avgDataCount,
        logicalPlan: node.logicalPlan,
        physicalPlan: node.physicalPlan,
        javaExpression: node.javaExpression,
        cacheTableName: node.cacheTableName,
        cacheTableDataFields: node.cacheTableDataFields,
        zScore: node.zScore
      }
    }
  })
}

const fetchDagData = async () => {
  if (!props.functionName) return
  
  loading.value = true
  error.value = null
  
  try {
    const response = await fetch(`/ui/api/functions-dag/${props.functionName}`)
    if (!response.ok) {
      throw new Error('Failed to fetch DAG data')
    }
    const data = await response.json()
    
    const nodesWithZScore = calculateZScores(data.nodes)
    const nodesWithDataCountZScore = calculateDataCountZScores(nodesWithZScore)
    nodes.value = layoutGraph(nodesWithDataCountZScore, data.edges)
    
    const nodeMap = new Map(nodesWithDataCountZScore.map(n => [n.id, n]))
    edges.value = data.edges.map(edge => {
      const sourceNode = nodeMap.get(edge.source)
      const dataCountZScore = sourceNode?.dataCountZScore ?? 0
      const strokeWidth = getEdgeWidth(dataCountZScore)
      
      return {
        id: edge.id,
        source: edge.source,
        target: edge.target,
        sourceHandle: null,
        targetHandle: null,
        style: { stroke: '#999', strokeWidth }
      }
    })
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

const onNodeClick = ({ node }) => {
  emit('node-click', node.data)
}

const onNodesInitialized = () => {
  nextTick(() => {
    fitView({ padding: 0.2 })
  })
}

watch(() => props.functionName, fetchDagData, { immediate: true })
</script>

<style scoped>
.dag-container {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
}

.dag-flow {
  flex: 1;
  min-height: 0;
  background: #fafafa;
}

.loading, .error {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: #666;
}

.error {
  color: #f56c6c;
}
</style>

<style>
.vue-flow__background {
  background-color: #fafafa !important;
}

.vue-flow {
  --vf-node-text: #333;
  --vf-node-color: transparent;
  --vf-handle: transparent;
  --vf-connection-path: #999;
}

.vue-flow__node {
  max-width: 200px;
  cursor: pointer;
  border: none !important;
  font-weight: bold;
}

.vue-flow__node-cache,
.vue-flow__node-sql,
.vue-flow__node-function,
.vue-flow__node-condition,
.vue-flow__node-set {
  color: var(--vf-node-text);
  border: none;
  border-radius: 6px;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.08);
  padding: 0;
  overflow: hidden;
  font-weight: bold;
}

.vue-flow__node-cache:hover,
.vue-flow__node-sql:hover,
.vue-flow__node-function:hover,
.vue-flow__node-condition:hover,
.vue-flow__node-set:hover {
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.12);
}

.vue-flow__node.selected {
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.12);
}

.vue-flow__handle {
  visibility: hidden;
}
</style>

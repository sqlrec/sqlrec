<template>
  <div class="content-state" :class="{ plain }" :role="action ? 'alert' : 'status'" aria-live="polite">
    <p>{{ message }}</p>
    <button v-if="action" type="button" @click="$emit('action')">{{ action }}</button>
  </div>
</template>

<script setup>
defineProps({
  message: { type: String, required: true },
  action: { type: String, default: '' },
  plain: { type: Boolean, default: false }
})
defineEmits(['action'])
</script>

<style scoped>
.content-state {
  margin: var(--page-padding) auto;
  width: calc(100% - 2 * var(--page-padding));
  padding: 48px 24px;
  max-width: var(--content-max-width);
  text-align: center;
  color: var(--text);
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-card);
}

.content-state.plain {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: auto;
  max-width: none;
  margin: 0;
  padding: var(--page-padding);
  border: 0;
  border-radius: 0;
  background: transparent;
  color: var(--text-muted);
}

button {
  margin-top: 16px;
  padding: 8px 16px;
  border: 1px solid var(--brand);
  border-radius: var(--radius-control);
  background: var(--surface);
  color: var(--brand);
  cursor: pointer;
}

button:hover,
button:focus-visible {
  background: var(--brand-soft);
}
</style>

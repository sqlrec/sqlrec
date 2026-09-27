import { nextTick, onUnmounted, watch } from 'vue'

export function useDrawerBehavior(visible, panel, close) {
  let previousFocus
  let previousOverflow

  const onKeydown = event => {
    if (event.key === 'Escape') {
      event.preventDefault()
      close()
    }
    if (event.key !== 'Tab' || !panel.value) return
    const focusable = [...panel.value.querySelectorAll('button, a[href], input, select, textarea, [tabindex]:not([tabindex="-1"])')]
      .filter(element => !element.disabled && element.getClientRects().length > 0)
    if (!focusable.length) return
    const first = focusable[0]
    const last = focusable[focusable.length - 1]
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault()
      last.focus()
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault()
      first.focus()
    }
  }

  const cleanup = () => {
    document.removeEventListener('keydown', onKeydown)
    if (previousOverflow !== undefined) {
      document.body.style.overflow = previousOverflow
      previousOverflow = undefined
    }
    if (previousFocus?.isConnected) previousFocus.focus()
    previousFocus = undefined
  }

  watch(visible, async isVisible => {
    if (!isVisible) return cleanup()
    previousFocus = document.activeElement
    previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    document.addEventListener('keydown', onKeydown)
    await nextTick()
    panel.value?.querySelector('.drawer-close')?.focus()
  })
  onUnmounted(cleanup)
}

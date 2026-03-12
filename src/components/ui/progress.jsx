import React from 'react'

export function Progress({ value = 0, className = '' }) {
  const v = Math.max(0, Math.min(100, value))
  return (
    <div className={`h-2 w-full rounded-full bg-muted overflow-hidden ${className}`}>
      <div className="h-full bg-black" style={{ width: `${v}%` }} />
    </div>
  )
}

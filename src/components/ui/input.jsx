import React from 'react'

export function Input({ className = '', ...props }) {
  return (
    <input
      className={`h-10 w-full rounded-2xl border border-border bg-background px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-offset-2 ${className}`}
      {...props}
    />
  )
}
